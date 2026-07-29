from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from alphapulse.runtime.agent_pool import AgentPoolStore
from alphapulse.runtime.config import AgentPoolSettings


def _store(tmp_path) -> AgentPoolStore:
    return AgentPoolStore(
        AgentPoolSettings(
            enabled=True,
            db_path=tmp_path / "agent-pool.db",
            allowed_hosts=["guba.eastmoney.com", "www.tgb.cn"],
            heartbeat_ttl_seconds=90,
            lease_seconds=10,
            blocked_cooldown_seconds=60,
        )
    )


def _heartbeat(store: AgentPoolStore, agent_id: str = "home-arm") -> None:
    store.heartbeat(
        agent_id=agent_id,
        version="test",
        os_name="linux",
        arch="arm",
        capabilities=["http"],
        max_concurrency=1,
    )


def test_agent_token_lifecycle(tmp_path) -> None:
    store = _store(tmp_path)

    token = store.issue_token("home-arm")

    assert store.authenticate("home-arm", token)
    assert not store.authenticate("home-arm", "wrong")
    assert store.revoke_token("home-arm")
    assert not store.authenticate("home-arm", token)


def test_agent_job_round_trip_and_outcome_metrics(tmp_path) -> None:
    store = _store(tmp_path)
    _heartbeat(store)
    assert store.has_eligible_agent("http")

    job_id = store.submit_job(
        source="guba",
        capability="http",
        method="GET",
        url="https://guba.eastmoney.com/list,600519.html",
        headers={"Accept": "text/html"},
        body=None,
        timeout_seconds=30,
        priority=100,
    )
    lease = store.lease_job(agent_id="home-arm", capabilities=["http"])

    assert lease is not None
    assert lease["job_id"] == job_id
    assert lease["method"] == "GET"
    assert store.complete_job(
        agent_id="home-arm",
        job_id=job_id,
        lease_id=lease["lease_id"],
        status_code=200,
        final_url="https://guba.eastmoney.com/list,600519.html",
        headers={"Content-Type": "text/html; charset=utf-8"},
        body="ok".encode(),
        duration_ms=123,
    )

    result = store.get_result(job_id)
    assert result is not None
    assert result.agent_id == "home-arm"
    assert result.status_code == 200
    assert result.text == "ok"

    store.record_outcome(job_id, "success")
    snapshot = store.snapshot()
    assert snapshot["online_nodes"] == 1
    assert snapshot["completed_jobs"] == 1
    assert snapshot["nodes"][0]["success_count"] == 1


def test_available_capacity_subtracts_active_leases(tmp_path) -> None:
    store = _store(tmp_path)
    store.heartbeat(
        agent_id="home-arm",
        version="test",
        os_name="linux",
        arch="arm",
        capabilities=["http"],
        max_concurrency=2,
    )
    for post_id in ("1", "2"):
        store.submit_job(
            source="guba",
            capability="http",
            method="GET",
            url=f"https://guba.eastmoney.com/news,600519,{post_id}.html",
            headers={},
            body=None,
            timeout_seconds=30,
        )

    assert store.available_capacity("http") == 2
    assert store.lease_job(agent_id="home-arm", capabilities=["http"]) is not None
    assert store.available_capacity("http") == 1


def test_blocked_outcome_benches_agent(tmp_path) -> None:
    store = _store(tmp_path)
    _heartbeat(store)
    job_id = store.submit_job(
        source="guba",
        capability="http",
        method="GET",
        url="https://guba.eastmoney.com/news,600519,1.html",
        headers={},
        body=None,
        timeout_seconds=30,
    )
    lease = store.lease_job(agent_id="home-arm", capabilities=["http"])
    assert lease is not None
    assert store.complete_job(
        agent_id="home-arm",
        job_id=job_id,
        lease_id=lease["lease_id"],
        status_code=403,
        final_url="https://guba.eastmoney.com/news,600519,1.html",
        headers={},
        body=b"blocked",
        duration_ms=5,
    )

    store.record_outcome(job_id, "blocked", "HTTP 403")

    assert not store.has_eligible_agent("http", source="guba")
    assert store.has_eligible_agent("http", source="tgb")
    snapshot = store.snapshot()
    assert snapshot["benched_nodes"] == 0
    assert snapshot["nodes"][0]["blocked_count"] == 1
    assert snapshot["nodes"][0]["source_health"][0]["source"] == "guba"
    assert snapshot["nodes"][0]["source_health"][0]["blocked_count"] == 1


def test_source_bench_does_not_block_other_source_jobs(tmp_path) -> None:
    store = _store(tmp_path)
    _heartbeat(store)
    guba_job = store.submit_job(
        source="guba",
        capability="http",
        method="GET",
        url="https://guba.eastmoney.com/news,600519,1.html",
        headers={},
        body=None,
        timeout_seconds=30,
    )
    lease = store.lease_job(agent_id="home-arm", capabilities=["http"])
    assert lease is not None
    assert store.complete_job(
        agent_id="home-arm",
        job_id=guba_job,
        lease_id=lease["lease_id"],
        status_code=200,
        final_url="https://guba.eastmoney.com/news,600519,1.html",
        headers={},
        body=b"blocked",
        duration_ms=5,
    )
    store.record_outcome(guba_job, "blocked", "soft block")
    tgb_job = store.submit_job(
        source="tgb",
        capability="http",
        method="GET",
        url="https://www.tgb.cn/a/post-1",
        headers={},
        body=None,
        timeout_seconds=30,
    )

    tgb_lease = store.lease_job(agent_id="home-arm", capabilities=["http"])

    assert tgb_lease is not None
    assert tgb_lease["job_id"] == tgb_job


def test_expired_lease_is_requeued(tmp_path) -> None:
    store = _store(tmp_path)
    _heartbeat(store)
    job_id = store.submit_job(
        source="guba",
        capability="http",
        method="GET",
        url="https://guba.eastmoney.com/list,600519.html",
        headers={},
        body=None,
        timeout_seconds=30,
    )
    first = store.lease_job(agent_id="home-arm", capabilities=["http"])
    assert first is not None
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE agent_jobs SET lease_expires_at = ? WHERE job_id = ?",
            (expired, job_id),
        )

    second = store.lease_job(agent_id="home-arm", capabilities=["http"])

    assert second is not None
    assert second["job_id"] == job_id
    assert second["lease_id"] != first["lease_id"]


def test_agent_store_rejects_unlisted_hosts(tmp_path) -> None:
    store = _store(tmp_path)

    with pytest.raises(ValueError, match="not allowed"):
        store.submit_job(
            source="guba",
            capability="http",
            method="GET",
            url="http://127.0.0.1/private",
            headers={},
            body=None,
            timeout_seconds=30,
        )


def test_agent_maintenance_prunes_old_response_bodies_and_metadata(tmp_path) -> None:
    store = _store(tmp_path)
    _heartbeat(store)
    job_id = store.submit_job(
        source="guba",
        capability="http",
        method="GET",
        url="https://guba.eastmoney.com/list,600519.html",
        headers={},
        body=None,
        timeout_seconds=30,
    )
    lease = store.lease_job(agent_id="home-arm", capabilities=["http"])
    assert lease is not None
    assert store.complete_job(
        agent_id="home-arm",
        job_id=job_id,
        lease_id=lease["lease_id"],
        status_code=200,
        final_url="https://guba.eastmoney.com/list,600519.html",
        headers={},
        body=b"large response",
        duration_ms=5,
    )
    old_body = (datetime.now(UTC) - timedelta(hours=25)).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE agent_jobs SET completed_at = ? WHERE job_id = ?",
            (old_body, job_id),
        )

    store.snapshot()
    with store.connection() as conn:
        row = conn.execute(
            "SELECT response_body_zlib FROM agent_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    assert row is not None
    assert row["response_body_zlib"] is None

    old_metadata = (datetime.now(UTC) - timedelta(days=31)).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE agent_jobs SET completed_at = ? WHERE job_id = ?",
            (old_metadata, job_id),
        )
    store.snapshot()
    with store.connection() as conn:
        row = conn.execute(
            "SELECT job_id FROM agent_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    assert row is None
