from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from alphapulse.runtime.agent_pool import AgentPoolStore
from alphapulse.runtime.config import Settings
from alphapulse.web.app import create_app


def _client(tmp_path):
    settings = Settings()
    settings.crawl.agent_pool.enabled = True
    settings.crawl.agent_pool.db_path = tmp_path / "agent-pool.db"
    settings.crawl.agent_pool.allowed_hosts = ["guba.eastmoney.com"]
    store = AgentPoolStore(settings.crawl.agent_pool)
    token = store.issue_token("home-arm")
    app = create_app(settings, queries=object(), agent_pool=store)  # type: ignore[arg-type]
    headers = {
        "X-AlphaPulse-Agent-ID": "home-arm",
        "X-AlphaPulse-Agent-Token": token,
    }
    return TestClient(app), store, headers


def _agent_payload() -> dict[str, object]:
    return {
        "agent_id": "home-arm",
        "version": "test",
        "os": "linux",
        "arch": "arm",
        "capabilities": ["http"],
        "max_concurrency": 1,
    }


def test_agent_api_authenticates_and_completes_job(tmp_path) -> None:
    client, store, headers = _client(tmp_path)
    heartbeat = client.post(
        "/api/agent/v1/heartbeat",
        headers={**headers, "CF-Connecting-IP": "198.51.100.24"},
        json=_agent_payload(),
    )
    assert heartbeat.status_code == 200
    assert store.snapshot()["nodes"][0]["last_ip_address"] == "198.51.100.24"

    job_id = store.submit_job(
        source="guba",
        capability="http",
        method="GET",
        url="https://guba.eastmoney.com/list,600519.html",
        headers={"Accept": "text/html"},
        body=None,
        timeout_seconds=30,
    )
    lease = client.post(
        "/api/agent/v1/jobs/lease",
        headers=headers,
        json={**_agent_payload(), "wait_seconds": 0},
    )
    assert lease.status_code == 200
    leased = lease.json()
    assert leased["job_id"] == job_id

    complete = client.post(
        f"/api/agent/v1/jobs/{job_id}/complete",
        headers=headers,
        json={
            "lease_id": leased["lease_id"],
            "status_code": 200,
            "final_url": "https://guba.eastmoney.com/list,600519.html",
            "headers": {"content-type": "text/html; charset=utf-8"},
            "body_base64": base64.b64encode(b"<html>ok</html>").decode(),
            "duration_ms": 10,
        },
    )
    assert complete.status_code == 200
    assert store.get_result(job_id).text == "<html>ok</html>"


def test_agent_api_rejects_bad_token(tmp_path) -> None:
    client, _, headers = _client(tmp_path)
    headers["X-AlphaPulse-Agent-Token"] = "wrong"

    response = client.post(
        "/api/agent/v1/heartbeat",
        headers=headers,
        json=_agent_payload(),
    )

    assert response.status_code == 401


def test_agent_pool_dashboard_endpoint(tmp_path) -> None:
    client, _, _ = _client(tmp_path)

    response = client.get("/api/agent-pool")

    assert response.status_code == 200
    assert response.json()["enabled"] is True
    assert response.json()["online_nodes"] == 0
    assert response.json()["routing_mode"] == "hybrid"
    assert response.json()["paid_slots"] == 1
    assert response.json()["combined_capacity"] == 1
