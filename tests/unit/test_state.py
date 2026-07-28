from datetime import UTC, datetime, timedelta

from alphapulse.pipeline.contracts import CrawlTask
from alphapulse.runtime.state import StateStore


def test_release_url_claim_makes_blocked_url_retryable(tmp_path) -> None:
    state = StateStore(tmp_path / "state.db")
    url = "https://guba.eastmoney.com/news,600519,42.html"
    assert state.try_claim_url(
        url=url,
        source="guba",
        kind="fetch_post",
        seed_name="test",
        min_age=timedelta(hours=6),
    )

    state.mark_url_fetched(url, 200)
    state.release_url_claim(url)

    with state.connection() as conn:
        row = conn.execute(
            "SELECT last_fetched_at, last_status FROM url_state WHERE url = ?",
            (url,),
        ).fetchone()
    assert row["last_fetched_at"] is None
    assert row["last_status"] is None
    assert state.try_claim_url(
        url=url,
        source="guba",
        kind="fetch_post",
        seed_name="test",
        min_age=timedelta(hours=6),
    )


def test_pending_tasks_round_trip_in_priority_order(tmp_path) -> None:
    state = StateStore(tmp_path / "state.db")
    older = CrawlTask(
        source="guba",
        kind="fetch_post",
        url="https://guba.eastmoney.com/news,600519,1.html",
        seed_name="cn-core",
        priority=150,
        metadata={"pubdate_ts": 100},
        discovered_at=datetime(2026, 7, 28, 1, tzinfo=UTC),
    )
    newer = CrawlTask(
        source="guba",
        kind="fetch_post",
        url="https://guba.eastmoney.com/news,600519,2.html",
        seed_name="cn-core",
        priority=150,
        metadata={"pubdate_ts": 200},
        discovered_at=datetime(2026, 7, 28, 2, tzinfo=UTC),
    )
    lower_priority = CrawlTask(
        source="guba",
        kind="discover",
        url="https://guba.eastmoney.com/list,600519_2.html",
        seed_name="cn-core",
        priority=119,
        discovered_at=datetime(2026, 7, 28, 3, tzinfo=UTC),
    )

    state.upsert_pending_tasks([older, lower_priority, newer])

    assert [task.dedupe_key for task in state.load_pending_tasks()] == [
        newer.dedupe_key,
        older.dedupe_key,
        lower_priority.dedupe_key,
    ]
    assert len(state.load_pending_tasks("cn-core")) == 3
    assert state.load_pending_tasks("other") == []

    updated = older.model_copy(
        update={"priority": 220, "metadata": {"pubdate_ts": 300}}
    )
    state.upsert_pending_tasks([updated])
    assert state.load_pending_tasks()[0].priority == 220

    state.delete_pending_task(updated.dedupe_key)
    assert {task.dedupe_key for task in state.load_pending_tasks()} == {
        newer.dedupe_key,
        lower_priority.dedupe_key,
    }
