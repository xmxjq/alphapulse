from datetime import timedelta

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
