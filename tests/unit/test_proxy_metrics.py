from datetime import UTC, datetime, timedelta

from alphapulse.runtime.proxy_metrics import ProxyMetricsStore


def test_proxy_metrics_snapshot_tracks_usage_without_addresses(tmp_path) -> None:
    store = ProxyMetricsStore(tmp_path / "proxy-metrics.db")
    now = datetime.now(UTC)
    first = "http://1.2.3.4:8080"
    second = "http://2.3.4.5:8081"
    store.record_batch(
        "kuaidaili",
        [first, second],
        expires_at=now + timedelta(minutes=10),
    )
    store.record_acquire("kuaidaili", first, source="guba")
    store.record_success("kuaidaili", first, source="guba")
    store.record_acquire("kuaidaili", second, source="tgb")
    store.record_failure(
        "kuaidaili",
        second,
        reason="dial 2.3.4.5 failed",
        benched_until=now + timedelta(minutes=5),
        source="tgb",
    )

    snapshot = store.snapshot(
        provider="kuaidaili",
        since=now - timedelta(hours=1),
        now=now,
    )

    assert snapshot["extracted"] == 2
    assert snapshot["leases"] == 2
    assert snapshot["successes"] == 1
    assert snapshot["failures"] == 1
    assert snapshot["success_rate"] == 0.5
    assert snapshot["requests_per_proxy"] == 1.0
    assert snapshot["active_nodes"] == 1
    assert snapshot["benched_nodes"] == 1
    sources = {item["source"]: item for item in snapshot["sources"]}
    assert sources["guba"]["successes"] == 1
    assert sources["guba"]["success_rate"] == 1.0
    assert sources["tgb"]["failures"] == 1
    assert sources["tgb"]["success_rate"] == 0.0
    assert {event["source"] for event in snapshot["events"]} >= {"guba", "tgb"}
    rendered = str(snapshot)
    assert "1.2.3.4" not in rendered
    assert "2.3.4.5" not in rendered


def test_proxy_metrics_tracks_failure_without_benching(tmp_path) -> None:
    store = ProxyMetricsStore(tmp_path / "proxy-metrics.db")
    now = datetime.now(UTC)
    proxy = "http://1.2.3.4:8080"
    store.record_batch(
        "kuaidaili",
        [proxy],
        expires_at=now + timedelta(minutes=10),
    )
    store.record_failure(
        "kuaidaili",
        proxy,
        reason="incomplete response",
        benched_until=None,
    )

    snapshot = store.snapshot(
        provider="kuaidaili",
        since=now - timedelta(hours=1),
        now=now,
    )

    assert snapshot["failures"] == 1
    assert snapshot["active_nodes"] == 1
    assert snapshot["benched_nodes"] == 0
    assert snapshot["events"][0]["event_type"] == "request_failure"
    assert snapshot["events"][0]["source"] == "unknown"
