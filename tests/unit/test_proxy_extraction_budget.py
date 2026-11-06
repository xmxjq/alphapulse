from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import sqlite3

import pytest

from alphapulse.runtime.config import CrawlKuaidailiSettings
from alphapulse.runtime.proxy_metrics import ProxyMetricsStore
from alphapulse.sources.fetching import KuaidailiProxyPool


def reserve(store, count=1, *, hourly=60, daily=600, unsuccessful=10, provider="kuaidaili"):
    store.reserve_extraction(
        provider, count, max_per_hour=hourly, max_per_day=daily,
        max_without_success=unsuccessful, source="guba",
    )


@pytest.fixture
def clock(monkeypatch):
    class Clock(datetime):
        value = datetime(2026, 11, 6, 6, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.value

    monkeypatch.setattr("alphapulse.runtime.proxy_metrics.datetime", Clock)
    return Clock


def test_hourly_limit_is_durable_and_provider_wide(tmp_path):
    path = tmp_path / "metrics.db"
    reserve(ProxyMetricsStore(path), 2, hourly=2)
    with pytest.raises(RuntimeError, match="hourly"):
        reserve(ProxyMetricsStore(path), hourly=2)
    reserve(ProxyMetricsStore(path), hourly=2, provider="unrelated")


def test_hourly_window_is_rolling_not_calendar(tmp_path, clock):
    store = ProxyMetricsStore(tmp_path / "metrics.db")
    clock.value += timedelta(minutes=59)
    reserve(store, 2, hourly=2)
    clock.value += timedelta(minutes=2)
    with pytest.raises(RuntimeError, match="hourly"):
        reserve(store, hourly=2)
    clock.value += timedelta(minutes=58)
    reserve(store, 2, hourly=2)


def test_daily_limit_survives_hourly_expiry_and_paid_success(tmp_path, clock):
    store = ProxyMetricsStore(tmp_path / "metrics.db")
    reserve(store, 2, hourly=2, daily=2)
    clock.value += timedelta(hours=2)
    store.record_success("kuaidaili", "http://198.51.100.1:8080")
    with pytest.raises(RuntimeError, match="daily"):
        reserve(store, hourly=2, daily=2)
    clock.value += timedelta(hours=22)
    reserve(store, 2, hourly=2, daily=2)


def test_no_success_limit_does_not_expire_or_reset_on_restart(tmp_path, clock):
    path = tmp_path / "metrics.db"
    reserve(ProxyMetricsStore(path), 2, unsuccessful=2)
    clock.value += timedelta(days=2)
    with pytest.raises(RuntimeError, match="no-success"):
        reserve(ProxyMetricsStore(path), unsuccessful=2)


def test_only_paid_success_resets_no_success_count(tmp_path):
    store = ProxyMetricsStore(tmp_path / "metrics.db")
    reserve(store, 2, unsuccessful=2)
    store.record_success("self_hosted", "http://198.51.100.2:8080")
    with pytest.raises(RuntimeError, match="no-success"):
        reserve(store, unsuccessful=2)
    store.record_success("kuaidaili", "http://198.51.100.1:8080")
    reserve(store, 2, unsuccessful=2)


def test_concurrent_independent_stores_cannot_overspend(tmp_path):
    path = tmp_path / "metrics.db"
    stores = [ProxyMetricsStore(path) for _ in range(8)]

    def attempt(index):
        try:
            reserve(stores[index % len(stores)], hourly=7, unsuccessful=100)
            return True
        except RuntimeError as exc:
            assert "hourly" in str(exc)
            return False

    with ThreadPoolExecutor(max_workers=8) as threads:
        assert sum(threads.map(attempt, range(40))) == 7
    with stores[0].connection() as connection:
        assert connection.execute(
            "SELECT SUM(count) FROM proxy_events WHERE event_type='extraction_reserved'"
        ).fetchone()[0] == 7


@pytest.mark.parametrize("count", [0, -1])
def test_non_positive_reservations_rejected(tmp_path, count):
    with pytest.raises(ValueError):
        reserve(ProxyMetricsStore(tmp_path / "metrics.db"), count)


def test_batch_cannot_overshoot_remaining_budget(tmp_path, monkeypatch):
    settings = CrawlKuaidailiSettings(
        metrics_path=tmp_path / "metrics.db", api_url_file=tmp_path / "absent",
        batch_size=2, low_watermark=0, max_extracted_per_hour=1,
    )
    pool = KuaidailiProxyPool(settings)
    calls = []
    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", lambda *a, **k: calls.append(1))
    with pytest.raises(RuntimeError, match="hourly"):
        pool.acquire("guba")
    assert calls == []


def test_failed_api_calls_consume_durable_reservation(tmp_path, monkeypatch):
    api = tmp_path / "api.txt"
    api.write_text("https://example.invalid/extract", encoding="utf-8")
    settings = CrawlKuaidailiSettings(
        metrics_path=tmp_path / "metrics.db", api_url_file=api,
        batch_size=1, low_watermark=0, max_extracted_without_success=1,
    )
    calls = []

    def fail(*args, **kwargs):
        calls.append(1)
        raise TimeoutError("fixture")

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", fail)
    with pytest.raises(TimeoutError):
        KuaidailiProxyPool(settings).acquire("guba")
    with pytest.raises(RuntimeError, match="no-success"):
        KuaidailiProxyPool(settings).acquire("tgb")
    assert calls == [1]


def test_database_failure_prevents_api_access(tmp_path, monkeypatch):
    pool = KuaidailiProxyPool(CrawlKuaidailiSettings(
        metrics_path=tmp_path / "metrics.db", batch_size=1, low_watermark=0,
    ))
    calls = []

    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("fixture")

    monkeypatch.setattr(pool.metrics, "reserve_extraction", fail)
    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", lambda *a, **k: calls.append(1))
    with pytest.raises(sqlite3.OperationalError):
        pool.acquire("guba")
    assert calls == []


def test_historical_batches_do_not_retroactively_exhaust_new_ledger(tmp_path):
    store = ProxyMetricsStore(tmp_path / "metrics.db")
    with store.connection() as connection:
        store._insert_event(
            connection, provider="kuaidaili", event_type="batch_fetched",
            occurred_at=datetime.now(UTC).isoformat(), count=1770,
        )
    reserve(store)


def test_auth_hold_does_not_spend_any_reservation(tmp_path):
    pool = KuaidailiProxyPool(CrawlKuaidailiSettings(
        metrics_path=tmp_path / "metrics.db", batch_size=1, low_watermark=0,
    ))
    pool.auth_block_path.write_text("fixture", encoding="utf-8")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("guba")
    with pool.metrics.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM proxy_events").fetchone()[0] == 0
