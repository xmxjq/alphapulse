from urllib.parse import parse_qs, urlsplit

import pytest

from alphapulse.runtime.config import CrawlKuaidailiSettings, Settings
from alphapulse.sources.fetching import KuaidailiProxyPool


class Response:
    def __init__(self, body):
        self.body = body

    def read(self):
        return self.body.encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


def pool_fixture(tmp_path, monkeypatch, **settings):
    api = tmp_path / "api.txt"
    api.write_text("https://example.invalid/getdps?num=2")
    config = CrawlKuaidailiSettings(
        api_url_file=api, metrics_path=tmp_path / "metrics.db",
        batch_size=2, low_watermark=0, adaptive_batching=True,
        use_api_expiry=True, expiry_safety_seconds=30, **settings,
    )
    clock = [1000.0]
    calls = []
    fail = [False]
    issued = [0]
    monkeypatch.setattr("alphapulse.sources.fetching.time.monotonic", lambda: clock[0])

    def extract(url, timeout):
        query = parse_qs(urlsplit(url).query)
        count = int(query["num"][0])
        assert query["f_et"] == ["1"]
        calls.append(count)
        if fail[0]:
            raise RuntimeError("fixture extraction failure")
        ips = []
        for _ in range(count):
            issued[0] += 1
            ips.append(f"198.51.100.{issued[0]}:8080,600")
        return Response("\n".join(ips))

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", extract)
    return KuaidailiProxyPool(config), clock, calls, fail


def test_low_demand_uses_one_ip_and_shares_across_sources(tmp_path, monkeypatch):
    pool, clock, calls, _ = pool_fixture(tmp_path, monkeypatch)
    first = pool.acquire("guba")
    second = pool.acquire("jiuyan")
    assert first.proxy_url == second.proxy_url
    assert calls == [1]
    assert pool._expires_at[first.proxy_url] - clock[0] == 570


def test_pressure_adds_only_missing_capacity_without_throttling(tmp_path, monkeypatch):
    pool, _, calls, _ = pool_fixture(tmp_path, monkeypatch, adaptive_request_limit_per_minute=2)
    first = pool.acquire("guba")
    pool.acquire("guba")
    second = pool.acquire("guba")
    assert first.proxy_url != second.proxy_url
    for _ in range(10):
        assert pool.acquire("guba") is not None
    assert calls == [1, 1]
    assert len(pool._urls) == 2


def test_usage_window_expires_without_unneeded_expansion(tmp_path, monkeypatch):
    pool, clock, calls, _ = pool_fixture(tmp_path, monkeypatch, adaptive_request_limit_per_minute=2)
    first = pool.acquire("guba")
    pool.acquire("guba")
    clock[0] += 61
    assert pool.acquire("guba").proxy_url == first.proxy_url
    assert calls == [1]


def test_low_demand_returns_to_single_purchase_after_expiry(tmp_path, monkeypatch):
    pool, clock, calls, _ = pool_fixture(tmp_path, monkeypatch, adaptive_request_limit_per_minute=2)
    for _ in range(3):
        pool.acquire("guba")
    assert calls == [1, 1]
    clock[0] += 601
    pool.acquire("guba")
    assert calls == [1, 1, 1]
    assert len(pool._recent_acquires) == 1


def test_pressure_cannot_repeatedly_top_up_partly_expired_round(tmp_path, monkeypatch):
    pool, clock, calls, _ = pool_fixture(tmp_path, monkeypatch, adaptive_request_limit_per_minute=2)
    first = pool.acquire("guba")
    pool.acquire("guba")
    clock[0] += 10
    second = pool.acquire("guba")
    assert calls == [1, 1]
    clock[0] = pool._expires_at[first.proxy_url] + 1
    for _ in range(10):
        assert pool.acquire("guba").proxy_url == second.proxy_url
    assert calls == [1, 1]
    clock[0] = pool._expires_at[second.proxy_url] + 1
    pool.acquire("guba")
    assert calls == [1, 1, 1]


def test_recovery_does_not_purchase_early_when_a_cached_ip_survives(tmp_path, monkeypatch):
    pool, clock, calls, _ = pool_fixture(tmp_path, monkeypatch)
    lease = pool.acquire("guba")
    pool._adaptive_fallback_until = clock[0] + 600
    assert pool.acquire("guba").proxy_url == lease.proxy_url
    assert calls == [1]
    clock[0] += 571
    pool.acquire("guba")
    assert calls == [1, 2]


def test_bench_restores_configured_capacity_and_keeps_source_isolation(tmp_path, monkeypatch):
    pool, clock, calls, _ = pool_fixture(tmp_path, monkeypatch)
    bad = pool.acquire("guba")
    pool.report_bad(bad, "HTTP 403", source="guba")
    new = pool.acquire("guba")
    assert new.proxy_url != bad.proxy_url
    assert calls == [1, 2]
    assert pool._adaptive_fallback_until == clock[0] + 600
    assert bad.proxy_url in pool._available(clock[0], "tgb")
    clock[0] += 601
    pool.acquire("guba")
    assert calls == [1, 2, 1]


def test_optional_expansion_failure_reuses_healthy_ip_and_backs_off(tmp_path, monkeypatch):
    pool, clock, calls, fail = pool_fixture(tmp_path, monkeypatch, adaptive_request_limit_per_minute=1)
    first = pool.acquire("guba")
    fail[0] = True
    assert pool.acquire("guba").proxy_url == first.proxy_url
    assert pool.acquire("guba").proxy_url == first.proxy_url
    assert calls == [1, 1]
    fail[0] = False
    clock[0] += 31
    assert pool.acquire("guba").proxy_url != first.proxy_url
    assert calls == [1, 1, 1]


def test_required_extraction_failure_stays_closed_and_backs_off(tmp_path, monkeypatch):
    pool, _, calls, fail = pool_fixture(tmp_path, monkeypatch)
    fail[0] = True
    with pytest.raises(RuntimeError, match="fixture extraction"):
        pool.acquire("guba")
    with pytest.raises(RuntimeError, match="cooling down"):
        pool.acquire("guba")
    assert calls == [1]


def test_expansion_failure_cannot_return_an_expired_route(tmp_path, monkeypatch):
    pool, clock, _, fail = pool_fixture(tmp_path, monkeypatch, adaptive_request_limit_per_minute=1)
    pool.acquire("guba")
    original_refresh = pool._refresh

    def delayed_refresh(*args, **kwargs):
        clock[0] += 601
        fail[0] = True
        return original_refresh(*args, **kwargs)

    monkeypatch.setattr(pool, "_refresh", delayed_refresh)
    with pytest.raises(RuntimeError):
        pool.acquire("guba")


def test_disabled_policy_retains_original_batching(tmp_path, monkeypatch):
    pool, _, calls, _ = pool_fixture(tmp_path, monkeypatch)
    pool.settings.adaptive_batching = False
    pool.acquire("guba")
    assert calls == [2]


def test_metrics_record_purchase_policy_and_acquisition_wait(tmp_path, monkeypatch):
    import json

    pool, clock, _, _ = pool_fixture(tmp_path, monkeypatch)
    original = pool._refresh

    def delayed(*args, **kwargs):
        clock[0] += 0.125
        return original(*args, **kwargs)

    monkeypatch.setattr(pool, "_refresh", delayed)
    pool.acquire("guba")
    with pool.metrics.connection() as conn:
        records = conn.execute("SELECT event_type,count,detail_json FROM proxy_events ORDER BY id").fetchall()
    batch = next(row for row in records if row["event_type"] == "batch_fetched")
    lease = next(row for row in records if row["event_type"] == "lease_acquired")
    assert batch["count"] == 1
    assert json.loads(batch["detail_json"])["allocation"] == "single"
    assert json.loads(batch["detail_json"])["requested"] == 1
    assert json.loads(lease["detail_json"])["wait_ms"] == 125


def test_scattered_failures_trigger_capacity_recovery(tmp_path, monkeypatch):
    pool, clock, _, _ = pool_fixture(tmp_path, monkeypatch)
    lease = pool.acquire("guba")
    for _ in range(17):
        pool.report_success(lease, source="guba")
    for source in ["guba", "jiuyan", "tgb"]:
        pool.report_bad(lease, "temporary network failure", source=source)
    assert not pool._benched_until
    assert pool._adaptive_fallback_until == clock[0] + 600


def test_adaptive_policy_rejects_incompatible_settings():
    with pytest.raises(ValueError, match="batch_size"):
        CrawlKuaidailiSettings(adaptive_batching=True, batch_size=1, low_watermark=0)
    with pytest.raises(ValueError, match="low_watermark"):
        CrawlKuaidailiSettings(adaptive_batching=True, batch_size=2, low_watermark=1)
    with pytest.raises(ValueError, match="paired-IP"):
        Settings.model_validate({
            "crawl": {"kuaidaili": {"adaptive_batching": True, "batch_size": 2, "low_watermark": 0}},
            "sources": {"guba": {
                "proxy_dual_endpoint_experiment_enabled": True,
                "proxy_dual_endpoint_experiment_until": "2030-01-01T00:00:00Z",
            }},
        })
