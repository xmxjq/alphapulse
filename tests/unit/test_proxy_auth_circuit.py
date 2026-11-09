import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from alphapulse.runtime.config import CrawlKuaidailiSettings
from alphapulse.sources.fetching import KuaidailiProxyPool


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return b"198.51.100.1:8080,600\n198.51.100.2:8080,600"


@pytest.fixture
def paid_pool(tmp_path, monkeypatch):
    api = tmp_path / "api.txt"
    api.write_text("https://example.invalid/getdps?num=2", encoding="utf-8")
    settings = CrawlKuaidailiSettings(
        api_url_file=api, metrics_path=tmp_path / "metrics.db",
        batch_size=2, low_watermark=0, use_api_expiry=True,
    )
    calls = []

    def extract(*args, **kwargs):
        calls.append(1)
        return Response()

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", extract)
    return KuaidailiProxyPool(settings), calls


@pytest.mark.parametrize("reason", [
    "curl: (56) CONNECT tunnel failed, response 407",
    "HTTP 407", "HTTP Error 407", "blocked status=407",
    "status_code: 407", "Proxy Authentication Required",
])
def test_auth_failure_blocks_all_sources_and_extraction(paid_pool, reason):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, reason, "guba")
    for source in ("guba", "guba_mobile_primary", "jiuyan", "tgb", "hupu"):
        with pytest.raises(RuntimeError, match="authentication circuit is open"):
            pool.acquire(source)
    with pytest.raises(RuntimeError, match="authentication circuit is open"):
        pool._refresh("tgb")
    assert calls == [1]
    assert json.loads(pool.auth_block_path.read_text())['reason'] == 'proxy_auth_407'


def test_marker_blocks_new_pool_after_restart(paid_pool):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, "HTTP 407", "guba")
    restarted = KuaidailiProxyPool(pool.settings)
    with pytest.raises(RuntimeError, match="bounded recovery probe"):
        restarted.acquire("tgb", experiment=True)
    assert calls == [1]


def test_existing_marker_blocks_without_reading_api_credentials(paid_pool):
    pool, calls = paid_pool
    pool.auth_block_path.write_text("manual emergency hold", encoding="utf-8")
    pool.settings.api_url_file = pool.settings.api_url_file.with_name("does-not-exist")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("jiuyan")
    assert calls == []


def test_success_does_not_clear_auth_circuit(paid_pool):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, "HTTP 407", "guba")
    pool.report_success(lease, "tgb")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("tgb")
    assert calls == [1]


def test_concurrent_retries_never_extract_more_ips(paid_pool):
    pool, calls = paid_pool
    pool.report_bad(pool.acquire("guba"), "HTTP 407", "guba")

    def retry(_):
        with pytest.raises(RuntimeError, match="authentication circuit"):
            pool.acquire("jiuyan")

    with ThreadPoolExecutor(max_workers=8) as threads:
        list(threads.map(retry, range(64)))
    assert calls == [1]


@pytest.mark.parametrize("reason", ["HTTP 403", "HTTP 429", "HTTP 544", "curl: (28) timed out", "post id 407", "https://example.invalid/407"])
def test_non_auth_errors_preserve_existing_source_isolation(paid_pool, reason):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, reason, "guba")
    assert pool.acquire("tgb") is not None
    assert not pool.auth_block_path.exists()
    assert calls == [1]


def test_memory_latch_survives_marker_write_failure(paid_pool, monkeypatch):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    original = type(pool.auth_block_path).open

    def fail_marker(path, *args, **kwargs):
        if path == pool.auth_block_path:
            raise PermissionError("fixture")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(type(pool.auth_block_path), "open", fail_marker)
    with pytest.raises(PermissionError):
        pool.report_bad(lease, "HTTP 407", "guba")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("tgb")
    assert calls == [1]


def open_due_hold(pool):
    pool.report_bad(pool.acquire("guba"), "HTTP 407", "guba")
    opened = datetime.now(UTC) - timedelta(minutes=6)
    pool.auth_block_path.write_text(json.dumps({
        "reason": "proxy_auth_407", "opened_at": opened.isoformat(),
    }), encoding="utf-8")
    pool._expires_at = {url: 0 for url in pool._urls}
    return opened


def test_fresh_hold_waits_before_any_probe(paid_pool, monkeypatch):
    pool, calls = paid_pool
    pool.report_bad(pool.acquire("guba"), "HTTP 407", "guba")
    monkeypatch.setattr(pool, "_probe_auth", lambda _: pytest.fail("not due"))
    assert not pool.maybe_recover_auth()
    assert calls == [1]


def test_retry_schedule_is_exponential_capped_and_durable(paid_pool, monkeypatch):
    pool, _ = paid_pool
    opened = datetime(2026, 11, 9, tzinfo=UTC)
    monkeypatch.setattr("alphapulse.runtime.proxy_metrics.random.uniform", lambda *_: 0)
    assert pool.metrics.reserve_auth_probe("kuaidaili", opened, now=opened) is None
    now = opened + timedelta(minutes=5)
    for attempt, minutes in enumerate([10, 20, 40, 60, 60, 60], 1):
        restarted = KuaidailiProxyPool(pool.settings)
        assert restarted.metrics.reserve_auth_probe("kuaidaili", opened, now=now) == attempt
        next_at = now + timedelta(minutes=minutes)
        with pool.metrics.connection() as c:
            row = c.execute("SELECT * FROM proxy_auth_recovery").fetchone()
        assert row["next_probe_at"] == next_at.isoformat()
        assert pool.metrics.reserve_auth_probe(
            "kuaidaili", opened, now=next_at - timedelta(seconds=1),
        ) is None
        now = next_at


@pytest.mark.parametrize("outcome", ["auth_407", "transport_error", "http_error"])
def test_failed_probe_remains_held_and_does_not_retry_per_source(paid_pool, monkeypatch, outcome):
    pool, calls = paid_pool
    open_due_hold(pool)
    probes = []
    monkeypatch.setattr(pool, "_probe_auth", lambda lease: probes.append(lease) or outcome)
    for _ in range(20):
        assert not pool.maybe_recover_auth()
        with pytest.raises(RuntimeError, match="authentication circuit"):
            pool.acquire("tgb")
    restarted = KuaidailiProxyPool(pool.settings)
    assert not restarted.maybe_recover_auth()
    assert calls == [1, 1]
    assert len(probes) == 1
    assert pool.auth_block_path.exists()
    with pool.metrics.connection() as c:
        reserved = c.execute("SELECT count FROM proxy_events WHERE source='auth_probe' "
                             "AND event_type='extraction_reserved'").fetchall()
    assert [r[0] for r in reserved] == [1]


def test_successful_probe_archives_hold_without_resetting_crawl_budget(paid_pool, monkeypatch):
    pool, calls = paid_pool
    open_due_hold(pool)
    body = pool.auth_block_path.read_bytes()
    another = KuaidailiProxyPool(pool.settings)
    monkeypatch.setattr(pool, "_probe_auth", lambda _: "success")
    assert pool.maybe_recover_auth()
    assert not pool.is_auth_blocked()
    assert not another.is_auth_blocked()
    archives = list(pool.auth_block_path.parent.glob("*.auth-blocked.recovered-*"))
    assert len(archives) == 1 and archives[0].read_bytes() == body
    assert calls == [1, 1]
    assert len(pool._urls) == 1
    assert pool.acquire("tgb") is not None
    with pool.metrics.connection() as c:
        assert c.execute("SELECT COUNT(*) FROM proxy_events "
                         "WHERE event_type='request_success'").fetchone()[0] == 0
        assert c.execute("SELECT last_outcome FROM proxy_auth_recovery").fetchone()[0] == "success"


def test_concurrent_pool_instances_share_one_probe_claim(paid_pool, monkeypatch):
    pool, calls = paid_pool
    open_due_hold(pool)
    instances = [KuaidailiProxyPool(pool.settings) for _ in range(8)]
    probes = []
    monkeypatch.setattr(KuaidailiProxyPool, "_probe_auth", lambda _, lease: probes.append(lease) or "auth_407")
    with ThreadPoolExecutor(max_workers=8) as threads:
        assert not any(threads.map(lambda p: p.maybe_recover_auth(), instances))
    assert calls == [1, 1]
    assert len(probes) == 1


@pytest.mark.parametrize("kind", ["hourly", "daily", "no_success"])
def test_probe_cannot_bypass_extraction_budgets(paid_pool, monkeypatch, kind):
    pool, calls = paid_pool
    open_due_hold(pool)
    setattr(pool.settings, {
        "hourly": "max_extracted_per_hour", "daily": "max_extracted_per_day",
        "no_success": "max_extracted_without_success",
    }[kind], 1)
    monkeypatch.setattr(pool, "_probe_auth", lambda _: pytest.fail("budget must block network"))
    assert not pool.maybe_recover_auth()
    assert pool.is_auth_blocked()
    assert calls == [1]
    with pool.metrics.connection() as c:
        assert c.execute("SELECT last_outcome FROM proxy_auth_recovery").fetchone()[0] == "budget_exhausted"


@pytest.mark.parametrize("body", ["manual emergency hold", "{}", "[]", '{"reason":"manual"}',
                                  '{"reason":"proxy_auth_407","opened_at":"invalid"}'])
def test_manual_and_malformed_holds_never_auto_release(paid_pool, monkeypatch, body):
    pool, calls = paid_pool
    pool.auth_block_path.write_text(body, encoding="utf-8")
    monkeypatch.setattr(pool, "_probe_auth", lambda _: pytest.fail("manual hold"))
    assert not pool.maybe_recover_auth()
    assert pool.is_auth_blocked()
    assert pool.auth_block_path.read_text() == body
    assert calls == []


def test_probe_reuses_valid_unbenched_cached_route(paid_pool, monkeypatch):
    pool, calls = paid_pool
    open_due_hold(pool)
    pool._expires_at[pool._urls[-1]] = time.monotonic() + 60
    monkeypatch.setattr(pool, "_probe_auth", lambda _: "success")
    assert pool.maybe_recover_auth()
    assert calls == [1]


def test_probe_does_not_release_replaced_operator_hold(paid_pool, monkeypatch):
    pool, _ = paid_pool
    open_due_hold(pool)
    def replace(_):
        pool.auth_block_path.write_text("manual emergency hold", encoding="utf-8")
        return "success"
    monkeypatch.setattr(pool, "_probe_auth", replace)
    assert not pool.maybe_recover_auth()
    assert pool.is_auth_blocked()
    assert pool.auth_block_path.read_text() == "manual emergency hold"


def test_neutral_probe_uses_single_verified_https_proxy_request(paid_pool, monkeypatch):
    pool, _ = paid_pool
    lease = pool.acquire("guba")
    calls = []
    class Session:
        def __init__(self, **kwargs):
            assert kwargs == {"trust_env": False}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def head(self, url, **kwargs):
            calls.append((url, kwargs))
            return type("Response", (), {"status_code": 200})()
    monkeypatch.setattr("curl_cffi.requests.Session", Session)
    assert pool._probe_auth(lease) == "success"
    assert calls == [("https://www.baidu.com/", {
        "proxy": lease.proxy_url, "timeout": 10, "allow_redirects": False,
    })]


@pytest.mark.parametrize("error,expected", [
    ("curl: CONNECT tunnel failed, response 407", "auth_407"),
    ("timeout with private proxy details", "transport_error"),
])
def test_probe_exception_is_categorized_without_persisting_details(paid_pool, monkeypatch, error, expected):
    pool, _ = paid_pool
    lease = pool.acquire("guba")
    class Session:
        def __init__(self, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def head(self, *args, **kwargs):
            raise RuntimeError(error)
    monkeypatch.setattr("curl_cffi.requests.Session", Session)
    assert pool._probe_auth(lease) == expected


def test_api_failure_keeps_hold_and_respects_persisted_backoff(paid_pool, monkeypatch):
    pool, calls = paid_pool
    open_due_hold(pool)
    def unavailable(*args, **kwargs):
        raise TimeoutError("fixture")
    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", unavailable)
    assert not pool.maybe_recover_auth()
    assert pool.is_auth_blocked()
    with pool.metrics.connection() as c:
        row = c.execute("SELECT * FROM proxy_auth_recovery").fetchone()
    assert row["attempts"] == 1 and row["last_outcome"] == "extraction_error"
    assert not KuaidailiProxyPool(pool.settings).maybe_recover_auth()
    assert calls == [1]
