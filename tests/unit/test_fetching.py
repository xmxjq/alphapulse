import sys
import time
from datetime import UTC, datetime
from types import ModuleType

import pytest

from alphapulse.runtime.config import CrawlSettings, XueqiuSettings
from alphapulse.sources.fetching import (
    KuaidailiProxyPool,
    KuaidailiProxyProvider,
    ProxyLease,
    ProxyPoolProvider,
    ScraplingClient,
    StaticListProxyProvider,
    _browser_cookies,
    _build_proxy_provider,
    _response_text,
)


class DummyResponse:
    def __init__(self, text: str = "", body: bytes | None = None, html_content: str | None = None) -> None:
        self.text = text
        self.body = body
        self.html_content = html_content


class DummySettings:
    def __init__(self) -> None:
        self.cookies = {"xq_a_token": "token-value"}
        self.base_url = "https://xueqiu.com"


class DummyUrlopenResponse:
    def __init__(self, payload: str = "") -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_response_text_falls_back_to_body_when_text_is_empty() -> None:
    response = DummyResponse(text="", body=b'{"ok": true}')
    assert _response_text(response) == '{"ok": true}'


def test_browser_cookies_converts_cookie_dict_for_browser_fetchers() -> None:
    cookies = _browser_cookies(DummySettings())
    assert cookies == [
        {
            "name": "xq_a_token",
            "value": "token-value",
            "domain": "xueqiu.com",
            "path": "/",
        }
    ]


def _install_fake_scrapling(monkeypatch: pytest.MonkeyPatch, calls: dict[str, list[dict]]) -> None:
    class StaticFetcher:
        @staticmethod
        def get(url: str, **kwargs):
            calls["static"].append({"url": url, **kwargs})
            return type(
                "Response",
                (),
                {
                    "url": url,
                    "status": 200,
                    "text": "<html>ok</html>",
                    "headers": {"content-type": "text/html"},
                },
            )()

    class DynamicFetcher:
        @staticmethod
        def fetch(url: str, **kwargs):
            calls["dynamic"].append({"url": url, **kwargs})
            return type(
                "Response",
                (),
                {
                    "url": url,
                    "status": 200,
                    "text": "<html>ok</html>",
                    "headers": {},
                },
            )()

    class StealthyFetcher:
        @staticmethod
        def fetch(url: str, **kwargs):
            calls["stealth"].append({"url": url, **kwargs})
            return type(
                "Response",
                (),
                {
                    "url": url,
                    "status": 200,
                    "text": "<html>ok</html>",
                    "headers": {},
                },
            )()

    scrapling_module = ModuleType("scrapling")
    fetchers_module = ModuleType("scrapling.fetchers")
    fetchers_module.Fetcher = StaticFetcher
    fetchers_module.DynamicFetcher = DynamicFetcher
    fetchers_module.StealthyFetcher = StealthyFetcher
    monkeypatch.setitem(sys.modules, "scrapling", scrapling_module)
    monkeypatch.setitem(sys.modules, "scrapling.fetchers", fetchers_module)


def test_proxy_pool_provider_acquires_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ProxyPoolProvider(CrawlSettings().proxy_pool)

    def fake_urlopen(url: str, timeout: int):
        assert url == "http://proxy_pool:5010/get/?type=https"
        assert timeout == 3
        return DummyUrlopenResponse('{"proxy":"1.2.3.4:8080"}')

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", fake_urlopen)
    lease = provider.acquire()

    assert lease == ProxyLease(
        proxy_url="http://1.2.3.4:8080",
        delete_key="1.2.3.4:8080",
        provider_name="proxy_pool",
    )


def test_proxy_pool_provider_handles_empty_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ProxyPoolProvider(CrawlSettings().proxy_pool)

    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse('{"code":0,"src":"no proxy"}'),
    )

    assert provider.acquire() is None


def test_proxy_pool_provider_reports_bad_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ProxyPoolProvider(CrawlSettings().proxy_pool)
    seen: list[tuple[str, int]] = []

    def fake_urlopen(url: str, timeout: int):
        seen.append((url, timeout))
        return DummyUrlopenResponse()

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", fake_urlopen)

    provider.report_bad(
        ProxyLease(
            proxy_url="http://1.2.3.4:8080",
            delete_key="1.2.3.4:8080",
            provider_name="proxy_pool",
        ),
        "blocked",
    )

    assert seen == [("http://proxy_pool:5010/delete/?proxy=1.2.3.4%3A8080", 3)]


def _kuaidaili_settings(tmp_path, **overrides) -> CrawlSettings:
    api_file = tmp_path / "kuaidaili-api-url.txt"
    api_file.write_text(
        "https://dps.kdlapi.com/api/getdps/?secret_id=test&signature=hidden&num=1"
    )
    payload = {
        "proxy": {"enabled": True, "provider": "kuaidaili", "sources": ["guba"]},
        "kuaidaili": {
            "api_url_file": str(api_file),
            "metrics_path": str(tmp_path / "proxy-metrics.db"),
            "batch_size": 2,
            "low_watermark": 0,
            "lease_ttl_seconds": 600,
            "cooldown_seconds": 60,
        },
    }
    payload["kuaidaili"].update(overrides)
    return CrawlSettings.model_validate(payload)


def test_kuaidaili_provider_extracts_and_rotates_text_proxies(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(_kuaidaili_settings(tmp_path).kuaidaili)
    requested: list[str] = []

    def fake_urlopen(url: str, timeout: int):
        requested.append(url)
        assert timeout == 20
        return DummyUrlopenResponse("1.2.3.4:8080\n2.3.4.5:8081")

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", fake_urlopen)

    first = provider.acquire()
    second = provider.acquire()

    assert first == ProxyLease(
        proxy_url="http://1.2.3.4:8080",
        delete_key="1.2.3.4:8080",
        provider_name="kuaidaili",
    )
    assert second.proxy_url == "http://2.3.4.5:8081"
    assert len(requested) == 1
    assert "num=2" in requested[0]


def test_kuaidaili_dual_endpoint_experiment_assigns_stable_roles_and_metrics(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path).kuaidaili,
        source="guba",
        experiment_active=lambda: True,
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse(
            "1.2.3.4:8080\n2.3.4.5:8081"
        ),
    )

    dual_desktop = provider.acquire_for_experiment(eligible=True)
    control = provider.acquire_for_experiment(eligible=True)
    dual_mobile = provider.acquire_for_experiment(eligible=True)

    assert dual_desktop.proxy_url == dual_mobile.proxy_url
    assert dual_desktop.cohort == dual_mobile.cohort == "dual"
    assert dual_desktop.channel == "desktop"
    assert dual_mobile.channel == "mobile"
    assert control.cohort == "control"
    assert control.channel == "desktop"
    assert control.proxy_url != dual_desktop.proxy_url

    provider.report_success(dual_desktop)
    provider.report_success(control)
    provider.report_success(dual_mobile)
    snapshot = provider.metrics.snapshot(
        provider="kuaidaili",
        since=datetime.now(UTC).replace(year=2020),
        now=datetime.now(UTC),
    )
    sources = {item["source"]: item for item in snapshot["sources"]}
    assert sources["guba_ab_dual_desktop"]["successes"] == 1
    assert sources["guba_ab_dual_mobile"]["successes"] == 1
    assert sources["guba_ab_control_desktop"]["successes"] == 1

    provider.report_bad(dual_mobile, "HTTP 403")
    after_block = provider.acquire_for_experiment(eligible=True)
    assert after_block.proxy_url == control.proxy_url
    assert after_block.cohort == "control"


def test_kuaidaili_experiment_does_not_label_ineligible_requests(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path).kuaidaili,
        source="guba",
        experiment_active=lambda: True,
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse(
            "1.2.3.4:8080\n2.3.4.5:8081"
        ),
    )

    lease = provider.acquire_for_experiment(eligible=False)

    assert lease.cohort is None
    assert lease.channel is None


def test_kuaidaili_experiment_rejects_incomplete_proxy_batch(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path).kuaidaili,
        source="guba",
        experiment_active=lambda: True,
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse("1.2.3.4:8080"),
    )

    assert provider.acquire_for_experiment(eligible=True) is None
    ordinary = provider.acquire()
    assert ordinary is not None
    assert ordinary.cohort is None


def test_kuaidaili_provider_uses_reported_api_expiry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(
            tmp_path,
            batch_size=1,
            use_api_expiry=True,
            expiry_safety_seconds=30,
        ).kuaidaili,
        source="guba",
    )
    requested: list[str] = []

    def fake_urlopen(url: str, timeout: int):
        requested.append(url)
        return DummyUrlopenResponse(
            '{"code":0,"data":{"proxy_list":["1.2.3.4:8080,600"]}}'
        )

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", fake_urlopen)

    lease = provider.acquire()
    remaining = provider.pool._expires_at[lease.proxy_url] - time.monotonic()

    assert "f_et=1" in requested[0]
    assert 569 <= remaining <= 570
    snapshot = provider.metrics.snapshot(
        provider="kuaidaili",
        since=datetime(2026, 1, 1, tzinfo=UTC),
        now=datetime.now(UTC),
    )
    batch = next(
        event for event in snapshot["events"] if event["event_type"] == "batch_fetched"
    )
    assert batch["detail"]["ttl_mode"] == "api"
    assert batch["detail"]["reported_ttl_min"] == 600
    assert batch["detail"]["effective_ttl_max"] == 570
    assert batch["detail"]["expiry_safety_seconds"] == 30


def test_kuaidaili_bench_lasts_until_dynamic_expiry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _kuaidaili_settings(
        tmp_path,
        batch_size=1,
        cooldown_seconds=60,
        failure_threshold=1,
        use_api_expiry=True,
        expiry_safety_seconds=30,
    ).kuaidaili
    pool = KuaidailiProxyPool(settings)
    guba = pool.provider("guba")
    tgb = pool.provider("tgb")
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse(
            '{"code":0,"data":{"proxy_list":["1.2.3.4:8080,600"]}}'
        ),
    )

    lease = guba.acquire()
    guba.report_bad(lease, "blocked: soft_block")

    assert pool._benched_until[(lease.proxy_url, "guba")] >= pool._expires_at[
        lease.proxy_url
    ]
    assert tgb.acquire().proxy_url == lease.proxy_url


def test_kuaidaili_provider_falls_back_when_api_omits_expiry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(
            tmp_path,
            batch_size=1,
            lease_ttl_seconds=240,
            use_api_expiry=True,
        ).kuaidaili,
        source="tgb",
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse("1.2.3.4:8080"),
    )

    lease = provider.acquire()
    remaining = provider.pool._expires_at[lease.proxy_url] - time.monotonic()

    assert 239 <= remaining <= 240
    snapshot = provider.metrics.snapshot(
        provider="kuaidaili",
        since=datetime(2026, 1, 1, tzinfo=UTC),
        now=datetime.now(UTC),
    )
    batch = next(
        event for event in snapshot["events"] if event["event_type"] == "batch_fetched"
    )
    assert batch["detail"]["ttl_mode"] == "fallback"
    assert batch["detail"]["effective_ttl_min"] == 240


def test_kuaidaili_provider_records_api_error_for_source(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path, batch_size=1).kuaidaili,
        source="jiuyan",
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: (_ for _ in ()).throw(RuntimeError("api down")),
    )

    with pytest.raises(RuntimeError, match="api down"):
        provider.acquire()

    snapshot = provider.metrics.snapshot(
        provider="kuaidaili",
        since=datetime(2026, 1, 1, tzinfo=UTC),
        now=datetime.now(UTC),
    )
    assert snapshot["api_errors"] == 1
    assert snapshot["sources"][0]["source"] == "jiuyan"


def test_kuaidaili_provider_benches_bad_proxy(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path, failure_threshold=1).kuaidaili
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse("1.2.3.4:8080\n2.3.4.5:8081"),
    )

    first = provider.acquire()
    provider.report_bad(first, "incomplete response")
    second = provider.acquire()

    assert second.proxy_url == "http://2.3.4.5:8081"
    snapshot = provider.metrics.snapshot(
        provider="kuaidaili",
        since=datetime(2026, 1, 1, tzinfo=UTC),
        now=datetime.now(UTC),
    )
    assert snapshot["failures"] == 1
    assert all("1.2.3.4" not in str(value) for value in snapshot.values())


def test_kuaidaili_provider_reuses_paid_ip_until_failure_threshold(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(
            tmp_path,
            batch_size=1,
            failure_threshold=3,
        ).kuaidaili
    )
    responses = iter(
        [
            DummyUrlopenResponse("1.2.3.4:8080"),
            DummyUrlopenResponse("2.3.4.5:8081"),
        ]
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: next(responses),
    )

    first = provider.acquire()
    provider.report_bad(first, "IncompleteRead")
    assert provider.acquire().proxy_url == first.proxy_url
    provider.report_bad(first, "connection reset")
    assert provider.acquire().proxy_url == first.proxy_url
    provider.report_bad(first, "timed out")

    assert provider.acquire().proxy_url == "http://2.3.4.5:8081"


def test_kuaidaili_provider_soft_block_uses_failure_streak_not_instant_bench(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path, batch_size=1, failure_threshold=3).kuaidaili
    )
    responses = iter(
        [
            DummyUrlopenResponse("1.2.3.4:8080"),
            DummyUrlopenResponse("2.3.4.5:8081"),
        ]
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: next(responses),
    )

    first = provider.acquire()
    provider.report_bad(first, "blocked: soft_block")
    assert provider.acquire().proxy_url == first.proxy_url
    provider.report_bad(first, "blocked: soft_block")
    assert provider.acquire().proxy_url == first.proxy_url
    provider.report_bad(first, "blocked: soft_block")

    assert provider.acquire().proxy_url == "http://2.3.4.5:8081"


def test_kuaidaili_provider_benches_non_soft_block_reason_immediately(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(tmp_path, batch_size=1, failure_threshold=3).kuaidaili
    )
    responses = iter(
        [
            DummyUrlopenResponse("1.2.3.4:8080"),
            DummyUrlopenResponse("2.3.4.5:8081"),
        ]
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: next(responses),
    )

    first = provider.acquire()
    provider.report_bad(first, "blocked: http_403")

    assert provider.acquire().proxy_url == "http://2.3.4.5:8081"


def test_kuaidaili_provider_benches_explicit_block_immediately(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(
        _kuaidaili_settings(
            tmp_path,
            batch_size=1,
            failure_threshold=3,
        ).kuaidaili
    )
    responses = iter(
        [
            DummyUrlopenResponse("1.2.3.4:8080"),
            DummyUrlopenResponse("2.3.4.5:8081"),
        ]
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: next(responses),
    )

    first = provider.acquire()
    provider.report_bad(first, "HTTP 403")

    assert provider.acquire().proxy_url == "http://2.3.4.5:8081"


def test_kuaidaili_provider_records_success(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = KuaidailiProxyProvider(_kuaidaili_settings(tmp_path).kuaidaili)
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: DummyUrlopenResponse("1.2.3.4:8080"),
    )

    lease = provider.acquire()
    provider.report_success(lease)

    snapshot = provider.metrics.snapshot(
        provider="kuaidaili",
        since=datetime(2026, 1, 1, tzinfo=UTC),
        now=datetime.now(UTC),
    )
    assert snapshot["successes"] == 1


def test_kuaidaili_pool_shares_one_extraction_across_sources(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _kuaidaili_settings(
        tmp_path,
        batch_size=1,
        low_watermark=0,
    ).kuaidaili
    pool = KuaidailiProxyPool(settings)
    guba = pool.provider("guba")
    tgb = pool.provider("tgb")
    requested: list[str] = []

    def fake_urlopen(url: str, timeout: int):
        requested.append(url)
        return DummyUrlopenResponse("1.2.3.4:8080")

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", fake_urlopen)

    assert guba.acquire().proxy_url == "http://1.2.3.4:8080"
    assert tgb.acquire().proxy_url == "http://1.2.3.4:8080"
    assert len(requested) == 1

    snapshot = pool.metrics.snapshot(
        provider="kuaidaili",
        since=datetime(2026, 1, 1, tzinfo=UTC),
        now=datetime.now(UTC),
    )
    assert {source["source"]: source["leases"] for source in snapshot["sources"]} == {
        "guba": 1,
        "tgb": 1,
    }


def test_kuaidaili_pool_isolates_benches_by_source(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _kuaidaili_settings(
        tmp_path,
        batch_size=1,
        low_watermark=0,
        failure_threshold=1,
    ).kuaidaili
    pool = KuaidailiProxyPool(settings)
    guba = pool.provider("guba")
    tgb = pool.provider("tgb")
    responses = iter(
        [
            DummyUrlopenResponse("1.2.3.4:8080"),
            DummyUrlopenResponse("2.3.4.5:8081"),
        ]
    )
    monkeypatch.setattr(
        "alphapulse.sources.fetching.request.urlopen",
        lambda url, timeout: next(responses),
    )

    first = guba.acquire()
    guba.report_bad(first, "blocked: soft_block")

    assert tgb.acquire().proxy_url == first.proxy_url
    assert guba.acquire().proxy_url == "http://2.3.4.5:8081"


@pytest.mark.parametrize(
    ("mode", "bucket"),
    [
        ("static", "static"),
        ("dynamic", "dynamic"),
        ("stealth", "stealth"),
    ],
)
def test_scrapling_client_passes_proxy_to_selected_fetcher(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    bucket: str,
) -> None:
    calls = {"static": [], "dynamic": [], "stealth": []}
    _install_fake_scrapling(monkeypatch, calls)

    settings = CrawlSettings.model_validate(
        {
            "proxy": {"enabled": True, "provider": "proxy_pool"},
            "proxy_pool": {"base_url": "http://proxy_pool:5010"},
        }
    )
    source_settings = XueqiuSettings.model_validate({"fetch_mode": mode})
    client = ScraplingClient(source_settings, settings)
    monkeypatch.setattr(
        client.proxy_provider,
        "acquire",
        lambda: ProxyLease(
            proxy_url="http://1.2.3.4:8080",
            delete_key="1.2.3.4:8080",
            provider_name="proxy_pool",
        ),
    )

    result = client.fetch("https://xueqiu.com/test")

    assert result.proxy_url == "http://1.2.3.4:8080"
    assert calls[bucket][0]["proxy"] == "http://1.2.3.4:8080"


def test_scrapling_client_retries_blocked_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = CrawlSettings.model_validate(
        {
            "proxy": {"enabled": True, "provider": "proxy_pool", "max_attempts": 2},
            "proxy_pool": {"base_url": "http://proxy_pool:5010", "report_bad_on_block": True},
        }
    )
    client = ScraplingClient(XueqiuSettings(), settings)
    leases = iter(
        [
            ProxyLease("http://1.1.1.1:8080", "1.1.1.1:8080", "proxy_pool"),
            ProxyLease("http://2.2.2.2:8080", "2.2.2.2:8080", "proxy_pool"),
        ]
    )
    reported: list[tuple[ProxyLease, str]] = []
    responses = iter(
        [
            type("Response", (), {"url": "https://xueqiu.com/test", "status": 403, "text": "captcha", "headers": {}})(),
            type("Response", (), {"url": "https://xueqiu.com/test", "status": 200, "text": "<html>ok</html>", "headers": {}})(),
        ]
    )

    monkeypatch.setattr(client.proxy_provider, "acquire", lambda: next(leases))
    monkeypatch.setattr(client.proxy_provider, "report_bad", lambda lease, reason: reported.append((lease, reason)))
    monkeypatch.setattr(client, "_dispatch_fetch", lambda url, proxy_url: next(responses))

    result = client.fetch("https://xueqiu.com/test")

    assert result.status_code == 200
    assert result.proxy_url == "http://2.2.2.2:8080"
    assert reported == [
        (
            ProxyLease("http://1.1.1.1:8080", "1.1.1.1:8080", "proxy_pool"),
            "blocked status=403",
        )
    ]


def test_scrapling_client_returns_error_after_retry_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = CrawlSettings.model_validate(
        {
            "proxy": {"enabled": True, "provider": "proxy_pool", "max_attempts": 2},
            "proxy_pool": {"base_url": "http://proxy_pool:5010"},
        }
    )
    client = ScraplingClient(XueqiuSettings(), settings)
    leases = iter(
        [
            ProxyLease("http://1.1.1.1:8080", "1.1.1.1:8080", "proxy_pool"),
            ProxyLease("http://2.2.2.2:8080", "2.2.2.2:8080", "proxy_pool"),
        ]
    )
    reported: list[str] = []

    monkeypatch.setattr(client.proxy_provider, "acquire", lambda: next(leases))
    monkeypatch.setattr(client.proxy_provider, "report_bad", lambda lease, reason: reported.append(lease.delete_key))
    monkeypatch.setattr(client, "_dispatch_fetch", lambda url, proxy_url: (_ for _ in ()).throw(RuntimeError("dial tcp failed")))

    result = client.fetch("https://xueqiu.com/test")

    assert result.error_message == "dial tcp failed"
    assert result.proxy_url == "http://2.2.2.2:8080"
    assert reported == ["1.1.1.1:8080", "2.2.2.2:8080"]


def test_scrapling_client_retries_after_proxy_acquire_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = CrawlSettings.model_validate(
        {
            "proxy": {"enabled": True, "provider": "proxy_pool", "max_attempts": 2, "fail_open": False},
            "proxy_pool": {"base_url": "http://proxy_pool:5010"},
        }
    )
    client = ScraplingClient(XueqiuSettings(), settings)
    acquire_calls = {"count": 0}

    def fake_acquire():
        acquire_calls["count"] += 1
        if acquire_calls["count"] == 1:
            raise RuntimeError("kuaidaili api blip")
        return ProxyLease("http://2.2.2.2:8080", "2.2.2.2:8080", "proxy_pool")

    monkeypatch.setattr(client.proxy_provider, "acquire", fake_acquire)
    monkeypatch.setattr(
        client,
        "_dispatch_fetch",
        lambda url, proxy_url: type(
            "Response", (), {"url": url, "status": 200, "text": "<html>ok</html>", "headers": {}}
        )(),
    )

    result = client.fetch("https://xueqiu.com/test")

    assert result.status_code == 200
    assert result.proxy_url == "http://2.2.2.2:8080"
    assert acquire_calls["count"] == 2


def test_scrapling_client_retries_after_no_proxy_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = CrawlSettings.model_validate(
        {
            "proxy": {"enabled": True, "provider": "proxy_pool", "max_attempts": 2, "fail_open": False},
            "proxy_pool": {"base_url": "http://proxy_pool:5010"},
        }
    )
    client = ScraplingClient(XueqiuSettings(), settings)
    acquire_calls = {"count": 0}

    def fake_acquire():
        acquire_calls["count"] += 1
        if acquire_calls["count"] == 1:
            return None
        return ProxyLease("http://2.2.2.2:8080", "2.2.2.2:8080", "proxy_pool")

    monkeypatch.setattr(client.proxy_provider, "acquire", fake_acquire)
    monkeypatch.setattr(
        client,
        "_dispatch_fetch",
        lambda url, proxy_url: type(
            "Response", (), {"url": url, "status": 200, "text": "<html>ok</html>", "headers": {}}
        )(),
    )

    result = client.fetch("https://xueqiu.com/test")

    assert result.status_code == 200
    assert result.proxy_url == "http://2.2.2.2:8080"
    assert acquire_calls["count"] == 2


def test_scrapling_client_returns_error_after_acquire_failures_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = CrawlSettings.model_validate(
        {
            "proxy": {"enabled": True, "provider": "proxy_pool", "max_attempts": 2, "fail_open": False},
            "proxy_pool": {"base_url": "http://proxy_pool:5010"},
        }
    )
    client = ScraplingClient(XueqiuSettings(), settings)

    monkeypatch.setattr(
        client.proxy_provider,
        "acquire",
        lambda: (_ for _ in ()).throw(RuntimeError("kuaidaili api down")),
    )

    result = client.fetch("https://xueqiu.com/test")

    assert result.error_message == "Failed to acquire proxy: kuaidaili api down"


def _static_settings(**overrides) -> CrawlSettings:
    payload = {
        "proxy": {"enabled": True, "provider": "static_list"},
        "static_proxies": {"urls": ["http://xray:10809", "xray:10810"], "cooldown_seconds": 60},
    }
    payload.update(overrides)
    return CrawlSettings.model_validate(payload)


def test_static_list_provider_rotates_round_robin() -> None:
    provider = StaticListProxyProvider(_static_settings().static_proxies)

    acquired = [provider.acquire().proxy_url for _ in range(4)]

    assert acquired == [
        "http://xray:10809",
        "http://xray:10810",
        "http://xray:10809",
        "http://xray:10810",
    ]


def test_static_list_provider_benches_bad_proxy_until_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = StaticListProxyProvider(_static_settings().static_proxies)
    clock = {"now": 1000.0}
    monkeypatch.setattr("alphapulse.sources.fetching.time.monotonic", lambda: clock["now"])

    first = provider.acquire()
    provider.report_bad(first, "blocked status=403")

    assert [provider.acquire().proxy_url for _ in range(2)] == [
        "http://xray:10810",
        "http://xray:10810",
    ]

    clock["now"] += 61
    assert provider.acquire().proxy_url == "http://xray:10809"


def test_static_list_provider_uses_soonest_recovering_proxy_when_all_benched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = StaticListProxyProvider(_static_settings().static_proxies)
    clock = {"now": 1000.0}
    monkeypatch.setattr("alphapulse.sources.fetching.time.monotonic", lambda: clock["now"])

    first = provider.acquire()
    provider.report_bad(first, "blocked")
    clock["now"] += 10
    second = provider.acquire()
    provider.report_bad(second, "blocked")

    assert provider.acquire().proxy_url == "http://xray:10809"


def test_build_proxy_provider_scopes_to_configured_sources() -> None:
    settings = _static_settings(
        proxy={"enabled": True, "provider": "static_list", "sources": ["guba"]}
    )

    assert isinstance(_build_proxy_provider(settings, source="guba"), StaticListProxyProvider)
    assert _build_proxy_provider(settings, source="bilibili") is None
    assert isinstance(_build_proxy_provider(settings), StaticListProxyProvider)


def test_build_kuaidaili_provider_scopes_to_guba(tmp_path) -> None:
    settings = _kuaidaili_settings(tmp_path)

    provider = _build_proxy_provider(settings, source="guba")
    assert isinstance(provider, KuaidailiProxyProvider)
    assert provider.source == "guba"
    assert _build_proxy_provider(settings, source="bilibili") is None


def test_static_list_provider_requires_urls_when_selected() -> None:
    with pytest.raises(ValueError):
        CrawlSettings.model_validate(
            {"proxy": {"enabled": True, "provider": "static_list"}, "static_proxies": {"urls": []}}
        )
