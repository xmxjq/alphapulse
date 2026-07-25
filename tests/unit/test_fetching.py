import sys
from datetime import UTC, datetime
from types import ModuleType

import pytest

from alphapulse.runtime.config import CrawlSettings, XueqiuSettings
from alphapulse.sources.fetching import (
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

    assert isinstance(
        _build_proxy_provider(settings, source="guba"),
        KuaidailiProxyProvider,
    )
    assert _build_proxy_provider(settings, source="bilibili") is None


def test_static_list_provider_requires_urls_when_selected() -> None:
    with pytest.raises(ValueError):
        CrawlSettings.model_validate(
            {"proxy": {"enabled": True, "provider": "static_list"}, "static_proxies": {"urls": []}}
        )
