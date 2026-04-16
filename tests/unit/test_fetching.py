import sys
from types import ModuleType

import pytest

from alphapulse.runtime.config import CrawlSettings, XueqiuSettings
from alphapulse.sources.fetching import (
    ProxyLease,
    ProxyPoolProvider,
    ScraplingClient,
    _browser_cookies,
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
