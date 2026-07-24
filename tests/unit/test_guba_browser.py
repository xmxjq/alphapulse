from __future__ import annotations

import json

import alphapulse.sources.guba.browser as browser_module
from alphapulse.runtime.config import GubaBrowserSettings
from alphapulse.sources.guba.browser import GubaBrowserClient


class FakeResponse:
    status = 200


class FakePage:
    def __init__(
        self,
        *,
        html: str,
        payload: dict | None,
        url: str = "https://guba.eastmoney.com/news,600519,42.html",
    ) -> None:
        self._html = html
        self._payload = payload
        self.url = url

    def goto(self, *args, **kwargs):
        return FakeResponse()

    def content(self) -> str:
        return self._html

    def evaluate(self, expression: str):
        del expression
        return json.dumps(self._payload) if self._payload is not None else None


class StubBrowserClient(GubaBrowserClient):
    def __init__(self, page: FakePage) -> None:
        super().__init__(
            GubaBrowserSettings(
                enabled=True,
                navigation_timeout_seconds=1,
                settle_timeout_seconds=1,
                request_interval_min_seconds=0,
                request_interval_max_seconds=0,
            )
        )
        self.test_page = page

    def _ensure_page(self):
        return self.test_page

    def _wait_until_resolved(self, page) -> None:
        del page


def test_browser_client_appends_embedded_payload_for_existing_parser() -> None:
    payload = {"post_id": 42, "post_content": "browser content"}
    client = StubBrowserClient(FakePage(html="<html></html>", payload=payload))

    result = client.get("https://guba.eastmoney.com/news,600519,42.html")

    assert result.status_code == 200
    assert not result.blocked
    assert f"var post_article={json.dumps(payload)};" in result.text


def test_browser_client_classifies_rendered_captcha() -> None:
    client = StubBrowserClient(
        FakePage(
            html='<html><script src="/fd_guba_validate/validate.js"></script></html>',
            payload=None,
        )
    )

    result = client.get("https://guba.eastmoney.com/news,600519,42.html")

    assert result.blocked
    assert result.block_kind == "captcha"


def test_browser_client_accepts_embedded_payload_when_global_is_unavailable() -> None:
    payload = {"post_id": 42, "post_content": "browser content"}
    html = f"<html><script>var post_article={json.dumps(payload)};</script></html>"
    client = StubBrowserClient(FakePage(html=html, payload=None))

    result = client.get("https://guba.eastmoney.com/news,600519,42.html")

    assert not result.blocked
    assert result.block_kind is None


def test_browser_client_waits_for_configured_request_slot(monkeypatch) -> None:
    client = GubaBrowserClient(
        GubaBrowserSettings(
            request_interval_min_seconds=30,
            request_interval_max_seconds=90,
        )
    )
    client._last_request_finished_at = 100.0
    sleeps: list[float] = []
    monkeypatch.setattr(browser_module.time, "monotonic", lambda: 110.0)
    monkeypatch.setattr(browser_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(browser_module.random, "uniform", lambda minimum, maximum: 30.0)

    client._wait_for_request_slot()

    assert sleeps == [20.0]


class FakeRouteRequest:
    def __init__(self, resource_type: str) -> None:
        self.resource_type = resource_type


class FakeRoute:
    def __init__(self, resource_type: str) -> None:
        self.request = FakeRouteRequest(resource_type)
        self.aborted = False
        self.continued = False

    def abort(self) -> None:
        self.aborted = True

    def continue_(self) -> None:
        self.continued = True


def test_browser_client_blocks_heavy_resources() -> None:
    image_route = FakeRoute("image")
    script_route = FakeRoute("script")

    GubaBrowserClient._route_request(image_route)
    GubaBrowserClient._route_request(script_route)

    assert image_route.aborted
    assert not image_route.continued
    assert script_route.continued
    assert not script_route.aborted
