from __future__ import annotations

import json

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
