from alphapulse.sources.fetching import _browser_cookies, _response_text


class DummyResponse:
    def __init__(self, text: str = "", body: bytes | None = None, html_content: str | None = None) -> None:
        self.text = text
        self.body = body
        self.html_content = html_content


class DummySettings:
    def __init__(self) -> None:
        self.cookies = {"xq_a_token": "token-value"}
        self.base_url = "https://xueqiu.com"


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
