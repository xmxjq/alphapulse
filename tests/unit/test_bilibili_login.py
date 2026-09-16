from types import SimpleNamespace

import pytest

from alphapulse.sources.bilibili.login import (
    GENERATE_URL, NAV_URL, POLL_URL, LoginFailure, credential_fields, run_qr_login,
)


def test_credentials_use_response_cookies_when_url_has_none():
    fields = credential_fields({"code": 0, "url": "https://passport.bilibili.com/crossDomain"},
                               {"SESSDATA": "fixture-session", "bili_jct": "fixture-csrf"})
    assert fields["sessdata"] == "fixture-session"
    assert fields["bili_jct"] == "fixture-csrf"


def test_legacy_url_credentials_are_parsed_as_query_parameters():
    fields = credential_fields({"code": 0, "url": "https://passport.bilibili.com/crossDomain?SESSDATA=fixture%2Csession&DedeUserID=42"}, {})
    assert fields["sessdata"] == "fixture,session"
    assert fields["dedeuserid"] == "42"


@pytest.mark.parametrize("data", [{"code": 0, "url": "https://passport.bilibili.com/crossDomain"},
                                  {"code": 99999, "url": "https://passport.bilibili.com/crossDomain?SESSDATA=fixture"}])
def test_empty_or_unapproved_session_is_rejected(data):
    with pytest.raises(LoginFailure):
        credential_fields(data, {})


class Session:
    def __init__(self, *, nav_code=0, state=0, cookie=True):
        self.calls = []
        self.cookies = SimpleNamespace(get_dict=lambda: {})
        self.nav_code, self.state, self.cookie = nav_code, state, cookie

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if url == GENERATE_URL:
            data = {"url": "https://passport.bilibili.com/h5-app/passport/login/scan?fixture=1",
                    "qrcode_key": "fixture-key"}
        elif url == POLL_URL:
            data = {"code": self.state, "url": "https://passport.bilibili.com/crossDomain"}
        else:
            assert url == NAV_URL
            data = {"isLogin": self.nav_code == 0}
        code = self.nav_code if url == NAV_URL else 0
        cookies = {"SESSDATA": "fixture-session"} if url == POLL_URL and self.cookie else {}
        return SimpleNamespace(status_code=200, json=lambda: {"code": code, "data": data},
                               cookies=SimpleNamespace(get_dict=lambda: cookies))


def execute(session, saved):
    return run_qr_login(
        session, render_qr=lambda url: None,
        credential_factory=lambda **fields: SimpleNamespace(get_cookies=lambda: {"SESSDATA": fields["sessdata"]}),
        save_credential=saved.append,
    )


def test_login_saves_only_after_independent_nav_validation():
    session, saved = Session(), []
    execute(session, saved)
    assert len(saved) == 1
    assert [url for url, _ in session.calls] == [GENERATE_URL, POLL_URL, NAV_URL]
    assert all(kwargs["allow_redirects"] is False for _, kwargs in session.calls)


@pytest.mark.parametrize("session", [Session(nav_code=-101), Session(state=99999), Session(cookie=False)])
def test_failure_never_saves_credentials(session):
    saved = []
    with pytest.raises(LoginFailure):
        execute(session, saved)
    assert saved == []
