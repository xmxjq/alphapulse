from types import SimpleNamespace

from alphapulse.sources.bilibili import space_cli


def test_saved_credential_preferred_without_automatic_auth(monkeypatch):
    saved = object()
    calls = []

    async def videos(**kwargs):
        calls.append(kwargs)
        return [{"bvid": "fixture"}]

    auth = SimpleNamespace(_load_saved_credential=lambda: saved)
    monkeypatch.setattr(space_cli, "_import_bili_cli", lambda: (auth, SimpleNamespace(get_user_videos=videos)))
    client = space_cli.BilibiliCliSpaceDiscoveryClient(cookies={"SESSDATA": "fake-expired"})
    assert client.get_user_videos(uid=42, count=1) == [{"bvid": "fixture"}]
    assert calls[0]["credential"] is saved


def test_configured_cookie_fallback_without_persisting_or_refreshing():
    auth = SimpleNamespace(_load_saved_credential=lambda: None, Credential=lambda **kwargs: kwargs)
    client = space_cli.BilibiliCliSpaceDiscoveryClient(cookies={
        "SESSDATA": "fake-session", "bili_jct": "fake-csrf", "buvid3": "fake-device",
    })
    result = client._credential(auth)
    assert result["sessdata"] == "fake-session"
    assert result["buvid3"] == "fake-device"


def test_missing_credentials_remain_guest_without_browser_scan():
    auth = SimpleNamespace(_load_saved_credential=lambda: None)
    assert space_cli.BilibiliCliSpaceDiscoveryClient()._credential(auth) is None
