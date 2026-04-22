import json
from urllib import error

import pytest

from alphapulse.runtime.config import BilibiliSettings, CrawlSettings
from alphapulse.sources.bilibili.api import BilibiliApiClient
from alphapulse.sources.fetching import ProxyLease


def test_bilibili_api_retries_rate_limit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BilibiliApiClient(BilibiliSettings(), CrawlSettings())
    calls = iter(
        [
            (200, json.dumps({"code": -412, "message": "rate limited"})),
            (200, json.dumps({"code": 0, "data": {"aid": 1, "bvid": "BV1xx411c7mu"}})),
        ]
    )
    sleeps: list[float] = []

    monkeypatch.setattr(client, "_dispatch_request", lambda path, params, proxy_url: next(calls))
    monkeypatch.setattr("alphapulse.sources.bilibili.api.time.sleep", lambda value: sleeps.append(value))

    result = client.get_video_info(bvid="BV1xx411c7mu")

    assert result.error_message is None
    assert result.payload["data"]["aid"] == 1
    assert len(sleeps) == 2


def test_bilibili_get_user_videos_signs_wbi_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BilibiliApiClient(BilibiliSettings(), CrawlSettings())
    client._wbi_keys = ("a" * 32, "b" * 32)
    client._wbi_keys_fetched_at = 9e18

    captured: dict[str, dict[str, object]] = {}

    def fake_dispatch(path: str, params: dict[str, object], proxy_url: object) -> tuple[int, str]:
        del proxy_url
        captured["path"] = path
        captured["params"] = params
        return 200, json.dumps({"code": 0, "data": {"list": {"vlist": []}, "page": {"count": 0}}})

    monkeypatch.setattr(client, "_dispatch_request", fake_dispatch)
    monkeypatch.setattr("alphapulse.sources.bilibili.api.time.sleep", lambda value: None)
    monkeypatch.setattr("alphapulse.sources.bilibili.api.time.time", lambda: 1_700_000_000)

    result = client.get_user_videos(mid=7033507, page=2, page_size=5)

    assert result.error_message is None
    assert captured["path"] == "/x/space/wbi/arc/search"
    params = captured["params"]
    assert params["mid"] == "7033507"
    assert params["pn"] == "2"
    assert params["ps"] == "5"
    assert params["wts"] == "1700000000"
    assert len(params["w_rid"]) == 32
    ordered_keys = list(params.keys())
    assert ordered_keys == sorted(k for k in ordered_keys if k != "w_rid") + ["w_rid"]


def test_bilibili_adaptive_sleep_samples_random_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BilibiliApiClient(
        BilibiliSettings(request_interval_min_seconds=1.5, request_interval_max_seconds=4.0),
        CrawlSettings(),
    )
    sampled: list[tuple[float, float]] = []
    sleeps: list[float] = []

    def fake_uniform(low: float, high: float) -> float:
        sampled.append((low, high))
        return (low + high) / 2

    monkeypatch.setattr("alphapulse.sources.bilibili.api.random.uniform", fake_uniform)
    monkeypatch.setattr("alphapulse.sources.bilibili.api.time.sleep", lambda value: sleeps.append(value))

    client._adaptive_sleep(was_rate_limited=False)
    client._adaptive_sleep(was_rate_limited=True)
    client._adaptive_sleep(was_rate_limited=True)

    assert sampled == [(1.5, 4.0), (1.5, 4.0), (1.5, 4.0)]
    mid = (1.5 + 4.0) / 2
    assert sleeps == [mid * 1.0, mid * 2.0, mid * 4.0]


def test_bilibili_api_returns_error_after_retry_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BilibiliApiClient(
        BilibiliSettings(),
        CrawlSettings.model_validate(
            {
                "proxy": {"enabled": True, "provider": "proxy_pool", "max_attempts": 2},
                "proxy_pool": {"base_url": "http://proxy_pool:5010"},
            }
        ),
    )
    reported: list[str] = []

    monkeypatch.setattr(
        client.proxy_provider,
        "acquire",
        lambda: ProxyLease("http://1.1.1.1:8080", "1", "proxy_pool"),
    )
    monkeypatch.setattr(client.proxy_provider, "report_bad", lambda lease, reason: reported.append(reason))
    monkeypatch.setattr(
        client,
        "_dispatch_request",
        lambda path, params, proxy_url: (_ for _ in ()).throw(error.URLError("boom")),
    )
    monkeypatch.setattr("alphapulse.sources.bilibili.api.time.sleep", lambda value: None)

    result = client.get_video_info(aid=123)

    assert result.error_message == "<urlopen error boom>"
    assert reported == ["<urlopen error boom>", "<urlopen error boom>"]
