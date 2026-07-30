from __future__ import annotations

import hashlib

from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.jiuyan.api import JiuyanClient, JiuyanHttpResult


def test_headers_match_web_client_signature() -> None:
    client = JiuyanClient(
        JiuyanSettings(
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        CrawlSettings(),
    )
    headers = client._headers("/api/v2/article/search", {"start": 1})
    assert headers["token"] == hashlib.md5(
        f"Uu0KfOB8iUP69d3c:{headers['timestamp']}".encode()
    ).hexdigest()
    assert headers["Content-Type"] == "application/json"
    assert headers["platform"] == "3"


def test_page_time_is_sent_after_first_page() -> None:
    client = JiuyanClient(JiuyanSettings(), CrawlSettings())
    client._page_times["/api/v2/article/search"] = "123456"
    headers = client._headers("/api/v2/article/search", {"start": 2})
    assert headers["Page-Time"] == "123456"


def test_community_feed_maps_to_public_latest_publish_stream() -> None:
    client = JiuyanClient(JiuyanSettings(), CrawlSettings())
    captured: dict[str, object] = {}

    def fake_post(path: str, payload: dict[str, object]) -> JiuyanHttpResult:
        captured.update({"path": path, "payload": payload})
        return JiuyanHttpResult(url=path, status_code=200, text='{"errCode":"0"}')

    client.post = fake_post  # type: ignore[method-assign]
    client.community_articles("square", 3, page_size=30)

    assert captured == {
        "path": "/api/v2/article/community",
        "payload": {
            "category_id": "",
            "limit": 30,
            "order": 0,
            "start": 3,
            "type": 0,
            "back_garden": 1,
        },
    }
