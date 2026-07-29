from __future__ import annotations

import hashlib

from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.jiuyan.api import JiuyanClient


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
