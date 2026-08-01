from __future__ import annotations

import hashlib

from alphapulse.runtime.agent_pool import RemoteFetchResponse
from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.jiuyan.api import JiuyanClient, JiuyanHttpResult


class FakeOutcomeStore:
    def __init__(self) -> None:
        self.outcomes: list[tuple[str, str, str | None]] = []

    def record_outcome(
        self,
        job_id: str,
        outcome: str,
        reason: str | None = None,
    ) -> None:
        self.outcomes.append((job_id, outcome, reason))


class FakeAgentPool:
    def __init__(self, response: RemoteFetchResponse) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []
        self.store = FakeOutcomeStore()

    def fetch(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


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

    def fake_post(
        path: str,
        payload: dict[str, object],
        **kwargs,
    ) -> JiuyanHttpResult:
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


def test_agent_transport_omits_cookies_and_records_success(
    monkeypatch,
    tmp_path,
) -> None:
    crawl = CrawlSettings(
        agent_pool={
            "enabled": True,
            "db_path": tmp_path / "agent-pool.db",
            "sources": ["jiuyan"],
            "allowed_hosts": ["app.jiuyangongshe.com"],
        }
    )
    client = JiuyanClient(
        JiuyanSettings(
            cookies={"session": "secret"},
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
            max_retries=1,
        ),
        crawl,
    )
    agent_pool = FakeAgentPool(
        RemoteFetchResponse(
            job_id="job-1",
            agent_id="home-arm",
            status_code=200,
            final_url=(
                "https://app.jiuyangongshe.com/jystock-app/"
                "api/v2/article/detail?articleId=a1"
            ),
            headers={"content-type": "application/json"},
            body=b'{"errCode":"0","data":{}}',
            duration_ms=12,
        )
    )
    client.agent_pool = agent_pool
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("existing transport should not run")
        ),
    )

    result = client.article_detail("a1", transport="agent")

    assert result.status_code == 200
    assert "Cookie" not in agent_pool.calls[0]["headers"]
    assert agent_pool.store.outcomes == [("job-1", "success", None)]


def test_existing_transport_bypasses_agent(monkeypatch, tmp_path) -> None:
    crawl = CrawlSettings(
        agent_pool={
            "enabled": True,
            "db_path": tmp_path / "agent-pool.db",
            "sources": ["jiuyan"],
            "allowed_hosts": ["app.jiuyangongshe.com"],
        }
    )
    client = JiuyanClient(
        JiuyanSettings(
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
            max_retries=1,
        ),
        crawl,
    )
    agent_pool = FakeAgentPool(
        RemoteFetchResponse(
            job_id="unexpected",
            agent_id="home-arm",
            status_code=500,
            final_url="https://app.jiuyangongshe.com/",
            headers={},
            body=b"",
            duration_ms=1,
        )
    )
    client.agent_pool = agent_pool
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (
            200,
            '{"errCode":"0","data":{}}',
            "https://app.jiuyangongshe.com/jystock-app/api/v1/article/rank-board",
        ),
    )

    result = client.hot_rankings()

    assert result.status_code == 200
    assert agent_pool.calls == []


def test_article_detail_captcha_does_not_retry_within_one_task(monkeypatch) -> None:
    client = JiuyanClient(
        JiuyanSettings(
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
            max_retries=3,
        ),
        CrawlSettings(),
    )
    calls = 0

    def captcha_response(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        return 200, "captcha", "https://app.jiuyangongshe.com/captcha"

    monkeypatch.setattr(client, "_dispatch", captcha_response)

    result = client.article_detail("blocked", transport="existing")

    assert result.blocked is True
    assert result.block_kind == "captcha"
    assert calls == 1
