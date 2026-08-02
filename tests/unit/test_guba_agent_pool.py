from __future__ import annotations

from alphapulse.runtime.agent_pool import (
    AgentPoolUnavailable,
    RemoteFetchResponse,
    host_http_capability,
)
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.guba.api import GubaClient


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
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.calls = []
        self.store = FakeOutcomeStore()

    def fetch(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


def _client(tmp_path) -> GubaClient:
    crawl = CrawlSettings(
        agent_pool={
            "enabled": True,
            "db_path": tmp_path / "agent-pool.db",
            "sources": ["guba"],
            "allowed_hosts": ["guba.eastmoney.com"],
        }
    )
    return GubaClient(
        GubaSettings(
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
            max_retries=1,
        ),
        crawl,
    )


def test_guba_prefers_remote_agent_and_records_success(monkeypatch, tmp_path) -> None:
    client = _client(tmp_path)
    response = RemoteFetchResponse(
        job_id="job-1",
        agent_id="home-arm",
        status_code=200,
        final_url="https://guba.eastmoney.com/list,600519.html",
        headers={"content-type": "text/html; charset=utf-8"},
        body=b"<html>ok</html>",
        duration_ms=12,
    )
    agent_pool = FakeAgentPool(response=response)
    client.agent_pool = agent_pool
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("local transport should not run")
        ),
    )

    result = client.get("https://guba.eastmoney.com/list,600519.html")

    assert result.status_code == 200
    assert result.text == "<html>ok</html>"
    assert agent_pool.store.outcomes == [("job-1", "success", None)]
    assert "Cookie" not in agent_pool.calls[0]["headers"]
    assert agent_pool.calls[0]["source"] == "guba"
    assert agent_pool.calls[0]["method"] == "GET"
    assert agent_pool.calls[0]["capability"] == "http"


def test_guba_mobile_primary_uses_host_specific_agent(monkeypatch, tmp_path) -> None:
    crawl = CrawlSettings(
        agent_pool={
            "enabled": True,
            "db_path": tmp_path / "agent-pool.db",
            "sources": ["guba_mobile_primary"],
            "allowed_hosts": ["mguba.eastmoney.com"],
        }
    )
    client = GubaClient(
        GubaSettings(
            mobile_detail_api_enabled=True,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
            max_retries=1,
        ),
        crawl,
    )
    response = RemoteFetchResponse(
        job_id="job-mobile",
        agent_id="home-arm",
        status_code=200,
        final_url="https://mguba.eastmoney.com/api/getArticle?postid=42",
        headers={"content-type": "application/json; charset=utf-8"},
        body=b'{"rc":1,"post":{"post_id":42,"post_title":"mobile"}}',
        duration_ms=15,
    )
    agent_pool = FakeAgentPool(response=response)
    client.agent_pool = agent_pool
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("local transport should not run")
        ),
    )
    desktop_url = "https://guba.eastmoney.com/news,600519,42.html"

    result = client.get(desktop_url, expect_marker="var post_article")

    call = agent_pool.calls[0]
    assert result.url == desktop_url
    assert "var post_article=" in result.text
    assert call["source"] == "guba_mobile_primary"
    assert call["method"] == "POST"
    assert call["url"].endswith("/api/getArticle?postid=42")
    assert call["capability"] == host_http_capability("mguba.eastmoney.com")
    assert b"postid=42" in call["body"]
    assert agent_pool.store.outcomes == [("job-mobile", "success", None)]


def test_guba_falls_back_when_no_agent_is_available(monkeypatch, tmp_path) -> None:
    client = _client(tmp_path)
    client.agent_pool = FakeAgentPool(error=AgentPoolUnavailable("offline"))
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (
            200,
            "<html>local</html>",
            "https://guba.eastmoney.com/list,600519.html",
        ),
    )

    result = client.get("https://guba.eastmoney.com/list,600519.html")

    assert result.status_code == 200
    assert result.text == "<html>local</html>"


def test_guba_existing_transport_bypasses_agent(monkeypatch, tmp_path) -> None:
    client = _client(tmp_path)
    agent_pool = FakeAgentPool(
        error=AssertionError("existing route must not call the agent pool")
    )
    client.agent_pool = agent_pool
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (
            200,
            "<html>paid</html>",
            "https://guba.eastmoney.com/list,600519.html",
        ),
    )

    result = client.get(
        "https://guba.eastmoney.com/list,600519.html",
        transport="existing",
    )

    assert result.status_code == 200
    assert result.text == "<html>paid</html>"
    assert agent_pool.calls == []
