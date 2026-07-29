from __future__ import annotations

from alphapulse.runtime.agent_pool import AgentPoolUnavailable, RemoteFetchResponse
from alphapulse.runtime.config import CrawlSettings, TgbSettings
from alphapulse.sources.tgb.api import TgbClient


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


def _client(tmp_path) -> TgbClient:
    crawl = CrawlSettings(
        agent_pool={
            "enabled": True,
            "db_path": tmp_path / "agent-pool.db",
            "sources": ["tgb"],
            "allowed_hosts": ["www.tgb.cn"],
        }
    )
    return TgbClient(
        TgbSettings(
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
            max_retries=1,
            cookies={"session": "private"},
        ),
        crawl,
    )


def test_tgb_prefers_remote_agent_and_records_success(monkeypatch, tmp_path) -> None:
    client = _client(tmp_path)
    response = RemoteFetchResponse(
        job_id="job-1",
        agent_id="home-arm",
        status_code=200,
        final_url="https://www.tgb.cn/a/post-1",
        headers={"content-type": "text/html; charset=utf-8"},
        body=b"<html><div class='article-content'>ok</div></html>",
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

    result = client.get(
        "https://www.tgb.cn/a/post-1",
        expect_marker="article-content",
    )

    assert result.status_code == 200
    assert agent_pool.store.outcomes == [("job-1", "success", None)]
    assert "Cookie" not in agent_pool.calls[0]["headers"]


def test_tgb_falls_back_when_no_agent_is_available(monkeypatch, tmp_path) -> None:
    client = _client(tmp_path)
    client.agent_pool = FakeAgentPool(error=AgentPoolUnavailable("offline"))
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (
            200,
            "<html><div class='article-content'>local</div></html>",
            "https://www.tgb.cn/a/post-1",
        ),
    )

    result = client.get(
        "https://www.tgb.cn/a/post-1",
        expect_marker="article-content",
    )

    assert result.status_code == 200
    assert "local" in result.text


def test_tgb_records_blocked_agent_outcome(monkeypatch, tmp_path) -> None:
    client = _client(tmp_path)
    response = RemoteFetchResponse(
        job_id="job-1",
        agent_id="home-arm",
        status_code=200,
        final_url="https://www.tgb.cn/a/post-1",
        headers={"content-type": "text/html; charset=utf-8"},
        body=b"<html>blocked</html>",
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

    result = client.get(
        "https://www.tgb.cn/a/post-1",
        expect_marker="article-content",
    )

    assert result.blocked is True
    assert result.block_kind == "soft_block"
    assert agent_pool.store.outcomes == [
        ("job-1", "blocked", "blocked: soft_block")
    ]
