from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from alphapulse.pipeline.contracts import CrawlTask, SeedDefinition
from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.jiuyan.adapter import JiuyanAdapter
from alphapulse.sources.jiuyan.api import JiuyanHttpResult


BASE = "https://www.jiuyangongshe.com"


class FakeJiuyanClient:
    def __init__(self) -> None:
        self.search_responses: dict[tuple[str, int], JiuyanHttpResult] = {}
        self.detail_responses: dict[str, JiuyanHttpResult] = {}
        self.search_calls: list[tuple[str, int]] = []

    def search_articles(
        self, keyword: str, page: int, *, page_size: int = 15
    ) -> JiuyanHttpResult:
        self.search_calls.append((keyword, page))
        return self.search_responses[(keyword, page)]

    def article_detail(self, article_id: str) -> JiuyanHttpResult:
        return self.detail_responses[article_id]


def _ok(payload: dict) -> JiuyanHttpResult:
    return JiuyanHttpResult(
        url="https://app.jiuyangongshe.com/api",
        status_code=200,
        text=json.dumps(payload, ensure_ascii=False),
    )


def _today(hour: int = 9) -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).replace(
        hour=hour, minute=0, second=0, microsecond=0
    ).strftime("%Y-%m-%d %H:%M:%S")


def test_discover_prioritizes_fixed_targets() -> None:
    adapter = JiuyanAdapter(
        JiuyanSettings(enabled=True),
        CrawlSettings(),
        client=FakeJiuyanClient(),  # type: ignore[arg-type]
    )
    tasks = adapter.discover(
        SeedDefinition(
            name="jiuyan",
            jiuyan_target_codes=["上证指数", "机器人"],
        )
    )
    assert tasks[0].metadata["target_kind"] == "fixed"
    assert tasks[1].metadata["target_kind"] == "hot"
    assert tasks[0].priority > tasks[1].priority


def test_search_day_scopes_and_paginates_with_unique_url() -> None:
    client = FakeJiuyanClient()
    client.search_responses[("上证指数", 1)] = _ok(
        {
            "errCode": "0",
            "data": {
                "pageNo": 1,
                "pageSize": 15,
                "totalCount": 30,
                "result": [
                    {
                        "article_id": "today",
                        "title": "today",
                        "create_time": _today(),
                    },
                    {
                        "article_id": "stale",
                        "title": "stale",
                        "create_time": "2020-01-01 09:00:00",
                    },
                ],
            },
        }
    )
    adapter = JiuyanAdapter(
        JiuyanSettings(enabled=True, max_search_pages=3),
        CrawlSettings(),
        client=client,  # type: ignore[arg-type]
    )
    task = adapter.discover(
        SeedDefinition(name="jiuyan", jiuyan_target_codes=["上证指数"])
    )[0]

    outcome = adapter.fetch_item(task)

    post_tasks = [item for item in outcome.discovered_tasks if item.kind == "fetch_post"]
    assert [item.metadata["article_id"] for item in post_tasks] == ["today"]
    assert post_tasks[0].metadata["target_code"] == "上证指数"
    next_task = next(
        item for item in outcome.discovered_tasks if item.kind == "discover"
    )
    assert next_task.metadata["page"] == 2
    assert str(next_task.url).endswith("&page=2")
    assert next_task.dedupe_key != task.dedupe_key


def test_detail_sets_target_attribution() -> None:
    client = FakeJiuyanClient()
    client.detail_responses["a1"] = _ok(
        {
            "errCode": "0",
            "data": {
                "article_id": "a1",
                "user_id": "u1",
                "title": "标题",
                "content": "<p>完整正文</p>",
                "create_time": _today(),
                "user": {"user_id": "u1", "nickname": "作者"},
            },
        }
    )
    adapter = JiuyanAdapter(
        JiuyanSettings(enabled=True),
        CrawlSettings(),
        client=client,  # type: ignore[arg-type]
    )
    task = CrawlTask(
        source="jiuyan",
        kind="fetch_post",
        url=f"{BASE}/a/a1",
        seed_name="jiuyan",
        metadata={"article_id": "a1", "target_code": "机器人"},
    )
    outcome = adapter.fetch_item(task)
    assert len(outcome.posts) == 1
    assert outcome.posts[0].raw_topic_ids == ["机器人"]
    assert outcome.posts[0].content_text == "完整正文"


def test_api_error_is_reported() -> None:
    client = FakeJiuyanClient()
    client.search_responses[("上证指数", 1)] = _ok(
        {"errCode": "9", "msg": "请求异常", "data": {}}
    )
    adapter = JiuyanAdapter(
        JiuyanSettings(enabled=True),
        CrawlSettings(),
        client=client,  # type: ignore[arg-type]
    )
    task = adapter.discover(
        SeedDefinition(name="jiuyan", jiuyan_target_codes=["上证指数"])
    )[0]
    outcome = adapter.fetch_item(task)
    assert outcome.errors == ["Jiuyan API error 9: 请求异常"]
