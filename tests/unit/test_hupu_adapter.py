from datetime import UTC, date, datetime
from pathlib import Path

from alphapulse.pipeline.contracts import CrawlTask, SeedDefinition
from alphapulse.runtime.config import CrawlSettings, HupuSettings
from alphapulse.sources.hupu.adapter import HupuAdapter
from alphapulse.sources.hupu.api import HupuHttpResult


FIXTURES = Path(__file__).parent.parent / "fixtures" / "hupu"


class FakeHupuClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def get(self, url: str, *, expect_marker=None, transport="auto") -> HupuHttpResult:
        del expect_marker
        self.calls.append((url, transport))
        name = "list_postdate.html" if "postdate" in url else "post_detail.html"
        return HupuHttpResult(
            url=url,
            status_code=200,
            text=(FIXTURES / name).read_text(encoding="utf-8"),
        )


def _settings(**overrides) -> HupuSettings:
    return HupuSettings(
        enabled=True,
        authorization_expires_on=date(2029, 7, 31),
        request_interval_min_seconds=0,
        request_interval_max_seconds=0,
        **overrides,
    )


def test_hupu_discovery_filters_today_and_paginates() -> None:
    client = FakeHupuClient()
    adapter = HupuAdapter(_settings(), CrawlSettings(), client=client)
    adapter._day_start = lambda: datetime(2026, 7, 31, tzinfo=UTC)  # type: ignore[method-assign]
    tasks = adapter.discover(
        SeedDefinition(name="hupu-daily", hupu_board_codes=["stock"])
    )
    assert len(tasks) == 1
    outcome = adapter.fetch_item(tasks[0])
    post_tasks = [task for task in outcome.discovered_tasks if task.kind == "fetch_post"]
    list_tasks = [task for task in outcome.discovered_tasks if task.kind == "discover"]
    assert [task.metadata["post_id"] for task in post_tasks] == [
        "641439575",
        "641433410",
    ]
    assert str(list_tasks[0].url) == "https://bbs.hupu.com/stock-postdate-2"
    assert post_tasks[0].metadata["pubdate_ts"] > post_tasks[1].metadata["pubdate_ts"]


def test_hupu_detail_uses_transport_and_assigns_multiple_boards() -> None:
    client = FakeHupuClient()
    adapter = HupuAdapter(_settings(), CrawlSettings(), client=client)
    task = CrawlTask(
        source="hupu",
        kind="fetch_post",
        url="https://bbs.hupu.com/641433410.html",
        seed_name="hupu-daily",
        metadata={"post_id": "641433410"},
    )
    outcome = adapter.fetch_item_with_transport(task, "existing")
    assert client.calls == [("https://bbs.hupu.com/641433410.html", "existing")]
    assert outcome.posts[0].raw_topic_ids == ["stock", "上证指数", "科创50"]
    assert adapter.comment_task_for_post(outcome.posts[0], "hupu-daily") is None


def test_hupu_authorization_expiry_fails_closed() -> None:
    settings = HupuSettings(enabled=True, authorization_expires_on=date(2020, 1, 1))
    adapter = HupuAdapter(settings, CrawlSettings(), client=FakeHupuClient())
    assert settings.authorization_active(date(2019, 12, 31))
    assert not settings.authorization_active(date(2020, 1, 2))
    assert adapter.is_circuit_open()
    assert adapter.discover(
        SeedDefinition(name="hupu-daily", hupu_board_codes=["stock"])
    ) == []
