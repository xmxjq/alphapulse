from datetime import UTC, datetime

from alphapulse.pipeline.contracts import ItemReference, SeedDefinition
from alphapulse.runtime.config import BilibiliSettings, CrawlSettings
from alphapulse.sources.bilibili.adapter import BilibiliAdapter
from alphapulse.sources.bilibili.api import BilibiliApiResult


class FakeBilibiliApi:
    def __init__(self) -> None:
        self.comment_calls = 0

    def get_video_info(self, *, bvid=None, aid=None) -> BilibiliApiResult:
        del aid
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "aid": 123456,
                    "bvid": bvid or "BV1xx411c7mu",
                    "title": "Video title",
                    "desc": "Video description",
                    "pubdate": 1_776_205_307,
                    "owner": {"mid": 42, "name": "Uploader"},
                    "stat": {"like": 99, "reply": 2, "share": 8},
                },
            },
            status_code=200,
        )

    def get_comments(self, *, aid: int, next_cursor: int = 0, page: int = 1) -> BilibiliApiResult:
        del aid, page
        self.comment_calls += 1
        if self.comment_calls == 2:
            return BilibiliApiResult(
                payload={
                    "code": 0,
                    "data": {
                        "replies": [
                            {
                                "rpid": 1001,
                                "parent": 0,
                                "like": 5,
                                "rcount": 1,
                                "ctime": 1_776_205_308,
                                "member": {"mid": "7"},
                                "content": {"message": "root comment"},
                            }
                        ],
                        "cursor": {"is_end": False, "next": 99},
                    },
                },
                status_code=200,
            )
        if next_cursor > 0:
            return BilibiliApiResult(
                payload={"code": 0, "data": {"replies": [], "cursor": {"is_end": True, "next": 0}}},
                status_code=200,
            )
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "replies": [
                        {
                            "rpid": 1001,
                            "parent": 0,
                            "like": 5,
                            "rcount": 1,
                            "ctime": 1_776_205_308,
                            "member": {"mid": "7"},
                            "content": {"message": "root comment"},
                        }
                    ],
                    "cursor": {"is_end": False, "next": 1},
                },
            },
            status_code=200,
        )

    def get_replies(self, *, aid: int, root_rpid: int, page: int = 1) -> BilibiliApiResult:
        del aid, page
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "replies": [
                        {
                            "rpid": 2001,
                            "parent": root_rpid,
                            "like": 2,
                            "ctime": 1_776_205_309,
                            "member": {"mid": "8"},
                            "content": {"message": "reply body"},
                        }
                    ],
                    "cursor": {"is_end": True},
                },
            },
            status_code=200,
        )


def test_bilibili_adapter_discovers_manual_video_targets() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    tasks = adapter.discover(SeedDefinition(name="bili", bilibili_video_targets=["BV1xx411c7mu"]))
    assert len(tasks) == 1
    assert str(tasks[0].url) == "https://www.bilibili.com/video/BV1xx411c7mu"


def test_bilibili_adapter_fetch_item_maps_video_to_post_and_author() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    adapter.api = FakeBilibiliApi()
    task = adapter.discover(SeedDefinition(name="bili", bilibili_video_targets=["BV1xx411c7mu"]))[0]

    outcome = adapter.fetch_item(task)

    assert outcome.errors == []
    assert outcome.posts[0].source_entity_id == "123456"
    assert str(outcome.posts[0].canonical_url) == "https://www.bilibili.com/video/BV1xx411c7mu"
    assert outcome.authors[0].source_entity_id == "42"


def test_bilibili_adapter_refresh_comments_stops_on_duplicate_pages_and_maps_replies() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    adapter.api = FakeBilibiliApi()

    comments = adapter.refresh_comments(
        ItemReference(
            source="bilibili",
            source_entity_id="123456",
            canonical_url="https://www.bilibili.com/video/BV1xx411c7mu",
        )
    )

    assert len(comments) == 2
    assert comments[0].source_entity_id == "1001"
    assert comments[1].parent_comment_entity_id == "1001"


def test_bilibili_adapter_comment_task_uses_api_endpoint() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    task = adapter.comment_task_for_post(
        adapter._normalize_post(
            {
                "aid": 123456,
                "bvid": "BV1xx411c7mu",
                "title": "Video title",
                "desc": "Video description",
                "pubdate": datetime.now(UTC).timestamp(),
                "owner": {"mid": 42, "name": "Uploader"},
                "stat": {"like": 1, "reply": 1, "share": 1},
            },
            datetime.now(UTC),
        ),
        "bili",
    )
    assert str(task.url).startswith("https://api.bilibili.com/x/v2/reply/main?")
