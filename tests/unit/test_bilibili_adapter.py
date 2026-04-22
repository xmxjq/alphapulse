from datetime import UTC, datetime

from alphapulse.pipeline.contracts import ItemReference, SeedDefinition
from alphapulse.runtime.config import BilibiliSettings, CrawlSettings
from alphapulse.sources.bilibili.adapter import BilibiliAdapter
from alphapulse.sources.bilibili.api import BilibiliApiResult


class FakeBilibiliApi:
    def __init__(self) -> None:
        self.comment_calls = 0
        self.user_video_calls: list[tuple[str, int]] = []
        self.user_video_pages: list[BilibiliApiResult] = []

    def get_user_videos(self, *, mid, page: int = 1, page_size: int = 30, order: str = "pubdate") -> BilibiliApiResult:
        del page_size, order
        self.user_video_calls.append((str(mid), page))
        index = page - 1
        if index < len(self.user_video_pages):
            return self.user_video_pages[index]
        return BilibiliApiResult(
            payload={"code": 0, "data": {"list": {"vlist": []}, "page": {"count": 0}}},
            status_code=200,
        )

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


def test_bilibili_adapter_discovers_space_url_as_discover_task() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    tasks = adapter.discover(
        SeedDefinition(name="bili", bilibili_space_urls=["https://space.bilibili.com/7033507"])
    )
    assert len(tasks) == 1
    assert tasks[0].kind == "discover"
    assert str(tasks[0].url) == "https://space.bilibili.com/7033507"
    assert tasks[0].metadata == {"seed_kind": "space", "mid": "7033507"}


def test_bilibili_adapter_fetch_space_paginates_and_emits_fetch_post_tasks() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    fake = FakeBilibiliApi()
    fake.user_video_pages = [
        BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "list": {
                        "vlist": [
                            {"bvid": "BV1aaa000001", "aid": 111},
                            {"bvid": "BV1aaa000002", "aid": 222},
                        ]
                    },
                    "page": {"count": 3, "pn": 1, "ps": 2},
                },
            },
            status_code=200,
        ),
        BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "list": {"vlist": [{"bvid": "BV1aaa000003", "aid": 333}]},
                    "page": {"count": 3, "pn": 2, "ps": 2},
                },
            },
            status_code=200,
        ),
    ]
    adapter.api = fake

    task = adapter.discover(
        SeedDefinition(name="bili", bilibili_space_urls=["https://space.bilibili.com/7033507"])
    )[0]

    outcome = adapter.fetch_item(task)

    assert outcome.errors == []
    assert [(call[0], call[1]) for call in fake.user_video_calls] == [("7033507", 1), ("7033507", 2)]
    bvids = [t.metadata["bvid"] for t in outcome.discovered_tasks]
    assert bvids == ["BV1aaa000001", "BV1aaa000002", "BV1aaa000003"]
    assert all(t.kind == "fetch_post" and t.source == "bilibili" for t in outcome.discovered_tasks)
    assert all(t.metadata["owner_mid"] == "7033507" for t in outcome.discovered_tasks)
    assert str(outcome.discovered_tasks[0].url) == "https://www.bilibili.com/video/BV1aaa000001"


def test_bilibili_adapter_fetch_space_stops_on_empty_page() -> None:
    adapter = BilibiliAdapter(BilibiliSettings(), CrawlSettings())
    fake = FakeBilibiliApi()
    fake.user_video_pages = [
        BilibiliApiResult(
            payload={"code": 0, "data": {"list": {"vlist": []}, "page": {"count": 0}}},
            status_code=200,
        ),
    ]
    adapter.api = fake

    task = adapter.discover(SeedDefinition(name="bili", bilibili_space_urls=["7033507"]))[0]
    outcome = adapter.fetch_item(task)

    assert outcome.errors == []
    assert outcome.discovered_tasks == []
    assert fake.user_video_calls == [("7033507", 1)]


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
