from pathlib import Path

from alphapulse.runtime.config import Settings, load_settings
from alphapulse.runtime.service import AlphaPulseService
from alphapulse.sources.bilibili.api import BilibiliApiResult
from alphapulse.sources.fetching import FetchResult


class FakeStore:
    def __init__(self) -> None:
        self.posts = []
        self.comments = []
        self.authors = []
        self.errors = []

    def init_db(self) -> None:
        return None

    def healthcheck(self) -> bool:
        return True

    def upsert_posts(self, posts):
        self.posts.extend(posts)

    def upsert_comments(self, comments):
        self.comments.extend(comments)

    def upsert_authors(self, authors):
        self.authors.extend(authors)

    def insert_crawl_error(self, *, source: str, url: str, error_message: str) -> None:
        self.errors.append((source, url, error_message))

    def insert_crawl_run(self, *, run_id: str, started_at, finished_at, stats, status: str) -> None:
        return None


class FakeClient:
    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures

    def fetch(self, url: str) -> FetchResult:
        if "comments.json" in url:
            return FetchResult(url=url, status_code=200, text=(self.fixtures / "comments.json").read_text(), headers={})
        if url.endswith("/987654321"):
            return FetchResult(url=url, status_code=200, text=(self.fixtures / "post.html").read_text(), headers={})
        return FetchResult(url=url, status_code=200, text=(self.fixtures / "discovery.html").read_text(), headers={})


class FailingPostClient:
    def fetch(self, url: str) -> FetchResult:
        return FetchResult(url=url, status_code=0, text="", headers={}, error_message="dial tcp failed")


class BlockedClient:
    def fetch(self, url: str) -> FetchResult:
        return FetchResult(url=url, status_code=403, text="captcha", headers={}, proxy_url="http://1.2.3.4:8080")


class FailingCommentsClient:
    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures

    def fetch(self, url: str) -> FetchResult:
        if "comments.json" in url:
            return FetchResult(url=url, status_code=0, text="", headers={}, error_message="proxy connect failed")
        return FetchResult(url=url, status_code=200, text=(self.fixtures / "post.html").read_text(), headers={})


class FakeBilibiliApi:
    def get_video_info(self, *, bvid=None, aid=None) -> BilibiliApiResult:
        del bvid, aid
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "aid": 123456,
                    "bvid": "BV1xx411c7mu",
                    "title": "Test video",
                    "desc": "Video description",
                    "pubdate": 1_776_205_307,
                    "owner": {"mid": 42, "name": "Uploader"},
                    "stat": {"like": 88, "reply": 2, "share": 5},
                },
            },
            status_code=200,
        )

    def get_comments(self, *, aid: int, next_cursor: int = 0, page: int = 1) -> BilibiliApiResult:
        del aid, page
        if next_cursor > 0:
            return BilibiliApiResult(payload={"code": 0, "data": {"replies": [], "cursor": {"is_end": True}}}, status_code=200)
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "replies": [
                        {
                            "rpid": 1001,
                            "parent": 0,
                            "like": 3,
                            "rcount": 1,
                            "ctime": 1_776_205_308,
                            "member": {"mid": "7"},
                            "content": {"message": "root comment"},
                        }
                    ],
                    "cursor": {"is_end": True, "next": 0},
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
                            "like": 1,
                            "ctime": 1_776_205_309,
                            "member": {"mid": "8"},
                            "content": {"message": "child reply"},
                        }
                    ],
                    "cursor": {"is_end": True},
                },
            },
            status_code=200,
        )


def _xueqiu_adapter(service: AlphaPulseService):
    return service.sources["xueqiu"]


def test_service_runs_one_cycle(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )
    service = AlphaPulseService(settings, store=FakeStore())
    _xueqiu_adapter(service).client = FakeClient(fixtures)

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.posts_written >= 1
    assert stats.comments_written >= 2
    assert service.store.posts[0].source_entity_id == "987654321"
    assert service.state.list_compiled_seed_set_names() == ["cn-core"]


def test_service_reuses_fresh_compiled_seed_snapshot(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.post_recrawl_minutes = 0
    settings.crawl.comment_refresh_minutes = 0
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    service = AlphaPulseService(settings, store=FakeStore())
    _xueqiu_adapter(service).client = FakeClient(fixtures)

    first = service.run_cycle(seed_set_name="cn-core")
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1111111111/222222222"]
""".strip()
    )
    second = service.run_cycle(seed_set_name="cn-core")

    with service.state.connection() as conn:
        run_count = conn.execute("SELECT COUNT(*) AS count FROM generated_seed_runs").fetchone()["count"]

    assert first.posts_written >= 1
    assert second.posts_written >= 1
    assert run_count == 1


def test_service_handles_fetch_transport_failure_without_crashing(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    _xueqiu_adapter(service).client = FailingPostClient()

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.errors == 1
    assert stats.posts_written == 0
    assert store.errors[0][2].startswith("Fetch failed for https://xueqiu.com/1234567890/987654321")


def test_service_counts_blocked_responses(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    _xueqiu_adapter(service).client = BlockedClient()

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.blocked_responses == 1
    assert stats.errors == 1
    assert store.errors[0][2] == "Blocked response from https://xueqiu.com/1234567890/987654321"


def test_service_stops_comment_refresh_on_fetch_failure(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.comment_refresh_minutes = 0
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    _xueqiu_adapter(service).client = FailingCommentsClient(fixtures)

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.posts_written == 1
    assert stats.comments_written == 0


def test_service_runs_bilibili_cycle(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.comment_refresh_minutes = 0
    settings.sources.xueqiu.enabled = False
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "bili-core"
generators = ["manual-video"]

[[generators]]
name = "manual-video"
type = "manual"
bilibili_video_targets = ["BV1xx411c7mu"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    service.sources["bilibili"].api = FakeBilibiliApi()

    stats = service.run_cycle(seed_set_name="bili-core")

    assert stats.posts_written == 1
    assert stats.comments_written == 2
    assert store.posts[0].source == "bilibili"
    assert store.comments[1].parent_comment_entity_id == "1001"
