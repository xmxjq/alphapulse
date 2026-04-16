from pathlib import Path

from alphapulse.runtime.config import Settings, load_settings
from alphapulse.runtime.service import AlphaPulseService
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
    service.xueqiu.client = FakeClient(fixtures)

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
    service.xueqiu.client = FakeClient(fixtures)

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
    service.xueqiu.client = FailingPostClient()

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
    service.xueqiu.client = BlockedClient()

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
    service.xueqiu.client = FailingCommentsClient(fixtures)

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.posts_written == 1
    assert stats.comments_written == 0
