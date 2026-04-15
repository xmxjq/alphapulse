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


def test_service_runs_one_cycle(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_sets[0].post_urls = ["https://xueqiu.com/1234567890/987654321"]
    service = AlphaPulseService(settings, store=FakeStore())
    service.xueqiu.client = FakeClient(fixtures)

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.posts_written >= 1
    assert stats.comments_written >= 2
    assert service.store.posts[0].source_entity_id == "987654321"
