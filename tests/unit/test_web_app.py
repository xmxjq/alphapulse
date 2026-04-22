from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from alphapulse.runtime.config import load_settings
from alphapulse.runtime.state import StateStore
from alphapulse.web.app import create_app
from alphapulse.web.models import (
    Comment,
    CrawlError,
    CrawlRun,
    PostDetail,
    PostDetailResponse,
    PostSummary,
    SeedSetSummary,
    StatusResponse,
)
from alphapulse.web.queries import WebQueries


class FakeReader:
    def __init__(self) -> None:
        self.runs: list[CrawlRun] = []
        self.errors: list[CrawlError] = []
        self.posts_by_source: dict[str | None, list[PostSummary]] = {}
        self.post_details: dict[tuple[str, str], PostDetail] = {}
        self.comments_by_post: dict[tuple[str, str], list[Comment]] = {}

    def latest_run(self) -> CrawlRun | None:
        return self.runs[0] if self.runs else None

    def list_runs(self, limit: int) -> list[CrawlRun]:
        return self.runs[:limit]

    def list_errors(self, limit: int, source: str | None) -> list[CrawlError]:
        del source
        return self.errors[:limit]

    def list_posts(self, source: str | None, limit: int, offset: int) -> list[PostSummary]:
        return self.posts_by_source.get(source, [])[offset : offset + limit]

    def get_post(self, source: str, source_entity_id: str) -> PostDetail | None:
        return self.post_details.get((source, source_entity_id))

    def list_comments_for_post(self, source: str, post_entity_id: str) -> list[Comment]:
        return self.comments_by_post.get((source, post_entity_id), [])


def _build_client(tmp_path: Path, reader: FakeReader) -> TestClient:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    state = StateStore(settings.crawl.state_path)
    queries = WebQueries(reader=reader, state=state)
    app = create_app(settings, queries=queries)
    return TestClient(app)


def _run(run_id: str, posts_written: int = 0) -> CrawlRun:
    return CrawlRun(
        run_id=run_id,
        started_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 4, 22, 12, 1, tzinfo=UTC),
        status="succeeded",
        seeds_processed=1,
        tasks_enqueued=2,
        pages_fetched=3,
        posts_written=posts_written,
        comments_written=0,
        authors_written=0,
        blocked_responses=0,
        errors=0,
        skipped_tasks=0,
    )


def _post_summary(entity_id: str) -> PostSummary:
    return PostSummary(
        source="bilibili",
        source_entity_id=entity_id,
        canonical_url=f"https://www.bilibili.com/video/BV{entity_id}",
        author_entity_id="42",
        title=f"Video {entity_id}",
        content_preview="preview",
        published_at=datetime(2026, 4, 22, tzinfo=UTC),
        fetched_at=datetime(2026, 4, 22, 1, tzinfo=UTC),
        like_count=1,
        comment_count=0,
    )


def _post_detail(entity_id: str) -> PostDetail:
    return PostDetail(
        source="bilibili",
        source_entity_id=entity_id,
        canonical_url=f"https://www.bilibili.com/video/BV{entity_id}",
        author_entity_id="42",
        title=f"Video {entity_id}",
        content_text="full body",
        language=None,
        published_at=datetime(2026, 4, 22, tzinfo=UTC),
        fetched_at=datetime(2026, 4, 22, 1, tzinfo=UTC),
        like_count=1,
        comment_count=2,
        repost_count=0,
        raw_topic_ids=[],
    )


def test_status_returns_empty_when_no_runs(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    payload = client.get("/api/status").json()
    parsed = StatusResponse.model_validate(payload)
    assert parsed.latest_run is None
    assert parsed.recent_runs == []
    assert parsed.in_flight_urls == 0
    assert parsed.seed_sets == []


def test_status_surfaces_latest_run_and_seed_sets(tmp_path: Path) -> None:
    reader = FakeReader()
    reader.runs = [_run("r1", posts_written=5)]
    client = _build_client(tmp_path, reader)
    payload = client.get("/api/status").json()
    assert payload["latest_run"]["run_id"] == "r1"
    assert payload["latest_run"]["posts_written"] == 5


def test_posts_filters_by_source_and_respects_limit(tmp_path: Path) -> None:
    reader = FakeReader()
    reader.posts_by_source[None] = [_post_summary("1"), _post_summary("2")]
    reader.posts_by_source["bilibili"] = [_post_summary("1")]
    client = _build_client(tmp_path, reader)

    response = client.get("/api/posts", params={"source": "bilibili", "limit": 10})
    assert response.status_code == 200
    payload = response.json()
    assert [p["source_entity_id"] for p in payload["posts"]] == ["1"]
    assert payload["limit"] == 10
    assert payload["offset"] == 0


def test_posts_rejects_invalid_limit(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    response = client.get("/api/posts", params={"limit": 0})
    assert response.status_code == 422
    response = client.get("/api/posts", params={"limit": 500})
    assert response.status_code == 422


def test_posts_rejects_unknown_source(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    response = client.get("/api/posts", params={"source": "twitter"})
    assert response.status_code == 400


def test_post_detail_returns_post_with_comments(tmp_path: Path) -> None:
    reader = FakeReader()
    reader.post_details[("bilibili", "123")] = _post_detail("123")
    reader.comments_by_post[("bilibili", "123")] = [
        Comment(
            source="bilibili",
            source_entity_id="c1",
            post_entity_id="123",
            parent_comment_entity_id=None,
            author_entity_id="7",
            content_text="root comment",
            published_at=datetime(2026, 4, 22, tzinfo=UTC),
            fetched_at=datetime(2026, 4, 22, 1, tzinfo=UTC),
            like_count=5,
        )
    ]
    client = _build_client(tmp_path, reader)

    response = client.get("/api/posts/bilibili/123")
    assert response.status_code == 200
    parsed = PostDetailResponse.model_validate(response.json())
    assert parsed.post.source_entity_id == "123"
    assert parsed.comments[0].source_entity_id == "c1"


def test_post_detail_returns_404_for_missing_post(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    response = client.get("/api/posts/bilibili/999")
    assert response.status_code == 404


def test_post_detail_rejects_unknown_source(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    response = client.get("/api/posts/twitter/123")
    assert response.status_code == 400


def test_post_detail_rejects_invalid_entity_id(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    response = client.get("/api/posts/bilibili/abc%20xyz")
    assert response.status_code == 400


def test_index_serves_html(tmp_path: Path) -> None:
    client = _build_client(tmp_path, FakeReader())
    response = client.get("/")
    assert response.status_code == 200
    assert "AlphaPulse" in response.text


def test_seeds_endpoint_returns_compiled_sets(tmp_path: Path) -> None:
    reader = FakeReader()
    client = _build_client(tmp_path, reader)
    # Insert a compiled seed set directly via the injected queries.
    from alphapulse.pipeline.contracts import SeedDefinition
    client.app  # ensure app initialized
    # Reach in through the closure by re-creating a StateStore pointing at the same DB.
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    state = StateStore(settings.crawl.state_path)
    state.store_compiled_seed_set(
        SeedDefinition(name="cn-core", stock_ids=["SH600519"]),
        refreshed_at=datetime(2026, 4, 22, tzinfo=UTC),
    )
    response = client.get("/api/seeds")
    assert response.status_code == 200
    payload = response.json()
    names = [s["name"] for s in payload["seed_sets"]]
    assert "cn-core" in names
    summary = next(s for s in payload["seed_sets"] if s["name"] == "cn-core")
    SeedSetSummary.model_validate(summary)
    assert summary["stock_count"] == 1
