from datetime import UTC, datetime, timedelta
from pathlib import Path

from alphapulse.pipeline.contracts import SeedDefinition
from alphapulse.runtime.state import StateStore
from alphapulse.web.models import Comment, CrawlError, CrawlRun, PostDetail, PostSummary
from alphapulse.web.queries import (
    ClickHouseReader,
    RqliteReader,
    WebQueries,
    _content_preview,
    _coerce_datetime,
    _coerce_topic_ids,
)


class FakeClickHouseResult:
    def __init__(self, columns: list[str], rows: list[list[object]]) -> None:
        self.column_names = columns
        self.result_rows = rows


class FakeClickHouseClient:
    def __init__(self, response: FakeClickHouseResult) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    def query(self, sql: str, parameters: dict[str, object] | None = None) -> FakeClickHouseResult:
        self.calls.append((sql, dict(parameters or {})))
        return self.response


class FakeRqliteClient:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.calls: list[list[object]] = []

    def query_params(self, statements: list[list[object]]) -> dict[str, object]:
        self.calls.append(statements[0])
        return self.response


def test_content_preview_truncates_and_collapses_whitespace() -> None:
    assert _content_preview(None) == ""
    assert _content_preview("hello\n   world") == "hello world"
    long = "x" * 400
    preview = _content_preview(long)
    assert preview.endswith("…")
    assert len(preview) <= 281


def test_coerce_datetime_accepts_iso_strings_and_datetimes() -> None:
    parsed = _coerce_datetime("2026-04-22T12:00:00Z")
    assert parsed == datetime(2026, 4, 22, 12, 0, tzinfo=UTC)
    naive = datetime(2026, 4, 22, 12, 0)
    assert _coerce_datetime(naive).tzinfo is UTC


def test_coerce_topic_ids_handles_array_and_json_string() -> None:
    assert _coerce_topic_ids(["a", "b"]) == ["a", "b"]
    assert _coerce_topic_ids('["a","b"]') == ["a", "b"]
    assert _coerce_topic_ids(None) == []
    assert _coerce_topic_ids("") == []
    assert _coerce_topic_ids("not json") == []


def test_clickhouse_reader_list_posts_uses_parameterized_query() -> None:
    client = FakeClickHouseClient(
        FakeClickHouseResult(
            columns=[
                "source", "source_entity_id", "canonical_url", "author_entity_id",
                "title", "content_text", "published_at", "fetched_at",
                "like_count", "comment_count",
            ],
            rows=[[
                "bilibili", "123", "https://www.bilibili.com/video/BV1",
                "42", "Hello", "Body text", datetime(2026, 4, 22, tzinfo=UTC),
                datetime(2026, 4, 22, 1, tzinfo=UTC), 10, 2,
            ]],
        )
    )
    reader = ClickHouseReader(client=client, database="alphapulse")

    results = reader.list_posts(source="bilibili", limit=50, offset=0)

    assert len(results) == 1
    assert isinstance(results[0], PostSummary)
    assert results[0].source_entity_id == "123"
    assert results[0].canonical_url == "https://www.bilibili.com/video/BV1"
    assert client.calls[0][1] == {"limit": 50, "offset": 0, "source": "bilibili"}
    assert "{source:String}" in client.calls[0][0]


def test_clickhouse_reader_get_post_returns_none_when_empty() -> None:
    client = FakeClickHouseClient(FakeClickHouseResult(columns=["source"], rows=[]))
    reader = ClickHouseReader(client=client, database="alphapulse")
    assert reader.get_post("bilibili", "999") is None


def test_clickhouse_reader_parses_crawl_run_row() -> None:
    client = FakeClickHouseClient(
        FakeClickHouseResult(
            columns=[
                "run_id", "started_at", "finished_at", "status",
                "seeds_processed", "tasks_enqueued", "pages_fetched",
                "posts_written", "comments_written", "authors_written",
                "blocked_responses", "errors", "skipped_tasks",
            ],
            rows=[[
                "abc", datetime(2026, 4, 22, tzinfo=UTC),
                datetime(2026, 4, 22, 0, 1, tzinfo=UTC), "succeeded",
                1, 2, 3, 4, 5, 6, 0, 0, 0,
            ]],
        )
    )
    reader = ClickHouseReader(client=client, database="alphapulse")
    run = reader.latest_run()
    assert isinstance(run, CrawlRun)
    assert run.status == "succeeded"
    assert run.posts_written == 4


def test_rqlite_reader_list_errors_binds_params() -> None:
    client = FakeRqliteClient(
        response={
            "results": [{
                "columns": ["created_at", "source", "url", "error_message"],
                "values": [["2026-04-22T00:00:00Z", "bilibili", "https://x", "boom"]],
            }]
        }
    )
    reader = RqliteReader(client=client)
    errors = reader.list_errors(limit=10, source="bilibili")
    assert len(errors) == 1
    assert isinstance(errors[0], CrawlError)
    assert client.calls[0][1:] == ["bilibili", 10]


def test_rqlite_reader_list_comments_binds_params() -> None:
    client = FakeRqliteClient(
        response={
            "results": [{
                "columns": [
                    "source", "source_entity_id", "post_entity_id",
                    "parent_comment_entity_id", "author_entity_id",
                    "content_text", "published_at", "fetched_at", "like_count",
                ],
                "values": [[
                    "bilibili", "2", "1", None, "u1", "hi",
                    "2026-04-22T00:00:00Z", "2026-04-22T00:01:00Z", 3,
                ]],
            }]
        }
    )
    reader = RqliteReader(client=client)
    comments = reader.list_comments_for_post("bilibili", "1")
    assert len(comments) == 1
    assert isinstance(comments[0], Comment)
    assert client.calls[0][1:] == ["bilibili", "1"]


def test_rqlite_reader_parses_post_with_topic_ids_json() -> None:
    client = FakeRqliteClient(
        response={
            "results": [{
                "columns": [
                    "source", "source_entity_id", "canonical_url", "author_entity_id",
                    "title", "content_text", "language", "published_at", "fetched_at",
                    "like_count", "comment_count", "repost_count", "raw_topic_ids_json",
                ],
                "values": [[
                    "bilibili", "1", "https://x", "42", "t", "body", None,
                    "2026-04-22T00:00:00Z", "2026-04-22T00:01:00Z", 1, 0, 0,
                    '["topicA","topicB"]',
                ]],
            }]
        }
    )
    reader = RqliteReader(client=client)
    post = reader.get_post("bilibili", "1")
    assert isinstance(post, PostDetail)
    assert post.raw_topic_ids == ["topicA", "topicB"]


class StubReader:
    def __init__(self) -> None:
        self.latest = None
        self.runs: list[CrawlRun] = []
        self.errors: list[CrawlError] = []

    def latest_run(self) -> CrawlRun | None:
        return self.latest

    def list_runs(self, limit: int) -> list[CrawlRun]:
        return self.runs[:limit]

    def list_errors(self, limit: int, source: str | None) -> list[CrawlError]:
        del source
        return self.errors[:limit]

    def list_posts(self, source, limit, offset):  # noqa: ANN001
        return []

    def get_post(self, source, source_entity_id):  # noqa: ANN001
        return None

    def list_comments_for_post(self, source, post_entity_id):  # noqa: ANN001
        return []


def test_web_queries_status_counts_recent_url_activity(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.db")
    now = datetime(2026, 4, 22, 12, 0, tzinfo=UTC)
    with state.connection() as conn:
        conn.executemany(
            """
            INSERT INTO url_state (url, source, kind, seed_name, first_seen_at, last_seen_at, last_fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("https://a", "bilibili", "fetch_post", "s", now.isoformat(), now.isoformat(),
                 (now - timedelta(minutes=5)).isoformat()),
                ("https://b", "bilibili", "fetch_post", "s", now.isoformat(), now.isoformat(),
                 (now - timedelta(hours=3)).isoformat()),
            ],
        )
    state.store_compiled_seed_set(
        SeedDefinition(name="cn-core", stock_ids=["SH600519"], bilibili_video_targets=["BV1"]),
        refreshed_at=now,
    )

    queries = WebQueries(reader=StubReader(), state=state)

    assert queries.recent_url_activity(now) == 1
    summaries = queries.seed_set_summaries()
    assert summaries[0].name == "cn-core"
    assert summaries[0].stock_count == 1
    assert summaries[0].bilibili_video_count == 1
    # status() delegates to the reader/state; verify composition without re-asserting
    # the wall-clock sensitive recent-activity count.
    status = queries.status()
    assert status.seed_sets[0].name == "cn-core"
    assert status.latest_run is None
