from datetime import UTC, datetime, timedelta
from pathlib import Path

from alphapulse.pipeline.contracts import SeedDefinition
from alphapulse.runtime.state import StateStore
from alphapulse.web.models import (
    Comment,
    CrawlError,
    CrawlRun,
    GubaBoardSummary,
    PostDetail,
    PostSummary,
)
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
        self.guba_boards: list[GubaBoardSummary] = []

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

    def list_guba_boards(self, limit: int) -> list[GubaBoardSummary]:
        return self.guba_boards[:limit]

    def list_source_posts_in_range(self, source, start, end, limit):  # noqa: ANN001
        self.range_call = (source, start, end, limit)
        return getattr(self, "range_posts", [])


def test_clickhouse_reader_lists_guba_boards() -> None:
    client = FakeClickHouseClient(
        FakeClickHouseResult(
            columns=[
                "board_code", "post_count", "comment_count",
                "latest_published_at", "latest_fetched_at",
            ],
            rows=[["600900", 42, 310, datetime(2026, 7, 18, tzinfo=UTC),
                   datetime(2026, 7, 18, 1, tzinfo=UTC)]],
        )
    )
    reader = ClickHouseReader(client=client, database="alphapulse")

    boards = reader.list_guba_boards(limit=50)

    assert len(boards) == 1
    assert isinstance(boards[0], GubaBoardSummary)
    assert boards[0].board_code == "600900"
    assert boards[0].post_count == 42
    assert boards[0].comment_count == 310
    assert client.calls[0][1] == {"limit": 50}
    assert "GROUP BY board_code" in client.calls[0][0]


def test_rqlite_reader_lists_guba_boards() -> None:
    client = FakeRqliteClient(
        response={
            "results": [{
                "columns": [
                    "board_code", "post_count", "comment_count",
                    "latest_published_at", "latest_fetched_at",
                ],
                "values": [["zssh000001", 7, 15, "2026-07-18T00:00:00Z", "2026-07-18T01:00:00Z"]],
            }]
        }
    )
    reader = RqliteReader(client=client)

    boards = reader.list_guba_boards(limit=10)

    assert len(boards) == 1
    assert boards[0].board_code == "zssh000001"
    assert boards[0].post_count == 7
    assert client.calls[0][1:] == [10]
    assert "json_extract" in client.calls[0][0]


def _post_summary(source: str, entity_id: str, board_code: str, comments: int) -> PostSummary:
    return PostSummary(
        source=source,
        source_entity_id=entity_id,
        canonical_url=f"https://www.tgb.cn/a/{entity_id}",
        author_entity_id="42",
        title=f"post {entity_id}",
        content_preview="preview",
        published_at=datetime(2026, 7, 22, 6, 0, tzinfo=UTC),
        fetched_at=datetime(2026, 7, 22, 7, 0, tzinfo=UTC),
        like_count=0,
        comment_count=comments,
        board_code=board_code,
    )


def test_tgb_daily_report_splits_featured_and_general(tmp_path: Path) -> None:
    day = "2026-07-22"
    state = StateStore(tmp_path / "state.db")
    state.replace_tgb_ranking(
        day,
        [
            {"section": "featured", "rank": 1, "code": "jinghua", "name": "精华",
             "url": "https://www.tgb.cn/jinghua/1-1", "members": None},
            {"section": "general", "rank": 1, "code": "zongban", "name": "社区总版",
             "url": "https://www.tgb.cn/zongban/1/1", "members": None},
            {"section": "general", "rank": 2, "code": "sz000938", "name": "紫光股份",
             "url": "https://www.tgb.cn/quotes/sz000938", "members": None},
        ],
    )
    reader = StubReader()
    reader.range_posts = [
        _post_summary("tgb", "p1", "jinghua", 5),
        _post_summary("tgb", "p2", "zongban", 2),
        _post_summary("tgb", "p3", "sz000938", 1),
        _post_summary("tgb", "p4", "sz000938", 4),
    ]
    queries = WebQueries(reader=reader, state=state)

    report = queries.tgb_daily_report(day)

    assert reader.range_call[0] == "tgb"  # queried the tgb source
    assert report.has_snapshot is True
    assert report.total_posts == 4
    assert report.total_comments == 12
    assert [s.key for s in report.sections] == ["featured", "general"]
    featured, general = report.sections
    assert featured.title == "精华"
    assert [b.code for b in featured.entries] == ["jinghua"]
    assert featured.entries[0].post_count == 1
    # General keeps ranked order: general feed first, then the hot stock board.
    assert [b.code for b in general.entries] == ["zongban", "sz000938"]
    stock_board = general.entries[1]
    assert stock_board.post_count == 2
    assert stock_board.comment_count == 5


def test_tgb_daily_report_falls_back_without_snapshot(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.db")
    reader = StubReader()
    reader.range_posts = [_post_summary("tgb", "p1", "sz1", 3)]
    queries = WebQueries(reader=reader, state=state)

    report = queries.tgb_daily_report("2026-07-22")
    assert report.has_snapshot is False
    assert [s.key for s in report.sections] == ["all"]
    assert report.sections[0].entries[0].code == "sz1"


def test_jiuyan_daily_report_groups_posts_into_multiple_fixed_targets(
    tmp_path: Path,
) -> None:
    day = "2026-07-22"
    state = StateStore(tmp_path / "state.db")
    state.replace_jiuyan_ranking(
        day,
        [
            {
                "section": "fixed",
                "rank": 1,
                "code": "上证指数",
                "name": "上证指数",
                "url": "https://www.jiuyangongshe.com/search/new?k=index",
                "members": None,
            },
            {
                "section": "fixed",
                "rank": 2,
                "code": "科创50",
                "name": "科创50",
                "url": "https://www.jiuyangongshe.com/search/new?k=star50",
                "members": None,
            },
        ],
    )
    reader = StubReader()
    post = _post_summary("jiuyan", "p1", "上证指数", 0)
    reader.range_posts = [
        post.model_copy(
            update={"board_codes": ["上证指数", "科创50", "公社广场"]}
        )
    ]
    queries = WebQueries(reader=reader, state=state)

    report = queries.jiuyan_daily_report(day)

    fixed = report.sections[0]
    assert [entry.post_count for entry in fixed.entries] == [1, 1]
    assert report.sections[1].key == "other"
    assert report.sections[1].entries[0].code == "公社广场"
    assert report.total_posts == 1


def test_daily_report_keeps_posts_from_unranked_boards(tmp_path: Path) -> None:
    day = "2026-07-22"
    state = StateStore(tmp_path / "state.db")
    state.replace_guba_ranking(
        day,
        [
            {
                "section": "hot_stock",
                "rank": 1,
                "code": "600519",
                "name": "Kweichow Moutai",
                "url": "https://guba.eastmoney.com/list,600519.html",
                "members": None,
            }
        ],
    )
    reader = StubReader()
    reader.range_posts = [
        _post_summary("guba", "p1", "600519", 1),
        _post_summary("guba", "p2", "000001", 2),
    ]
    queries = WebQueries(reader=reader, state=state)

    report = queries.guba_daily_report(day)

    assert report.total_posts == 2
    assert [section.key for section in report.sections] == ["hot_stock", "other"]
    assert report.sections[1].entries[0].code == "000001"
    assert report.sections[1].entries[0].post_count == 1


def test_guba_daily_report_matches_lowercase_concept_posts_to_ranking(
    tmp_path: Path,
) -> None:
    day = "2026-07-24"
    state = StateStore(tmp_path / "state.db")
    state.replace_guba_ranking(
        day,
        [
            {
                "section": "hot_concept",
                "rank": 1,
                "code": "BK1152",
                "name": "Concept",
                "url": "https://guba.eastmoney.com/list,BK1152.html",
                "members": None,
            }
        ],
    )
    reader = StubReader()
    reader.range_posts = [_post_summary("guba", "p1", "bk1152", 1)]
    queries = WebQueries(reader=reader, state=state)

    report = queries.guba_daily_report(day)

    concept = next(section for section in report.sections if section.key == "hot_concept")
    assert concept.entries[0].code == "BK1152"
    assert concept.entries[0].post_count == 1
    assert all(section.key != "other" for section in report.sections)


def test_web_queries_annotates_guba_boards_with_seed_sets(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.db")
    state.store_compiled_seed_set(
        SeedDefinition(name="cn-core", guba_board_codes=["600900", "zssh000001"]),
        refreshed_at=datetime(2026, 7, 18, tzinfo=UTC),
    )
    reader = StubReader()
    reader.guba_boards = [
        GubaBoardSummary(
            board_code="600900", seed_sets=[], post_count=1, comment_count=0,
            latest_published_at=None, latest_fetched_at=None,
        ),
        GubaBoardSummary(
            board_code="999999", seed_sets=[], post_count=1, comment_count=0,
            latest_published_at=None, latest_fetched_at=None,
        ),
    ]
    queries = WebQueries(reader=reader, state=state)

    boards = queries.guba_boards()

    assert boards[0].seed_sets == ["cn-core"]
    assert boards[1].seed_sets == []


def test_web_queries_predicts_next_guba_crawl(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.db")
    now = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)
    with state.connection() as conn:
        conn.executemany(
            """
            INSERT INTO url_state (url, source, kind, seed_name, first_seen_at, last_seen_at, last_fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                # List page fetched 10 min ago: not due for 30m default interval.
                ("https://guba.eastmoney.com/list,600900.html", "guba", "discover", "cn-core",
                 now.isoformat(), now.isoformat(), (now - timedelta(minutes=10)).isoformat()),
                # List page fetched 45 min ago: past the 30m gate, due now.
                ("https://guba.eastmoney.com/list,zssh000001.html", "guba", "discover", "cn-core",
                 now.isoformat(), now.isoformat(), (now - timedelta(minutes=45)).isoformat()),
                # Deeper list page must not create its own board entry.
                ("https://guba.eastmoney.com/list,600900_2.html", "guba", "discover", "cn-core",
                 now.isoformat(), now.isoformat(), (now - timedelta(minutes=45)).isoformat()),
                # Post fetched 1h ago: 360m gate, not due.
                ("https://guba.eastmoney.com/news,600900,111.html", "guba", "fetch_post", "cn-core",
                 now.isoformat(), now.isoformat(), (now - timedelta(hours=1)).isoformat()),
                # Comment refresh fetched 2h ago: 60m gate, due now.
                ("https://guba.eastmoney.com/news,600900,111.html#comments", "guba", "refresh_comments",
                 "cn-core", now.isoformat(), now.isoformat(), (now - timedelta(hours=2)).isoformat()),
            ],
        )
    state.store_compiled_seed_set(
        SeedDefinition(name="cn-core", guba_board_codes=["600900", "zssh000001", "300750"]),
        refreshed_at=now,
    )
    reader = StubReader()
    reader.latest = CrawlRun(
        run_id="r1",
        started_at=now - timedelta(minutes=6),
        finished_at=now - timedelta(minutes=2),
        status="succeeded",
        seeds_processed=1, tasks_enqueued=0, pages_fetched=0, posts_written=0,
        comments_written=0, authors_written=0, blocked_responses=0, errors=0, skipped_tasks=0,
    )
    queries = WebQueries(reader=reader, state=state)

    plan = queries.guba_next_crawl(now=now)

    by_code = {b.board_code: b for b in plan.boards}
    assert set(by_code) == {"600900", "zssh000001", "300750"}
    assert not by_code["600900"].due_now
    assert by_code["600900"].eligible_at == now + timedelta(minutes=20)
    assert by_code["zssh000001"].due_now
    assert by_code["300750"].due_now  # seeded but never crawled
    assert by_code["300750"].last_fetched_at is None
    # Due boards sort ahead of waiting boards.
    assert plan.boards[-1].board_code == "600900"

    forecasts = {f.kind: f for f in plan.task_forecasts}
    assert forecasts["fetch_post"].tracked == 1
    assert forecasts["fetch_post"].due_now == 0
    assert forecasts["fetch_post"].next_eligible_at == now + timedelta(hours=5)
    assert forecasts["refresh_comments"].due_now == 1
    # Next cycle estimated from run end + default 300s poll interval.
    assert plan.next_cycle_at == now + timedelta(minutes=3)


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
