from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from alphapulse.runtime.config import Settings
from alphapulse.runtime.state import StateStore
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


RECENT_URL_WINDOW = timedelta(hours=1)
ALLOWED_SOURCES = {"bilibili", "xueqiu"}
CONTENT_PREVIEW_CHARS = 280


class StorageReader(Protocol):
    def latest_run(self) -> CrawlRun | None: ...

    def list_runs(self, limit: int) -> list[CrawlRun]: ...

    def list_errors(self, limit: int, source: str | None) -> list[CrawlError]: ...

    def list_posts(self, source: str | None, limit: int, offset: int) -> list[PostSummary]: ...

    def get_post(self, source: str, source_entity_id: str) -> PostDetail | None: ...

    def list_comments_for_post(self, source: str, post_entity_id: str) -> list[Comment]: ...


def _content_preview(text: str | None) -> str:
    if not text:
        return ""
    collapsed = " ".join(text.split())
    if len(collapsed) <= CONTENT_PREVIEW_CHARS:
        return collapsed
    return collapsed[:CONTENT_PREVIEW_CHARS].rstrip() + "…"


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    raise TypeError(f"Cannot coerce {type(value)!r} to datetime")


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _coerce_topic_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        if not value:
            return []
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return []
    raise TypeError(f"Cannot coerce {type(value)!r} to list[str]")


def _run_from_row(row: dict[str, Any]) -> CrawlRun:
    return CrawlRun(
        run_id=str(row["run_id"]),
        started_at=_coerce_datetime(row["started_at"]),
        finished_at=_coerce_datetime(row.get("finished_at")),
        status=str(row.get("status") or ""),
        seeds_processed=int(row.get("seeds_processed") or 0),
        tasks_enqueued=int(row.get("tasks_enqueued") or 0),
        pages_fetched=int(row.get("pages_fetched") or 0),
        posts_written=int(row.get("posts_written") or 0),
        comments_written=int(row.get("comments_written") or 0),
        authors_written=int(row.get("authors_written") or 0),
        blocked_responses=int(row.get("blocked_responses") or 0),
        errors=int(row.get("errors") or 0),
        skipped_tasks=int(row.get("skipped_tasks") or 0),
    )


def _error_from_row(row: dict[str, Any]) -> CrawlError:
    return CrawlError(
        created_at=_coerce_datetime(row["created_at"]),
        source=str(row.get("source") or ""),
        url=str(row.get("url") or ""),
        error_message=str(row.get("error_message") or ""),
    )


def _post_summary_from_row(row: dict[str, Any]) -> PostSummary:
    return PostSummary(
        source=str(row["source"]),
        source_entity_id=str(row["source_entity_id"]),
        canonical_url=str(row["canonical_url"]),
        author_entity_id=str(row["author_entity_id"]) if row.get("author_entity_id") else None,
        title=row.get("title"),
        content_preview=_content_preview(row.get("content_text")),
        published_at=_coerce_datetime(row.get("published_at")),
        fetched_at=_coerce_datetime(row["fetched_at"]),
        like_count=_coerce_int(row.get("like_count")),
        comment_count=_coerce_int(row.get("comment_count")),
    )


def _post_detail_from_row(row: dict[str, Any]) -> PostDetail:
    return PostDetail(
        source=str(row["source"]),
        source_entity_id=str(row["source_entity_id"]),
        canonical_url=str(row["canonical_url"]),
        author_entity_id=str(row["author_entity_id"]) if row.get("author_entity_id") else None,
        title=row.get("title"),
        content_text=str(row.get("content_text") or ""),
        language=row.get("language"),
        published_at=_coerce_datetime(row.get("published_at")),
        fetched_at=_coerce_datetime(row["fetched_at"]),
        like_count=_coerce_int(row.get("like_count")),
        comment_count=_coerce_int(row.get("comment_count")),
        repost_count=_coerce_int(row.get("repost_count")),
        raw_topic_ids=_coerce_topic_ids(row.get("raw_topic_ids") or row.get("raw_topic_ids_json")),
    )


def _comment_from_row(row: dict[str, Any]) -> Comment:
    return Comment(
        source=str(row["source"]),
        source_entity_id=str(row["source_entity_id"]),
        post_entity_id=str(row["post_entity_id"]),
        parent_comment_entity_id=(
            str(row["parent_comment_entity_id"]) if row.get("parent_comment_entity_id") else None
        ),
        author_entity_id=str(row["author_entity_id"]) if row.get("author_entity_id") else None,
        content_text=str(row.get("content_text") or ""),
        published_at=_coerce_datetime(row.get("published_at")),
        fetched_at=_coerce_datetime(row["fetched_at"]),
        like_count=_coerce_int(row.get("like_count")),
    )


@dataclass
class ClickHouseReader:
    client: Any
    database: str

    def _rows(self, sql: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self.client.query(sql, parameters=parameters or {})
        columns = list(result.column_names)
        return [dict(zip(columns, row, strict=True)) for row in result.result_rows]

    def latest_run(self) -> CrawlRun | None:
        rows = self._rows(
            f"SELECT * FROM {self.database}.crawl_runs ORDER BY started_at DESC LIMIT 1"
        )
        return _run_from_row(rows[0]) if rows else None

    def list_runs(self, limit: int) -> list[CrawlRun]:
        rows = self._rows(
            f"SELECT * FROM {self.database}.crawl_runs "
            f"ORDER BY started_at DESC LIMIT {{limit:UInt32}}",
            {"limit": limit},
        )
        return [_run_from_row(row) for row in rows]

    def list_errors(self, limit: int, source: str | None) -> list[CrawlError]:
        params: dict[str, Any] = {"limit": limit}
        where = ""
        if source is not None:
            where = "WHERE source = {source:String} "
            params["source"] = source
        rows = self._rows(
            f"SELECT created_at, source, url, error_message FROM {self.database}.crawl_errors "
            f"{where}ORDER BY created_at DESC LIMIT {{limit:UInt32}}",
            params,
        )
        return [_error_from_row(row) for row in rows]

    def list_posts(self, source: str | None, limit: int, offset: int) -> list[PostSummary]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        where = ""
        if source is not None:
            where = "WHERE source = {source:String} "
            params["source"] = source
        rows = self._rows(
            f"SELECT source, source_entity_id, canonical_url, author_entity_id, title, "
            f"content_text, published_at, fetched_at, like_count, comment_count "
            f"FROM {self.database}.posts FINAL "
            f"{where}ORDER BY coalesce(published_at, fetched_at) DESC, source_entity_id "
            f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            params,
        )
        return [_post_summary_from_row(row) for row in rows]

    def get_post(self, source: str, source_entity_id: str) -> PostDetail | None:
        rows = self._rows(
            f"SELECT * FROM {self.database}.posts FINAL "
            f"WHERE source = {{source:String}} AND source_entity_id = {{entity_id:String}} LIMIT 1",
            {"source": source, "entity_id": source_entity_id},
        )
        return _post_detail_from_row(rows[0]) if rows else None

    def list_comments_for_post(self, source: str, post_entity_id: str) -> list[Comment]:
        rows = self._rows(
            f"SELECT source, source_entity_id, post_entity_id, parent_comment_entity_id, "
            f"author_entity_id, content_text, published_at, fetched_at, like_count "
            f"FROM {self.database}.comments FINAL "
            f"WHERE source = {{source:String}} AND post_entity_id = {{post_id:String}} "
            f"ORDER BY coalesce(published_at, fetched_at) ASC, source_entity_id",
            {"source": source, "post_id": post_entity_id},
        )
        return [_comment_from_row(row) for row in rows]


@dataclass
class RqliteReader:
    client: Any

    def _rows(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        payload = self.client.query_params([[sql, *params]])
        result = (payload.get("results") or [{}])[0]
        columns = result.get("columns") or []
        values = result.get("values") or []
        return [dict(zip(columns, row, strict=True)) for row in values]

    def latest_run(self) -> CrawlRun | None:
        rows = self._rows(
            "SELECT * FROM crawl_runs ORDER BY started_at DESC LIMIT 1",
            [],
        )
        return _run_from_row(rows[0]) if rows else None

    def list_runs(self, limit: int) -> list[CrawlRun]:
        rows = self._rows(
            "SELECT * FROM crawl_runs ORDER BY started_at DESC LIMIT ?",
            [limit],
        )
        return [_run_from_row(row) for row in rows]

    def list_errors(self, limit: int, source: str | None) -> list[CrawlError]:
        if source is None:
            rows = self._rows(
                "SELECT created_at, source, url, error_message FROM crawl_errors "
                "ORDER BY created_at DESC LIMIT ?",
                [limit],
            )
        else:
            rows = self._rows(
                "SELECT created_at, source, url, error_message FROM crawl_errors "
                "WHERE source = ? ORDER BY created_at DESC LIMIT ?",
                [source, limit],
            )
        return [_error_from_row(row) for row in rows]

    def list_posts(self, source: str | None, limit: int, offset: int) -> list[PostSummary]:
        if source is None:
            rows = self._rows(
                "SELECT source, source_entity_id, canonical_url, author_entity_id, title, "
                "content_text, published_at, fetched_at, like_count, comment_count FROM posts "
                "ORDER BY coalesce(published_at, fetched_at) DESC, source_entity_id "
                "LIMIT ? OFFSET ?",
                [limit, offset],
            )
        else:
            rows = self._rows(
                "SELECT source, source_entity_id, canonical_url, author_entity_id, title, "
                "content_text, published_at, fetched_at, like_count, comment_count FROM posts "
                "WHERE source = ? ORDER BY coalesce(published_at, fetched_at) DESC, source_entity_id "
                "LIMIT ? OFFSET ?",
                [source, limit, offset],
            )
        return [_post_summary_from_row(row) for row in rows]

    def get_post(self, source: str, source_entity_id: str) -> PostDetail | None:
        rows = self._rows(
            "SELECT * FROM posts WHERE source = ? AND source_entity_id = ? LIMIT 1",
            [source, source_entity_id],
        )
        return _post_detail_from_row(rows[0]) if rows else None

    def list_comments_for_post(self, source: str, post_entity_id: str) -> list[Comment]:
        rows = self._rows(
            "SELECT source, source_entity_id, post_entity_id, parent_comment_entity_id, "
            "author_entity_id, content_text, published_at, fetched_at, like_count FROM comments "
            "WHERE source = ? AND post_entity_id = ? "
            "ORDER BY coalesce(published_at, fetched_at) ASC, source_entity_id",
            [source, post_entity_id],
        )
        return [_comment_from_row(row) for row in rows]


def build_reader(settings: Settings) -> StorageReader:
    if settings.storage.backend == "clickhouse":
        from alphapulse.storage.clickhouse import _client_from_settings

        return ClickHouseReader(
            client=_client_from_settings(settings.clickhouse),
            database=settings.clickhouse.database,
        )
    if settings.storage.backend == "rqlite":
        from alphapulse.storage.rqlite import RqliteClient

        return RqliteReader(client=RqliteClient(settings.rqlite))
    raise ValueError(f"Unsupported storage backend: {settings.storage.backend}")


@dataclass
class WebQueries:
    reader: StorageReader
    state: StateStore

    def recent_url_activity(self, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        threshold = (now - RECENT_URL_WINDOW).isoformat()
        with self.state.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM url_state WHERE last_fetched_at >= ?",
                (threshold,),
            ).fetchone()
        return int(row["count"]) if row else 0

    def seed_set_summaries(self) -> list[SeedSetSummary]:
        summaries: list[SeedSetSummary] = []
        for seed in self.state.load_compiled_seed_sets():
            refreshed_at = self.state.get_compiled_seed_set_refreshed_at(seed.name)
            summaries.append(
                SeedSetSummary(
                    name=seed.name,
                    refreshed_at=refreshed_at,
                    stock_count=len(seed.stock_ids),
                    topic_count=len(seed.topic_ids),
                    user_count=len(seed.user_ids),
                    bilibili_video_count=len(seed.bilibili_video_targets),
                    bilibili_space_count=len(seed.bilibili_space_urls),
                    post_url_count=len(seed.post_urls),
                )
            )
        return summaries

    def status(self, *, recent_runs_limit: int = 10, recent_errors_limit: int = 20) -> StatusResponse:
        return StatusResponse(
            latest_run=self.reader.latest_run(),
            recent_runs=self.reader.list_runs(recent_runs_limit),
            recent_errors=self.reader.list_errors(recent_errors_limit, source=None),
            in_flight_urls=self.recent_url_activity(),
            seed_sets=self.seed_set_summaries(),
        )

    def post_detail(self, source: str, source_entity_id: str) -> PostDetailResponse | None:
        post = self.reader.get_post(source, source_entity_id)
        if post is None:
            return None
        comments = self.reader.list_comments_for_post(source, source_entity_id)
        return PostDetailResponse(post=post, comments=comments)


def build_queries(settings: Settings) -> WebQueries:
    return WebQueries(
        reader=build_reader(settings),
        state=StateStore(settings.crawl.state_path),
    )
