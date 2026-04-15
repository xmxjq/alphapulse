from __future__ import annotations

from datetime import datetime
from importlib.resources import files
from typing import Any
from urllib.parse import urlparse

import clickhouse_connect

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost
from alphapulse.runtime.config import ClickHouseSettings


def _schema_sql() -> str:
    return files("alphapulse.storage").joinpath("schema.sql").read_text()


def _client_from_settings(settings: ClickHouseSettings):
    parsed = urlparse(settings.url)
    port = parsed.port or (8443 if settings.secure else 8123)
    return clickhouse_connect.get_client(
        host=parsed.hostname or "localhost",
        port=port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        secure=settings.secure,
    )


class ClickHouseStore:
    def __init__(self, settings: ClickHouseSettings, client: Any | None = None) -> None:
        self.settings = settings
        self.client = client or _client_from_settings(settings)

    def init_db(self) -> None:
        sql = _schema_sql().format(database=self.settings.database)
        for statement in [chunk.strip() for chunk in sql.split(";") if chunk.strip()]:
            self.client.command(statement)

    def healthcheck(self) -> bool:
        try:
            result = self.client.query("SELECT 1")
        except Exception:
            return False
        return bool(result.result_rows and result.result_rows[0][0] == 1)

    def upsert_authors(self, authors: list[NormalizedAuthor]) -> None:
        if not authors:
            return
        rows = [
            [
                item.source,
                item.source_entity_id,
                item.username,
                item.display_name,
                str(item.profile_url) if item.profile_url else None,
                item.bio,
                item.followers,
                item.following,
                item.fetched_at,
            ]
            for item in authors
        ]
        self.client.insert(
            f"{self.settings.database}.authors",
            rows,
            column_names=[
                "source",
                "source_entity_id",
                "username",
                "display_name",
                "profile_url",
                "bio",
                "followers",
                "following",
                "fetched_at",
            ],
        )

    def upsert_posts(self, posts: list[NormalizedPost]) -> None:
        if not posts:
            return
        deduped = {}
        for item in posts:
            deduped[(item.source, item.source_entity_id)] = item
        rows = [
            [
                item.source,
                item.source_entity_id,
                str(item.canonical_url),
                item.author_entity_id,
                item.title,
                item.content_text,
                item.language,
                item.published_at,
                item.fetched_at,
                item.like_count,
                item.comment_count,
                item.repost_count,
                item.raw_topic_ids,
            ]
            for item in deduped.values()
        ]
        self.client.insert(
            f"{self.settings.database}.posts",
            rows,
            column_names=[
                "source",
                "source_entity_id",
                "canonical_url",
                "author_entity_id",
                "title",
                "content_text",
                "language",
                "published_at",
                "fetched_at",
                "like_count",
                "comment_count",
                "repost_count",
                "raw_topic_ids",
            ],
        )

    def upsert_comments(self, comments: list[NormalizedComment]) -> None:
        if not comments:
            return
        deduped = {}
        for item in comments:
            deduped[(item.source, item.source_entity_id)] = item
        rows = [
            [
                item.source,
                item.source_entity_id,
                item.post_entity_id,
                str(item.canonical_url) if item.canonical_url else None,
                item.author_entity_id,
                item.parent_comment_entity_id,
                item.content_text,
                item.published_at,
                item.fetched_at,
                item.like_count,
            ]
            for item in deduped.values()
        ]
        self.client.insert(
            f"{self.settings.database}.comments",
            rows,
            column_names=[
                "source",
                "source_entity_id",
                "post_entity_id",
                "canonical_url",
                "author_entity_id",
                "parent_comment_entity_id",
                "content_text",
                "published_at",
                "fetched_at",
                "like_count",
            ],
        )

    def insert_crawl_error(self, *, source: str, url: str, error_message: str) -> None:
        self.client.insert(
            f"{self.settings.database}.crawl_errors",
            [[source, url, error_message]],
            column_names=["source", "url", "error_message"],
        )

    def insert_crawl_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        stats: dict[str, int],
        status: str,
    ) -> None:
        self.client.insert(
            f"{self.settings.database}.crawl_runs",
            [[
                run_id,
                started_at,
                finished_at,
                status,
                stats["seeds_processed"],
                stats["tasks_enqueued"],
                stats["pages_fetched"],
                stats["posts_written"],
                stats["comments_written"],
                stats["authors_written"],
                stats["blocked_responses"],
                stats["errors"],
                stats["skipped_tasks"],
            ]],
            column_names=[
                "run_id",
                "started_at",
                "finished_at",
                "status",
                "seeds_processed",
                "tasks_enqueued",
                "pages_fetched",
                "posts_written",
                "comments_written",
                "authors_written",
                "blocked_responses",
                "errors",
                "skipped_tasks",
            ],
        )
