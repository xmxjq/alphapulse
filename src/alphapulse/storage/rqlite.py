from __future__ import annotations

import base64
import json
from datetime import datetime
from importlib.resources import files
from typing import Any
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost
from alphapulse.runtime.config import RqliteSettings


def _schema_sql() -> str:
    return files("alphapulse.storage").joinpath("rqlite_schema.sql").read_text()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


class RqliteClient:
    def __init__(self, settings: RqliteSettings) -> None:
        self.settings = settings

    def execute(self, statements: list[str | list[Any]]) -> dict[str, Any]:
        params = {}
        if self.settings.queue_writes:
            params["queue"] = "true"
            params["timeout"] = f"{self.settings.queue_timeout_seconds}s"
        return self._request("POST", "/db/execute", payload=statements, params=params)

    def query(self, sql: str) -> dict[str, Any]:
        return self._request("GET", "/db/query", params={"q": sql})

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: list[str | list[Any]] | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = urljoin(self.settings.url.rstrip("/") + "/", path.lstrip("/"))
        if params:
            url = f"{url}?{urlencode(params)}"

        headers = {"Content-Type": "application/json"}
        if self.settings.username:
            token = f"{self.settings.username}:{self.settings.password or ''}".encode()
            headers["Authorization"] = f"Basic {base64.b64encode(token).decode()}"

        data = json.dumps(payload, ensure_ascii=True).encode() if payload is not None else None
        request = Request(url, data=data, method=method, headers=headers)
        with urlopen(request, timeout=self.settings.queue_timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))


class RqliteStore:
    def __init__(self, settings: RqliteSettings, client: RqliteClient | Any | None = None) -> None:
        self.settings = settings
        self.client = client or RqliteClient(settings)

    def init_db(self) -> None:
        statements = [chunk.strip() for chunk in _schema_sql().split(";") if chunk.strip()]
        self.client.execute(statements)

    def healthcheck(self) -> bool:
        try:
            payload = self.client.query("SELECT 1")
        except Exception:
            return False
        rows = (((payload.get("results") or [{}])[0]).get("values") or [])
        return bool(rows and rows[0][0] == 1)

    def upsert_authors(self, authors: list[NormalizedAuthor]) -> None:
        if not authors:
            return
        statements = [
            [
                """
                INSERT INTO authors (
                    source, source_entity_id, username, display_name, profile_url,
                    bio, followers, following, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_entity_id) DO UPDATE SET
                    username = excluded.username,
                    display_name = excluded.display_name,
                    profile_url = excluded.profile_url,
                    bio = excluded.bio,
                    followers = excluded.followers,
                    following = excluded.following,
                    fetched_at = excluded.fetched_at
                """,
                item.source,
                item.source_entity_id,
                item.username,
                item.display_name,
                str(item.profile_url) if item.profile_url else None,
                item.bio,
                item.followers,
                item.following,
                _iso(item.fetched_at),
            ]
            for item in authors
        ]
        self.client.execute(statements)

    def upsert_posts(self, posts: list[NormalizedPost]) -> None:
        if not posts:
            return
        deduped: dict[tuple[str, str], NormalizedPost] = {}
        for item in posts:
            deduped[(item.source, item.source_entity_id)] = item
        statements = [
            [
                """
                INSERT INTO posts (
                    source, source_entity_id, canonical_url, author_entity_id, title,
                    content_text, language, published_at, fetched_at, like_count,
                    comment_count, repost_count, raw_topic_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_entity_id) DO UPDATE SET
                    canonical_url = excluded.canonical_url,
                    author_entity_id = excluded.author_entity_id,
                    title = excluded.title,
                    content_text = excluded.content_text,
                    language = excluded.language,
                    published_at = excluded.published_at,
                    fetched_at = excluded.fetched_at,
                    like_count = excluded.like_count,
                    comment_count = excluded.comment_count,
                    repost_count = excluded.repost_count,
                    raw_topic_ids_json = excluded.raw_topic_ids_json
                """,
                item.source,
                item.source_entity_id,
                str(item.canonical_url),
                item.author_entity_id,
                item.title,
                item.content_text,
                item.language,
                _iso(item.published_at),
                _iso(item.fetched_at),
                item.like_count,
                item.comment_count,
                item.repost_count,
                json.dumps(item.raw_topic_ids, ensure_ascii=True),
            ]
            for item in deduped.values()
        ]
        self.client.execute(statements)

    def upsert_comments(self, comments: list[NormalizedComment]) -> None:
        if not comments:
            return
        deduped: dict[tuple[str, str], NormalizedComment] = {}
        for item in comments:
            deduped[(item.source, item.source_entity_id)] = item
        statements = [
            [
                """
                INSERT INTO comments (
                    source, source_entity_id, post_entity_id, canonical_url,
                    author_entity_id, parent_comment_entity_id, content_text,
                    published_at, fetched_at, like_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_entity_id) DO UPDATE SET
                    post_entity_id = excluded.post_entity_id,
                    canonical_url = excluded.canonical_url,
                    author_entity_id = excluded.author_entity_id,
                    parent_comment_entity_id = excluded.parent_comment_entity_id,
                    content_text = excluded.content_text,
                    published_at = excluded.published_at,
                    fetched_at = excluded.fetched_at,
                    like_count = excluded.like_count
                """,
                item.source,
                item.source_entity_id,
                item.post_entity_id,
                str(item.canonical_url) if item.canonical_url else None,
                item.author_entity_id,
                item.parent_comment_entity_id,
                item.content_text,
                _iso(item.published_at),
                _iso(item.fetched_at),
                item.like_count,
            ]
            for item in deduped.values()
        ]
        self.client.execute(statements)

    def insert_crawl_error(self, *, source: str, url: str, error_message: str) -> None:
        self.client.execute(
            [[
                """
                INSERT INTO crawl_errors (created_at, source, url, error_message)
                VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?, ?, ?)
                """,
                source,
                url,
                error_message,
            ]]
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
        self.client.execute(
            [[
                """
                INSERT INTO crawl_runs (
                    run_id, started_at, finished_at, status, seeds_processed, tasks_enqueued,
                    pages_fetched, posts_written, comments_written, authors_written,
                    blocked_responses, errors, skipped_tasks
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    finished_at = excluded.finished_at,
                    status = excluded.status,
                    seeds_processed = excluded.seeds_processed,
                    tasks_enqueued = excluded.tasks_enqueued,
                    pages_fetched = excluded.pages_fetched,
                    posts_written = excluded.posts_written,
                    comments_written = excluded.comments_written,
                    authors_written = excluded.authors_written,
                    blocked_responses = excluded.blocked_responses,
                    errors = excluded.errors,
                    skipped_tasks = excluded.skipped_tasks
                """,
                run_id,
                _iso(started_at),
                _iso(finished_at),
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
            ]]
        )
