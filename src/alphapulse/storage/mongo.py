from __future__ import annotations

from datetime import datetime
from typing import Any

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost
from alphapulse.runtime.config import MongoSettings


def _entity_id(source: str, source_entity_id: str) -> str:
    return f"{source}:{source_entity_id}"


def _author_doc(item: NormalizedAuthor) -> dict[str, Any]:
    return {
        "_id": _entity_id(item.source, item.source_entity_id),
        "source": item.source,
        "source_entity_id": item.source_entity_id,
        "username": item.username,
        "display_name": item.display_name,
        "profile_url": str(item.profile_url) if item.profile_url else None,
        "bio": item.bio,
        "followers": item.followers,
        "following": item.following,
        "fetched_at": item.fetched_at,
    }


def _post_doc(item: NormalizedPost) -> dict[str, Any]:
    return {
        "_id": _entity_id(item.source, item.source_entity_id),
        "source": item.source,
        "source_entity_id": item.source_entity_id,
        "canonical_url": str(item.canonical_url),
        "author_entity_id": item.author_entity_id,
        "title": item.title,
        "content_text": item.content_text,
        "language": item.language,
        "published_at": item.published_at,
        "fetched_at": item.fetched_at,
        "like_count": item.like_count,
        "comment_count": item.comment_count,
        "repost_count": item.repost_count,
        "raw_topic_ids": list(item.raw_topic_ids),
    }


def _comment_doc(item: NormalizedComment) -> dict[str, Any]:
    return {
        "_id": _entity_id(item.source, item.source_entity_id),
        "source": item.source,
        "source_entity_id": item.source_entity_id,
        "post_entity_id": item.post_entity_id,
        "canonical_url": str(item.canonical_url) if item.canonical_url else None,
        "author_entity_id": item.author_entity_id,
        "parent_comment_entity_id": item.parent_comment_entity_id,
        "content_text": item.content_text,
        "published_at": item.published_at,
        "fetched_at": item.fetched_at,
        "like_count": item.like_count,
    }


def _client_from_settings(settings: MongoSettings):
    from pymongo import MongoClient

    return MongoClient(
        settings.uri,
        serverSelectionTimeoutMS=settings.server_selection_timeout_ms,
    )


class MongoStore:
    def __init__(self, settings: MongoSettings, client: Any | None = None) -> None:
        self.settings = settings
        self.client = client or _client_from_settings(settings)
        self._db = self.client[settings.database]

    def _collection(self, name: str):
        return self._db[self.settings.resolved(name)]

    def init_db(self) -> None:
        posts = self._collection(self.settings.posts_collection)
        posts.create_index([("source", 1), ("published_at", -1)])
        posts.create_index([("source", 1), ("fetched_at", -1)])

        comments = self._collection(self.settings.comments_collection)
        comments.create_index([("source", 1), ("post_entity_id", 1), ("published_at", 1)])

        crawl_runs = self._collection(self.settings.crawl_runs_collection)
        crawl_runs.create_index([("started_at", -1)])

        crawl_errors = self._collection(self.settings.crawl_errors_collection)
        crawl_errors.create_index([("created_at", -1)])
        crawl_errors.create_index([("source", 1), ("created_at", -1)])

    def healthcheck(self) -> bool:
        try:
            self.client.admin.command("ping")
        except Exception:
            return False
        return True

    def upsert_authors(self, authors: list[NormalizedAuthor]) -> None:
        if not authors:
            return
        from pymongo import ReplaceOne

        deduped: dict[str, NormalizedAuthor] = {}
        for item in authors:
            deduped[_entity_id(item.source, item.source_entity_id)] = item
        operations = [
            ReplaceOne({"_id": key}, _author_doc(item), upsert=True)
            for key, item in deduped.items()
        ]
        self._collection(self.settings.authors_collection).bulk_write(operations, ordered=False)

    def upsert_posts(self, posts: list[NormalizedPost]) -> None:
        if not posts:
            return
        from pymongo import ReplaceOne

        deduped: dict[str, NormalizedPost] = {}
        for item in posts:
            deduped[_entity_id(item.source, item.source_entity_id)] = item
        operations = [
            ReplaceOne({"_id": key}, _post_doc(item), upsert=True)
            for key, item in deduped.items()
        ]
        self._collection(self.settings.posts_collection).bulk_write(operations, ordered=False)

    def upsert_comments(self, comments: list[NormalizedComment]) -> None:
        if not comments:
            return
        from pymongo import ReplaceOne

        deduped: dict[str, NormalizedComment] = {}
        for item in comments:
            deduped[_entity_id(item.source, item.source_entity_id)] = item
        operations = [
            ReplaceOne({"_id": key}, _comment_doc(item), upsert=True)
            for key, item in deduped.items()
        ]
        self._collection(self.settings.comments_collection).bulk_write(operations, ordered=False)

    def insert_crawl_error(self, *, source: str, url: str, error_message: str) -> None:
        from datetime import UTC

        self._collection(self.settings.crawl_errors_collection).insert_one(
            {
                "created_at": datetime.now(UTC),
                "source": source,
                "url": url,
                "error_message": error_message,
            }
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
        self._collection(self.settings.crawl_runs_collection).replace_one(
            {"_id": run_id},
            {
                "_id": run_id,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "seeds_processed": stats["seeds_processed"],
                "tasks_enqueued": stats["tasks_enqueued"],
                "pages_fetched": stats["pages_fetched"],
                "posts_written": stats["posts_written"],
                "comments_written": stats["comments_written"],
                "authors_written": stats["authors_written"],
                "blocked_responses": stats["blocked_responses"],
                "errors": stats["errors"],
                "skipped_tasks": stats["skipped_tasks"],
            },
            upsert=True,
        )
