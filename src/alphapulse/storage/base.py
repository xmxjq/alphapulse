from __future__ import annotations

from datetime import datetime
from typing import Protocol

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost


class StorageStore(Protocol):
    def init_db(self) -> None: ...

    def healthcheck(self) -> bool: ...

    def upsert_authors(self, authors: list[NormalizedAuthor]) -> None: ...

    def upsert_posts(self, posts: list[NormalizedPost]) -> None: ...

    def upsert_comments(self, comments: list[NormalizedComment]) -> None: ...

    def insert_crawl_error(self, *, source: str, url: str, error_message: str) -> None: ...

    def insert_crawl_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        stats: dict[str, int],
        status: str,
    ) -> None: ...

