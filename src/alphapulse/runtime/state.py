from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS url_state (
                    url TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    seed_name TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_fetched_at TEXT,
                    last_status INTEGER
                );

                CREATE TABLE IF NOT EXISTS item_state (
                    source TEXT NOT NULL,
                    source_entity_id TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    last_fetched_at TEXT,
                    last_comment_refresh_at TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (source, source_entity_id)
                );

                CREATE TABLE IF NOT EXISTS crawl_runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    stats_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )

    def should_fetch_url(self, url: str, min_age: timedelta) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT last_fetched_at FROM url_state WHERE url = ?",
                (url,),
            ).fetchone()
        if row is None or row["last_fetched_at"] is None:
            return True
        last_fetched_at = datetime.fromisoformat(row["last_fetched_at"])
        return datetime.now(UTC) - last_fetched_at >= min_age

    def remember_url(self, *, url: str, source: str, kind: str, seed_name: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO url_state (url, source, kind, seed_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    source = excluded.source,
                    kind = excluded.kind,
                    seed_name = excluded.seed_name,
                    last_seen_at = excluded.last_seen_at
                """,
                (url, source, kind, seed_name, now, now),
            )

    def mark_url_fetched(self, url: str, status: int | None) -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE url_state SET last_fetched_at = ?, last_status = ? WHERE url = ?",
                (datetime.now(UTC).isoformat(), status, url),
            )

    def should_refresh_comments(self, source: str, source_entity_id: str, min_age: timedelta) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT last_comment_refresh_at
                FROM item_state
                WHERE source = ? AND source_entity_id = ?
                """,
                (source, source_entity_id),
            ).fetchone()
        if row is None or row["last_comment_refresh_at"] is None:
            return True
        refreshed_at = datetime.fromisoformat(row["last_comment_refresh_at"])
        return datetime.now(UTC) - refreshed_at >= min_age

    def upsert_item(self, source: str, source_entity_id: str, canonical_url: str, metadata: dict[str, object]) -> None:
        now = datetime.now(UTC).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO item_state (source, source_entity_id, canonical_url, last_fetched_at, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source, source_entity_id) DO UPDATE SET
                    canonical_url = excluded.canonical_url,
                    last_fetched_at = excluded.last_fetched_at,
                    metadata_json = excluded.metadata_json
                """,
                (source, source_entity_id, canonical_url, now, json.dumps(metadata, ensure_ascii=True)),
            )

    def mark_comments_refreshed(self, source: str, source_entity_id: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE item_state
                SET last_comment_refresh_at = ?
                WHERE source = ? AND source_entity_id = ?
                """,
                (datetime.now(UTC).isoformat(), source, source_entity_id),
            )

    def start_run(self, run_id: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_runs (run_id, started_at, status)
                VALUES (?, ?, 'running')
                """,
                (run_id, datetime.now(UTC).isoformat()),
            )

    def finish_run(self, run_id: str, status: str, stats: dict[str, int]) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE crawl_runs
                SET finished_at = ?, status = ?, stats_json = ?
                WHERE run_id = ?
                """,
                (datetime.now(UTC).isoformat(), status, json.dumps(stats, ensure_ascii=True), run_id),
            )

