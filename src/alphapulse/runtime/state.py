from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator

from alphapulse.pipeline.contracts import SeedDefinition
from alphapulse.seeds.catalog import GeneratedSeedItem


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

                CREATE TABLE IF NOT EXISTS generated_seed_runs (
                    run_id TEXT PRIMARY KEY,
                    logical_set_name TEXT NOT NULL,
                    generator_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS generated_seed_items (
                    logical_set_name TEXT NOT NULL,
                    generator_name TEXT NOT NULL,
                    item_kind TEXT NOT NULL,
                    item_value TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (logical_set_name, generator_name, item_kind, item_value)
                );

                CREATE TABLE IF NOT EXISTS compiled_seed_sets (
                    seed_set_name TEXT PRIMARY KEY,
                    seed_json TEXT NOT NULL,
                    refreshed_at TEXT NOT NULL
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

    def upsert_generated_seed_items(
        self,
        logical_set_name: str,
        generator_name: str,
        items: list[GeneratedSeedItem],
        seen_at: datetime,
    ) -> None:
        if not items:
            return
        timestamp = seen_at.isoformat()
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO generated_seed_items (
                    logical_set_name,
                    generator_name,
                    item_kind,
                    item_value,
                    first_seen_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(logical_set_name, generator_name, item_kind, item_value) DO UPDATE SET
                    last_seen_at = excluded.last_seen_at
                """,
                [
                    (
                        logical_set_name,
                        generator_name,
                        item.kind,
                        item.value,
                        timestamp,
                        timestamp,
                    )
                    for item in items
                ],
            )

    def load_active_generated_seed_items(
        self,
        logical_set_name: str,
        *,
        ttl: timedelta,
        as_of: datetime,
    ) -> list[GeneratedSeedItem]:
        threshold = (as_of - ttl).isoformat()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT item_kind, item_value
                FROM generated_seed_items
                WHERE logical_set_name = ? AND last_seen_at >= ?
                """,
                (logical_set_name, threshold),
            ).fetchall()
        return [
            GeneratedSeedItem(kind=row["item_kind"], value=row["item_value"])
            for row in rows
        ]

    def record_generated_seed_run(
        self,
        *,
        run_id: str,
        logical_set_name: str,
        generator_name: str,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        item_count: int,
        error_message: str | None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO generated_seed_runs (
                    run_id,
                    logical_set_name,
                    generator_name,
                    started_at,
                    finished_at,
                    status,
                    item_count,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    logical_set_name,
                    generator_name,
                    started_at.isoformat(),
                    finished_at.isoformat(),
                    status,
                    item_count,
                    error_message,
                ),
            )

    def store_compiled_seed_set(self, seed: SeedDefinition, refreshed_at: datetime) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO compiled_seed_sets (seed_set_name, seed_json, refreshed_at)
                VALUES (?, ?, ?)
                ON CONFLICT(seed_set_name) DO UPDATE SET
                    seed_json = excluded.seed_json,
                    refreshed_at = excluded.refreshed_at
                """,
                (
                    seed.name,
                    json.dumps(seed.model_dump(mode="json"), ensure_ascii=True),
                    refreshed_at.isoformat(),
                ),
            )

    def load_compiled_seed_sets(self, seed_set_name: str | None = None) -> list[SeedDefinition]:
        with self.connection() as conn:
            if seed_set_name is None:
                rows = conn.execute(
                    """
                    SELECT seed_json
                    FROM compiled_seed_sets
                    ORDER BY seed_set_name
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT seed_json
                    FROM compiled_seed_sets
                    WHERE seed_set_name = ?
                    ORDER BY seed_set_name
                    """,
                    (seed_set_name,),
                ).fetchall()
        return [SeedDefinition.model_validate(json.loads(row["seed_json"])) for row in rows]

    def list_compiled_seed_set_names(self) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT seed_set_name
                FROM compiled_seed_sets
                ORDER BY seed_set_name
                """
            ).fetchall()
        return [row["seed_set_name"] for row in rows]

    def get_compiled_seed_set_refreshed_at(self, seed_set_name: str) -> datetime | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT refreshed_at
                FROM compiled_seed_sets
                WHERE seed_set_name = ?
                """,
                (seed_set_name,),
            ).fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(row["refreshed_at"])
