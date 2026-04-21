from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from typing import Any

from alphapulse.pipeline.contracts import SeedDefinition
from alphapulse.runtime.config import RqliteSettings
from alphapulse.seeds.catalog import GeneratedSeedItem
from alphapulse.storage.rqlite import RqliteClient


def _schema_sql() -> str:
    return files("alphapulse.runtime").joinpath("rqlite_state_schema.sql").read_text()


def _rows_affected(response: dict[str, Any]) -> int:
    results = response.get("results") or []
    if not results:
        return 0
    first = results[0]
    if "error" in first:
        raise RuntimeError(f"rqlite error: {first['error']}")
    return int(first.get("rows_affected") or 0)


def _values(response: dict[str, Any]) -> list[list[Any]]:
    results = response.get("results") or []
    if not results:
        return []
    first = results[0]
    if "error" in first:
        raise RuntimeError(f"rqlite error: {first['error']}")
    return list(first.get("values") or [])


class RqliteStateStore:
    def __init__(self, settings: RqliteSettings, client: RqliteClient | None = None) -> None:
        self.settings = settings
        self.client = client or RqliteClient(settings)

    def init_db(self) -> None:
        statements: list[str | list[Any]] = [
            chunk.strip() for chunk in _schema_sql().split(";") if chunk.strip()
        ]
        self.client.execute(statements, queued=False)

    def try_claim_url(
        self,
        *,
        url: str,
        source: str,
        kind: str,
        seed_name: str,
        min_age: timedelta,
    ) -> bool:
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        threshold_iso = (now - min_age).isoformat()
        response = self.client.execute(
            [
                [
                    """
                    INSERT INTO url_state (url, source, kind, seed_name, first_seen_at, last_seen_at, last_fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        source = excluded.source,
                        kind = excluded.kind,
                        seed_name = excluded.seed_name,
                        last_seen_at = excluded.last_seen_at,
                        last_fetched_at = excluded.last_fetched_at
                    WHERE url_state.last_fetched_at IS NULL
                       OR url_state.last_fetched_at < ?
                    """,
                    url,
                    source,
                    kind,
                    seed_name,
                    now_iso,
                    now_iso,
                    now_iso,
                    threshold_iso,
                ]
            ],
            queued=False,
        )
        return _rows_affected(response) > 0

    def mark_url_fetched(self, url: str, status: int | None) -> None:
        self.client.execute(
            [
                [
                    "UPDATE url_state SET last_fetched_at = ?, last_status = ? WHERE url = ?",
                    datetime.now(UTC).isoformat(),
                    status,
                    url,
                ]
            ],
            queued=False,
        )

    def should_refresh_comments(
        self, source: str, source_entity_id: str, min_age: timedelta
    ) -> bool:
        response = self.client.query_params(
            [
                [
                    """
                    SELECT last_comment_refresh_at
                    FROM item_state
                    WHERE source = ? AND source_entity_id = ?
                    """,
                    source,
                    source_entity_id,
                ]
            ]
        )
        rows = _values(response)
        if not rows or rows[0][0] is None:
            return True
        refreshed_at = datetime.fromisoformat(rows[0][0])
        return datetime.now(UTC) - refreshed_at >= min_age

    def upsert_item(
        self,
        source: str,
        source_entity_id: str,
        canonical_url: str,
        metadata: dict[str, object],
    ) -> None:
        now = datetime.now(UTC).isoformat()
        self.client.execute(
            [
                [
                    """
                    INSERT INTO item_state (source, source_entity_id, canonical_url, last_fetched_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(source, source_entity_id) DO UPDATE SET
                        canonical_url = excluded.canonical_url,
                        last_fetched_at = excluded.last_fetched_at,
                        metadata_json = excluded.metadata_json
                    """,
                    source,
                    source_entity_id,
                    canonical_url,
                    now,
                    json.dumps(metadata, ensure_ascii=True),
                ]
            ],
            queued=False,
        )

    def mark_comments_refreshed(self, source: str, source_entity_id: str) -> None:
        self.client.execute(
            [
                [
                    """
                    UPDATE item_state
                    SET last_comment_refresh_at = ?
                    WHERE source = ? AND source_entity_id = ?
                    """,
                    datetime.now(UTC).isoformat(),
                    source,
                    source_entity_id,
                ]
            ],
            queued=False,
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
        statements: list[str | list[Any]] = [
            [
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
                logical_set_name,
                generator_name,
                item.kind,
                item.value,
                timestamp,
                timestamp,
            ]
            for item in items
        ]
        self.client.execute(statements, queued=False)

    def load_active_generated_seed_items(
        self,
        logical_set_name: str,
        *,
        ttl: timedelta,
        as_of: datetime,
    ) -> list[GeneratedSeedItem]:
        threshold = (as_of - ttl).isoformat()
        response = self.client.query_params(
            [
                [
                    """
                    SELECT item_kind, item_value
                    FROM generated_seed_items
                    WHERE logical_set_name = ? AND last_seen_at >= ?
                    """,
                    logical_set_name,
                    threshold,
                ]
            ]
        )
        return [GeneratedSeedItem(kind=row[0], value=row[1]) for row in _values(response)]

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
        self.client.execute(
            [
                [
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
                    run_id,
                    logical_set_name,
                    generator_name,
                    started_at.isoformat(),
                    finished_at.isoformat(),
                    status,
                    item_count,
                    error_message,
                ]
            ],
            queued=False,
        )

    def store_compiled_seed_set(self, seed: SeedDefinition, refreshed_at: datetime) -> None:
        self.client.execute(
            [
                [
                    """
                    INSERT INTO compiled_seed_sets (seed_set_name, seed_json, refreshed_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(seed_set_name) DO UPDATE SET
                        seed_json = excluded.seed_json,
                        refreshed_at = excluded.refreshed_at
                    """,
                    seed.name,
                    json.dumps(seed.model_dump(mode="json"), ensure_ascii=True),
                    refreshed_at.isoformat(),
                ]
            ],
            queued=False,
        )

    def load_compiled_seed_sets(self, seed_set_name: str | None = None) -> list[SeedDefinition]:
        if seed_set_name is None:
            response = self.client.query_params(
                [
                    [
                        "SELECT seed_json FROM compiled_seed_sets ORDER BY seed_set_name",
                    ]
                ]
            )
        else:
            response = self.client.query_params(
                [
                    [
                        "SELECT seed_json FROM compiled_seed_sets WHERE seed_set_name = ? ORDER BY seed_set_name",
                        seed_set_name,
                    ]
                ]
            )
        return [SeedDefinition.model_validate(json.loads(row[0])) for row in _values(response)]

    def list_compiled_seed_set_names(self) -> list[str]:
        response = self.client.query_params(
            [["SELECT seed_set_name FROM compiled_seed_sets ORDER BY seed_set_name"]]
        )
        return [row[0] for row in _values(response)]

    def get_compiled_seed_set_refreshed_at(self, seed_set_name: str) -> datetime | None:
        response = self.client.query_params(
            [
                [
                    "SELECT refreshed_at FROM compiled_seed_sets WHERE seed_set_name = ?",
                    seed_set_name,
                ]
            ]
        )
        rows = _values(response)
        if not rows:
            return None
        return datetime.fromisoformat(rows[0][0])
