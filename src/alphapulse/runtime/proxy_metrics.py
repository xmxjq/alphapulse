from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse


IP_ADDRESS_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def proxy_id(proxy_url: str) -> str:
    parsed = urlparse(proxy_url)
    identity = parsed.netloc or proxy_url
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]


def _safe_reason(reason: str) -> str:
    return IP_ADDRESS_RE.sub("<ip>", reason).replace("\n", " ")[:200]


class ProxyMetricsStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS proxy_nodes (
                    provider TEXT NOT NULL,
                    proxy_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    benched_until TEXT,
                    acquire_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_acquired_at TEXT,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    last_failure_reason TEXT,
                    PRIMARY KEY (provider, proxy_id)
                );

                CREATE TABLE IF NOT EXISTS proxy_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    proxy_id TEXT,
                    count INTEGER NOT NULL DEFAULT 1,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_proxy_events_provider_time
                    ON proxy_events (provider, occurred_at);
                """
            )
            event_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(proxy_events)").fetchall()
            }
            if "source" not in event_columns:
                try:
                    conn.execute("ALTER TABLE proxy_events ADD COLUMN source TEXT")
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_proxy_events_provider_source_time
                ON proxy_events (provider, source, occurred_at)
                """
            )

    def record_batch(
        self,
        provider: str,
        proxy_urls: list[str],
        *,
        expires_at: datetime,
        source: str | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        expiry = expires_at.isoformat()
        identifiers = list(dict.fromkeys(proxy_id(url) for url in proxy_urls))
        with self.connection() as conn:
            existing = {
                row["proxy_id"]
                for row in conn.execute(
                    "SELECT proxy_id FROM proxy_nodes WHERE provider = ?",
                    (provider,),
                )
            }
            for identifier in identifiers:
                conn.execute(
                    """
                    INSERT INTO proxy_nodes (
                        provider, proxy_id, first_seen_at, last_seen_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(provider, proxy_id) DO UPDATE SET
                        last_seen_at = excluded.last_seen_at,
                        expires_at = excluded.expires_at
                    """,
                    (provider, identifier, now, now, expiry),
                )
            self._insert_event(
                conn,
                provider=provider,
                event_type="batch_fetched",
                occurred_at=now,
                count=len(identifiers),
                source=source,
                detail={
                    "returned": len(identifiers),
                    "new": sum(identifier not in existing for identifier in identifiers),
                },
            )

    def record_acquire(
        self, provider: str, proxy_url: str, *, source: str | None = None
    ) -> None:
        now = datetime.now(UTC).isoformat()
        identifier = proxy_id(proxy_url)
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE proxy_nodes
                SET acquire_count = acquire_count + 1, last_acquired_at = ?
                WHERE provider = ? AND proxy_id = ?
                """,
                (now, provider, identifier),
            )
            self._insert_event(
                conn,
                provider=provider,
                event_type="lease_acquired",
                occurred_at=now,
                proxy_identifier=identifier,
                source=source,
            )

    def record_success(
        self, provider: str, proxy_url: str, *, source: str | None = None
    ) -> None:
        now = datetime.now(UTC).isoformat()
        identifier = proxy_id(proxy_url)
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE proxy_nodes
                SET success_count = success_count + 1, last_success_at = ?
                WHERE provider = ? AND proxy_id = ?
                """,
                (now, provider, identifier),
            )
            self._insert_event(
                conn,
                provider=provider,
                event_type="request_success",
                occurred_at=now,
                proxy_identifier=identifier,
                source=source,
            )

    def record_failure(
        self,
        provider: str,
        proxy_url: str,
        *,
        reason: str,
        benched_until: datetime | None,
        source: str | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        identifier = proxy_id(proxy_url)
        safe_reason = _safe_reason(reason)
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE proxy_nodes
                SET failure_count = failure_count + 1,
                    last_failure_at = ?,
                    last_failure_reason = ?,
                    benched_until = CASE
                        WHEN ? IS NULL THEN benched_until
                        ELSE ?
                    END
                WHERE provider = ? AND proxy_id = ?
                """,
                (
                    now,
                    safe_reason,
                    benched_until.isoformat() if benched_until else None,
                    benched_until.isoformat() if benched_until else None,
                    provider,
                    identifier,
                ),
            )
            self._insert_event(
                conn,
                provider=provider,
                event_type="proxy_benched" if benched_until else "request_failure",
                occurred_at=now,
                proxy_identifier=identifier,
                source=source,
                detail={"reason": safe_reason, "benched": benched_until is not None},
            )

    def record_api_error(
        self, provider: str, reason: str, *, source: str | None = None
    ) -> None:
        with self.connection() as conn:
            self._insert_event(
                conn,
                provider=provider,
                event_type="api_error",
                occurred_at=datetime.now(UTC).isoformat(),
                source=source,
                detail={"reason": _safe_reason(reason)},
            )

    def record_pool_empty(self, provider: str, *, source: str | None = None) -> None:
        with self.connection() as conn:
            self._insert_event(
                conn,
                provider=provider,
                event_type="pool_empty",
                occurred_at=datetime.now(UTC).isoformat(),
                source=source,
            )

    @staticmethod
    def _insert_event(
        conn: sqlite3.Connection,
        *,
        provider: str,
        event_type: str,
        occurred_at: str,
        proxy_identifier: str | None = None,
        count: int = 1,
        source: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO proxy_events (
                occurred_at, provider, event_type, proxy_id, count, source, detail_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                occurred_at,
                provider,
                event_type,
                proxy_identifier,
                count,
                source,
                json.dumps(detail or {}, ensure_ascii=True),
            ),
        )

    def snapshot(
        self,
        *,
        provider: str,
        since: datetime,
        now: datetime,
        event_limit: int = 50,
    ) -> dict[str, Any]:
        since_iso = since.isoformat()
        now_iso = now.isoformat()
        with self.connection() as conn:
            node_rows = conn.execute(
                """
                SELECT * FROM proxy_nodes
                WHERE provider = ?
                ORDER BY COALESCE(last_acquired_at, last_seen_at) DESC
                """,
                (provider,),
            ).fetchall()
            event_rows = conn.execute(
                """
                SELECT occurred_at, event_type, proxy_id, count, source, detail_json
                FROM proxy_events
                WHERE provider = ? AND occurred_at >= ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (provider, since_iso, event_limit),
            ).fetchall()
            aggregate_rows = conn.execute(
                """
                SELECT
                    event_type,
                    COUNT(*) AS events,
                    SUM(count) AS count,
                    MAX(occurred_at) AS last_at
                FROM proxy_events
                WHERE provider = ? AND occurred_at >= ?
                GROUP BY event_type
                """,
                (provider, since_iso),
            ).fetchall()
            source_aggregate_rows = conn.execute(
                """
                SELECT
                    COALESCE(source, 'unknown') AS source,
                    event_type,
                    SUM(count) AS count,
                    MAX(occurred_at) AS last_at
                FROM proxy_events
                WHERE provider = ? AND occurred_at >= ?
                GROUP BY COALESCE(source, 'unknown'), event_type
                ORDER BY source, event_type
                """,
                (provider, since_iso),
            ).fetchall()
            trend_rows = conn.execute(
                """
                SELECT
                    substr(occurred_at, 1, 13) AS hour,
                    event_type,
                    SUM(count) AS count
                FROM proxy_events
                WHERE provider = ? AND occurred_at >= ?
                GROUP BY hour, event_type
                ORDER BY hour
                """,
                (provider, since_iso),
            ).fetchall()

        aggregates = {
            row["event_type"]: {
                "count": int(row["count"] or 0),
                "events": int(row["events"] or 0),
                "last_at": row["last_at"],
            }
            for row in aggregate_rows
        }
        nodes = []
        active_nodes = 0
        benched_nodes = 0
        expired_nodes = 0
        for row in node_rows:
            if row["expires_at"] <= now_iso:
                status = "expired"
                expired_nodes += 1
            elif row["benched_until"] and row["benched_until"] > now_iso:
                status = "benched"
                benched_nodes += 1
            else:
                status = "active"
                active_nodes += 1
            attempts = int(row["success_count"]) + int(row["failure_count"])
            nodes.append(
                {
                    **dict(row),
                    "status": status,
                    "success_rate": (
                        int(row["success_count"]) / attempts if attempts else None
                    ),
                }
            )

        trend: dict[str, dict[str, Any]] = {}
        for row in trend_rows:
            bucket = trend.setdefault(
                row["hour"],
                {
                    "hour": f"{row['hour']}:00:00+00:00",
                    "extracted": 0,
                    "leases": 0,
                    "successes": 0,
                    "failures": 0,
                    "api_errors": 0,
                },
            )
            field = {
                "batch_fetched": "extracted",
                "lease_acquired": "leases",
                "request_success": "successes",
                "proxy_benched": "failures",
                "request_failure": "failures",
                "api_error": "api_errors",
            }.get(row["event_type"])
            if field:
                bucket[field] += int(row["count"] or 0)

        successes = aggregates.get("request_success", {}).get("count", 0)
        failures = sum(
            aggregates.get(event_type, {}).get("count", 0)
            for event_type in ("proxy_benched", "request_failure")
        )
        attempts = successes + failures
        extracted = aggregates.get("batch_fetched", {}).get("count", 0)
        source_summaries: dict[str, dict[str, Any]] = {}
        for row in source_aggregate_rows:
            source = str(row["source"])
            summary = source_summaries.setdefault(
                source,
                {
                    "source": source,
                    "extracted": 0,
                    "leases": 0,
                    "successes": 0,
                    "failures": 0,
                    "api_errors": 0,
                    "pool_empty_events": 0,
                    "success_rate": None,
                    "last_activity_at": None,
                },
            )
            event_type = str(row["event_type"])
            count = int(row["count"] or 0)
            field = {
                "batch_fetched": "extracted",
                "lease_acquired": "leases",
                "request_success": "successes",
                "proxy_benched": "failures",
                "request_failure": "failures",
                "api_error": "api_errors",
                "pool_empty": "pool_empty_events",
            }.get(event_type)
            if field:
                summary[field] += count
            last_at = row["last_at"]
            if (
                last_at
                and (
                    summary["last_activity_at"] is None
                    or last_at > summary["last_activity_at"]
                )
            ):
                summary["last_activity_at"] = last_at
        for summary in source_summaries.values():
            source_attempts = summary["successes"] + summary["failures"]
            summary["success_rate"] = (
                summary["successes"] / source_attempts
                if source_attempts
                else None
            )
        return {
            "provider": provider,
            "generated_at": now,
            "active_nodes": active_nodes,
            "benched_nodes": benched_nodes,
            "expired_nodes": expired_nodes,
            "extracted": extracted,
            "unique_nodes": len(node_rows),
            "leases": aggregates.get("lease_acquired", {}).get("count", 0),
            "successes": successes,
            "failures": failures,
            "api_errors": aggregates.get("api_error", {}).get("count", 0),
            "pool_empty_events": aggregates.get("pool_empty", {}).get("count", 0),
            "batches": aggregates.get("batch_fetched", {}).get("events", 0),
            "success_rate": successes / attempts if attempts else None,
            "requests_per_proxy": attempts / extracted if extracted else None,
            "last_batch_at": aggregates.get("batch_fetched", {}).get("last_at"),
            "last_success_at": aggregates.get("request_success", {}).get("last_at"),
            "last_failure_at": max(
                (
                    aggregates.get(event_type, {}).get("last_at")
                    for event_type in ("proxy_benched", "request_failure")
                    if aggregates.get(event_type, {}).get("last_at")
                ),
                default=None,
            ),
            "sources": list(source_summaries.values()),
            "nodes": nodes,
            "trend": list(trend.values()),
            "events": [
                {
                    "occurred_at": row["occurred_at"],
                    "event_type": row["event_type"],
                    "proxy_id": row["proxy_id"],
                    "count": int(row["count"]),
                    "source": row["source"] or "unknown",
                    "detail": json.loads(row["detail_json"] or "{}"),
                }
                for row in event_rows
            ],
        }
