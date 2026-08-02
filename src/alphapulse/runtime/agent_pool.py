from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import sqlite3
import time
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

from alphapulse.runtime.config import AgentPoolSettings


class AgentPoolUnavailable(RuntimeError):
    pass


class AgentJobFailed(RuntimeError):
    pass


def host_http_capability(host: str) -> str:
    normalized = host.strip().lower().rstrip(".")
    if not normalized:
        raise ValueError("Agent host capability requires a hostname")
    return f"http-host:{normalized}"


@dataclass(frozen=True)
class RemoteFetchResponse:
    job_id: str
    agent_id: str
    status_code: int
    final_url: str
    headers: dict[str, str]
    body: bytes
    duration_ms: int | None

    @property
    def text(self) -> str:
        content_type = self.headers.get("content-type", "")
        charset = None
        for part in content_type.split(";")[1:]:
            key, separator, value = part.strip().partition("=")
            if separator and key.lower() == "charset":
                charset = value.strip("\"'")
                break
        if charset:
            try:
                return self.body.decode(charset, errors="replace")
            except LookupError:
                pass
        try:
            return self.body.decode("utf-8")
        except UnicodeDecodeError:
            return self.body.decode("gb18030", errors="replace")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _safe_detail(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace("\r", " ").replace("\n", " ")[:300]


class AgentPoolStore:
    def __init__(self, settings: AgentPoolSettings) -> None:
        self.settings = settings
        self.path = settings.db_path
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
                CREATE TABLE IF NOT EXISTS agent_tokens (
                    agent_id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    revoked_at TEXT
                );

                CREATE TABLE IF NOT EXISTS agent_nodes (
                    agent_id TEXT PRIMARY KEY,
                    version TEXT NOT NULL,
                    os TEXT NOT NULL,
                    arch TEXT NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    max_concurrency INTEGER NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    benched_until TEXT,
                    leased_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    blocked_count INTEGER NOT NULL DEFAULT 0,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    last_failure_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS agent_source_health (
                    agent_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    benched_until TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    blocked_count INTEGER NOT NULL DEFAULT 0,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    last_failure_reason TEXT,
                    PRIMARY KEY (agent_id, source)
                );
                CREATE INDEX IF NOT EXISTS idx_agent_source_health_bench
                    ON agent_source_health (source, benched_until);

                CREATE TABLE IF NOT EXISTS agent_jobs (
                    job_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    method TEXT NOT NULL,
                    url TEXT NOT NULL,
                    request_headers_json TEXT NOT NULL,
                    request_body BLOB,
                    timeout_seconds INTEGER NOT NULL,
                    max_response_bytes INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    leased_at TEXT,
                    lease_expires_at TEXT,
                    lease_id TEXT,
                    leased_by TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    completed_at TEXT,
                    response_status INTEGER,
                    response_url TEXT,
                    response_headers_json TEXT,
                    response_body_zlib BLOB,
                    duration_ms INTEGER,
                    error_message TEXT,
                    outcome TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_agent_jobs_queue
                    ON agent_jobs (status, available_at, priority DESC, created_at);
                CREATE INDEX IF NOT EXISTS idx_agent_jobs_agent
                    ON agent_jobs (leased_by, status, lease_expires_at);

                CREATE TABLE IF NOT EXISTS agent_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    agent_id TEXT,
                    event_type TEXT NOT NULL,
                    job_id TEXT,
                    detail TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_agent_events_time
                    ON agent_events (occurred_at);
                """
            )
            node_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(agent_nodes)").fetchall()
            }
            if "last_ip_address" not in node_columns:
                try:
                    conn.execute(
                        "ALTER TABLE agent_nodes ADD COLUMN last_ip_address TEXT"
                    )
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise
            # Before source-specific cooldowns, the pool only served guba and
            # stored a single node-wide bench. Preserve that state as guba
            # health, then stop applying it globally.
            conn.execute(
                """
                INSERT INTO agent_source_health (
                    agent_id, source, benched_until, blocked_count,
                    last_failure_at, last_failure_reason
                )
                SELECT
                    agent_id, 'guba', benched_until, blocked_count,
                    last_failure_at, last_failure_reason
                FROM agent_nodes
                WHERE benched_until IS NOT NULL
                ON CONFLICT(agent_id, source) DO UPDATE SET
                    benched_until = CASE
                        WHEN agent_source_health.benched_until IS NULL
                          OR excluded.benched_until > agent_source_health.benched_until
                        THEN excluded.benched_until
                        ELSE agent_source_health.benched_until
                    END
                """
            )
            conn.execute("UPDATE agent_nodes SET benched_until = NULL")

    def issue_token(self, agent_id: str) -> str:
        token = secrets.token_urlsafe(32)
        now = _utcnow().isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO agent_tokens (agent_id, token_hash, created_at, revoked_at)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(agent_id) DO UPDATE SET
                    token_hash = excluded.token_hash,
                    created_at = excluded.created_at,
                    revoked_at = NULL
                """,
                (agent_id, _token_hash(token), now),
            )
            self._event(conn, "token_issued", agent_id=agent_id)
        return token

    def revoke_token(self, agent_id: str) -> bool:
        now = _utcnow().isoformat()
        with self.connection() as conn:
            cursor = conn.execute(
                "UPDATE agent_tokens SET revoked_at = ? WHERE agent_id = ? AND revoked_at IS NULL",
                (now, agent_id),
            )
            if cursor.rowcount:
                self._event(conn, "token_revoked", agent_id=agent_id)
            return bool(cursor.rowcount)

    def list_tokens(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT agent_id, created_at, revoked_at
                FROM agent_tokens
                ORDER BY agent_id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def authenticate(self, agent_id: str, token: str) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT token_hash
                FROM agent_tokens
                WHERE agent_id = ? AND revoked_at IS NULL
                """,
                (agent_id,),
            ).fetchone()
        return bool(row) and hmac.compare_digest(str(row["token_hash"]), _token_hash(token))

    def heartbeat(
        self,
        *,
        agent_id: str,
        version: str,
        os_name: str,
        arch: str,
        capabilities: list[str],
        max_concurrency: int,
        ip_address: str | None = None,
    ) -> None:
        now = _utcnow().isoformat()
        rendered_capabilities = json.dumps(
            sorted(set(capabilities)), ensure_ascii=True, separators=(",", ":")
        )
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO agent_nodes (
                    agent_id, version, os, arch, capabilities_json,
                    max_concurrency, first_seen_at, last_seen_at, last_ip_address
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    version = excluded.version,
                    os = excluded.os,
                    arch = excluded.arch,
                    capabilities_json = excluded.capabilities_json,
                    max_concurrency = excluded.max_concurrency,
                    last_seen_at = excluded.last_seen_at,
                    last_ip_address = COALESCE(
                        excluded.last_ip_address,
                        agent_nodes.last_ip_address
                    )
                """,
                (
                    agent_id,
                    version[:64],
                    os_name[:32],
                    arch[:32],
                    rendered_capabilities,
                    max_concurrency,
                    now,
                    now,
                    ip_address,
                ),
            )

    def has_eligible_agent(self, capability: str, *, source: str | None = None) -> bool:
        return self.available_capacity(capability, source=source) > 0

    def available_capacity(self, capability: str, *, source: str | None = None) -> int:
        now = _utcnow()
        threshold = (now - timedelta(seconds=self.settings.heartbeat_ttl_seconds)).isoformat()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    node.agent_id,
                    node.capabilities_json,
                    node.max_concurrency,
                    COUNT(job.job_id) AS active_jobs
                FROM agent_nodes AS node
                LEFT JOIN agent_jobs AS job
                  ON job.leased_by = node.agent_id
                 AND job.status = 'leased'
                 AND job.lease_expires_at > ?
                LEFT JOIN agent_source_health AS health
                  ON health.agent_id = node.agent_id
                 AND health.source = ?
                WHERE node.last_seen_at >= ?
                  AND (node.benched_until IS NULL OR node.benched_until <= ?)
                  AND (
                    ? IS NULL
                    OR health.benched_until IS NULL
                    OR health.benched_until <= ?
                  )
                GROUP BY
                    node.agent_id,
                    node.capabilities_json,
                    node.max_concurrency
                """,
                (
                    now.isoformat(),
                    source,
                    threshold,
                    now.isoformat(),
                    source,
                    now.isoformat(),
                ),
            ).fetchall()
        return sum(
            max(0, int(row["max_concurrency"]) - int(row["active_jobs"]))
            for row in rows
            if capability in json.loads(row["capabilities_json"])
        )

    def submit_job(
        self,
        *,
        source: str,
        capability: str,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_seconds: int,
        priority: int = 0,
        max_attempts: int = 3,
    ) -> str:
        self._validate_url(url)
        with self.connection() as conn:
            pending = conn.execute(
                "SELECT COUNT(*) AS count FROM agent_jobs WHERE status IN ('queued', 'leased')"
            ).fetchone()
            if pending and int(pending["count"]) >= self.settings.max_pending_jobs:
                raise AgentPoolUnavailable("Agent job queue is full")
            job_id = secrets.token_urlsafe(18)
            now = _utcnow().isoformat()
            conn.execute(
                """
                INSERT INTO agent_jobs (
                    job_id, source, capability, method, url,
                    request_headers_json, request_body, timeout_seconds,
                    max_response_bytes, priority, status, created_at,
                    available_at, max_attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                """,
                (
                    job_id,
                    source,
                    capability,
                    method.upper(),
                    url,
                    json.dumps(headers, ensure_ascii=True, separators=(",", ":")),
                    body,
                    timeout_seconds,
                    self.settings.max_response_bytes,
                    priority,
                    now,
                    now,
                    max_attempts,
                ),
            )
            self._event(conn, "job_queued", job_id=job_id, detail=source)
        return job_id

    def lease_job(self, *, agent_id: str, capabilities: list[str]) -> dict[str, Any] | None:
        now = _utcnow()
        now_iso = now.isoformat()
        lease_expires = (now + timedelta(seconds=self.settings.lease_seconds)).isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._maintenance(conn, now)
            node = conn.execute(
                "SELECT benched_until FROM agent_nodes WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if node is None or (
                node["benched_until"] is not None and node["benched_until"] > now_iso
            ):
                return None
            rows = conn.execute(
                """
                SELECT *
                FROM agent_jobs
                WHERE status = 'queued' AND available_at <= ?
                ORDER BY priority DESC, created_at
                LIMIT 100
                """,
                (now_iso,),
            ).fetchall()
            benched_sources = {
                str(row["source"])
                for row in conn.execute(
                    """
                    SELECT source
                    FROM agent_source_health
                    WHERE agent_id = ?
                      AND benched_until IS NOT NULL
                      AND benched_until > ?
                    """,
                    (agent_id, now_iso),
                ).fetchall()
            }
            capability_set = set(capabilities)
            row = next(
                (
                    candidate
                    for candidate in rows
                    if candidate["capability"] in capability_set
                    and candidate["source"] not in benched_sources
                ),
                None,
            )
            if row is None:
                return None
            lease_id = secrets.token_urlsafe(18)
            updated = conn.execute(
                """
                UPDATE agent_jobs
                SET status = 'leased', leased_at = ?, lease_expires_at = ?,
                    lease_id = ?, leased_by = ?, attempts = attempts + 1
                WHERE job_id = ? AND status = 'queued'
                """,
                (now_iso, lease_expires, lease_id, agent_id, row["job_id"]),
            )
            if not updated.rowcount:
                return None
            conn.execute(
                "UPDATE agent_nodes SET leased_count = leased_count + 1 WHERE agent_id = ?",
                (agent_id,),
            )
            self._event(
                conn,
                "job_leased",
                agent_id=agent_id,
                job_id=row["job_id"],
            )
            return {
                "job_id": row["job_id"],
                "lease_id": lease_id,
                "source": row["source"],
                "capability": row["capability"],
                "method": row["method"],
                "url": row["url"],
                "headers": json.loads(row["request_headers_json"]),
                "body_base64": (
                    base64.b64encode(row["request_body"]).decode("ascii")
                    if row["request_body"] is not None
                    else None
                ),
                "timeout_seconds": int(row["timeout_seconds"]),
                "max_response_bytes": int(row["max_response_bytes"]),
                "lease_expires_at": lease_expires,
            }

    def complete_job(
        self,
        *,
        agent_id: str,
        job_id: str,
        lease_id: str,
        status_code: int,
        final_url: str,
        headers: dict[str, str],
        body: bytes,
        duration_ms: int | None,
    ) -> bool:
        if len(body) > self.settings.max_response_bytes:
            raise ValueError("Response exceeds configured maximum")
        self._validate_url(final_url)
        now = _utcnow().isoformat()
        compressed = zlib.compress(body, level=6)
        normalized_headers = {
            str(key).lower()[:100]: str(value)[:2000]
            for key, value in headers.items()
        }
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE agent_jobs
                SET status = 'completed', completed_at = ?,
                    response_status = ?, response_url = ?,
                    response_headers_json = ?, response_body_zlib = ?,
                    duration_ms = ?, error_message = NULL
                WHERE job_id = ? AND status = 'leased'
                  AND leased_by = ? AND lease_id = ?
                """,
                (
                    now,
                    status_code,
                    final_url,
                    json.dumps(normalized_headers, ensure_ascii=True, separators=(",", ":")),
                    compressed,
                    duration_ms,
                    job_id,
                    agent_id,
                    lease_id,
                ),
            )
            if cursor.rowcount:
                self._event(conn, "job_completed", agent_id=agent_id, job_id=job_id)
            return bool(cursor.rowcount)

    def fail_job(
        self,
        *,
        agent_id: str,
        job_id: str,
        lease_id: str,
        error_message: str,
        retryable: bool,
    ) -> bool:
        now = _utcnow()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT attempts, max_attempts
                FROM agent_jobs
                WHERE job_id = ? AND status = 'leased'
                  AND leased_by = ? AND lease_id = ?
                """,
                (job_id, agent_id, lease_id),
            ).fetchone()
            if row is None:
                return False
            should_retry = retryable and int(row["attempts"]) < int(row["max_attempts"])
            status = "queued" if should_retry else "failed"
            available_at = (
                now + timedelta(seconds=min(30, 2 ** int(row["attempts"])))
            ).isoformat()
            conn.execute(
                """
                UPDATE agent_jobs
                SET status = ?, available_at = ?, completed_at = ?,
                    leased_at = NULL, lease_id = NULL,
                    lease_expires_at = NULL, leased_by = NULL,
                    error_message = ?
                WHERE job_id = ?
                """,
                (
                    status,
                    available_at,
                    None if should_retry else now.isoformat(),
                    _safe_detail(error_message),
                    job_id,
                ),
            )
            conn.execute(
                """
                UPDATE agent_nodes
                SET failure_count = failure_count + 1,
                    last_failure_at = ?, last_failure_reason = ?
                WHERE agent_id = ?
                """,
                (now.isoformat(), _safe_detail(error_message), agent_id),
            )
            self._event(
                conn,
                "job_requeued" if should_retry else "job_failed",
                agent_id=agent_id,
                job_id=job_id,
                detail=_safe_detail(error_message),
            )
            return True

    def get_result(self, job_id: str) -> RemoteFetchResponse | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM agent_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise AgentJobFailed("Agent job disappeared")
        if row["status"] == "failed":
            raise AgentJobFailed(str(row["error_message"] or "Agent job failed"))
        if row["status"] == "cancelled":
            raise AgentJobFailed("Agent job was cancelled")
        if row["status"] != "completed":
            return None
        if row["response_body_zlib"] is None:
            raise AgentJobFailed("Agent result body has expired")
        body = zlib.decompress(row["response_body_zlib"])
        return RemoteFetchResponse(
            job_id=job_id,
            agent_id=str(row["leased_by"]),
            status_code=int(row["response_status"]),
            final_url=str(row["response_url"]),
            headers=json.loads(row["response_headers_json"] or "{}"),
            body=body,
            duration_ms=row["duration_ms"],
        )

    def get_job_status(self, job_id: str) -> str:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT status FROM agent_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise AgentJobFailed("Agent job disappeared")
        return str(row["status"])

    def cancel_job(self, job_id: str, reason: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE agent_jobs
                SET status = 'cancelled', completed_at = ?, error_message = ?
                WHERE job_id = ? AND status IN ('queued', 'leased')
                """,
                (_utcnow().isoformat(), _safe_detail(reason), job_id),
            )

    def record_outcome(self, job_id: str, outcome: str, reason: str | None = None) -> None:
        if outcome not in {"success", "blocked", "failure"}:
            raise ValueError(f"Unknown agent outcome: {outcome}")
        now = _utcnow()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT leased_by, source FROM agent_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None or row["leased_by"] is None:
                return
            agent_id = str(row["leased_by"])
            source = str(row["source"])
            conn.execute(
                "UPDATE agent_jobs SET outcome = ? WHERE job_id = ?",
                (outcome, job_id),
            )
            if outcome == "success":
                conn.execute(
                    """
                    UPDATE agent_nodes
                    SET success_count = success_count + 1, last_success_at = ?
                    WHERE agent_id = ?
                    """,
                    (now.isoformat(), agent_id),
                )
                conn.execute(
                    """
                    INSERT INTO agent_source_health (
                        agent_id, source, success_count, last_success_at
                    ) VALUES (?, ?, 1, ?)
                    ON CONFLICT(agent_id, source) DO UPDATE SET
                        success_count = success_count + 1,
                        last_success_at = excluded.last_success_at
                    """,
                    (agent_id, source, now.isoformat()),
                )
            else:
                bench = (
                    now + timedelta(seconds=self.settings.blocked_cooldown_seconds)
                ).isoformat() if outcome == "blocked" else None
                conn.execute(
                    """
                    UPDATE agent_nodes
                    SET failure_count = failure_count + 1,
                        blocked_count = blocked_count + ?,
                        last_failure_at = ?,
                        last_failure_reason = ?
                    WHERE agent_id = ?
                    """,
                    (
                        1 if outcome == "blocked" else 0,
                        now.isoformat(),
                        _safe_detail(reason),
                        agent_id,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO agent_source_health (
                        agent_id, source, benched_until,
                        failure_count, blocked_count,
                        last_failure_at, last_failure_reason
                    ) VALUES (?, ?, ?, 1, ?, ?, ?)
                    ON CONFLICT(agent_id, source) DO UPDATE SET
                        benched_until = CASE
                            WHEN excluded.benched_until IS NULL
                            THEN agent_source_health.benched_until
                            ELSE excluded.benched_until
                        END,
                        failure_count = failure_count + 1,
                        blocked_count = blocked_count + excluded.blocked_count,
                        last_failure_at = excluded.last_failure_at,
                        last_failure_reason = excluded.last_failure_reason
                    """,
                    (
                        agent_id,
                        source,
                        bench,
                        1 if outcome == "blocked" else 0,
                        now.isoformat(),
                        _safe_detail(reason),
                    ),
                )
            self._event(
                conn,
                f"request_{outcome}",
                agent_id=agent_id,
                job_id=job_id,
                detail=_safe_detail(reason),
            )

    def snapshot(self, *, recent_limit: int = 50) -> dict[str, Any]:
        now = _utcnow()
        online_threshold = (
            now - timedelta(seconds=self.settings.heartbeat_ttl_seconds)
        ).isoformat()
        now_iso = now.isoformat()
        with self.connection() as conn:
            self._maintenance(conn, now)
            node_rows = conn.execute(
                "SELECT * FROM agent_nodes ORDER BY last_seen_at DESC"
            ).fetchall()
            source_health_rows = conn.execute(
                """
                SELECT *
                FROM agent_source_health
                ORDER BY agent_id, source
                """
            ).fetchall()
            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM agent_jobs
                GROUP BY status
                """
            ).fetchall()
            source_job_rows = conn.execute(
                """
                SELECT
                    source,
                    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_jobs,
                    SUM(CASE WHEN status = 'leased' THEN 1 ELSE 0 END) AS leased_jobs,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_jobs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_jobs,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_jobs,
                    MAX(COALESCE(completed_at, leased_at, created_at)) AS last_activity_at
                FROM agent_jobs
                GROUP BY source
                ORDER BY source
                """
            ).fetchall()
            source_outcome_rows = conn.execute(
                """
                SELECT
                    source,
                    SUM(success_count) AS successes,
                    SUM(failure_count) AS failures,
                    SUM(blocked_count) AS blocked,
                    MAX(
                        CASE
                            WHEN last_success_at IS NULL THEN last_failure_at
                            WHEN last_failure_at IS NULL THEN last_success_at
                            WHEN last_success_at >= last_failure_at THEN last_success_at
                            ELSE last_failure_at
                        END
                    ) AS last_outcome_at
                FROM agent_source_health
                GROUP BY source
                ORDER BY source
                """
            ).fetchall()
            job_rows = conn.execute(
                """
                SELECT job_id, source, capability, url, status, created_at,
                       leased_by, attempts, response_status, duration_ms,
                       error_message, outcome
                FROM agent_jobs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()
        source_health_by_agent: dict[str, list[dict[str, Any]]] = {}
        for row in source_health_rows:
            source_health_by_agent.setdefault(str(row["agent_id"]), []).append(dict(row))
        nodes = []
        for row in node_rows:
            online = row["last_seen_at"] >= online_threshold
            benched = row["benched_until"] is not None and row["benched_until"] > now_iso
            attempts = int(row["success_count"]) + int(row["failure_count"])
            nodes.append(
                {
                    **dict(row),
                    "capabilities": json.loads(row["capabilities_json"]),
                    "source_health": source_health_by_agent.get(str(row["agent_id"]), []),
                    "status": "benched" if benched else ("online" if online else "offline"),
                    "success_rate": (
                        int(row["success_count"]) / attempts if attempts else None
                    ),
                }
            )
        statuses = {row["status"]: int(row["count"]) for row in status_rows}
        source_summaries: dict[str, dict[str, Any]] = {}
        for row in source_job_rows:
            source = str(row["source"])
            source_summaries[source] = {
                "source": source,
                "queued_jobs": int(row["queued_jobs"] or 0),
                "leased_jobs": int(row["leased_jobs"] or 0),
                "completed_jobs": int(row["completed_jobs"] or 0),
                "failed_jobs": int(row["failed_jobs"] or 0),
                "cancelled_jobs": int(row["cancelled_jobs"] or 0),
                "successes": 0,
                "failures": 0,
                "blocked": 0,
                "last_activity_at": row["last_activity_at"],
            }
        for row in source_outcome_rows:
            source = str(row["source"])
            summary = source_summaries.setdefault(
                source,
                {
                    "source": source,
                    "queued_jobs": 0,
                    "leased_jobs": 0,
                    "completed_jobs": 0,
                    "failed_jobs": 0,
                    "cancelled_jobs": 0,
                    "successes": 0,
                    "failures": 0,
                    "blocked": 0,
                    "last_activity_at": None,
                },
            )
            summary["successes"] = int(row["successes"] or 0)
            summary["failures"] = int(row["failures"] or 0)
            summary["blocked"] = int(row["blocked"] or 0)
            last_outcome_at = row["last_outcome_at"]
            if (
                last_outcome_at
                and (
                    summary["last_activity_at"] is None
                    or last_outcome_at > summary["last_activity_at"]
                )
            ):
                summary["last_activity_at"] = last_outcome_at
        jobs = []
        for row in job_rows:
            parsed = urlparse(str(row["url"]))
            jobs.append(
                {
                    **dict(row),
                    "host": parsed.hostname or "",
                }
            )
        return {
            "generated_at": now,
            "enabled": self.settings.enabled,
            "online_capacity": self.available_capacity("http"),
            "online_nodes": sum(node["status"] == "online" for node in nodes),
            "offline_nodes": sum(node["status"] == "offline" for node in nodes),
            "benched_nodes": sum(node["status"] == "benched" for node in nodes),
            "queued_jobs": statuses.get("queued", 0),
            "leased_jobs": statuses.get("leased", 0),
            "completed_jobs": statuses.get("completed", 0),
            "failed_jobs": statuses.get("failed", 0),
            "cancelled_jobs": statuses.get("cancelled", 0),
            "sources": list(source_summaries.values()),
            "nodes": nodes,
            "jobs": jobs,
        }

    def _validate_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Agent jobs require an absolute HTTP(S) URL")
        host = parsed.hostname.lower().rstrip(".")
        allowed = {
            configured.lower().rstrip(".")
            for configured in self.settings.allowed_hosts
        }
        if host not in allowed:
            raise ValueError(f"Agent job host is not allowed: {host}")

    def _maintenance(self, conn: sqlite3.Connection, now: datetime) -> None:
        now_iso = now.isoformat()
        conn.execute(
            """
            UPDATE agent_jobs
            SET status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
                available_at = ?,
                completed_at = CASE WHEN attempts < max_attempts THEN NULL ELSE ? END,
                error_message = CASE
                    WHEN attempts < max_attempts THEN error_message
                    ELSE 'Agent lease expired too many times'
                END,
                lease_id = NULL,
                lease_expires_at = NULL,
                leased_at = CASE WHEN attempts < max_attempts THEN NULL ELSE leased_at END,
                leased_by = CASE WHEN attempts < max_attempts THEN NULL ELSE leased_by END
            WHERE status = 'leased' AND lease_expires_at <= ?
            """,
            (now_iso, now_iso, now_iso),
        )
        body_cutoff = (
            now - timedelta(hours=self.settings.response_body_retention_hours)
        ).isoformat()
        metadata_cutoff = (
            now - timedelta(days=self.settings.job_metadata_retention_days)
        ).isoformat()
        conn.execute(
            """
            UPDATE agent_jobs
            SET response_body_zlib = NULL
            WHERE status = 'completed'
              AND completed_at < ?
              AND response_body_zlib IS NOT NULL
            """,
            (body_cutoff,),
        )
        conn.execute(
            """
            DELETE FROM agent_jobs
            WHERE status IN ('completed', 'failed', 'cancelled')
              AND completed_at < ?
            """,
            (metadata_cutoff,),
        )
        conn.execute(
            "DELETE FROM agent_events WHERE occurred_at < ?",
            (metadata_cutoff,),
        )

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        event_type: str,
        *,
        agent_id: str | None = None,
        job_id: str | None = None,
        detail: str | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO agent_events (occurred_at, agent_id, event_type, job_id, detail)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_utcnow().isoformat(), agent_id, event_type, job_id, detail),
        )


class AgentPoolClient:
    def __init__(self, settings: AgentPoolSettings) -> None:
        self.settings = settings
        self.store = AgentPoolStore(settings)

    def fetch(
        self,
        *,
        source: str,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_seconds: int,
        priority: int = 0,
        capability: str = "http",
    ) -> RemoteFetchResponse:
        if not self.settings.enabled:
            raise AgentPoolUnavailable("Agent pool is disabled")
        if self.settings.sources and source not in self.settings.sources:
            raise AgentPoolUnavailable(f"Agent pool is not enabled for source {source}")
        if not self.store.has_eligible_agent(capability, source=source):
            raise AgentPoolUnavailable(
                f"No online agent is available for capability {capability}"
            )
        job_id = self.store.submit_job(
            source=source,
            capability=capability,
            method=method,
            url=url,
            headers=headers,
            body=body,
            timeout_seconds=timeout_seconds,
            priority=priority,
        )
        started = time.monotonic()
        queue_deadline = started + self.settings.queue_wait_seconds
        deadline = started + self.settings.job_wait_seconds
        while True:
            result = self.store.get_result(job_id)
            if result is not None:
                return result
            now = time.monotonic()
            status = self.store.get_job_status(job_id)
            if status == "queued" and now >= queue_deadline:
                self.store.cancel_job(job_id, "No agent leased the job in time")
                raise AgentPoolUnavailable("No agent leased the job in time")
            if now >= deadline:
                break
            time.sleep(self.settings.result_poll_interval_seconds)
        self.store.cancel_job(job_id, "Crawler timed out waiting for agent result")
        raise AgentPoolUnavailable("Timed out waiting for agent result")
