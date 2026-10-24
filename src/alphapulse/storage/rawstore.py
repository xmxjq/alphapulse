from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from alphapulse.runtime.config import Settings


MIN_RETENTION_DAYS = 1
MAX_BLOB_RETENTION_DAYS = 365
MAX_FETCH_LOG_RETENTION_DAYS = 3650


@dataclass
class FetchRecord:
    source: str
    url: str
    method: str
    status_code: int | None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    duration_ms: int | None = None
    task_kind: str | None = None
    block_kind: str | None = None
    parser_error: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class PruneStats:
    blobs_deleted: int = 0
    bytes_deleted: int = 0
    fetch_log_rows_deleted: int = 0
    shards_removed: int = 0  # kept for output compatibility; shard dirs stay
    skipped: bool = False


def _check_retention_days(value: int, *, maximum: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer, got {value!r}")
    if not MIN_RETENTION_DAYS <= value <= maximum:
        raise ValueError(
            f"{label} must be between {MIN_RETENTION_DAYS} and {maximum} days, got {value}"
        )
    return value


def _check_optional_retention_days(
    value: int | None, *, maximum: int, label: str
) -> int | None:
    if value is None:
        return None
    return _check_retention_days(value, maximum=maximum, label=label)


class RawResponseStore:
    """Content-addressed archive of raw HTTP responses plus a fetch log.

    Bodies live as gzip blobs under ``{root}/blobs/{sha256[:2]}/{sha256}.gz``
    (identical bodies are stored once); per-fetch metadata lives in
    ``{root}/fetch_log.db``. Independent of the StorageStore backends so it
    works the same for mongo/rqlite/clickhouse deployments.
    """

    def __init__(self, root: Path, *, compress: bool = True) -> None:
        self.root = root
        self.compress = compress
        self.blobs_dir = root / "blobs"
        self.db_path = root / "fetch_log.db"
        self.blobs_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
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
                CREATE TABLE IF NOT EXISTS fetch_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    task_kind TEXT,
                    status_code INTEGER,
                    duration_ms INTEGER,
                    content_sha256 TEXT,
                    content_length INTEGER,
                    block_kind TEXT,
                    parser_error TEXT,
                    meta_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_fetch_log_source_time
                    ON fetch_log (source, fetched_at);

                CREATE INDEX IF NOT EXISTS idx_fetch_log_sha
                    ON fetch_log (content_sha256);

                CREATE TABLE IF NOT EXISTS raw_store_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    def save(self, record: FetchRecord, body: bytes | str | None) -> str | None:
        sha: str | None = None
        length: int | None = None
        if body is not None:
            data = body.encode("utf-8") if isinstance(body, str) else body
            length = len(data)
            sha = hashlib.sha256(data).hexdigest()
            self._write_blob(sha, data)
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO fetch_log (
                    fetched_at, source, url, method, task_kind, status_code,
                    duration_ms, content_sha256, content_length, block_kind,
                    parser_error, meta_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.fetched_at.isoformat(),
                    record.source,
                    record.url,
                    record.method,
                    record.task_kind,
                    record.status_code,
                    record.duration_ms,
                    sha,
                    length,
                    record.block_kind,
                    record.parser_error,
                    json.dumps(record.meta, ensure_ascii=False, default=str),
                ),
            )
        return sha

    def load_body(self, sha256: str) -> bytes | None:
        path = self._blob_path(sha256)
        if not path.exists():
            return None
        raw = path.read_bytes()
        if path.suffix == ".gz":
            return gzip.decompress(raw)
        return raw

    def _write_blob(self, sha256: str, data: bytes) -> None:
        path = self._blob_path(sha256)
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = gzip.compress(data) if self.compress else data
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_bytes(payload)
        tmp_path.replace(path)

    def _blob_path(self, sha256: str) -> Path:
        suffix = ".gz" if self.compress else ".bin"
        return self.blobs_dir / sha256[:2] / f"{sha256}{suffix}"


    def prune(
        self,
        *,
        retention_days: int,
        fetch_log_retention_days: int | None = None,
        dry_run: bool = False,
        min_interval: timedelta = timedelta(hours=6),
        now: datetime | None = None,
    ) -> PruneStats:
        """Delete fetch-log rows and blob bodies past their retention windows.

        Metadata outlives bodies, mirroring response_body_retention_hours versus
        job_metadata_retention_days: a blob file is removed once it is older than
        retention_days, so load_body can miss for a row still in the log.

        Emptied shard directories are kept: removing one races with a
        concurrent save that already created the directory and is about to
        write its blob, which would fail with FileNotFoundError.
        """
        retention_days = _check_retention_days(
            retention_days, maximum=MAX_BLOB_RETENTION_DAYS, label="retention_days"
        )
        fetch_log_retention_days = _check_optional_retention_days(
            fetch_log_retention_days,
            maximum=MAX_FETCH_LOG_RETENTION_DAYS,
            label="fetch_log_retention_days",
        )
        moment = now or datetime.now(UTC)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        stats = PruneStats()
        if (
            not dry_run
            and min_interval.total_seconds() > 0
            and self._pruned_recently(moment, min_interval)
        ):
            stats.skipped = True
            return stats

        if fetch_log_retention_days is not None:
            cutoff = (moment - timedelta(days=fetch_log_retention_days)).isoformat()
            with self.connection() as conn:
                if dry_run:
                    row = conn.execute(
                        "SELECT COUNT(*) AS total FROM fetch_log WHERE fetched_at < ?",
                        (cutoff,),
                    ).fetchone()
                    stats.fetch_log_rows_deleted = int(row["total"])
                else:
                    cursor = conn.execute(
                        "DELETE FROM fetch_log WHERE fetched_at < ?", (cutoff,)
                    )
                    stats.fetch_log_rows_deleted = max(cursor.rowcount, 0)

        suffix = ".gz" if self.compress else ".bin"
        blob_cutoff = (moment - timedelta(days=retention_days)).timestamp()
        for shard in sorted(self.blobs_dir.iterdir()):
            if not shard.is_dir():
                continue
            for blob in shard.iterdir():
                if blob.suffix != suffix:
                    continue
                try:
                    blob_stat = blob.stat()
                except FileNotFoundError:
                    continue
                if blob_stat.st_mtime >= blob_cutoff:
                    continue
                stats.blobs_deleted += 1
                stats.bytes_deleted += blob_stat.st_size
                if not dry_run:
                    blob.unlink(missing_ok=True)

        if not dry_run:
            self._record_prune(moment)
        return stats

    def _pruned_recently(self, moment: datetime, min_interval: timedelta) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT value FROM raw_store_meta WHERE key = 'last_prune'"
            ).fetchone()
        if row is None:
            return False
        try:
            last = datetime.fromisoformat(str(row["value"]))
        except ValueError:
            return False
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        return moment - last < min_interval

    def _record_prune(self, moment: datetime) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO raw_store_meta (key, value) VALUES ('last_prune', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (moment.isoformat(),),
            )


def build_raw_store(settings: "Settings") -> RawResponseStore | None:
    raw_settings = settings.crawl.raw_store
    if not raw_settings.enabled:
        return None
    return RawResponseStore(raw_settings.root_path, compress=raw_settings.compress)
