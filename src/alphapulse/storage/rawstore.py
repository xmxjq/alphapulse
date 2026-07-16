from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from alphapulse.runtime.config import Settings


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
        conn = sqlite3.connect(self.db_path)
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


def build_raw_store(settings: "Settings") -> RawResponseStore | None:
    raw_settings = settings.crawl.raw_store
    if not raw_settings.enabled:
        return None
    return RawResponseStore(raw_settings.root_path, compress=raw_settings.compress)
