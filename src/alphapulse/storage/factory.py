from __future__ import annotations

from alphapulse.runtime.config import Settings
from alphapulse.storage.base import StorageStore
from alphapulse.storage.clickhouse import ClickHouseStore
from alphapulse.storage.rqlite import RqliteStore


def build_store(settings: Settings) -> StorageStore:
    if settings.storage.backend == "rqlite":
        return RqliteStore(settings.rqlite)
    return ClickHouseStore(settings.clickhouse)

