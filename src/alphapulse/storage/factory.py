from __future__ import annotations

from alphapulse.runtime.config import Settings
from alphapulse.storage.base import StorageStore
from alphapulse.storage.clickhouse import ClickHouseStore
from alphapulse.storage.mongo import MongoStore
from alphapulse.storage.rqlite import RqliteStore


def build_store(settings: Settings) -> StorageStore:
    if settings.storage.backend == "rqlite":
        return RqliteStore(settings.rqlite)
    if settings.storage.backend == "mongo":
        return MongoStore(settings.mongo)
    return ClickHouseStore(settings.clickhouse)
