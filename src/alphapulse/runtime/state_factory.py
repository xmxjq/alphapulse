from __future__ import annotations

from typing import Protocol, runtime_checkable

from alphapulse.runtime.config import Settings
from alphapulse.runtime.rqlite_state import RqliteStateStore
from alphapulse.runtime.state import StateStore


@runtime_checkable
class StateStoreProtocol(Protocol):
    def init_db(self) -> None: ...
    def try_claim_url(self, *, url: str, source: str, kind: str, seed_name: str, min_age) -> bool: ...
    def mark_url_fetched(self, url: str, status: int | None) -> None: ...
    def should_refresh_comments(self, source: str, source_entity_id: str, min_age) -> bool: ...
    def upsert_item(self, source: str, source_entity_id: str, canonical_url: str, metadata: dict) -> None: ...
    def mark_comments_refreshed(self, source: str, source_entity_id: str) -> None: ...


def build_state_store(settings: Settings) -> StateStore | RqliteStateStore:
    if settings.crawl.state_backend == "rqlite":
        return RqliteStateStore(settings.rqlite)
    return StateStore(settings.crawl.state_path)
