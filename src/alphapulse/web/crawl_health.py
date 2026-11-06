"""Bounded, read-only signals for an out-of-process crawl watchdog."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sqlite3
import time
from typing import Iterator

from alphapulse.runtime.config import Settings


@contextmanager
def _read_only(path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    deadline = time.monotonic() + 3
    conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        yield conn
    finally:
        conn.close()


def crawl_health(
    settings: Settings, *, last_cycle_at: datetime | None = None,
    cycle_readable: bool = True, now: datetime | None = None,
) -> dict:
    now = now or datetime.now(UTC)
    errors: list[str] = [] if cycle_readable else ["cycle_store_unavailable"]
    recent_guards: set[str] = set()
    names = [name for name in ("guba", "tgb", "jiuyan", "hupu", "bilibili", "xueqiu")
             if getattr(settings.sources, name).enabled]
    sources = {name: {"name": name, "pending_tasks": 0,
                      "last_attempt_at": None, "last_success_at": None} for name in names}
    if settings.crawl.state_backend != "sqlite":
        errors.append("unsupported_state_backend")
    else:
        try:
            with _read_only(settings.crawl.state_path) as conn:
                for row in conn.execute("SELECT source, count(*) n FROM pending_tasks GROUP BY source"):
                    if row["source"] in sources:
                        sources[row["source"]]["pending_tasks"] = row["n"]
                # url_state timestamps are optimistic while a task is claimed.
                # Raw-enabled sources below use completed responses instead.
                for name in names:
                    if name in {"bilibili", "xueqiu"}:
                        row = conn.execute(
                            "SELECT max(last_fetched_at) last_at FROM url_state "
                            "WHERE source=? AND last_status>=200 AND last_status<400",
                            (name,),
                        ).fetchone()
                        sources[name]["last_success_at"] = row["last_at"]
        except (OSError, sqlite3.Error):
            errors.append("state_store_unavailable")
    if settings.crawl.raw_store.enabled:
        try:
            with _read_only(settings.crawl.raw_store.root_path / "fetch_log.db") as conn:
                for name in names:
                    if name in {"bilibili", "xueqiu"}:
                        continue
                    row = conn.execute(
                        "SELECT max(fetched_at) last_at, max(CASE WHEN status_code>=200 "
                        "AND status_code<400 AND coalesce(block_kind,'')='' "
                        "AND coalesce(parser_error,'')='' THEN fetched_at END) success_at "
                        "FROM fetch_log WHERE source=? AND fetched_at>=?",
                        (name, (now - timedelta(days=1)).isoformat()),
                    ).fetchone()
                    sources[name]["last_attempt_at"] = row["last_at"]
                    sources[name]["last_success_at"] = row["success_at"]
                    for label in ("hourly", "daily", "no-success"):
                        hit = conn.execute(
                            "SELECT 1 FROM fetch_log WHERE source=? AND fetched_at>=? "
                            "AND instr(coalesce(parser_error,'') || meta_json,?)>0 LIMIT 1",
                            (name, (now - timedelta(minutes=10)).isoformat(),
                             label + " extraction budget exhausted"),
                        ).fetchone()
                        if hit:
                            recent_guards.add(label.replace("-", "_"))
        except (OSError, sqlite3.Error):
            errors.append("fetch_store_unavailable")
    elif any(name in sources for name in ("guba", "tgb", "jiuyan", "hupu")):
        errors.append("raw_monitoring_disabled")
    proxy = {"enabled": settings.crawl.proxy.enabled and settings.crawl.proxy.provider == "kuaidaili",
             "auth_blocked": False, "budget_guards": []}
    if proxy["enabled"]:
        k = settings.crawl.kuaidaili
        proxy["auth_blocked"] = k.metrics_path.with_name(k.metrics_path.name + ".auth-blocked").exists()
        try:
            with _read_only(k.metrics_path) as conn:
                # Check the shared durable reservation ledger, not successful
                # extractions only: rejected provider calls also spend budget.
                # Force reverse row-ID order: the provider/time index otherwise
                # makes this latest-event lookup scan the whole provider history.
                last_success = conn.execute(
                    "SELECT id FROM proxy_events NOT INDEXED "
                    "WHERE provider='kuaidaili' AND event_type='request_success' "
                    "ORDER BY id DESC LIMIT 1",
                ).fetchone()
                last_success_id = last_success[0] if last_success else 0
                usage = conn.execute(
                    "SELECT coalesce(sum(CASE WHEN occurred_at>? THEN count ELSE 0 END),0) hourly, "
                    "coalesce(sum(count),0) daily FROM proxy_events WHERE provider='kuaidaili' "
                    "AND event_type='extraction_reserved' AND occurred_at>?",
                    ((now - timedelta(hours=1)).isoformat(), (now - timedelta(days=1)).isoformat()),
                ).fetchone()
                without_success = conn.execute(
                    "SELECT coalesce(sum(count),0) FROM proxy_events NOT INDEXED WHERE provider='kuaidaili' "
                    "AND event_type='extraction_reserved' AND id>?", (last_success_id,),
                ).fetchone()[0]
                for label, value, limit in (
                    ("hourly", usage["hourly"], k.max_extracted_per_hour),
                    ("daily", usage["daily"], k.max_extracted_per_day),
                    ("no_success", without_success, k.max_extracted_without_success),
                ):
                    if value >= limit or label in recent_guards:
                        proxy["budget_guards"].append(label)
        except (OSError, sqlite3.Error):
            errors.append("proxy_store_unavailable")
    return {"schema_version": 1, "generated_at": now.isoformat(), "probe_errors": errors,
            "poll_interval_seconds": settings.crawl.poll_interval_seconds,
            "last_cycle_at": last_cycle_at.isoformat() if last_cycle_at else None,
            "sources": list(sources.values()), "proxy": proxy}
