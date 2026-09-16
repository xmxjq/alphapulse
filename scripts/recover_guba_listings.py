"""Recover today's Guba board associations and stale 544 tasks from local archives."""

import argparse
from datetime import UTC, datetime, timedelta
import gzip
import json
import logging
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from alphapulse.pipeline.contracts import CrawlTask
from alphapulse.runtime.config import load_settings
from alphapulse.runtime.state import StateStore
from alphapulse.sources.guba.parser import parse_article_list
from alphapulse.sources.guba.urls import normalize_board_code, post_detail_url
from alphapulse.storage.mongo import MongoStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    settings = load_settings(args.config)
    if settings.storage.backend != "mongo" or settings.crawl.state_backend != "sqlite":
        raise ValueError("This repair requires the existing MongoDB/SQLite deployment")
    now = datetime.now(UTC)
    day = now.astimezone(ZoneInfo(settings.sources.guba.ranking_timezone)).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    root = Path(settings.crawl.raw_store.root_path)
    with sqlite3.connect((root / "fetch_log.db").resolve().as_uri() + "?mode=ro", uri=True) as raw:
        raw.row_factory = sqlite3.Row
        rows = raw.execute(
            "SELECT url, content_sha256 FROM fetch_log WHERE source='guba' "
            "AND fetched_at>=? AND task_kind='discover' AND status_code=200 "
            "ORDER BY fetched_at DESC LIMIT 4000",
            (day.astimezone(UTC).isoformat(),),
        ).fetchall()
    memberships: dict[str, set[str]] = {}
    entries = {}
    seen_urls = set()
    for row in rows:
        url = row["url"]
        if "/list," not in url or url in seen_urls:
            continue
        seen_urls.add(url)
        sha = row["content_sha256"]
        if not sha:
            continue
        path = root / "blobs" / sha[:2] / (sha + (".gz" if settings.crawl.raw_store.compress else ".bin"))
        if not path.exists():
            continue
        body = path.read_bytes()
        text = (gzip.decompress(body) if settings.crawl.raw_store.compress else body).decode(errors="replace")
        data = parse_article_list(text)
        if data is None:
            continue
        board = normalize_board_code(url.split("/list,", 1)[1].split(".", 1)[0].split("_", 1)[0])
        if not board:
            continue
        for entry in data.entries:
            if entry.publish_time is None or not (day <= entry.publish_time <= now):
                continue
            if entry.post_state not in (None, 0):
                continue
            memberships.setdefault(entry.post_id, set()).add(board)
            entries[entry.post_id] = entry

    store = MongoStore(settings.mongo)
    collection = store._collection(settings.mongo.posts_collection)
    present = {
        str(row["_id"]).split(":", 1)[1]
        for row in collection.find(
            {"_id": {"$in": ["guba:" + post_id for post_id in memberships]}}, {"_id": 1},
        )
    }
    stale = (now - timedelta(minutes=10)).isoformat()
    pending = []
    with sqlite3.connect(Path(settings.crawl.state_path).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        for post_id in memberships.keys() - present:
            entry = entries[post_id]
            code = entry.stockbar_code
            if not code:
                continue
            url = post_detail_url(str(settings.sources.guba.base_url), code, post_id)
            row = conn.execute(
                "SELECT seed_name FROM url_state WHERE source='guba' AND kind='fetch_post' "
                "AND url=? AND last_status=544 AND last_fetched_at<?", (url, stale),
            ).fetchone()
            if row is None:
                continue
            boards = sorted(memberships[post_id])
            listing = next((board for board in boards if board.startswith("BK")), boards[0])
            pending.append(CrawlTask(source="guba", kind="fetch_post", url=url,
                seed_name=row[0] or "cn-core", priority=150, metadata={
                    "post_id": post_id, "board_code": normalize_board_code(code),
                    "listing_board_code": listing, "title": entry.title,
                    "pubdate_ts": int(entry.publish_time.timestamp()),
                    "incident_recovery": day.date().isoformat(),
                }))
    out = {"checked_at": now.isoformat(), "day": day.date().isoformat(), "apply": args.apply,
        "archive_rows": len(rows), "unique_list_pages": len(seen_urls),
        "listed_today_posts": len(memberships), "existing_posts": len(present),
        "eligible_stale_544_tasks": len(pending)}
    if args.apply:
        # Update existing posts only: never create partial/bodyless documents.
        store.add_post_board_memberships("guba", {k: sorted(v) for k, v in memberships.items() if k in present})
        state = StateStore(settings.crawl.state_path)
        released = []
        with state.connection() as conn:
            for task in pending:
                result = conn.execute(
                    "UPDATE url_state SET last_fetched_at=NULL,last_status=NULL "
                    "WHERE source='guba' AND kind='fetch_post' AND url=? "
                    "AND last_status=544 AND last_fetched_at<?", (str(task.url), stale),
                )
                if result.rowcount:
                    released.append(task)
        state.upsert_pending_tasks(released)
        out["queued_tasks"] = len(released)
        out["queued_post_ids"] = [task.metadata["post_id"] for task in released]
        out["memberships_verified"] = all(
            set(memberships[str(row["_id"]).split(":", 1)[1]]).issubset(row.get("raw_topic_ids", []))
            for row in collection.find({"_id": {"$in": ["guba:" + x for x in present]}},
                                       {"_id": 1, "raw_topic_ids": 1})
        )
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error_type": type(exc).__name__}))
        raise SystemExit(1)
