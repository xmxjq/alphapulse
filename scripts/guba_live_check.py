"""Live smoke test for the Eastmoney Guba adapter.

Drives the real adapter code paths (list discovery -> post details -> reply
refresh) against guba.eastmoney.com from a fixed IP with single concurrency
and randomized request intervals, then prints a crawl report: success rate,
status distribution, block events, parser errors, timing, and a proxy verdict.

Usage:
    uv run python scripts/guba_live_check.py \
        --boards zssh000001,600519 --list-pages 2 --details 15 --reply-posts 5
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from alphapulse.pipeline.contracts import CrawlTask, ItemReference
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.guba.adapter import GubaAdapter
from alphapulse.sources.guba.urls import board_list_url
from alphapulse.storage.rawstore import RawResponseStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--boards", default="zssh000001,600519", help="Comma-separated board codes")
    parser.add_argument("--list-pages", type=int, default=2, help="List pages per board")
    parser.add_argument("--details", type=int, default=15, help="Post details to fetch (total)")
    parser.add_argument("--reply-posts", type=int, default=5, help="Posts to refresh replies for")
    parser.add_argument("--raw-dir", default=".runtime/raw-livecheck", help="Raw store directory")
    parser.add_argument("--interval-min", type=float, default=2.0)
    parser.add_argument("--interval-max", type=float, default=6.0)
    parser.add_argument(
        "--proxy",
        action="append",
        default=[],
        metavar="URL",
        help="Route requests through this proxy via the static_list provider "
        "(e.g. http://127.0.0.1:10809); repeat for a rotating pool",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    boards = [code.strip() for code in args.boards.split(",") if code.strip()]

    settings = GubaSettings(
        enabled=True,
        max_list_pages=args.list_pages,
        request_interval_min_seconds=args.interval_min,
        request_interval_max_seconds=args.interval_max,
    )
    crawl_settings = CrawlSettings()
    if args.proxy:
        crawl_settings.proxy.enabled = True
        crawl_settings.proxy.provider = "static_list"
        crawl_settings.static_proxies.urls = list(args.proxy)
    raw_store = RawResponseStore(Path(args.raw_dir))
    adapter = GubaAdapter(settings, crawl_settings, raw_store=raw_store)

    started = datetime.now(UTC)
    posts_parsed = 0
    comments_parsed = 0
    published_ats: list[datetime] = []
    errors: list[str] = []
    blocked_urls: list[str] = []

    # Phase 1: list pages (discover), collecting fetch_post / refresh_comments tasks.
    post_tasks: list[CrawlTask] = []
    refresh_tasks: list[CrawlTask] = []
    for board in boards:
        page_tasks = [
            CrawlTask(
                source="guba",
                kind="discover",
                url=board_list_url(str(settings.base_url), board),
                seed_name="livecheck",
                metadata={"seed_kind": "board", "board_code": board, "page": 1},
            )
        ]
        while page_tasks:
            task = page_tasks.pop(0)
            print(f"[list] {task.url}")
            outcome = adapter.fetch_item(task)
            errors.extend(outcome.errors)
            if outcome.blocked:
                blocked_urls.append(str(task.url))
            for discovered in outcome.discovered_tasks:
                if discovered.kind == "discover":
                    page_tasks.append(discovered)
                elif discovered.kind == "fetch_post":
                    post_tasks.append(discovered)
                elif discovered.kind == "refresh_comments":
                    refresh_tasks.append(discovered)

    # Phase 2: post details, spread across boards.
    seen_posts: set[str] = set()
    detail_targets = []
    for task in post_tasks:
        post_id = str(task.metadata.get("post_id"))
        if post_id in seen_posts:
            continue
        seen_posts.add(post_id)
        detail_targets.append(task)
    detail_targets = detail_targets[: args.details]
    for task in detail_targets:
        print(f"[post] {task.url}")
        outcome = adapter.fetch_item(task)
        errors.extend(outcome.errors)
        if outcome.blocked:
            blocked_urls.append(str(task.url))
        posts_parsed += len(outcome.posts)
        for post in outcome.posts:
            if post.published_at is not None:
                published_ats.append(post.published_at)

    # Phase 3: reply refresh for posts with comments (highest counts first).
    refresh_targets = sorted(
        {task.metadata["post_id"]: task for task in refresh_tasks}.values(),
        key=lambda task: task.metadata.get("pubdate_ts") or 0,
        reverse=True,
    )[: args.reply_posts]
    for task in refresh_targets:
        print(f"[replies] post {task.metadata['post_id']}")
        comments = adapter.refresh_comments(
            ItemReference(
                source="guba",
                source_entity_id=task.metadata["post_id"],
                canonical_url=task.metadata["canonical_url"],
                metadata=task.metadata,
            )
        )
        comments_parsed += len(comments)
        for comment in comments:
            if comment.published_at is not None:
                published_ats.append(comment.published_at)

    wall_seconds = (datetime.now(UTC) - started).total_seconds()

    # Report from the fetch log this run just wrote.
    with raw_store.connection() as conn:
        rows = conn.execute(
            "SELECT status_code, duration_ms, block_kind, parser_error, url FROM fetch_log"
            " WHERE fetched_at >= ?",
            (started.isoformat(),),
        ).fetchall()

    total = len(rows)
    status_counts = Counter(row["status_code"] for row in rows)
    block_counts = Counter(row["block_kind"] for row in rows if row["block_kind"])
    parser_errors = [row["parser_error"] for row in rows if row["parser_error"]]
    hard_blocks = sum(
        count
        for kind, count in block_counts.items()
        if kind in {"http_403", "http_429", "captcha", "login_redirect"}
    )
    ok = sum(count for status, count in status_counts.items() if status == 200) - hard_blocks
    success_rate = (ok / total * 100) if total else 0.0
    durations = sorted(row["duration_ms"] for row in rows if row["duration_ms"] is not None)

    def pct(values: list[int], q: float) -> int | None:
        if not values:
            return None
        return values[min(len(values) - 1, int(len(values) * q))]

    print("\n" + "=" * 62)
    print("GUBA LIVE CHECK REPORT")
    print("=" * 62)
    print(f"boards:            {', '.join(boards)}")
    if args.proxy:
        print(f"egress:            static_list proxies: {', '.join(args.proxy)}")
    print(f"wall time:         {wall_seconds:.0f}s")
    print(f"requests:          {total}")
    print(f"success rate:      {success_rate:.1f}%")
    print(f"status dist:       {dict(status_counts)}")
    print(f"block events:      {dict(block_counts) or 'none'}")
    if blocked_urls:
        for url in blocked_urls:
            print(f"  blocked url:     {url}")
    print(f"parser errors:     {len(parser_errors)}")
    for message in parser_errors[:5]:
        print(f"  parser error:    {message}")
    print(f"posts parsed:      {posts_parsed}")
    print(f"comments parsed:   {comments_parsed}")
    if published_ats:
        print(f"published_at min:  {min(published_ats).isoformat()}")
        print(f"published_at max:  {max(published_ats).isoformat()}")
    if durations:
        avg = sum(durations) / len(durations)
        print(f"latency ms:        min={durations[0]} avg={avg:.0f} p95={pct(durations, 0.95)} max={durations[-1]}")
    print(f"adapter errors:    {len(errors)}")
    for message in errors[:5]:
        print(f"  adapter error:   {message}")

    if hard_blocks:
        verdict = (
            "BLOCKING OBSERVED - re-run at lower frequency; consider proxies only if"
            " blocking repeats across runs at conservative intervals."
        )
    else:
        verdict = "no IP-based blocking observed; fixed IP + conservative jitter is sufficient (no proxy pool needed)."
    print(f"proxy verdict:     {verdict}")
    print(f"raw store:         {raw_store.root} ({total} fetch_log rows this run)")

    failed = success_rate < 90.0 or any(
        kind in block_counts for kind in ("captcha", "login_redirect")
    )
    print(f"result:            {'FAIL' if failed else 'PASS'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
