"""Diagnose why a day's guba report has no ranking catalog.

Reads the crawler state DB and prints:
  - which days have guba_daily_ranking snapshot rows,
  - the recent guba_hot_boards generator runs (status / errors / timing),
  - when each seed set was last compiled,
  - the relevant guba/seed settings.

Run on the worker host, in the repo dir:
    docker compose exec crawler uv run python scripts/guba_snapshot_debug.py
    # or, if not in docker:
    uv run python scripts/guba_snapshot_debug.py --config settings.toml
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from alphapulse.runtime.config import load_settings


def _rows(conn: sqlite3.Connection, sql: str) -> list[dict]:
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql)]
    except sqlite3.OperationalError as exc:
        return [{"error": str(exc)}]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="/app/settings.toml", help="Path to settings.toml")
    args = parser.parse_args()

    settings = load_settings(Path(args.config))
    db = str(settings.crawl.state_path)
    guba = settings.sources.guba

    print(f"config:               {args.config}")
    print(f"state_db:             {db}  (exists: {Path(db).exists()})")
    print(f"seed_refresh_minutes: {settings.sources.xueqiu.seed_refresh_minutes}")
    print(f"generated_seed_ttl:   {settings.sources.xueqiu.generated_seed_ttl_minutes} min")
    print(f"guba.enabled:         {guba.enabled}")
    print(f"guba.day_scoped:      {guba.day_scoped}")
    print(f"guba.ranking_timezone:{guba.ranking_timezone}")
    print(f"guba.hot_per_section: {guba.hot_boards_per_section}")

    if not Path(db).exists():
        print("\n!! state DB not found — check crawl.state_path / the mounted volume")
        return 1

    conn = sqlite3.connect(db)

    print("\n== guba_daily_ranking: rows per day / section ==")
    day_rows = _rows(
        conn,
        "SELECT day, section, COUNT(*) AS n FROM guba_daily_ranking "
        "GROUP BY day, section ORDER BY day, section",
    )
    if not day_rows:
        print("  (no snapshot rows at all — generator has never written one)")
    for r in day_rows:
        print(f"  {r}")

    print("\n== recent guba_hot_boards generator runs ==")
    run_rows = _rows(
        conn,
        "SELECT started_at, finished_at, status, item_count, "
        "substr(error_message, 1, 500) AS error "
        "FROM generated_seed_runs "
        "WHERE generator_name LIKE '%guba%' OR generator_name LIKE '%hot%' "
        "ORDER BY started_at DESC LIMIT 20",
    )
    if not run_rows:
        print("  (no generator runs recorded for a guba/hot generator)")
    for r in run_rows:
        print(f"  {r}")

    print("\n== all generator names seen (to confirm the guba-hot name) ==")
    for r in _rows(
        conn,
        "SELECT generator_name, COUNT(*) AS runs, MAX(started_at) AS last_run "
        "FROM generated_seed_runs GROUP BY generator_name ORDER BY last_run DESC",
    ):
        print(f"  {r}")

    print("\n== compiled seed sets (last refresh) ==")
    for r in _rows(conn, "SELECT seed_set_name, refreshed_at FROM compiled_seed_sets"):
        print(f"  {r}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
