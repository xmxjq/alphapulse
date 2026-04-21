from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from alphapulse.cli.sql_shell import run_once, run_repl, SqlExecutor
from alphapulse.runtime.config import Settings, load_settings
from alphapulse.runtime.logging import configure_logging
from alphapulse.runtime.service import AlphaPulseService
from alphapulse.seeds.catalog import SeedCatalogLoader


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="alphapulse")
    parser.add_argument("--config", default="settings.toml", help="Path to TOML config file.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the long-lived crawler service.")
    run_parser.add_argument("--once", action="store_true", help="Run a single crawl cycle and exit.")

    backfill_parser = subparsers.add_parser("backfill", help="Run a single crawl cycle for one seed set.")
    backfill_parser.add_argument("--seed-set", required=True, help="Seed set name.")
    refresh_parser = subparsers.add_parser("refresh-seeds", help="Refresh generated seed sets from the seed catalog.")
    refresh_parser.add_argument("--seed-set", help="Refresh only one logical seed set.")
    sql_parser = subparsers.add_parser("sql", help="Run SQL or start an interactive SQL shell.")
    sql_parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    sql_parser.add_argument("sql", nargs="?", help="SQL statement to run. If omitted, start the shell.")

    subparsers.add_parser("validate-config", help="Validate config and print normalized settings.")
    subparsers.add_parser("init-db", help="Create configured storage schema.")
    subparsers.add_parser("health", help="Check configured storage connectivity and local state.")
    return parser


def _load_runtime(args: argparse.Namespace) -> tuple[Settings, AlphaPulseService]:
    settings = load_settings(Path(args.config))
    configure_logging(settings.crawl.log_level)
    return settings, AlphaPulseService(settings)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate-config":
        settings = load_settings(Path(args.config))
        catalog = SeedCatalogLoader(settings.sources.xueqiu.seed_catalog_path).load()
        print(
            json.dumps(
                {
                    "settings": settings.model_dump(mode="json"),
                    "seed_catalog": catalog.model_dump(mode="json"),
                },
                indent=2,
            )
        )
        return 0

    if args.command == "sql":
        settings = load_settings(Path(args.config))
        executor = SqlExecutor(settings)
        if args.sql:
            return run_once(executor, args.sql, args.pretty)
        if not sys.stdin.isatty():
            sql = sys.stdin.read().strip()
            if not sql:
                raise SystemExit("No SQL provided on stdin.")
            return run_once(executor, sql, args.pretty)
        return run_repl(executor, args.pretty)

    settings, service = _load_runtime(args)

    if args.command == "init-db":
        service.store.init_db()
        service.state.init_db()
        print(f"{settings.storage.backend} storage + {settings.crawl.state_backend} state schema initialized.")
        return 0

    if args.command == "health":
        status: dict[str, object] = {
            "storage_backend": settings.storage.backend,
            "storage_ok": service.store.healthcheck(),
            "state_backend": settings.crawl.state_backend,
        }
        if settings.crawl.state_backend == "sqlite":
            status["state_path"] = str(settings.crawl.state_path)
            status["state_exists"] = settings.crawl.state_path.exists()
        print(json.dumps(status, indent=2))
        return 0 if status["storage_ok"] else 1

    if args.command == "refresh-seeds":
        result = service.seed_discovery.refresh(seed_set_name=args.seed_set)
        print(json.dumps(result.to_dict(), indent=2))
        return 0

    if args.command == "backfill":
        stats = service.run_cycle(seed_set_name=args.seed_set)
        print(json.dumps(stats.to_dict(), indent=2))
        return 0

    if args.command == "run":
        if args.once:
            stats = service.run_cycle()
            print(json.dumps(stats.to_dict(), indent=2))
            return 0
        service.run_forever()
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2
