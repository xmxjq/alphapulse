from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from alphapulse.runtime.config import Settings, load_settings
from alphapulse.storage.clickhouse import _client_from_settings
from alphapulse.storage.rqlite import RqliteClient


READ_PREFIXES = ("select", "pragma", "explain", "describe", "show", "with")
EXIT_COMMANDS = {".quit", ".exit", "quit", "exit"}


def is_read_query(sql: str) -> bool:
    stripped = sql.strip().lstrip("(").lower()
    return stripped.startswith(READ_PREFIXES)


def statement_complete(buffer: list[str]) -> bool:
    text = "\n".join(buffer).strip()
    return bool(text) and text.endswith(";")


@dataclass
class SqlExecutor:
    settings: Settings

    def __post_init__(self) -> None:
        if self.settings.storage.backend == "rqlite":
            self.backend = "rqlite"
            self.client = RqliteClient(self.settings.rqlite)
        elif self.settings.storage.backend == "clickhouse":
            self.backend = "clickhouse"
            self.client = _client_from_settings(self.settings.clickhouse)
        elif self.settings.storage.backend == "mongo":
            raise ValueError(
                "SQL shell is not supported for the mongo backend; use a Mongo client (e.g. mongosh) instead."
            )
        else:
            raise ValueError(f"Unsupported storage backend: {self.settings.storage.backend}")

    def execute(self, sql: str) -> dict[str, Any]:
        sql = sql.strip()
        if self.backend == "rqlite":
            if is_read_query(sql):
                return {
                    "backend": "rqlite",
                    "mode": "query",
                    "sql": sql,
                    "result": self.client.query(sql),
                }
            return {
                "backend": "rqlite",
                "mode": "execute",
                "sql": sql,
                "result": self.client.execute([sql]),
            }

        if is_read_query(sql):
            result = self.client.query(sql)
            return {
                "backend": "clickhouse",
                "mode": "query",
                "sql": sql,
                "columns": list(result.column_names),
                "rows": result.result_rows,
                "row_count": len(result.result_rows),
            }
        command_result = self.client.command(sql)
        return {
            "backend": "clickhouse",
            "mode": "execute",
            "sql": sql,
            "result": command_result,
        }

    def tables_sql(self) -> str:
        if self.backend == "rqlite":
            return 'SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name;'
        return "SHOW TABLES;"

    def schema_sql(self, table_name: str) -> str:
        if self.backend == "rqlite":
            escaped = table_name.replace("'", "''")
            return f"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{escaped}';"
        return f"SHOW CREATE TABLE {table_name};"


def print_payload(payload: dict[str, Any], pretty: bool) -> None:
    indent = 2 if pretty else None
    print(json.dumps(payload, indent=indent, ensure_ascii=True, default=str))


def handle_meta_command(command: str, executor: SqlExecutor, pretty: bool) -> bool:
    stripped = command.strip()
    if stripped == ".help":
        print(
            "\n".join(
                [
                    "Meta commands:",
                    "  .help            show this help",
                    "  .tables          list tables",
                    "  .schema <table>  show table schema",
                    "  .quit            exit shell",
                ]
            )
        )
        return True
    if stripped == ".tables":
        print_payload(executor.execute(executor.tables_sql()), pretty)
        return True
    if stripped.startswith(".schema "):
        table_name = stripped.split(maxsplit=1)[1].strip()
        if table_name:
            print_payload(executor.execute(executor.schema_sql(table_name)), pretty)
            return True
    return False


def run_repl(executor: SqlExecutor, pretty: bool) -> int:
    print(f"AlphaPulse SQL shell connected to {executor.backend}. Type .help for commands.")
    buffer: list[str] = []
    while True:
        prompt = "sql> " if not buffer else "...> "
        try:
            line = input(prompt)
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print()
            buffer.clear()
            continue

        stripped = line.strip()
        if not buffer and stripped in EXIT_COMMANDS:
            return 0
        if not buffer and stripped.startswith("."):
            if handle_meta_command(stripped, executor, pretty):
                continue
            print(f"Unknown command: {stripped}")
            continue
        if not stripped and not buffer:
            continue

        buffer.append(line)
        if not statement_complete(buffer):
            continue

        sql = "\n".join(buffer).strip()
        buffer.clear()
        try:
            print_payload(executor.execute(sql), pretty)
        except Exception as exc:
            print(json.dumps({"error": str(exc)}, ensure_ascii=True))


def run_once(executor: SqlExecutor, sql: str, pretty: bool) -> int:
    print_payload(executor.execute(sql), pretty)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run SQL against the storage backend configured in settings.toml."
    )
    parser.add_argument(
        "--config",
        default="settings.toml",
        help="Path to config file. Defaults to settings.toml.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    parser.add_argument(
        "sql",
        nargs="?",
        help="SQL statement to run. If omitted in a TTY, an interactive shell starts.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
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

