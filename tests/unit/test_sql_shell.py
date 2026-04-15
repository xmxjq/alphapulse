from pathlib import Path

from alphapulse.cli.sql_shell import SqlExecutor, is_read_query, statement_complete
from alphapulse.runtime.config import load_settings


def test_read_query_detection() -> None:
    assert is_read_query("SELECT * FROM posts")
    assert is_read_query(" show tables")
    assert not is_read_query("INSERT INTO posts VALUES (1)")


def test_statement_complete_requires_trailing_semicolon() -> None:
    assert statement_complete(["SELECT *", "FROM posts;"])
    assert not statement_complete(["SELECT *", "FROM posts"])


def test_sql_executor_uses_configured_backend() -> None:
    settings = load_settings(Path("settings.example.toml"))
    executor = SqlExecutor(settings)
    assert executor.backend == "rqlite"
    assert "sqlite_master" in executor.tables_sql()
