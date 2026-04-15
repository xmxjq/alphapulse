from datetime import UTC, datetime

from alphapulse.pipeline.contracts import NormalizedPost
from alphapulse.runtime.config import ClickHouseSettings
from alphapulse.storage.clickhouse import ClickHouseStore


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list, list[str]]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, rows: list, column_names: list[str]) -> None:
        self.inserts.append((table, rows, column_names))

    def query(self, sql: str) -> FakeQueryResult:
        assert sql == "SELECT 1"
        return FakeQueryResult([[1]])


def test_init_db_emits_schema_statements() -> None:
    client = FakeClient()
    store = ClickHouseStore(ClickHouseSettings(), client=client)
    store.init_db()
    assert any("CREATE TABLE IF NOT EXISTS alphapulse.posts" in sql for sql in client.commands)


def test_post_upsert_dedupes_by_source_entity_id() -> None:
    client = FakeClient()
    store = ClickHouseStore(ClickHouseSettings(), client=client)
    ts = datetime.now(UTC)
    post = NormalizedPost(
        source="xueqiu",
        source_entity_id="987654321",
        canonical_url="https://xueqiu.com/1234567890/987654321",
        content_text="a",
        fetched_at=ts,
    )
    newer = post.model_copy(update={"content_text": "b"})
    store.upsert_posts([post, newer])
    table, rows, _columns = client.inserts[0]
    assert table.endswith(".posts")
    assert len(rows) == 1
    assert rows[0][5] == "b"

