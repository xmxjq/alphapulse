from datetime import UTC, datetime

from alphapulse.pipeline.contracts import NormalizedPost
from alphapulse.runtime.config import RqliteSettings
from alphapulse.storage.rqlite import RqliteStore


class FakeClient:
    def __init__(self) -> None:
        self.executed: list[list[str | list]] = []
        self.queries: list[str] = []

    def execute(self, statements):
        self.executed.append(statements)
        return {"results": [{"rows_affected": 1}]}

    def query(self, sql: str):
        self.queries.append(sql)
        return {"results": [{"values": [[1]]}]}


def test_init_db_emits_schema_statements() -> None:
    client = FakeClient()
    store = RqliteStore(RqliteSettings(), client=client)
    store.init_db()
    assert any("CREATE TABLE IF NOT EXISTS posts" in statement for statement in client.executed[0])


def test_post_upsert_dedupes_by_source_entity_id() -> None:
    client = FakeClient()
    store = RqliteStore(RqliteSettings(), client=client)
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
    statements = client.executed[0]
    assert len(statements) == 1
    assert statements[0][6] == "b"


def test_healthcheck_queries_rqlite() -> None:
    client = FakeClient()
    store = RqliteStore(RqliteSettings(), client=client)
    assert store.healthcheck() is True
    assert client.queries == ["SELECT 1"]
