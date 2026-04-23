from datetime import UTC, datetime

from alphapulse.pipeline.contracts import NormalizedComment, NormalizedPost
from alphapulse.runtime.config import MongoSettings
from alphapulse.storage.mongo import MongoStore


class FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name
        self.bulk_writes: list[list] = []
        self.inserted: list[dict] = []
        self.replacements: list[tuple[dict, dict, bool]] = []
        self.indexes: list[tuple[list[tuple[str, int]], dict]] = []

    def bulk_write(self, operations, ordered: bool = True):  # noqa: ARG002
        self.bulk_writes.append(list(operations))

    def insert_one(self, doc):
        self.inserted.append(doc)

    def replace_one(self, filter_, replacement, upsert: bool = False):
        self.replacements.append((filter_, replacement, upsert))

    def create_index(self, keys, **options):
        self.indexes.append((list(keys), options))


class FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection(name))


class FakeAdmin:
    def __init__(self) -> None:
        self.pings = 0

    def command(self, name: str):
        assert name == "ping"
        self.pings += 1
        return {"ok": 1}


class FakeMongoClient:
    def __init__(self) -> None:
        self.databases: dict[str, FakeDatabase] = {}
        self.admin = FakeAdmin()

    def __getitem__(self, name: str) -> FakeDatabase:
        return self.databases.setdefault(name, FakeDatabase())


def _make_store() -> tuple[MongoStore, FakeMongoClient]:
    client = FakeMongoClient()
    store = MongoStore(MongoSettings(), client=client)
    return store, client


def test_init_db_creates_expected_indexes() -> None:
    store, client = _make_store()
    store.init_db()
    db = client.databases["alphapulse"]
    posts = db.collections["posts"]
    comments = db.collections["comments"]
    runs = db.collections["crawl_runs"]
    errors = db.collections["crawl_errors"]

    assert any(keys == [("source", 1), ("published_at", -1)] for keys, _ in posts.indexes)
    assert any(
        keys == [("source", 1), ("post_entity_id", 1), ("published_at", 1)]
        for keys, _ in comments.indexes
    )
    assert any(keys == [("started_at", -1)] for keys, _ in runs.indexes)
    assert any(keys == [("created_at", -1)] for keys, _ in errors.indexes)


def test_post_upsert_dedupes_by_source_entity_id() -> None:
    store, client = _make_store()
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

    operations = client.databases["alphapulse"].collections["posts"].bulk_writes[0]
    assert len(operations) == 1
    op = operations[0]
    assert op._filter == {"_id": "xueqiu:987654321"}
    assert op._doc["content_text"] == "b"
    assert op._doc["canonical_url"] == "https://xueqiu.com/1234567890/987654321"
    assert op._upsert is True


def test_comment_upsert_writes_replace_one_ops() -> None:
    store, client = _make_store()
    ts = datetime.now(UTC)
    comment = NormalizedComment(
        source="xueqiu",
        source_entity_id="c1",
        post_entity_id="p1",
        content_text="hi",
        fetched_at=ts,
    )
    store.upsert_comments([comment])
    operations = client.databases["alphapulse"].collections["comments"].bulk_writes[0]
    assert len(operations) == 1
    assert operations[0]._doc["post_entity_id"] == "p1"


def test_insert_crawl_run_upserts_by_run_id() -> None:
    store, client = _make_store()
    started = datetime(2026, 4, 22, 12, 0, tzinfo=UTC)
    finished = datetime(2026, 4, 22, 12, 1, tzinfo=UTC)
    stats = {
        "seeds_processed": 1,
        "tasks_enqueued": 2,
        "pages_fetched": 3,
        "posts_written": 4,
        "comments_written": 5,
        "authors_written": 6,
        "blocked_responses": 0,
        "errors": 0,
        "skipped_tasks": 0,
    }
    store.insert_crawl_run(
        run_id="run-1",
        started_at=started,
        finished_at=finished,
        stats=stats,
        status="succeeded",
    )
    replacements = client.databases["alphapulse"].collections["crawl_runs"].replacements
    assert replacements[0][0] == {"_id": "run-1"}
    assert replacements[0][1]["posts_written"] == 4
    assert replacements[0][2] is True


def test_insert_crawl_error_writes_document() -> None:
    store, client = _make_store()
    store.insert_crawl_error(source="xueqiu", url="https://x", error_message="boom")
    inserted = client.databases["alphapulse"].collections["crawl_errors"].inserted
    assert inserted[0]["source"] == "xueqiu"
    assert inserted[0]["error_message"] == "boom"


def test_healthcheck_pings_admin() -> None:
    store, client = _make_store()
    assert store.healthcheck() is True
    assert client.admin.pings == 1
