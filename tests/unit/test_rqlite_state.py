import json

from alphapulse.pipeline.contracts import CrawlTask
from alphapulse.runtime.config import RqliteSettings
from alphapulse.runtime.rqlite_state import RqliteStateStore


class FakeRqliteClient:
    def __init__(self) -> None:
        self.executed: list[tuple[list, bool]] = []
        self.query_response = {"results": [{"values": []}]}

    def execute(self, statements, queued=False):
        self.executed.append((statements, queued))
        return {"results": [{"rows_affected": 1}]}

    def query_params(self, statements):
        self.queried = statements
        return self.query_response


def test_release_url_claim_clears_fetch_state() -> None:
    client = FakeRqliteClient()
    state = RqliteStateStore(RqliteSettings(), client=client)

    state.release_url_claim("https://guba.eastmoney.com/news,600519,42.html")

    statements, queued = client.executed[0]
    assert queued is False
    assert "last_fetched_at = NULL" in statements[0][0]
    assert statements[0][1] == "https://guba.eastmoney.com/news,600519,42.html"


def test_pending_tasks_use_parameterized_rqlite_statements() -> None:
    client = FakeRqliteClient()
    state = RqliteStateStore(RqliteSettings(), client=client)
    task = CrawlTask(
        source="guba",
        kind="fetch_post",
        url="https://guba.eastmoney.com/news,600519,42.html",
        seed_name="cn-core",
        priority=150,
        metadata={"pubdate_ts": 123},
    )

    state.upsert_pending_tasks([task])

    statements, queued = client.executed[0]
    assert queued is False
    assert "INSERT INTO pending_tasks" in statements[0][0]
    assert statements[0][1] == task.dedupe_key
    assert json.loads(statements[0][7])["metadata"]["pubdate_ts"] == 123

    client.query_response = {
        "results": [
            {
                "values": [
                    [json.dumps(task.model_dump(mode="json"))],
                ]
            }
        ]
    }
    loaded = state.load_pending_tasks("cn-core")
    assert [item.dedupe_key for item in loaded] == [task.dedupe_key]
    assert client.queried[0][1] == "cn-core"

    state.delete_pending_task(task.dedupe_key)
    delete_statements, delete_queued = client.executed[1]
    assert delete_queued is False
    assert "DELETE FROM pending_tasks" in delete_statements[0][0]
    assert delete_statements[0][1] == task.dedupe_key


def test_pending_pruning_and_failure_tracking_are_parameterized() -> None:
    client = FakeRqliteClient()
    state = RqliteStateStore(RqliteSettings(), client=client)

    assert state.prune_pending_tasks_outside_pubdate_range(
        source="guba",
        kind="fetch_post",
        start_ts=100,
        end_ts=200,
    ) == 1
    prune_statement = client.executed[0][0][0]
    assert "DELETE FROM pending_tasks" in prune_statement[0]
    assert prune_statement[1:] == ["guba", "guba:fetch_post:%", 100, 200]

    client.query_response = {"results": [{"values": [[2]]}]}
    attempts = state.record_task_failure(
        dedupe_key="jiuyan:fetch_post:https://www.jiuyangongshe.com/a/blocked",
        source="jiuyan",
        failure_kind="captcha",
    )
    assert attempts == 2
    failure_statement = client.executed[1][0][0]
    assert "INSERT INTO task_failures" in failure_statement[0]
    assert failure_statement[1:4] == [
        "jiuyan:fetch_post:https://www.jiuyangongshe.com/a/blocked",
        "jiuyan",
        "captcha",
    ]

    state.clear_task_failures(
        "jiuyan:fetch_post:https://www.jiuyangongshe.com/a/blocked"
    )
    clear_statement = client.executed[2][0][0]
    assert "DELETE FROM task_failures" in clear_statement[0]


def test_jiuyan_ranking_uses_parameterized_statements() -> None:
    client = FakeRqliteClient()
    state = RqliteStateStore(RqliteSettings(), client=client)
    state.replace_jiuyan_ranking(
        "2026-07-29",
        [
            {
                "section": "hot",
                "rank": 1,
                "code": "机器人",
                "name": "机器人",
                "url": "https://www.jiuyangongshe.com/search/new?k=robot",
                "members": None,
            }
        ],
    )
    statements, queued = client.executed[0]
    assert queued is False
    assert "DELETE FROM jiuyan_daily_ranking" in statements[0][0]
    assert "INSERT INTO jiuyan_daily_ranking" in statements[1][0]
    assert statements[1][4] == "机器人"
