from alphapulse.runtime.config import RqliteSettings
from alphapulse.runtime.rqlite_state import RqliteStateStore


class FakeRqliteClient:
    def __init__(self) -> None:
        self.executed: list[tuple[list, bool]] = []

    def execute(self, statements, queued=False):
        self.executed.append((statements, queued))
        return {"results": [{"rows_affected": 1}]}


def test_release_url_claim_clears_fetch_state() -> None:
    client = FakeRqliteClient()
    state = RqliteStateStore(RqliteSettings(), client=client)

    state.release_url_claim("https://guba.eastmoney.com/news,600519,42.html")

    statements, queued = client.executed[0]
    assert queued is False
    assert "last_fetched_at = NULL" in statements[0][0]
    assert statements[0][1] == "https://guba.eastmoney.com/news,600519,42.html"
