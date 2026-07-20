from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from alphapulse.pipeline.contracts import CrawlTask, ItemReference, SeedDefinition
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.guba.adapter import GubaAdapter
from alphapulse.sources.guba.api import GubaHttpResult, classify_block
from alphapulse.sources.guba.parser import parse_cn_datetime
from alphapulse.storage.rawstore import RawResponseStore

FIXTURES = Path(__file__).parent.parent / "fixtures" / "guba"
BASE = "https://guba.eastmoney.com"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FakeGubaClient:
    def __init__(
        self,
        get_responses: dict[str, GubaHttpResult] | None = None,
        reply_pages: dict[int, GubaHttpResult] | None = None,
    ) -> None:
        self.get_responses = get_responses or {}
        self.reply_pages = reply_pages or {}
        self.get_calls: list[str] = []
        self.get_markers: list[str | None] = []
        self.reply_calls: list[tuple[str, str, int]] = []

    def get(self, url: str, *, expect_marker: str | None = None) -> GubaHttpResult:
        self.get_calls.append(url)
        self.get_markers.append(expect_marker)
        return self.get_responses[url]

    def post_replies(self, *, post_id: str, board_code: str, page: int) -> GubaHttpResult:
        self.reply_calls.append((post_id, board_code, page))
        return self.reply_pages[page]


def _adapter(tmp_path, client: FakeGubaClient, **settings_overrides) -> GubaAdapter:
    settings = GubaSettings(enabled=True, **settings_overrides)
    return GubaAdapter(
        settings,
        CrawlSettings(),
        client=client,  # type: ignore[arg-type]
        raw_store=RawResponseStore(tmp_path / "raw"),
    )


def _ok(url: str, text: str) -> GubaHttpResult:
    return GubaHttpResult(url=url, status_code=200, text=text, duration_ms=100)


def _discover_task(url: str, board_code: str, page: int = 1) -> CrawlTask:
    return CrawlTask(
        source="guba",
        kind="discover",
        url=url,
        seed_name="test-seed",
        priority=120,
        metadata={"seed_kind": "board", "board_code": board_code, "page": page},
    )


def _fetch_log_rows(adapter: GubaAdapter):
    with adapter.raw_store.connection() as conn:
        return conn.execute("SELECT * FROM fetch_log ORDER BY id").fetchall()


def test_discover_consumes_only_guba_buckets(tmp_path) -> None:
    adapter = _adapter(tmp_path, FakeGubaClient())
    seed = SeedDefinition(
        name="test-seed",
        guba_board_codes=["zssh000001", "600519"],
        stock_ids=["SH600519"],
        topic_ids=["新能源"],
        post_urls=[
            "https://guba.eastmoney.com/news,600519,42.html",
            "https://xueqiu.com/1234/5678",
        ],
    )
    tasks = adapter.discover(seed)

    discover_urls = [str(task.url) for task in tasks if task.kind == "discover"]
    assert discover_urls == [
        f"{BASE}/list,zssh000001.html",
        f"{BASE}/list,600519.html",
    ]
    post_tasks = [task for task in tasks if task.kind == "fetch_post"]
    assert len(post_tasks) == 1
    assert str(post_tasks[0].url) == f"{BASE}/news,600519,42.html"
    assert len(tasks) == 3


def test_list_page_emits_posts_comments_and_next_page(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    client = FakeGubaClient({list_url: _ok(list_url, _read("list_stock.html"))})
    adapter = _adapter(tmp_path, client)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519"))

    assert not outcome.blocked
    assert outcome.errors == []
    by_kind: dict[str, list[CrawlTask]] = {}
    for task in outcome.discovered_tasks:
        by_kind.setdefault(task.kind, []).append(task)

    posts = by_kind["fetch_post"]
    assert [task.metadata["post_id"] for task in posts] == [
        "1743987733",
        "1743507860",
        "1741989503",
    ]
    # Entry without stockbar_code falls back to the board code.
    assert str(posts[1].url) == f"{BASE}/news,600519,1743507860.html"
    # pubdate_ts comes from post_last_time so resurfaced posts sort first.
    expected_ts = int(parse_cn_datetime("2026-07-15 11:20:33").timestamp())
    assert posts[1].metadata["pubdate_ts"] == expected_ts

    refreshes = by_kind["refresh_comments"]
    assert [task.metadata["post_id"] for task in refreshes] == ["1743987733", "1743507860"]
    assert refreshes[1].metadata["canonical_url"] == f"{BASE}/news,600519,1743507860.html"
    assert refreshes[1].priority == 300

    next_pages = by_kind["discover"]
    assert len(next_pages) == 1
    assert str(next_pages[0].url) == f"{BASE}/list,600519_2.html"
    assert next_pages[0].metadata["page"] == 2

    rows = _fetch_log_rows(adapter)
    assert len(rows) == 1
    assert rows[0]["task_kind"] == "discover"
    assert rows[0]["status_code"] == 200
    assert rows[0]["content_sha256"] is not None


def test_list_pagination_stops_at_max_pages(tmp_path) -> None:
    list_url = f"{BASE}/list,600519_3.html"
    client = FakeGubaClient({list_url: _ok(list_url, _read("list_stock.html"))})
    adapter = _adapter(tmp_path, client, max_list_pages=3)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519", page=3))
    assert all(task.kind != "discover" for task in outcome.discovered_tasks)


def test_list_page_without_payload_records_empty_payload(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    client = FakeGubaClient({list_url: _ok(list_url, "<html>maintenance</html>")})
    adapter = _adapter(tmp_path, client)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519"))
    assert outcome.discovered_tasks == []
    assert outcome.errors
    rows = _fetch_log_rows(adapter)
    assert rows[0]["block_kind"] == "empty_payload"
    assert rows[0]["parser_error"] is not None


def test_blocked_response_marks_outcome(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    blocked = GubaHttpResult(
        url=list_url,
        status_code=403,
        text="",
        blocked=True,
        block_kind="http_403",
        error_message="HTTP 403",
    )
    client = FakeGubaClient({list_url: blocked})
    adapter = _adapter(tmp_path, client)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519"))
    assert outcome.blocked
    assert outcome.status_code == 403
    rows = _fetch_log_rows(adapter)
    assert rows[0]["block_kind"] == "http_403"


def test_post_detail_parses_and_records_mod_count(tmp_path) -> None:
    url = f"{BASE}/news,600519,1743987733.html"
    client = FakeGubaClient({url: _ok(url, _read("post_detail.html"))})
    adapter = _adapter(tmp_path, client)

    task = CrawlTask(
        source="guba",
        kind="fetch_post",
        url=url,
        seed_name="test-seed",
        metadata={"post_id": "1743987733", "board_code": "600519"},
    )
    outcome = adapter.fetch_item(task)

    assert len(outcome.posts) == 1
    assert outcome.posts[0].source_entity_id == "1743987733"
    assert len(outcome.authors) == 1
    rows = _fetch_log_rows(adapter)
    meta = json.loads(rows[0]["meta_json"])
    assert meta["post_mod_count"] == 2
    assert meta["post_mod_time"] == "2026-07-15 15:10:00"
    assert rows[0]["block_kind"] is None


def test_deleted_post_redirected_to_error_page(tmp_path) -> None:
    url = f"{BASE}/news,600519,999999999999.html"
    response = GubaHttpResult(
        url=f"{BASE}/error?type=2",
        status_code=200,
        text=_read("post_deleted.html"),
    )
    client = FakeGubaClient({url: response})
    adapter = _adapter(tmp_path, client)

    task = CrawlTask(
        source="guba",
        kind="fetch_post",
        url=url,
        seed_name="test-seed",
        metadata={"post_id": "999999999999", "board_code": "600519"},
    )
    outcome = adapter.fetch_item(task)

    assert outcome.posts == []
    assert not outcome.blocked
    assert any("deleted" in error for error in outcome.errors)
    rows = _fetch_log_rows(adapter)
    assert rows[0]["block_kind"] == "deleted"
    assert json.loads(rows[0]["meta_json"])["final_url"] == f"{BASE}/error?type=2"


def test_refresh_comments_paginates_and_flattens(tmp_path) -> None:
    payload = {**json.loads(_read("replies.json")), "count": 6}
    getdata = f"{BASE}/interface/GetData.aspx"
    client = FakeGubaClient(
        reply_pages={
            1: _ok(getdata, json.dumps(payload, ensure_ascii=False)),
            2: _ok(getdata, json.dumps({"re": [], "count": 6})),
        }
    )
    adapter = _adapter(tmp_path, client, reply_page_size=3)

    item_ref = ItemReference(
        source="guba",
        source_entity_id="1743300821",
        canonical_url=f"{BASE}/news,zssh000001,1743300821.html",
        metadata={"post_id": "1743300821", "board_code": "zssh000001"},
    )
    comments = adapter.refresh_comments(item_ref)

    # Fixture has 3 raw top-level replies (one deleted) + 1 child. Page size 3
    # means a second page is requested and stops on the empty payload.
    assert [comment.source_entity_id for comment in comments] == [
        "9926112093",
        "9926134826",
        "9926194424",
    ]
    assert comments[1].parent_comment_entity_id == "9926112093"
    assert client.reply_calls == [
        ("1743300821", "zssh000001", 1),
        ("1743300821", "zssh000001", 2),
    ]
    rows = _fetch_log_rows(adapter)
    assert all(row["method"] == "POST" for row in rows)
    assert len(rows) == 2


def test_refresh_comments_stops_when_page_not_full(tmp_path) -> None:
    payload = json.loads(_read("replies.json"))
    getdata = f"{BASE}/interface/GetData.aspx"
    client = FakeGubaClient(reply_pages={1: _ok(getdata, json.dumps(payload, ensure_ascii=False))})
    adapter = _adapter(tmp_path, client, reply_page_size=30)

    item_ref = ItemReference(
        source="guba",
        source_entity_id="1743300821",
        canonical_url=f"{BASE}/news,zssh000001,1743300821.html",
        metadata={},
    )
    comments = adapter.refresh_comments(item_ref)
    assert len(comments) == 3
    assert client.reply_calls == [("1743300821", "zssh000001", 1)]


def test_comment_task_matches_list_emitted_url(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    detail_url = f"{BASE}/news,600519,1743987733.html"
    client = FakeGubaClient(
        {
            list_url: _ok(list_url, _read("list_stock.html")),
            detail_url: _ok(detail_url, _read("post_detail.html")),
        }
    )
    adapter = _adapter(tmp_path, client)

    list_outcome = adapter.fetch_item(_discover_task(list_url, "600519"))
    list_refresh = next(
        task
        for task in list_outcome.discovered_tasks
        if task.kind == "refresh_comments" and task.metadata["post_id"] == "1743987733"
    )

    detail_task = CrawlTask(
        source="guba",
        kind="fetch_post",
        url=detail_url,
        seed_name="test-seed",
        metadata={"post_id": "1743987733", "board_code": "600519"},
    )
    detail_outcome = adapter.fetch_item(detail_task)
    from_post = adapter.comment_task_for_post(detail_outcome.posts[0], "test-seed")

    # Same claim URL => the state store gates both trigger paths together.
    assert str(from_post.url) == str(list_refresh.url)
    assert from_post.metadata["post_id"] == list_refresh.metadata["post_id"]
    assert from_post.metadata["canonical_url"] == list_refresh.metadata["canonical_url"]


def test_classify_block() -> None:
    assert classify_block(403, "", "https://guba.eastmoney.com/x") == "http_403"
    assert classify_block(429, "", "https://guba.eastmoney.com/x") == "http_429"
    assert classify_block(502, "", "https://guba.eastmoney.com/x") == "http_5xx"
    assert (
        classify_block(200, "", "https://passport2.eastmoney.com/pub/login")
        == "login_redirect"
    )
    assert classify_block(200, "<html>请输入验证码</html>", "https://guba.eastmoney.com/x") == "captcha"
    normal_page = "<html>" + "content " * 2000 + "em_capt.js 验证码</html>"
    assert classify_block(200, normal_page, "https://guba.eastmoney.com/list,600519.html") is None
    assert classify_block(200, "<html>ok</html>", "https://guba.eastmoney.com/x") is None


def test_fetches_pass_soft_block_markers(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    detail_url = f"{BASE}/news,600519,1743987733.html"
    client = FakeGubaClient(
        {
            list_url: _ok(list_url, _read("list_stock.html")),
            detail_url: _ok(detail_url, _read("post_detail.html")),
        }
    )
    adapter = _adapter(tmp_path, client)

    adapter.fetch_item(_discover_task(list_url, "600519"))
    adapter.fetch_item(
        CrawlTask(
            source="guba",
            kind="fetch_post",
            url=detail_url,
            seed_name="test-seed",
            metadata={"post_id": "1743987733", "board_code": "600519"},
        )
    )
    assert client.get_markers == ["var article_list", "var post_article"]


def _client_with_dispatch(monkeypatch, pages: list[tuple[int, str, str]], max_retries: int = 3):
    from alphapulse.sources.guba import api as guba_api

    settings = GubaSettings(
        enabled=True,
        max_retries=max_retries,
        request_interval_min_seconds=0.0,
        request_interval_max_seconds=0.0,
    )
    client = guba_api.GubaClient(settings, CrawlSettings())
    calls: list[str] = []

    def fake_dispatch(method, url, form, referer, proxy_url):
        calls.append(url)
        return pages[min(len(calls), len(pages)) - 1]

    monkeypatch.setattr(client, "_dispatch", fake_dispatch)
    monkeypatch.setattr(guba_api.time, "sleep", lambda _: None)
    return client, calls


def test_soft_block_when_expected_marker_missing(monkeypatch) -> None:
    url = f"{BASE}/list,600519.html"
    client, calls = _client_with_dispatch(
        monkeypatch, [(200, "<html>access denied by waf</html>", url)], max_retries=3
    )
    result = client.get(url, expect_marker="var article_list")

    assert result.blocked
    assert result.block_kind == "soft_block"
    assert len(calls) == 3  # retried up to max_retries


def test_soft_block_recovers_on_retry(monkeypatch) -> None:
    url = f"{BASE}/list,600519.html"
    good = "<html><script>var article_list={\"re\":[]};</script></html>"
    client, calls = _client_with_dispatch(
        monkeypatch,
        [(200, "<html>access denied</html>", url), (200, good, url)],
        max_retries=3,
    )
    result = client.get(url, expect_marker="var article_list")

    assert not result.blocked
    assert result.block_kind is None
    assert len(calls) == 2


def test_marker_check_skips_error_redirects(monkeypatch) -> None:
    url = f"{BASE}/news,600519,999.html"
    client, calls = _client_with_dispatch(
        monkeypatch, [(200, "<html>gone</html>", f"{BASE}/error?type=2")]
    )
    result = client.get(url, expect_marker="var article_list")

    assert not result.blocked
    assert result.block_kind is None
    assert len(calls) == 1


def test_adaptive_sleep_backoff(monkeypatch) -> None:
    from alphapulse.sources.guba import api as guba_api

    settings = GubaSettings(
        enabled=True,
        request_interval_min_seconds=1.0,
        request_interval_max_seconds=1.0,
    )
    client = guba_api.GubaClient(settings, CrawlSettings())

    sleeps: list[float] = []
    monkeypatch.setattr(guba_api.time, "sleep", sleeps.append)
    monkeypatch.setattr(guba_api.random, "uniform", lambda low, high: high)

    client._adaptive_sleep(was_rate_limited=False)
    client._adaptive_sleep(was_rate_limited=True)
    client._adaptive_sleep(was_rate_limited=True)
    client._adaptive_sleep(was_rate_limited=False)

    assert sleeps == [1.0, 2.0, 4.0, 2.0]
