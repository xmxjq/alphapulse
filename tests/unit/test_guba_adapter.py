from __future__ import annotations

import json
import http.client
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


class FakeGubaBrowserClient:
    def __init__(self, responses: dict[str, GubaHttpResult]) -> None:
        self.responses = responses
        self.get_calls: list[str] = []

    def get(self, url: str) -> GubaHttpResult:
        self.get_calls.append(url)
        return self.responses[url]


def _adapter(tmp_path, client: FakeGubaClient, **settings_overrides) -> GubaAdapter:
    # These tests exercise the classic (total-count based) pagination path;
    # day-scoping is covered by its own tests below.
    settings_overrides.setdefault("day_scoped", False)
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
    assert all(task.priority == 160 for task in tasks if task.kind == "discover")
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
    assert refreshes[1].priority == 100

    next_pages = by_kind["discover"]
    assert len(next_pages) == 1
    assert str(next_pages[0].url) == f"{BASE}/list,600519_2.html"
    assert next_pages[0].metadata["page"] == 2
    assert posts[0].priority > next_pages[0].priority > refreshes[0].priority

    rows = _fetch_log_rows(adapter)
    assert len(rows) == 1
    assert rows[0]["task_kind"] == "discover"
    assert rows[0]["status_code"] == 200
    assert rows[0]["content_sha256"] is not None


def test_list_page_skips_comments_when_fetch_comments_disabled(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    client = FakeGubaClient({list_url: _ok(list_url, _read("list_stock.html"))})
    adapter = _adapter(tmp_path, client, fetch_comments=False)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519"))

    assert all(task.kind != "refresh_comments" for task in outcome.discovered_tasks)
    assert any(task.kind == "fetch_post" for task in outcome.discovered_tasks)


def test_list_pagination_stops_at_max_pages(tmp_path) -> None:
    list_url = f"{BASE}/list,600519_3.html"
    client = FakeGubaClient({list_url: _ok(list_url, _read("list_stock.html"))})
    adapter = _adapter(tmp_path, client, max_list_pages=3)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519", page=3))
    assert all(task.kind != "discover" for task in outcome.discovered_tasks)


def _list_html(entries: list[dict]) -> str:
    payload = {"bar_code": "600519", "bar_name": "贵州茅台", "count": 5000, "re": entries}
    return f"<html><script>var article_list={json.dumps(payload, ensure_ascii=False)};</script></html>"


def _entry(post_id: str, publish: str, last: str, *, comments: int = 0) -> dict:
    return {
        "post_id": post_id,
        "post_title": f"title {post_id}",
        "stockbar_code": "600519",
        "user_id": "1",
        "user_nickname": "u",
        "post_click_count": 1,
        "post_comment_count": comments,
        "post_publish_time": publish,
        "post_last_time": last,
        "post_type": 0,
        "post_state": 0,
    }


def test_concept_board_codes_are_normalized_to_ranking_case(tmp_path) -> None:
    list_url = f"{BASE}/list,BK1152.html"
    entries = [
        {
            **_entry(
                "1749169645",
                "2026-07-24 09:30:00",
                "2026-07-24 10:00:00",
            ),
            "stockbar_code": "bk1152",
        }
    ]
    client = FakeGubaClient({list_url: _ok(list_url, _list_html(entries))})
    adapter = _adapter(tmp_path, client)

    outcome = adapter.fetch_item(_discover_task(list_url, "BK1152"))

    post_task = next(
        task for task in outcome.discovered_tasks if task.kind == "fetch_post"
    )
    assert str(post_task.url) == f"{BASE}/news,bk1152,1749169645.html"
    assert post_task.metadata["board_code"] == "BK1152"


def _bj_now():
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Shanghai"))


def test_day_scoped_emits_only_todays_posts(tmp_path) -> None:
    today = _bj_now().strftime("%Y-%m-%d")
    list_url = f"{BASE}/list,600519.html"
    entries = [
        _entry("today1", f"{today} 09:30:00", f"{today} 10:00:00", comments=3),
        _entry("stale1", "2020-01-01 09:30:00", "2020-01-01 09:30:00"),
    ]
    client = FakeGubaClient({list_url: _ok(list_url, _list_html(entries))})
    adapter = _adapter(tmp_path, client, day_scoped=True)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519"))

    posts = [t.metadata["post_id"] for t in outcome.discovered_tasks if t.kind == "fetch_post"]
    assert posts == ["today1"]
    # The one comment-bearing today post gets a refresh task; stale one does not.
    assert [t.metadata["post_id"] for t in outcome.discovered_tasks if t.kind == "refresh_comments"] == ["today1"]
    # A page holding a post active today keeps paginating.
    assert any(t.kind == "discover" for t in outcome.discovered_tasks)


def test_day_scoped_stops_when_page_all_stale(tmp_path) -> None:
    list_url = f"{BASE}/list,600519.html"
    entries = [
        _entry("stale1", "2020-01-01 09:30:00", "2020-01-02 09:30:00"),
        _entry("stale2", "2019-05-01 09:30:00", "2019-05-01 09:30:00"),
    ]
    client = FakeGubaClient({list_url: _ok(list_url, _list_html(entries))})
    adapter = _adapter(tmp_path, client, day_scoped=True)

    outcome = adapter.fetch_item(_discover_task(list_url, "600519"))

    assert [t.kind for t in outcome.discovered_tasks] == []


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


def test_blocked_response_opens_circuit_without_more_requests(tmp_path) -> None:
    first_url = f"{BASE}/list,600519.html"
    second_url = f"{BASE}/list,600000.html"
    blocked = GubaHttpResult(
        url=first_url,
        status_code=200,
        text="<html>verify</html>",
        blocked=True,
        block_kind="captcha",
    )
    client = FakeGubaClient({first_url: blocked})
    adapter = _adapter(tmp_path, client, block_cooldown_seconds=3600)

    first = adapter.fetch_item(_discover_task(first_url, "600519"))
    second = adapter.fetch_item(_discover_task(second_url, "600000"))

    assert first.blocked
    assert second.blocked
    assert second.errors == []
    assert client.get_calls == [first_url]
    assert adapter.is_circuit_open()


def test_soft_blocked_response_marks_outcome_without_opening_circuit(tmp_path) -> None:
    first_url = f"{BASE}/list,600519.html"
    second_url = f"{BASE}/list,600000.html"
    soft_blocked = GubaHttpResult(
        url=first_url,
        status_code=200,
        text="<html>access denied by waf</html>",
        blocked=True,
        block_kind="soft_block",
    )
    good_second = _ok(second_url, "<html><script>var article_list={\"re\":[]};</script></html>")
    client = FakeGubaClient({first_url: soft_blocked, second_url: good_second})
    adapter = _adapter(tmp_path, client, block_cooldown_seconds=3600)

    first = adapter.fetch_item(_discover_task(first_url, "600519"))
    assert first.blocked
    assert not adapter.is_circuit_open()

    second = adapter.fetch_item(_discover_task(second_url, "600000"))
    assert not second.blocked
    assert client.get_calls == [first_url, second_url]


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


def test_post_detail_uses_browser_client_when_configured(tmp_path) -> None:
    url = f"{BASE}/news,600519,1743987733.html"
    http_client = FakeGubaClient()
    browser_client = FakeGubaBrowserClient({url: _ok(url, _read("post_detail.html"))})
    adapter = GubaAdapter(
        GubaSettings(enabled=True, day_scoped=False),
        CrawlSettings(),
        client=http_client,  # type: ignore[arg-type]
        browser_client=browser_client,  # type: ignore[arg-type]
        raw_store=RawResponseStore(tmp_path / "raw"),
    )

    outcome = adapter.fetch_item(
        CrawlTask(
            source="guba",
            kind="fetch_post",
            url=url,
            seed_name="test-seed",
            metadata={"post_id": "1743987733", "board_code": "600519"},
        )
    )

    assert len(outcome.posts) == 1
    assert browser_client.get_calls == [url]
    assert http_client.get_calls == []


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
    adapter = _adapter(tmp_path, client, reply_page_size=3, max_reply_pages=2)

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


def test_refresh_comments_defaults_to_first_page(tmp_path) -> None:
    payload = {**json.loads(_read("replies.json")), "count": 60}
    getdata = f"{BASE}/interface/GetData.aspx"
    client = FakeGubaClient(
        reply_pages={1: _ok(getdata, json.dumps(payload, ensure_ascii=False))}
    )
    adapter = _adapter(tmp_path, client, reply_page_size=3)

    item_ref = ItemReference(
        source="guba",
        source_entity_id="1743300821",
        canonical_url=f"{BASE}/news,zssh000001,1743300821.html",
        metadata={"board_code": "zssh000001"},
    )

    comments = adapter.refresh_comments(item_ref)

    assert len(comments) == 3
    assert client.reply_calls == [("1743300821", "zssh000001", 1)]
    assert len(_fetch_log_rows(adapter)) == 1


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


def test_blocked_comment_refresh_opens_circuit(tmp_path) -> None:
    blocked = GubaHttpResult(
        url=f"{BASE}/interface/GetData.aspx",
        status_code=200,
        text="<html>verify</html>",
        blocked=True,
        block_kind="captcha",
    )
    client = FakeGubaClient(reply_pages={1: blocked})
    adapter = _adapter(tmp_path, client, block_cooldown_seconds=3600)
    item_ref = ItemReference(
        source="guba",
        source_entity_id="1743987733",
        canonical_url=f"{BASE}/news,600519,1743987733.html",
        metadata={"board_code": "600519"},
    )

    comments = adapter.refresh_comments(item_ref)

    assert comments == []
    assert client.reply_calls == [("1743987733", "600519", 1)]
    assert adapter.is_circuit_open()


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
    assert from_post.priority == 100


def test_comment_task_for_post_returns_none_when_fetch_comments_disabled(tmp_path) -> None:
    detail_url = f"{BASE}/news,600519,1743987733.html"
    client = FakeGubaClient({detail_url: _ok(detail_url, _read("post_detail.html"))})
    adapter = _adapter(tmp_path, client, fetch_comments=False)

    detail_task = CrawlTask(
        source="guba",
        kind="fetch_post",
        url=detail_url,
        seed_name="test-seed",
        priority=150,
        metadata={"post_id": "1743987733", "board_code": "600519"},
    )
    detail_outcome = adapter.fetch_item(detail_task)

    assert adapter.comment_task_for_post(detail_outcome.posts[0], "test-seed") is None


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

    def fake_dispatch(method, url, form, referer, proxy_url, json_body=None):
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


def test_incomplete_proxy_response_recovers_on_retry(monkeypatch) -> None:
    from alphapulse.sources.guba import api as guba_api

    url = f"{BASE}/list,600519.html"
    good = "<html><script>var article_list={\"re\":[]};</script></html>"
    settings = GubaSettings(
        enabled=True,
        max_retries=2,
        request_interval_min_seconds=0,
        request_interval_max_seconds=0,
    )
    client = guba_api.GubaClient(settings, CrawlSettings())
    calls = 0

    def fake_dispatch(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            raise http.client.IncompleteRead(b"partial", 100)
        return 200, good, url

    monkeypatch.setattr(client, "_dispatch", fake_dispatch)
    monkeypatch.setattr(guba_api.time, "sleep", lambda _: None)

    result = client.get(url, expect_marker="var article_list")

    assert not result.blocked
    assert result.status_code == 200
    assert calls == 2


def test_transport_retry_does_not_trigger_rate_limit_backoff(monkeypatch) -> None:
    from alphapulse.sources.guba import api as guba_api

    url = f"{BASE}/list,600519.html"
    client = guba_api.GubaClient(
        GubaSettings(
            enabled=True,
            max_retries=2,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        CrawlSettings(),
    )
    calls = 0
    backoff_flags: list[bool] = []

    def fake_dispatch(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            raise http.client.IncompleteRead(b"partial", 100)
        return 200, '<script>var article_list={"re":[]};</script>', url

    monkeypatch.setattr(client, "_dispatch", fake_dispatch)
    monkeypatch.setattr(
        client,
        "_adaptive_sleep",
        lambda *, was_rate_limited: backoff_flags.append(was_rate_limited),
    )
    monkeypatch.setattr(guba_api.time, "sleep", lambda _: None)

    result = client.get(url, expect_marker="var article_list")

    assert result.status_code == 200
    assert backoff_flags == [False, False]


def test_blocked_retry_triggers_rate_limit_backoff(monkeypatch) -> None:
    url = f"{BASE}/list,600519.html"
    good = '<script>var article_list={"re":[]};</script>'
    client, _ = _client_with_dispatch(
        monkeypatch,
        [
            (200, "<html>blocked</html>", url),
            (200, good, url),
        ],
        max_retries=2,
    )
    backoff_flags: list[bool] = []
    monkeypatch.setattr(
        client,
        "_adaptive_sleep",
        lambda *, was_rate_limited: backoff_flags.append(was_rate_limited),
    )

    result = client.get(url, expect_marker="var article_list")

    assert result.status_code == 200
    assert backoff_flags == [False, True]


def test_guba_client_uses_curl_transport_for_proxy(monkeypatch) -> None:
    from alphapulse.sources.guba import api as guba_api

    calls: list[dict[str, object]] = []

    class Response:
        status_code = 200
        text = '<script>var article_list={"re":[]};</script>'
        url = f"{BASE}/list,600519.html"

    def fake_request(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        return Response()

    monkeypatch.setattr(guba_api.curl_requests, "request", fake_request)
    client = guba_api.GubaClient(GubaSettings(), CrawlSettings())

    result = client._dispatch(
        "GET",
        Response.url,
        None,
        None,
        "http://proxy.example:8080",
    )

    assert result == (200, Response.text, Response.url)
    assert calls[0]["proxy"] == "http://proxy.example:8080"
    assert calls[0]["impersonate"] == "chrome"
    assert calls[0]["allow_redirects"] is True


def test_guba_client_stops_after_complete_embedded_payload() -> None:
    from alphapulse.sources.guba import api as guba_api

    class PartialTailResponse:
        def __init__(self) -> None:
            self.calls = 0

        def read(self, amount: int) -> bytes:
            assert amount == 16 * 1024
            self.calls += 1
            if self.calls == 1:
                return (
                    b'<html><script>var article_list={"re":[],"count":0};'
                    b"</script>"
                )
            raise http.client.IncompleteRead(b"<footer", 100)

    client = guba_api.GubaClient(GubaSettings(), CrawlSettings())
    response = PartialTailResponse()

    raw = client._read_body(
        response,
        f"{BASE}/list,600519.html",
        "utf-8",
    )

    assert b"var article_list" in raw
    assert response.calls == 1


def test_guba_client_rejects_truncated_embedded_payload() -> None:
    from alphapulse.sources.guba import api as guba_api

    class TruncatedPayloadResponse:
        def read(self, amount: int) -> bytes:
            assert amount == 16 * 1024
            raise http.client.IncompleteRead(
                b'<html><script>var article_list={"re":[{"post_id":"1"}',
                100,
            )

    client = guba_api.GubaClient(GubaSettings(), CrawlSettings())

    try:
        client._read_body(
            TruncatedPayloadResponse(),
            f"{BASE}/list,600519.html",
            "utf-8",
        )
    except http.client.IncompleteRead as exc:
        assert b"article_list" in exc.partial
    else:
        raise AssertionError("truncated embedded JSON must not be accepted")


def test_guba_client_repairs_one_missing_json_closer() -> None:
    from alphapulse.sources.guba import api as guba_api

    class OneByteShortResponse:
        def read(self, amount: int) -> bytes:
            assert amount == 16 * 1024
            raise http.client.IncompleteRead(
                b'<html><script>var article_list={"re":[],"count":0',
                1,
            )

    client = guba_api.GubaClient(GubaSettings(), CrawlSettings())

    raw = client._read_body(
        OneByteShortResponse(),
        f"{BASE}/list,600519.html",
        "utf-8",
    )

    assert raw.endswith(b"}")
    assert b'"count":0}' in raw


def test_guba_client_rejects_clean_eof_before_embedded_payload_completes() -> None:
    from alphapulse.sources.guba import api as guba_api

    class CleanEofTruncatedResponse:
        def __init__(self) -> None:
            self.chunks = iter(
                [
                    b'<html><script>var article_list={"re":[{"post_id":"1"}',
                    b"",
                ]
            )

        def read(self, amount: int) -> bytes:
            assert amount == 16 * 1024
            return next(self.chunks)

    client = guba_api.GubaClient(GubaSettings(), CrawlSettings())

    try:
        client._read_body(
            CleanEofTruncatedResponse(),
            f"{BASE}/list,600519.html",
            "utf-8",
        )
    except http.client.IncompleteRead as exc:
        assert b"article_list" in exc.partial
    else:
        raise AssertionError("clean EOF with truncated JSON must not be accepted")


def test_guba_client_does_not_fail_open_without_proxy(monkeypatch, tmp_path) -> None:
    from alphapulse.sources.guba import api as guba_api

    api_file = tmp_path / "kuaidaili-api-url.txt"
    api_file.write_text("https://dps.kdlapi.com/api/getdps/?secret_id=test")
    crawl = CrawlSettings.model_validate(
        {
            "proxy": {
                "enabled": True,
                "provider": "kuaidaili",
                "sources": ["guba"],
                "fail_open": False,
            },
            "kuaidaili": {
                "api_url_file": str(api_file),
                "metrics_path": str(tmp_path / "proxy-metrics.db"),
            },
        }
    )
    client = guba_api.GubaClient(
        GubaSettings(
            enabled=True,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        crawl,
    )
    monkeypatch.setattr(client.proxy_provider, "acquire", lambda: None)
    dispatched: list[str] = []
    monkeypatch.setattr(client, "_dispatch", lambda *args: dispatched.append("called"))

    result = client.get(f"{BASE}/list,600519.html", expect_marker="var article_list")

    assert result.status_code == 0
    assert result.error_message == "No proxy available from proxy provider"
    assert dispatched == []


def test_soft_block_costs_one_paid_ip_extraction_not_three(tmp_path, monkeypatch) -> None:
    """End-to-end: a request that soft-blocks on every attempt reuses the same
    paid IP across all retries instead of forcing a fresh extraction each time.
    """
    from alphapulse.sources import fetching
    from alphapulse.sources.guba import api as guba_api

    api_file = tmp_path / "kuaidaili-api-url.txt"
    api_file.write_text("https://dps.kdlapi.com/api/getdps/?secret_id=test&num=1")
    crawl = CrawlSettings.model_validate(
        {
            "proxy": {
                "enabled": True,
                "provider": "kuaidaili",
                "sources": ["guba"],
                "fail_open": False,
            },
            "kuaidaili": {
                "api_url_file": str(api_file),
                "metrics_path": str(tmp_path / "proxy-metrics.db"),
                "batch_size": 1,
                "low_watermark": 0,
                "failure_threshold": 3,
            },
        }
    )
    client = guba_api.GubaClient(
        GubaSettings(
            enabled=True,
            max_retries=3,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        crawl,
    )

    class FakeUrlopenResponse:
        def __init__(self, payload: str) -> None:
            self.payload = payload

        def read(self) -> bytes:
            return self.payload.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    urlopen_calls: list[str] = []

    def fake_urlopen(url, timeout):
        urlopen_calls.append(url)
        return FakeUrlopenResponse("1.2.3.4:8080")

    monkeypatch.setattr(fetching.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(client, "_adaptive_sleep", lambda *, was_rate_limited: None)

    dispatched_proxies: list[str | None] = []

    def fake_dispatch(method, url, form, referer, proxy_url, json_body=None):
        dispatched_proxies.append(proxy_url)
        return 200, "<html>access denied by waf</html>", url

    monkeypatch.setattr(client, "_dispatch", fake_dispatch)

    result = client.get(f"{BASE}/list,600519.html", expect_marker="var article_list")

    assert result.blocked
    assert result.block_kind == "soft_block"
    assert len(urlopen_calls) == 1
    assert dispatched_proxies == ["http://1.2.3.4:8080"] * 3


def test_guba_client_retries_after_proxy_acquire_exception(monkeypatch) -> None:
    from alphapulse.sources.fetching import ProxyLease
    from alphapulse.sources.guba import api as guba_api

    client = guba_api.GubaClient(
        GubaSettings(
            enabled=True,
            max_retries=2,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        CrawlSettings(),
    )
    acquire_calls = {"count": 0}

    class FlakyProxyProvider:
        def acquire(self):
            acquire_calls["count"] += 1
            if acquire_calls["count"] == 1:
                raise RuntimeError("kuaidaili api blip")
            return ProxyLease("http://1.2.3.4:8080", "1.2.3.4:8080", "test")

        def report_bad(self, lease, reason):
            del lease, reason

        def report_success(self, lease):
            del lease

    client.proxy_provider = FlakyProxyProvider()
    monkeypatch.setattr(client, "_adaptive_sleep", lambda *, was_rate_limited: None)
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (200, '<script>var article_list={"re":[]};</script>', args[1]),
    )

    result = client.get(f"{BASE}/list,600519.html", expect_marker="var article_list")

    assert result.status_code == 200
    assert not result.blocked
    assert acquire_calls["count"] == 2


def test_guba_client_retries_after_no_proxy_available(monkeypatch) -> None:
    from alphapulse.sources.fetching import ProxyLease
    from alphapulse.sources.guba import api as guba_api

    client = guba_api.GubaClient(
        GubaSettings(
            enabled=True,
            max_retries=2,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        CrawlSettings(),
    )
    acquire_calls = {"count": 0}

    class EmptyThenFullProxyProvider:
        def acquire(self):
            acquire_calls["count"] += 1
            if acquire_calls["count"] == 1:
                return None
            return ProxyLease("http://1.2.3.4:8080", "1.2.3.4:8080", "test")

        def report_bad(self, lease, reason):
            del lease, reason

        def report_success(self, lease):
            del lease

    client.proxy_provider = EmptyThenFullProxyProvider()
    monkeypatch.setattr(client, "_adaptive_sleep", lambda *, was_rate_limited: None)
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (200, '<script>var article_list={"re":[]};</script>', args[1]),
    )

    result = client.get(f"{BASE}/list,600519.html", expect_marker="var article_list")

    assert result.status_code == 200
    assert acquire_calls["count"] == 2


def test_guba_client_waits_before_acquiring_short_lived_proxy(monkeypatch) -> None:
    from alphapulse.sources.fetching import ProxyLease
    from alphapulse.sources.guba import api as guba_api

    client = guba_api.GubaClient(
        GubaSettings(
            enabled=True,
            request_interval_min_seconds=0,
            request_interval_max_seconds=0,
        ),
        CrawlSettings(),
    )
    events: list[str] = []

    class FakeProxyProvider:
        def acquire(self):
            events.append("acquire")
            return ProxyLease("http://1.2.3.4:8080", "1.2.3.4:8080", "test")

        def report_bad(self, lease, reason):
            del lease, reason

        def report_success(self, lease):
            del lease
            events.append("success")

    client.proxy_provider = FakeProxyProvider()
    monkeypatch.setattr(
        client,
        "_adaptive_sleep",
        lambda *, was_rate_limited: events.append("sleep"),
    )
    monkeypatch.setattr(
        client,
        "_dispatch",
        lambda *args, **kwargs: (
            events.append("dispatch") or (200, "<html>ok</html>", args[1])
        ),
    )

    result = client.get(f"{BASE}/list,600519.html")

    assert result.status_code == 200
    assert events == ["sleep", "acquire", "dispatch", "success"]


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
