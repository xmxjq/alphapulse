from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from alphapulse.pipeline.contracts import CrawlTask, ItemReference, SeedDefinition
from alphapulse.runtime.config import CrawlSettings, TgbSettings
from alphapulse.sources.tgb.adapter import TgbAdapter
from alphapulse.sources.tgb.api import TgbHttpResult, classify_block
from alphapulse.storage.rawstore import RawResponseStore

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tgb"
BASE = "https://www.tgb.cn"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FakeTgbClient:
    def __init__(self, responses: dict[str, TgbHttpResult] | None = None) -> None:
        self.responses = responses or {}
        self.get_calls: list[str] = []
        self.get_markers: list[str | None] = []

    def get(self, url: str, *, expect_marker: str | None = None) -> TgbHttpResult:
        self.get_calls.append(url)
        self.get_markers.append(expect_marker)
        return self.responses[url]


def _adapter(tmp_path, client: FakeTgbClient, **overrides) -> TgbAdapter:
    settings = TgbSettings(enabled=True, **overrides)
    return TgbAdapter(
        settings,
        CrawlSettings(),
        client=client,  # type: ignore[arg-type]
        raw_store=RawResponseStore(tmp_path / "raw"),
    )


def _ok(url: str, text: str) -> TgbHttpResult:
    return TgbHttpResult(url=url, status_code=200, text=text, duration_ms=10)


def _bj_today() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%m-%d %H:%M")


def _bj_today_full() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")


def _nbbs_row(post_id: str, title: str, when: str) -> str:
    return f"""
    <div class="Nbbs-tiezi-lists">
      <div class="left middle-list-tittle"><a class="overhide" href="/a/{post_id}" title="{title}">{title}</a></div>
      <div class="left middle-list-talk overhide">5 / 99</div>
      <div class="left middle-list-reply">{when}</div>
      <div class="left middle-list-user"><a href="/blog/42" data-user-id="42">tester</a></div>
      <div class="left middle-list-post">{when}</div>
    </div>"""


def _list_html(rows: list[str]) -> str:
    return f"<html><body><div class='Nbbs-middle-list'>{''.join(rows)}</div></body></html>"


def _forum_row(idx: int, post_id: str, title: str, when_full: str) -> str:
    return f"""
    <div id="forumRow_{idx}" class="stockNews">
      <div class="related-sources">{when_full}  跟帖回复</div>
      <div class="Rlink">来自 <a href="/blog/7">tester</a>
        <a href="/a/{post_id}" target="_blank">《{title}》</a></div>
    </div>"""


def _stock_html(rows: list[str]) -> str:
    return f"<html><body><div id='stockContent'>{''.join(rows)}</div></body></html>"


def _shuo_row(shuo_id: str, body: str, when_full: str) -> str:
    return f"""
    <div id="forumRow_{shuo_id}" class="stockNews">
      <div class="user-name"><span>极速快讯</span></div>
      <div class="related-sources">{when_full}</div>
      <a class="related-body" href="https://shuo.tgb.cn/shuo/toViewShuo?shuoID={shuo_id}">
        {body} 评论(2)
      </a>
    </div>"""


def test_discover_builds_kind_specific_urls(tmp_path) -> None:
    adapter = _adapter(tmp_path, FakeTgbClient())
    seed = SeedDefinition(name="tgb", tgb_board_codes=["jinghua", "zongban", "sz000938"])
    tasks = adapter.discover(seed)

    by_code = {t.metadata["board_code"]: t for t in tasks}
    assert str(by_code["jinghua"].url) == f"{BASE}/jinghua/1-1"
    assert by_code["jinghua"].metadata["board_kind"] == "featured"
    assert str(by_code["zongban"].url) == f"{BASE}/zongban/1/1"
    assert by_code["zongban"].metadata["board_kind"] == "general"
    assert str(by_code["sz000938"].url) == f"{BASE}/quotes/sz000938"
    assert by_code["sz000938"].metadata["board_kind"] == "stock"
    # Featured outranks stock outranks general so featured attribution wins on overlap.
    assert by_code["jinghua"].priority > by_code["sz000938"].priority > by_code["zongban"].priority
    # Fresh list discovery runs before recovered detail tasks so today's posts
    # can be identified and prioritized ahead of stale pending work.
    assert min(task.priority for task in tasks) > 152


def _discover_task(url: str, code: str, kind: str, page: int = 1) -> CrawlTask:
    return CrawlTask(
        source="tgb", kind="discover", url=url, seed_name="tgb",
        metadata={"board_code": code, "board_kind": kind, "page": page},
    )


def test_list_page_day_scopes_and_paginates(tmp_path) -> None:
    url = f"{BASE}/zongban/1/1"
    html = _list_html([
        _nbbs_row("todayA", "今日帖", _bj_today()),
        _nbbs_row("staleB", "旧帖", "01-01 09:30"),
    ])
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, html)}), max_list_pages=3)

    outcome = adapter.fetch_item(_discover_task(url, "zongban", "general"))

    posts = [t.metadata["post_id"] for t in outcome.discovered_tasks if t.kind == "fetch_post"]
    assert posts == ["todayA"]
    nexts = [t for t in outcome.discovered_tasks if t.kind == "discover"]
    assert len(nexts) == 1
    assert str(nexts[0].url) == f"{BASE}/zongban/2/1"
    assert nexts[0].metadata["page"] == 2
    # fetch_post carries the board code for report attribution.
    fp = next(t for t in outcome.discovered_tasks if t.kind == "fetch_post")
    assert fp.metadata["board_code"] == "zongban"
    assert fp.metadata["pubdate_ts"] > 0


def test_list_page_all_stale_stops(tmp_path) -> None:
    url = f"{BASE}/zongban/1/1"
    html = _list_html([_nbbs_row("s1", "旧", "01-01 09:30"), _nbbs_row("s2", "旧", "02-02 09:30")])
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, html)}))

    outcome = adapter.fetch_item(_discover_task(url, "zongban", "general"))
    assert outcome.discovered_tasks == []


def test_list_page_cap_stops_pagination(tmp_path) -> None:
    url = f"{BASE}/zongban/3/1"
    html = _list_html([_nbbs_row("todayA", "今日", _bj_today())])
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, html)}), max_list_pages=3)

    outcome = adapter.fetch_item(_discover_task(url, "zongban", "general", page=3))
    assert all(t.kind != "discover" for t in outcome.discovered_tasks)


def test_stock_feed_day_scopes(tmp_path) -> None:
    url = f"{BASE}/quotes/sz000938"
    html = _stock_html([
        _forum_row(1, "todayX", "今日研股", _bj_today_full()),
        _forum_row(2, "staleY", "去年帖", "2020-01-01 09:30"),
    ])
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, html)}))

    outcome = adapter.fetch_item(_discover_task(url, "sz000938", "stock"))
    posts = [(t.metadata["post_id"], t.metadata["board_code"]) for t in outcome.discovered_tasks if t.kind == "fetch_post"]
    assert posts == [("todayX", "sz000938")]


def test_stock_feed_writes_shuo_without_detail_task(tmp_path) -> None:
    url = f"{BASE}/quotes/sh000688"
    html = _stock_html([_shuo_row("2084897797165408308", "科创50指数大涨", _bj_today_full())])
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, html)}))

    outcome = adapter.fetch_item(_discover_task(url, "sh000688", "stock"))

    assert len(outcome.discovered_tasks) == 0
    assert len(outcome.posts) == 1
    post = outcome.posts[0]
    assert post.source_entity_id == "shuo:2084897797165408308"
    assert post.raw_topic_ids == ["sh000688"]
    assert post.comment_count == 2
    assert str(post.canonical_url).startswith("https://shuo.tgb.cn/shuo/toViewShuo")


def test_post_detail_sets_board_from_metadata(tmp_path) -> None:
    url = f"{BASE}/a/2tDBakf13jC"
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, _read("post_detail.html"))}))
    task = CrawlTask(
        source="tgb", kind="fetch_post", url=url, seed_name="tgb",
        metadata={"post_id": "2tDBakf13jC", "board_code": "sz000938", "board_kind": "stock"},
    )
    outcome = adapter.fetch_item(task)
    assert len(outcome.posts) == 1
    assert outcome.posts[0].raw_topic_ids == ["sz000938"]
    assert len(outcome.authors) == 1


def test_refresh_comments_parses_inline(tmp_path) -> None:
    url = f"{BASE}/a/2tDBakf13jC"
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, _read("post_detail.html"))}))
    item_ref = ItemReference(
        source="tgb", source_entity_id="2tDBakf13jC", canonical_url=url,
        metadata={"post_id": "2tDBakf13jC"},
    )
    comments = adapter.refresh_comments(item_ref)
    assert len(comments) >= 3
    assert all(c.post_entity_id == "2tDBakf13jC" for c in comments)


def test_comment_task_targets_post_url(tmp_path) -> None:
    url = f"{BASE}/a/2tDBakf13jC"
    adapter = _adapter(tmp_path, FakeTgbClient({url: _ok(url, _read("post_detail.html"))}))
    task = CrawlTask(
        source="tgb", kind="fetch_post", url=url, seed_name="tgb",
        metadata={"post_id": "2tDBakf13jC", "board_code": "jinghua", "board_kind": "featured"},
    )
    post = adapter.fetch_item(task).posts[0]
    ctask = adapter.comment_task_for_post(post, "tgb")
    assert ctask.kind == "refresh_comments"
    assert str(ctask.url) == url
    assert ctask.metadata["board_code"] == "jinghua"


def test_blocked_response(tmp_path) -> None:
    url = f"{BASE}/zongban/1/1"
    blocked = TgbHttpResult(url=url, status_code=403, text="", blocked=True, block_kind="http_403")
    adapter = _adapter(tmp_path, FakeTgbClient({url: blocked}))
    outcome = adapter.fetch_item(_discover_task(url, "zongban", "general"))
    assert outcome.blocked
    assert outcome.status_code == 403


def test_missing_post_page_does_not_open_block_circuit(tmp_path) -> None:
    url = f"{BASE}/a/missing"
    response = _ok(url, "<html><title>错误页面_淘股吧</title></html>")
    adapter = _adapter(tmp_path, FakeTgbClient({url: response}))
    task = CrawlTask(
        source="tgb",
        kind="fetch_post",
        url=url,
        seed_name="tgb",
        metadata={"post_id": "missing", "board_code": "zongban"},
    )

    outcome = adapter.fetch_item(task)

    assert outcome.blocked is False
    assert outcome.status_code == 200
    assert outcome.errors == [f"Post deleted or missing: {url}"]


def test_expect_markers_by_task_kind(tmp_path) -> None:
    list_url = f"{BASE}/zongban/1/1"
    stock_url = f"{BASE}/quotes/sz1"
    detail_url = f"{BASE}/a/pid"
    client = FakeTgbClient({
        list_url: _ok(list_url, _list_html([])),
        stock_url: _ok(stock_url, _stock_html([])),
        detail_url: _ok(detail_url, "<html><div class='article-content'></div></html>"),
    })
    adapter = _adapter(tmp_path, client)
    adapter.fetch_item(_discover_task(list_url, "zongban", "general"))
    adapter.fetch_item(_discover_task(stock_url, "sz1", "stock"))
    adapter.fetch_item(CrawlTask(
        source="tgb", kind="fetch_post", url=detail_url, seed_name="tgb",
        metadata={"post_id": "pid", "board_code": "sz1", "board_kind": "stock"},
    ))
    assert client.get_markers == ["Nbbs-middle-list", "stockContent", "article-content"]


def test_classify_block() -> None:
    assert classify_block(403, "", f"{BASE}/x") == "http_403"
    assert classify_block(429, "", f"{BASE}/x") == "http_429"
    assert classify_block(502, "", f"{BASE}/x") == "http_5xx"
    assert classify_block(200, "", "https://sso.tgb.cn/web/login") == "login_redirect"
    assert classify_block(200, "<html>请输入验证码</html>", f"{BASE}/x") == "captcha"
    assert classify_block(200, "<html>ok</html>", f"{BASE}/x") is None
