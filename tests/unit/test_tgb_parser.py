from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from alphapulse.sources.tgb.parser import (
    parse_comments,
    parse_full_datetime,
    parse_list_page,
    parse_post_detail,
    parse_short_datetime,
    parse_shuo_feed,
    parse_stock_feed,
)
from alphapulse.sources.tgb.rankings import parse_hot_stocks

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tgb"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_parse_full_datetime_to_utc() -> None:
    # 2026-07-22 14:46 Beijing == 06:46 UTC.
    dt = parse_full_datetime("2026-07-22 14:46")
    assert dt == datetime(2026, 7, 22, 6, 46, tzinfo=UTC)
    assert parse_full_datetime("garbage") is None


def test_parse_short_datetime_infers_year_and_rolls_back() -> None:
    ref = datetime(2026, 7, 22, 12, 0, tzinfo=None).astimezone(UTC)
    # A yearless "07-21 09:30" with a July 2026 reference lands in 2026.
    from zoneinfo import ZoneInfo

    ref_bj = datetime(2026, 7, 22, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    dt = parse_short_datetime("07-21 09:30", reference=ref_bj)
    assert dt == datetime(2026, 7, 21, 1, 30, tzinfo=UTC)
    # A December date read from a January reference rolls back a year.
    ref_jan = datetime(2026, 1, 2, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    dt2 = parse_short_datetime("12-30 09:30", reference=ref_jan)
    assert dt2 is not None and dt2.year == 2025


def test_parse_list_page_rows() -> None:
    entries = parse_list_page(_read("list_zongban.html"))
    assert len(entries) == 3
    first = entries[0]
    assert first.post_id == "2tDJArVCoKz"
    assert "明日早盘" in first.title
    assert first.user_id == "11520081"
    assert first.user_nickname == "风一样的胖刺猬"
    assert first.comment_count == 25
    assert first.click_count == 329
    assert first.publish_time is not None and first.last_time is not None


def test_parse_list_page_strips_featured_marker() -> None:
    # The 精华 list carries a [精] marker span that must not pollute the title.
    entries = parse_list_page(_read("list_jinghua.html"))
    assert entries
    assert not entries[0].title.startswith("[")


def test_parse_stock_feed_dedupes_to_posts() -> None:
    entries = parse_stock_feed(_read("quotes_feed.html"))
    assert entries
    ids = [e.post_id for e in entries]
    assert len(ids) == len(set(ids))  # deduped to underlying posts
    assert all(e.publish_time is not None for e in entries)
    assert entries[0].post_id == "2tDH0LW6lEd"


def test_parse_shuo_feed_extracts_fast_news() -> None:
    html = """
    <div id="forumRow_2084897797165408308" class="stockNews">
      <div class="user-name"><span>极速快讯</span></div>
      <div class="related-sources">2026-08-05 15:02</div>
      <a class="related-body"
         href="https://shuo.tgb.cn/shuo/toViewShuo?shuoID=2084897797165408308">
        【收评：科创50指数大涨4.78%】 评论(3)
      </a>
    </div>
    """

    entries = parse_shuo_feed(html)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.shuo_id == "2084897797165408308"
    assert entry.canonical_url.endswith("shuoID=2084897797165408308")
    assert entry.content_text.startswith("【收评")
    assert entry.user_nickname == "极速快讯"
    assert entry.comment_count == 3
    assert entry.publish_time == datetime(2026, 8, 5, 7, 2, tzinfo=UTC)


def test_parse_post_detail() -> None:
    post, author = parse_post_detail(
        _read("post_detail.html"),
        "https://www.tgb.cn/a/2tDBakf13jC",
        datetime(2026, 7, 22, tzinfo=UTC),
        board_code="jinghua",
    )
    assert post is not None
    assert post.source == "tgb"
    assert post.source_entity_id == "2tDBakf13jC"
    assert post.title and post.title.startswith("7.22")
    assert post.author_entity_id == "13145799"
    assert post.comment_count == 184
    assert post.raw_topic_ids == ["jinghua"]
    assert post.published_at == datetime(2026, 7, 22, 6, 46, tzinfo=UTC)
    assert len(post.content_text) > 0
    assert author is not None and author.display_name == "橘子洲炒家实战"
    assert author.source_entity_id == "13145799"


def test_parse_comments_inline_first_page() -> None:
    comments = parse_comments(
        _read("post_detail.html"),
        "2tDBakf13jC",
        "https://www.tgb.cn/a/2tDBakf13jC",
        datetime(2026, 7, 22, tzinfo=UTC),
    )
    assert len(comments) >= 3
    ids = [c.source_entity_id for c in comments]
    assert len(ids) == len(set(ids))  # deduped by reply id
    first = comments[0]
    assert first.post_entity_id == "2tDBakf13jC"
    assert first.author_entity_id
    assert first.content_text
    assert first.published_at is not None
    assert str(first.canonical_url).startswith("https://www.tgb.cn/a/2tDBakf13jC")


def test_parse_hot_stocks() -> None:
    stocks = parse_hot_stocks(_read("home_rankings.html"), limit=12)
    assert stocks
    assert stocks[0].code == "sz000938"
    assert stocks[0].name == "紫光股份"
    assert stocks[0].rank == 1
    # respects the limit
    assert len(parse_hot_stocks(_read("home_rankings.html"), limit=2)) == 2
