from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from alphapulse.sources.guba.parser import (
    extract_embedded_json,
    parse_article_list,
    parse_cn_datetime,
    parse_post_detail,
    parse_replies,
)
from alphapulse.sources.guba.urls import (
    board_list_url,
    comment_refresh_url,
    extract_post_ref,
    post_detail_url,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "guba"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_board_list_url_pagination() -> None:
    base = "https://guba.eastmoney.com"
    assert board_list_url(base, "600519") == "https://guba.eastmoney.com/list,600519.html"
    assert board_list_url(base, "600519", 1) == "https://guba.eastmoney.com/list,600519.html"
    assert board_list_url(base, "zssh000001", 3) == "https://guba.eastmoney.com/list,zssh000001_3.html"


def test_post_detail_url_roundtrip() -> None:
    url = post_detail_url("https://guba.eastmoney.com/", "600519", "1743987733")
    assert url == "https://guba.eastmoney.com/news,600519,1743987733.html"
    assert extract_post_ref(url) == ("600519", "1743987733")
    assert extract_post_ref("https://guba.eastmoney.com/list,600519.html") is None


def test_comment_refresh_url_is_stable() -> None:
    first = comment_refresh_url("https://guba.eastmoney.com", "600519", "42")
    second = comment_refresh_url("https://guba.eastmoney.com/", "600519", "42")
    assert first == second


def test_parse_cn_datetime_converts_beijing_to_utc() -> None:
    parsed = parse_cn_datetime("2026-07-15 10:00:00")
    assert parsed == datetime(2026, 7, 15, 2, 0, 0, tzinfo=UTC)
    assert parse_cn_datetime("2026-07-15") == datetime(2026, 7, 14, 16, 0, 0, tzinfo=UTC)
    assert parse_cn_datetime(None) is None
    assert parse_cn_datetime("not a date") is None


def test_extract_embedded_json_survives_braces_in_strings() -> None:
    html = '<script>var post_article={"post_id": 1, "post_content": "if(x){y();}; more"};</script>'
    payload = extract_embedded_json(html, "post_article")
    assert payload == {"post_id": 1, "post_content": "if(x){y();}; more"}


def test_extract_embedded_json_missing_var() -> None:
    assert extract_embedded_json("<html></html>", "article_list") is None


def test_parse_article_list_stock_board() -> None:
    result = parse_article_list(_read("list_stock.html"))
    assert result is not None
    assert result.bar_code == "600519"
    assert result.bar_name == "贵州茅台"
    assert result.total_count == 959849
    assert len(result.entries) == 3

    first = result.entries[0]
    assert first.post_id == "1743987733"
    assert first.stockbar_code == "600519"
    assert first.user_id == "7344113638256342"
    assert first.comment_count == 5
    assert first.publish_time == parse_cn_datetime("2026-07-15 14:03:09")
    assert first.last_time == parse_cn_datetime("2026-07-15 14:40:26")

    # Resurfaced post: old publish time, recent last-reply time, entry lacks
    # stockbar_code/user_id (real payloads omit these on some entries).
    resurfaced = result.entries[1]
    assert resurfaced.post_id == "1743507860"
    assert resurfaced.stockbar_code is None
    assert resurfaced.user_id is None
    assert resurfaced.publish_time == parse_cn_datetime("2025-11-02 09:15:00")
    assert resurfaced.last_time == parse_cn_datetime("2026-07-15 11:20:33")
    assert resurfaced.comment_count == 42


def test_parse_article_list_index_board() -> None:
    result = parse_article_list(_read("list_index.html"))
    assert result is not None
    assert result.bar_code == "zssh000001"
    assert result.total_count == 8948506
    assert [entry.post_id for entry in result.entries] == ["1744137528", "1744089164"]
    assert all(entry.stockbar_code == "zssh000001" for entry in result.entries)


def test_parse_article_list_missing_payload() -> None:
    assert parse_article_list("<html>no payload</html>") is None


def test_parse_post_detail() -> None:
    fetched_at = datetime(2026, 7, 15, 10, 0, 0, tzinfo=UTC)
    url = "https://guba.eastmoney.com/news,600519,1743987733.html"
    post, author, meta = parse_post_detail(_read("post_detail.html"), url, fetched_at)

    assert post is not None
    assert post.source == "guba"
    assert post.source_entity_id == "1743987733"
    assert str(post.canonical_url) == url
    assert post.title == "贵州茅台酒销售有限公司2026年半年市场工作会召开"
    assert post.author_entity_id == "7344113638256342"
    assert post.published_at == parse_cn_datetime("2026-07-15 14:03:09")
    assert post.fetched_at == fetched_at
    assert post.like_count == 6
    assert post.comment_count == 5
    assert post.repost_count == 1
    assert post.language == "zh"
    assert post.raw_topic_ids == ["600519"]
    assert "edge-case brace text" in post.content_text
    assert "<div" not in post.content_text

    assert author is not None
    assert author.source_entity_id == "7344113638256342"
    assert author.display_name == "贵州茅台资讯"
    assert author.username == "600519_1"
    assert str(author.profile_url) == "https://i.eastmoney.com/7344113638256342"

    assert meta is not None
    assert meta.mod_count == 2
    assert meta.mod_time == "2026-07-15 15:10:00"
    assert meta.state == 0


def test_parse_post_detail_normalizes_concept_board_code() -> None:
    post, _, _ = parse_post_detail(
        _read("post_detail.html"),
        "https://guba.eastmoney.com/news,bk1152,1743987733.html",
    )

    assert post is not None
    assert post.raw_topic_ids == ["BK1152"]


def test_parse_post_detail_uses_list_title_when_mobile_payload_omits_it() -> None:
    html = '<script>var post_article={"post_id":42,"post_content":"body"};</script>'

    post, _, _ = parse_post_detail(
        html,
        "https://guba.eastmoney.com/news,600519,42.html",
        fallback_title="list title",
    )

    assert post is not None
    assert post.title == "list title"


def test_parse_post_detail_deleted_page() -> None:
    post, author, meta = parse_post_detail(
        _read("post_deleted.html"),
        "https://guba.eastmoney.com/news,600519,999999999999.html",
    )
    assert post is None
    assert author is None
    assert meta is None


def test_parse_replies_flattens_children_and_skips_deleted() -> None:
    payload = json.loads(_read("replies.json"))
    fetched_at = datetime(2026, 7, 15, 10, 0, 0, tzinfo=UTC)
    url = "https://guba.eastmoney.com/news,zssh000001,1743300821.html"
    comments, total = parse_replies(payload, "1743300821", url, fetched_at)

    assert total == 2
    # 2 top-level live replies + 1 child; the reply_state=1 entry is skipped.
    assert [comment.source_entity_id for comment in comments] == [
        "9926112093",
        "9926134826",
        "9926194424",
    ]

    parent = comments[0]
    assert parent.post_entity_id == "1743300821"
    assert parent.parent_comment_entity_id is None
    assert parent.author_entity_id == "6172065195623584"
    assert parent.published_at == parse_cn_datetime("2026-07-14 14:32:15")
    assert parent.content_text == "快了吧"
    assert str(parent.canonical_url) == f"{url}#reply9926112093"

    child = comments[1]
    assert child.parent_comment_entity_id == "9926112093"
    assert child.author_entity_id == "1988097204925520"
    assert child.published_at == parse_cn_datetime("2026-07-14 14:53:09")
