from datetime import datetime, timedelta, timezone
from pathlib import Path

from alphapulse.sources.hupu.parser import (
    infer_fixed_targets,
    parse_list_page,
    parse_post_detail,
)
from alphapulse.sources.hupu.urls import (
    extract_post_id,
    latest_posts_url,
    post_detail_url,
)


FIXTURES = Path(__file__).parent.parent / "fixtures" / "hupu"
CN_TZ = timezone(timedelta(hours=8))


def test_hupu_urls() -> None:
    assert latest_posts_url("https://bbs.hupu.com", "stock") == (
        "https://bbs.hupu.com/stock-postdate"
    )
    assert latest_posts_url("https://bbs.hupu.com/", "stock", 3) == (
        "https://bbs.hupu.com/stock-postdate-3"
    )
    assert post_detail_url("https://bbs.hupu.com", "641433410") == (
        "https://bbs.hupu.com/641433410.html"
    )
    assert extract_post_id("https://bbs.hupu.com/641433410-2.html") == "641433410"


def test_parse_hupu_postdate_list() -> None:
    entries = parse_list_page(
        (FIXTURES / "list_postdate.html").read_text(encoding="utf-8"),
        reference=datetime(2026, 7, 31, 15, 0, tzinfo=CN_TZ),
    )
    assert [entry.post_id for entry in entries] == [
        "641439575",
        "641433410",
        "641400000",
    ]
    assert entries[0].author_id == "138998165058061"
    assert entries[0].comment_count == 7
    assert entries[0].view_count == 1321
    assert entries[1].published_at == datetime(
        2026, 7, 31, 1, 43, tzinfo=timezone.utc
    )


def test_parse_hupu_detail_and_fixed_targets() -> None:
    url = "https://bbs.hupu.com/641433410.html"
    post, author = parse_post_detail(
        (FIXTURES / "post_detail.html").read_text(encoding="utf-8"), url
    )
    assert post is not None
    assert post.source_entity_id == "641433410"
    assert post.title == "沪指与科创50反弹观察"
    assert post.content_text == "今天关注上证综指和科创50ETF，创业板暂时观望。"
    assert post.comment_count == 7
    assert post.like_count == 3
    assert post.published_at == datetime(2026, 7, 31, 1, 43, 39, tzinfo=timezone.utc)
    assert author is not None
    assert author.source_entity_id == "103253382929146"
    assert author.display_name == "南京路老久"

    targets = infer_fixed_targets(
        post.title,
        post.content_text,
        ["上证指数", "创业板指", "科创50", "上证50"],
        {
            "上证指数": ["沪指", "上证综指"],
            "创业板指": ["创指"],
            "科创50": ["科创50ETF"],
            "上证50": ["上证50ETF"],
        },
    )
    assert targets == ["上证指数", "科创50"]
