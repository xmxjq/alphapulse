from __future__ import annotations

from datetime import UTC, datetime

from alphapulse.sources.jiuyan.parser import (
    html_to_text,
    parse_post_detail,
    parse_search_page,
)
from alphapulse.sources.jiuyan.rankings import fetch_hot_targets


def test_parse_search_page() -> None:
    page = parse_search_page(
        {
            "data": {
                "pageNo": 1,
                "pageSize": 15,
                "totalCount": 2,
                "result": [
                    {
                        "article_id": "a1",
                        "title": "今日复盘",
                        "create_time": "2026-07-29 09:30:00",
                        "comment_count": 3,
                        "like_count": 5,
                    },
                    {
                        "article_id": "a1",
                        "title": "duplicate",
                        "create_time": "2026-07-29 09:30:00",
                    },
                ],
            }
        }
    )
    assert page is not None
    assert page.total_count == 2
    assert len(page.entries) == 1
    assert page.entries[0].published_at == datetime(
        2026, 7, 29, 1, 30, tzinfo=UTC
    )


def test_parse_post_detail_cleans_html_and_sets_target() -> None:
    post, author = parse_post_detail(
        {
            "data": {
                "article_id": "a1",
                "user_id": "u1",
                "title": "市场观察",
                "content": "<p>第一段<br>第二段</p><script>bad()</script>",
                "create_time": "2026-07-29 10:00:00",
                "like_count": 8,
                "comment_count": 2,
                "forward_count": 1,
                "user": {"user_id": "u1", "nickname": "测试用户"},
            }
        },
        "https://www.jiuyangongshe.com/a/a1",
        target_code="上证指数",
        fetched_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    assert post is not None
    assert post.source == "jiuyan"
    assert post.content_text == "第一段第二段"
    assert post.raw_topic_ids == ["上证指数"]
    assert post.published_at == datetime(2026, 7, 29, 2, 0, tzinfo=UTC)
    assert author is not None
    assert author.display_name == "测试用户"


def test_html_to_text_handles_plain_text() -> None:
    assert html_to_text("already plain") == "already plain"


class _RankingClient:
    def hot_rankings(self):
        from alphapulse.sources.jiuyan.api import JiuyanHttpResult

        return JiuyanHttpResult(
            url="https://example.test",
            status_code=200,
            text=(
                '{"errCode":"0","data":{"hot_search_list":['
                '{"keyword":"机器人"},{"keyword":"机器人"},{"keyword":"消费"}]}}'
            ),
        )


def test_fetch_hot_targets_dedupes() -> None:
    from alphapulse.runtime.config import JiuyanSettings

    targets = fetch_hot_targets(_RankingClient(), JiuyanSettings(hot_targets_limit=10))
    assert [(item.rank, item.keyword) for item in targets] == [
        (1, "机器人"),
        (2, "消费"),
    ]
