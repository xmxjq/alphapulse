import json
from pathlib import Path

from alphapulse.sources.xueqiu.parser import (
    discover_post_urls,
    discover_post_urls_from_timeline_payload,
    parse_comments,
    parse_post,
)


FIXTURES = Path("tests/fixtures/xueqiu")


def test_discovery_parser_extracts_post_urls() -> None:
    html = (FIXTURES / "discovery.html").read_text()
    urls = discover_post_urls(html)
    assert urls == [
        "https://xueqiu.com/1234567890/123123123",
        "https://xueqiu.com/1234567890/987654321",
    ]


def test_post_parser_extracts_normalized_post_and_author() -> None:
    html = (FIXTURES / "post.html").read_text()
    post, author = parse_post(html, "https://xueqiu.com/1234567890/987654321")
    assert post is not None
    assert author is not None
    assert post.source_entity_id == "987654321"
    assert post.comment_count == 12
    assert "Margins still need attention." in post.content_text
    assert author.source_entity_id == "1234567890"


def test_stock_timeline_parser_extracts_post_urls_from_json_payload() -> None:
    payload = {
        "list": [
            {
                "id": 987654321,
                "user_id": 1234567890,
            },
            {
                "target": "https://xueqiu.com/2222222222/333333333",
            },
        ]
    }
    urls = discover_post_urls_from_timeline_payload(payload)
    assert urls == [
        "https://xueqiu.com/1234567890/987654321",
        "https://xueqiu.com/2222222222/333333333",
    ]


def test_comment_parser_extracts_threads() -> None:
    payload = json.loads((FIXTURES / "comments.json").read_text())
    comments = parse_comments(payload, "987654321")
    assert len(comments) == 2
    assert comments[1].parent_comment_entity_id == "2001"


def test_comment_parser_accepts_epoch_millis_created_at() -> None:
    payload = {
        "comments": [
            {
                "id": 1,
                "created_at": 1776205307000,
                "text": "hello",
                "user": {"id": 2},
            }
        ]
    }
    comments = parse_comments(payload, "987654321")
    assert len(comments) == 1
    assert comments[0].published_at is not None
