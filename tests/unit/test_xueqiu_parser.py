import json
from pathlib import Path

from alphapulse.sources.xueqiu.parser import discover_post_urls, parse_comments, parse_post


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


def test_comment_parser_extracts_threads() -> None:
    payload = json.loads((FIXTURES / "comments.json").read_text())
    comments = parse_comments(payload, "987654321")
    assert len(comments) == 2
    assert comments[1].parent_comment_entity_id == "2001"

