import pytest

from alphapulse.runtime.service import classify_crawl_error


@pytest.mark.parametrize(
    "message, expected",
    [
        ("Blocked (soft_block) from https://guba.eastmoney.com/news,x,1.html", "blocked"),
        ("Blocked response from https://xueqiu.com/1/2", "blocked"),
        ("Could not parse post payload from https://guba.eastmoney.com/news,x,1.html", "parse_error"),
        ("No article_list payload in https://guba.eastmoney.com/list,x.html", "parse_error"),
        ("Post deleted or missing: https://guba.eastmoney.com/news,x,1.html", "deleted"),
        ("Post state 2 (removed/hidden): https://guba.eastmoney.com/news,x,1.html", "removed"),
        ("Fetch failed for https://xueqiu.com/1/2: timed out", "fetch_failed"),
        ("something unexpected happened", "other"),
    ],
)
def test_classify_crawl_error(message: str, expected: str) -> None:
    assert classify_crawl_error(message) == expected
