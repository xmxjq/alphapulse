from __future__ import annotations

import re
from urllib.parse import quote, urljoin, urlparse

from pydantic import HttpUrl


POST_ID_RE = re.compile(r"/(\d{6,})/?$")
POST_URL_RE = re.compile(r"https://xueqiu\.com/\d+/\d+/?")


def stock_url(base_url: str, stock_id: str) -> str:
    return urljoin(base_url, f"/S/{stock_id}")


def stock_timeline_url(base_url: str, stock_id: str, page: int = 1, count: int = 20) -> str:
    return urljoin(
        base_url,
        f"/statuses/stock_timeline.json?symbol_id={quote(stock_id)}&count={count}&source=all&sort=time&page={page}",
    )


def stock_status_search_url(base_url: str, stock_id: str, page: int = 1, count: int = 20) -> str:
    return urljoin(
        base_url,
        (
            "/query/v1/symbol/search/status.json"
            f"?count={count}&comment=0&symbol={quote(stock_id)}&hl=0&source=all&sort=time"
            f"&page={page}&q=&type=90"
        ),
    )


def topic_url(base_url: str, topic_id: str) -> str:
    return urljoin(base_url, f"/k?q={quote(topic_id)}")


def user_url(base_url: str, user_id: str) -> str:
    return urljoin(base_url, f"/u/{user_id}")


def extract_post_id(url: str) -> str | None:
    match = POST_ID_RE.search(urlparse(url).path)
    return match.group(1) if match else None


def is_post_url(url: str) -> bool:
    return bool(POST_URL_RE.fullmatch(url.rstrip("/")))


def normalize_url(url: str | HttpUrl) -> str:
    return str(url).rstrip("/")
