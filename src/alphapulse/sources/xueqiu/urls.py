from __future__ import annotations

import re
from urllib.parse import quote, urljoin, urlparse

from pydantic import HttpUrl


POST_ID_RE = re.compile(r"/(\d{6,})/?$")
POST_URL_RE = re.compile(r"https://xueqiu\.com/\d+/\d+/?")


def stock_url(base_url: str, stock_id: str) -> str:
    return urljoin(base_url, f"/S/{stock_id}")


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

