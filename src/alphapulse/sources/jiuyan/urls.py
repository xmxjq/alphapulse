from __future__ import annotations

from urllib.parse import quote


COMMUNITY_FEED_PATHS = {
    "study": "study_publish",
    "square": "square_publish",
    "live": "live_publish",
}


def search_url(base_url: str, keyword: str, page: int = 1) -> str:
    url = f"{base_url.rstrip('/')}/search/new?k={quote(keyword)}"
    return url if page <= 1 else f"{url}&page={page}"


def community_feed_url(base_url: str, feed: str, page: int = 1) -> str:
    path = COMMUNITY_FEED_PATHS[feed]
    url = f"{base_url.rstrip('/')}/{path}"
    return url if page <= 1 else f"{url}?page={page}"


def post_detail_url(base_url: str, article_id: str) -> str:
    return f"{base_url.rstrip('/')}/a/{article_id}"
