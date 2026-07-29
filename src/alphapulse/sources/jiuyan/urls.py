from __future__ import annotations

from urllib.parse import quote


def search_url(base_url: str, keyword: str, page: int = 1) -> str:
    url = f"{base_url.rstrip('/')}/search/new?k={quote(keyword)}"
    return url if page <= 1 else f"{url}&page={page}"


def post_detail_url(base_url: str, article_id: str) -> str:
    return f"{base_url.rstrip('/')}/a/{article_id}"
