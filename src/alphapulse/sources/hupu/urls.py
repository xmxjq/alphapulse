from __future__ import annotations

import re


_POST_ID_RE = re.compile(r"/(\d+)(?:-\d+)?\.html(?:[?#]|$)")


def latest_posts_url(base_url: str, board_slug: str, page: int = 1) -> str:
    base = base_url.rstrip("/")
    suffix = f"-{page}" if page > 1 else ""
    return f"{base}/{board_slug}-postdate{suffix}"


def post_detail_url(base_url: str, post_id: str) -> str:
    return f"{base_url.rstrip('/')}/{post_id}.html"


def extract_post_id(url: str) -> str | None:
    match = _POST_ID_RE.search(url)
    return match.group(1) if match else None
