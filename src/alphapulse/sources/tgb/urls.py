from __future__ import annotations

import re


# Post permalinks look like /a/{id}, optionally with a page suffix (/a/{id}-2) or a
# comment anchor (/a/{id}/{replyId}#{replyId}). The base62 id is the canonical post id.
POST_ID_RE = re.compile(r"/a/([0-9A-Za-z]+)")

# Board kinds understood by the adapter.
KIND_FEATURED = "featured"
KIND_GENERAL = "general"
KIND_STOCK = "stock"

# List sort flag: 0 = by last-reply date, 1 = by post date (descending). We use the
# post-date sort so day-scoped pagination can stop once a page holds no more of today.
SORT_BY_POST_DATE = 1


def general_list_url(base_url: str, slug: str, page: int = 1, flag: int = SORT_BY_POST_DATE) -> str:
    """社区总版-style feed: /{slug}/{page}/{flag} (slash-separated)."""
    return f"{base_url.rstrip('/')}/{slug}/{page}/{flag}"


def featured_list_url(base_url: str, slug: str, page: int = 1, flag: int = SORT_BY_POST_DATE) -> str:
    """精华-style feed: /{slug}/{page}-{flag} (dash-separated — differs from general!)."""
    return f"{base_url.rstrip('/')}/{slug}/{page}-{flag}"


def stock_list_url(base_url: str, code: str) -> str:
    """Per-stock discussion board: /quotes/{code}."""
    return f"{base_url.rstrip('/')}/quotes/{code}"


def post_detail_url(base_url: str, post_id: str) -> str:
    return f"{base_url.rstrip('/')}/a/{post_id}"


def extract_post_id(url: str) -> str | None:
    """Return the canonical post id from any /a/{id}... URL."""
    match = POST_ID_RE.search(url)
    return match.group(1) if match else None
