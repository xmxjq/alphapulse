from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from html import unescape
from typing import Any
from urllib.parse import urljoin

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost
from alphapulse.sources.xueqiu.urls import extract_post_id, is_post_url


POST_LINK_RE = re.compile(r'href="(https://xueqiu\.com/\d+/\d+/?)"')
SCRIPT_JSON_RE = re.compile(r"<script[^>]*>\s*(\{.*?\})\s*</script>", re.DOTALL)
TEXT_TAG_RE = re.compile(r"<[^>]+>")
META_RE = re.compile(r'<meta[^>]+(?:property|name)="([^"]+)"[^>]+content="([^"]*)"[^>]*>', re.I)
TIME_RE = re.compile(r'<time[^>]+datetime="([^"]+)"')


def _strip_html(html: str) -> str:
    text = TEXT_TAG_RE.sub(" ", html)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _meta_map(html: str) -> dict[str, str]:
    return {name: value for name, value in META_RE.findall(html)}


def _parse_datetime(value: str | int | float | None) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000
        return datetime.fromtimestamp(timestamp, tz=UTC)
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _parse_embedded_json(html: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for raw in SCRIPT_JSON_RE.findall(html):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            objects.append(parsed)
    return objects


def discover_post_urls(html: str) -> list[str]:
    urls = sorted({url.rstrip("/") for url in POST_LINK_RE.findall(html)})
    return urls


def discover_post_urls_from_timeline_payload(payload: dict[str, Any], base_url: str = "https://xueqiu.com") -> list[str]:
    urls: set[str] = set()

    def add_candidate(value: str | None) -> None:
        if not value:
            return
        normalized = value.rstrip("/")
        if is_post_url(normalized):
            urls.add(normalized)

    def visit_status(record: Any) -> None:
        if not isinstance(record, dict):
            return

        for key in ("target", "url", "canonical_url", "share_url"):
            raw = record.get(key)
            if isinstance(raw, str):
                add_candidate(raw)

        post_id = record.get("id") or record.get("status_id")
        user_id = record.get("user_id")
        user = record.get("user")
        if user_id is None and isinstance(user, dict):
            user_id = user.get("id")

        if post_id is not None and user_id is not None:
            candidate = f"{base_url.rstrip('/')}/{user_id}/{post_id}"
            add_candidate(candidate)

        for key in ("status", "retweeted_status", "retweet_status", "original_status"):
            visit_status(record.get(key))

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in {"list", "items", "statuses", "data", "cards"} and isinstance(value, list):
                    for item in value:
                        visit_status(item)
                        walk(item)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                visit_status(item)
                walk(item)

    walk(payload)
    return sorted(urls)


def parse_post(html: str, url: str, fetched_at: datetime | None = None) -> tuple[NormalizedPost | None, NormalizedAuthor | None]:
    fetched_at = fetched_at or datetime.now(UTC)
    meta = _meta_map(html)
    post_id = extract_post_id(url)
    if post_id is None:
        return None, None

    embedded = _parse_embedded_json(html)
    author_id: str | None = None
    author_name: str | None = None
    content_text = ""
    like_count = comment_count = repost_count = None

    for item in embedded:
        if "article" in item and isinstance(item["article"], dict):
            article = item["article"]
            content_text = article.get("description") or article.get("content") or content_text
            like_count = article.get("like_count", like_count)
            comment_count = article.get("comment_count", comment_count)
            repost_count = article.get("retweet_count", repost_count)
        if "user" in item and isinstance(item["user"], dict):
            user = item["user"]
            author_id = str(user.get("id") or author_id) if user.get("id") else author_id
            author_name = user.get("screen_name") or user.get("name") or author_name

    title = meta.get("og:title") or meta.get("twitter:title")
    if not title:
        title_match = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
        title = _strip_html(title_match.group(1)) if title_match else None

    body_match = re.search(
        r'<div[^>]+(?:class="[^"]*(?:article__bd|status-content|reply-container)[^"]*"|data-testid="article-content")[^>]*>(.*?)</div>',
        html,
        re.I | re.S,
    )
    if body_match:
        content_text = _strip_html(body_match.group(1))
    elif not content_text:
        description = meta.get("description") or meta.get("og:description")
        content_text = _strip_html(description or "")

    if not content_text:
        return None, None

    published_at = _parse_datetime(meta.get("article:published_time"))
    if not published_at:
        time_match = TIME_RE.search(html)
        published_at = _parse_datetime(time_match.group(1) if time_match else None)

    canonical_url = meta.get("og:url") or url
    language = meta.get("og:locale")

    post = NormalizedPost(
        source="xueqiu",
        source_entity_id=post_id,
        canonical_url=canonical_url,
        author_entity_id=author_id,
        title=title,
        content_text=content_text,
        language=language,
        published_at=published_at,
        fetched_at=fetched_at,
        like_count=like_count,
        comment_count=comment_count,
        repost_count=repost_count,
    )

    author = None
    if author_id or author_name:
        author = NormalizedAuthor(
            source="xueqiu",
            source_entity_id=author_id or "unknown",
            username=author_name,
            display_name=author_name,
            profile_url=urljoin(url, f"/u/{author_id}") if author_id else None,
            fetched_at=fetched_at,
        )

    return post, author


def parse_comments(payload: dict[str, Any], post_id: str, fetched_at: datetime | None = None) -> list[NormalizedComment]:
    fetched_at = fetched_at or datetime.now(UTC)
    records = payload.get("comments") or payload.get("list") or []
    results: list[NormalizedComment] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        comment_id = record.get("id")
        if comment_id is None:
            continue
        user = record.get("user") or {}
        created_at = _parse_datetime(record.get("created_at") or record.get("created_at_text"))
        text = _strip_html(str(record.get("text") or record.get("description") or ""))
        if not text:
            continue
        results.append(
            NormalizedComment(
                source="xueqiu",
                source_entity_id=str(comment_id),
                post_entity_id=post_id,
                canonical_url=record.get("url"),
                author_entity_id=str(user.get("id")) if user.get("id") is not None else None,
                parent_comment_entity_id=str(record.get("reply_id")) if record.get("reply_id") is not None else None,
                content_text=text,
                published_at=created_at,
                fetched_at=fetched_at,
                like_count=record.get("like_count"),
            )
        )
    return results
