from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from html import unescape
from typing import Any

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost
from alphapulse.sources.guba.urls import extract_post_ref, normalize_board_code


CN_TZ = timezone(timedelta(hours=8))
TEXT_TAG_RE = re.compile(r"<[^>]+>")
PROFILE_URL_TEMPLATE = "https://i.eastmoney.com/{user_id}"


def _strip_html(html: str) -> str:
    text = TEXT_TAG_RE.sub(" ", html)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_cn_datetime(value: str | None) -> datetime | None:
    """Parse a Guba timestamp (naive Beijing wall-clock) into aware UTC."""
    if not value:
        return None
    cleaned = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=CN_TZ).astimezone(UTC)
        except ValueError:
            continue
    return None


def extract_embedded_json(html: str, var_name: str) -> dict[str, Any] | None:
    """Extract `var {var_name}={...};` from page HTML by brace matching.

    Brace matching (instead of a regex up to `};`) because post bodies can
    legitimately contain `};` sequences.
    """
    match = re.search(rf"var\s+{re.escape(var_name)}\s*=\s*\{{", html)
    if match is None:
        return None
    start = match.end() - 1
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(html)):
        char = html[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(html[start : index + 1])
                except json.JSONDecodeError:
                    return None
                return parsed if isinstance(parsed, dict) else None
    return None


@dataclass
class GubaListEntry:
    post_id: str
    title: str
    stockbar_code: str | None
    user_id: str | None
    user_nickname: str | None
    click_count: int | None
    comment_count: int | None
    publish_time: datetime | None
    last_time: datetime | None
    post_type: int | None
    post_state: int | None


@dataclass
class GubaArticleList:
    bar_code: str | None
    bar_name: str | None
    total_count: int | None
    entries: list[GubaListEntry]


def parse_article_list(html: str) -> GubaArticleList | None:
    payload = extract_embedded_json(html, "article_list")
    if payload is None:
        return None
    entries: list[GubaListEntry] = []
    for record in payload.get("re") or []:
        if not isinstance(record, dict):
            continue
        post_id = record.get("post_id")
        if post_id is None:
            continue
        entries.append(
            GubaListEntry(
                post_id=str(post_id),
                title=str(record.get("post_title") or ""),
                stockbar_code=record.get("stockbar_code"),
                user_id=str(record["user_id"]) if record.get("user_id") is not None else None,
                user_nickname=record.get("user_nickname"),
                click_count=record.get("post_click_count"),
                comment_count=record.get("post_comment_count"),
                publish_time=parse_cn_datetime(record.get("post_publish_time")),
                last_time=parse_cn_datetime(record.get("post_last_time")),
                post_type=record.get("post_type"),
                post_state=record.get("post_state"),
            )
        )
    return GubaArticleList(
        bar_code=payload.get("bar_code"),
        bar_name=payload.get("bar_name"),
        total_count=payload.get("count"),
        entries=entries,
    )


@dataclass
class GubaPostMeta:
    mod_count: int | None
    mod_time: str | None
    state: int | None
    ip_address: str | None


def parse_post_detail(
    html: str,
    url: str,
    fetched_at: datetime | None = None,
    *,
    fallback_title: str | None = None,
) -> tuple[NormalizedPost | None, NormalizedAuthor | None, GubaPostMeta | None]:
    fetched_at = fetched_at or datetime.now(UTC)
    payload = extract_embedded_json(html, "post_article")
    if payload is None:
        return None, None, None

    post_id = payload.get("post_id")
    if post_id is None:
        return None, None, None
    post_id = str(post_id)

    meta = GubaPostMeta(
        mod_count=payload.get("post_mod_count"),
        mod_time=payload.get("post_mod_time"),
        state=payload.get("post_state"),
        ip_address=payload.get("post_ip_address"),
    )

    content_text = _strip_html(str(payload.get("post_content") or ""))
    if not content_text:
        content_text = _strip_html(str(payload.get("post_abstract") or ""))
    title = payload.get("post_title") or fallback_title or None
    if not content_text and title:
        content_text = str(title)
    if not content_text:
        return None, None, meta

    user = payload.get("post_user") or {}
    user_id = str(user["user_id"]) if user.get("user_id") is not None else None

    ref = extract_post_ref(url)
    board_code = normalize_board_code(ref[0]) if ref else None

    post = NormalizedPost(
        source="guba",
        source_entity_id=post_id,
        canonical_url=url,
        author_entity_id=user_id,
        title=str(title) if title else None,
        content_text=content_text,
        language="zh",
        published_at=parse_cn_datetime(payload.get("post_publish_time")),
        fetched_at=fetched_at,
        like_count=payload.get("post_like_count"),
        comment_count=payload.get("post_comment_count"),
        repost_count=payload.get("post_forward_count"),
        raw_topic_ids=[board_code] if board_code else [],
    )

    author = None
    if user_id or user.get("user_nickname"):
        author = NormalizedAuthor(
            source="guba",
            source_entity_id=user_id or "unknown",
            username=user.get("user_name"),
            display_name=user.get("user_nickname"),
            profile_url=PROFILE_URL_TEMPLATE.format(user_id=user_id) if user_id else None,
            bio=user.get("user_introduce"),
            fetched_at=fetched_at,
        )

    return post, author, meta


def parse_replies(
    payload: dict[str, Any],
    post_id: str,
    canonical_url: str,
    fetched_at: datetime | None = None,
) -> tuple[list[NormalizedComment], int | None]:
    """Normalize a GetData.aspx reply payload; returns (comments, total_count)."""
    fetched_at = fetched_at or datetime.now(UTC)
    comments: list[NormalizedComment] = []

    def append_reply(record: dict[str, Any], parent_id: str | None) -> str | None:
        reply_id = record.get("reply_id")
        if reply_id is None:
            return None
        state = record.get("reply_state")
        if state is not None and state != 0:
            return None
        text = _strip_html(str(record.get("reply_text") or ""))
        if not text:
            return None
        user = record.get("reply_user") or {}
        user_id = record.get("user_id") or user.get("user_id")
        comments.append(
            NormalizedComment(
                source="guba",
                source_entity_id=str(reply_id),
                post_entity_id=post_id,
                canonical_url=f"{canonical_url}#reply{reply_id}",
                author_entity_id=str(user_id) if user_id is not None else None,
                parent_comment_entity_id=parent_id,
                content_text=text,
                published_at=parse_cn_datetime(record.get("reply_publish_time") or record.get("reply_time")),
                fetched_at=fetched_at,
                like_count=record.get("reply_like_count"),
            )
        )
        return str(reply_id)

    for record in payload.get("re") or []:
        if not isinstance(record, dict):
            continue
        reply_id = append_reply(record, None)
        for child in record.get("child_replys") or []:
            if isinstance(child, dict):
                append_reply(child, reply_id)

    return comments, payload.get("count")
