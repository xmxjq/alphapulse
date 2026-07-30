from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from lxml import html as lxml_html

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedPost


CN_TZ = ZoneInfo("Asia/Shanghai")


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=CN_TZ
        ).astimezone(UTC)
    except ValueError:
        return None


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    try:
        root = lxml_html.fragment_fromstring(value, create_parent="div")
        for node in root.xpath(".//script|.//style"):
            node.drop_tree()
        text = root.text_content()
    except (ValueError, TypeError):
        text = re.sub(r"<[^>]+>", " ", value)
    return " ".join(html.unescape(text).split())


@dataclass
class JiuyanSearchEntry:
    article_id: str
    title: str | None
    published_at: datetime | None
    comment_count: int | None
    like_count: int | None


@dataclass
class JiuyanSearchPage:
    page_no: int
    page_size: int
    total_count: int
    entries: list[JiuyanSearchEntry]


def infer_fixed_targets(
    title: str | None,
    content_text: str,
    fixed_targets: list[str],
    aliases: dict[str, list[str]],
) -> list[str]:
    haystack = f"{title or ''}\n{content_text}".casefold()
    matches: list[str] = []
    for target in fixed_targets:
        terms = [target, *aliases.get(target, [])]
        if any(term and term.casefold() in haystack for term in terms):
            matches.append(target)
    return matches


def parse_search_page(payload: dict[str, Any]) -> JiuyanSearchPage | None:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    rows = data.get("result")
    if not isinstance(rows, list):
        return None
    entries: list[JiuyanSearchEntry] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        article_id = str(row.get("article_id") or "").strip()
        if not article_id or article_id in seen:
            continue
        seen.add(article_id)
        entries.append(
            JiuyanSearchEntry(
                article_id=article_id,
                title=str(row.get("title") or "").strip() or None,
                published_at=parse_datetime(row.get("create_time")),
                comment_count=_int_or_none(row.get("comment_count")),
                like_count=_int_or_none(row.get("like_count")),
            )
        )
    return JiuyanSearchPage(
        page_no=int(data.get("pageNo") or 1),
        page_size=int(data.get("pageSize") or len(entries) or 15),
        total_count=int(data.get("totalCount") or 0),
        entries=entries,
    )


def parse_post_detail(
    payload: dict[str, Any],
    canonical_url: str,
    *,
    target_code: str | None,
    fetched_at: datetime | None = None,
) -> tuple[NormalizedPost | None, NormalizedAuthor | None]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None, None
    article_id = str(data.get("article_id") or "").strip()
    content_text = html_to_text(data.get("content"))
    title = str(data.get("title") or "").strip() or None
    if not article_id or (not content_text and not title):
        return None, None
    if not content_text:
        content_text = title or ""
    fetched_at = fetched_at or datetime.now(UTC)
    user = data.get("user") if isinstance(data.get("user"), dict) else {}
    user_id = str(data.get("user_id") or user.get("user_id") or "").strip() or None

    post = NormalizedPost(
        source="jiuyan",
        source_entity_id=article_id,
        canonical_url=canonical_url,
        author_entity_id=user_id,
        title=title,
        content_text=content_text,
        language="zh",
        published_at=parse_datetime(data.get("create_time")),
        fetched_at=fetched_at,
        like_count=_int_or_none(data.get("like_count")),
        comment_count=_int_or_none(data.get("comment_count")),
        repost_count=_int_or_none(data.get("forward_count")),
        raw_topic_ids=[target_code] if target_code else [],
    )

    author = None
    nickname = str(user.get("nickname") or "").strip() or None
    if user_id or nickname:
        author = NormalizedAuthor(
            source="jiuyan",
            source_entity_id=user_id or "unknown",
            display_name=nickname,
            profile_url=(
                f"https://www.jiuyangongshe.com/u/{user_id}" if user_id else None
            ),
            fetched_at=fetched_at,
        )
    return post, author


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
