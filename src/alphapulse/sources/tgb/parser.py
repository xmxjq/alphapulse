from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

from lxml import html as lxml_html

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedComment, NormalizedPost
from alphapulse.sources.tgb.urls import extract_post_id


CN_TZ = timezone(timedelta(hours=8))
PROFILE_URL_TEMPLATE = "https://www.tgb.cn/blog/{user_id}"

_FULL_DT_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?")
_SHORT_DT_RE = re.compile(r"(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}))?")
# Leading list markers such as [精] / [顶] / [荐] prepended to a title.
_TITLE_MARKER_RE = re.compile(r"^(?:\[[^\]]{1,3}\]\s*)+")
_BLOG_ID_RE = re.compile(r"/blog/(\d+)")
_COUNTS_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
_REPLY_ID_RE = re.compile(r"reply(\d+)")


def _text(element) -> str:
    return re.sub(r"\s+", " ", element.text_content()).strip()


def _first(node, selector):
    matches = node.cssselect(selector)
    return matches[0] if matches else None


def parse_full_datetime(value: str | None) -> datetime | None:
    """Parse a full 'YYYY-MM-DD[ HH:MM[:SS]]' Beijing wall-clock string into aware UTC."""
    if not value:
        return None
    match = _FULL_DT_RE.search(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = match.groups()
    try:
        dt = datetime(
            int(year), int(month), int(day),
            int(hour or 0), int(minute or 0), int(second or 0),
            tzinfo=CN_TZ,
        )
    except ValueError:
        return None
    return dt.astimezone(UTC)


def parse_short_datetime(value: str | None, *, reference: datetime | None = None) -> datetime | None:
    """Parse a yearless 'MM-DD[ HH:MM]' list timestamp, inferring the year from `reference`.

    tgb list rows omit the year. We assume the current Beijing year; if that lands the
    date more than a day in the future (a late-December post read in early January), we
    roll back a year.
    """
    if not value:
        return None
    match = _SHORT_DT_RE.search(value)
    if match is None:
        return None
    reference = reference or datetime.now(CN_TZ)
    month, day, hour, minute = match.groups()
    try:
        dt = datetime(
            reference.year, int(month), int(day),
            int(hour or 0), int(minute or 0),
            tzinfo=CN_TZ,
        )
    except ValueError:
        return None
    if dt - reference.astimezone(CN_TZ) > timedelta(days=1):
        try:
            dt = dt.replace(year=reference.year - 1)
        except ValueError:
            return None
    return dt.astimezone(UTC)


def _clean_title(title: str) -> str:
    return _TITLE_MARKER_RE.sub("", title).strip()


def _split_counts(text: str) -> tuple[int | None, int | None]:
    """Parse a 'comments / views' cell like '25 / 329' -> (25, 329)."""
    match = _COUNTS_RE.search(text)
    if match is None:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _blog_id(href: str | None) -> str | None:
    if not href:
        return None
    match = _BLOG_ID_RE.search(href)
    return match.group(1) if match else None


@dataclass
class TgbListEntry:
    post_id: str
    title: str
    user_id: str | None
    user_nickname: str | None
    comment_count: int | None
    click_count: int | None
    publish_time: datetime | None
    last_time: datetime | None


def parse_list_page(html: str, *, reference: datetime | None = None) -> list[TgbListEntry]:
    """Parse a 社区总版 / 精华 feed page (``.Nbbs-tiezi-lists`` rows)."""
    doc = lxml_html.fromstring(html)
    entries: list[TgbListEntry] = []
    seen: set[str] = set()
    for row in doc.cssselect("div.Nbbs-tiezi-lists"):
        title_link = _first(row, "div.middle-list-tittle a")
        if title_link is None:
            continue
        post_id = extract_post_id(title_link.get("href") or "")
        if not post_id or post_id in seen:
            continue
        seen.add(post_id)

        user_link = _first(row, "div.middle-list-user a")
        talk = _first(row, "div.middle-list-talk")
        reply_cell = _first(row, "div.middle-list-reply")
        post_cell = _first(row, "div.middle-list-post")
        comment_count, click_count = _split_counts(_text(talk)) if talk is not None else (None, None)

        entries.append(
            TgbListEntry(
                post_id=post_id,
                title=_clean_title(_text(title_link)),
                user_id=_blog_id(user_link.get("href")) if user_link is not None else None,
                user_nickname=_text(user_link) if user_link is not None else None,
                comment_count=comment_count,
                click_count=click_count,
                publish_time=parse_short_datetime(_text(post_cell), reference=reference)
                if post_cell is not None
                else None,
                last_time=parse_short_datetime(_text(reply_cell), reference=reference)
                if reply_cell is not None
                else None,
            )
        )
    return entries


def parse_stock_feed(html: str) -> list[TgbListEntry]:
    """Parse a /quotes/{code} discussion feed (``#forumRow_*`` mention blocks).

    Each block references an underlying post via an /a/{id} link and carries a full
    'YYYY-MM-DD HH:MM' timestamp; we de-duplicate to the underlying posts.
    """
    doc = lxml_html.fromstring(html)
    entries: list[TgbListEntry] = []
    seen: set[str] = set()
    for row in doc.cssselect("div[id^='forumRow_']"):
        post_link = None
        post_id = None
        for link in row.cssselect("a[href*='/a/']"):
            candidate = extract_post_id(link.get("href") or "")
            if candidate:
                post_link, post_id = link, candidate
                # Prefer the ".Rlink" source-post link (its text is the post title).
                if "Rlink" in (link.getparent().get("class") or ""):
                    break
        if not post_id or post_id in seen:
            continue
        seen.add(post_id)

        source_cell = _first(row, "div.related-sources")
        user_link = _first(row, "div.Rlink a[href*='/blog/']")
        if user_link is None:
            user_link = _first(row, "a[href*='/blog/']")
        title = _text(post_link).strip("《》 ") if post_link is not None else ""
        entries.append(
            TgbListEntry(
                post_id=post_id,
                title=_clean_title(title),
                user_id=_blog_id(user_link.get("href")) if user_link is not None else None,
                user_nickname=_text(user_link) if user_link is not None else None,
                comment_count=None,
                click_count=None,
                publish_time=parse_full_datetime(_text(source_cell)) if source_cell is not None else None,
                last_time=None,
            )
        )
    return entries


def parse_post_detail(
    html: str,
    url: str,
    fetched_at: datetime | None = None,
    *,
    board_code: str | None = None,
) -> tuple[NormalizedPost | None, NormalizedAuthor | None]:
    """Parse an /a/{id} post page into (post, author)."""
    fetched_at = fetched_at or datetime.now(UTC)
    doc = lxml_html.fromstring(html)

    post_id = extract_post_id(url)
    if not post_id:
        return None, None

    gio = _first(doc, "#gioMsg")
    title = None
    user_id = None
    user_name = None
    if gio is not None:
        title = (gio.get("subject") or "").strip() or None
        user_id = (gio.get("userID") or "").strip() or None
        user_name = (gio.get("username") or gio.get("userName") or "").strip() or None

    title_el = _first(doc, "div.article-tittle")
    if not title and title_el is not None:
        title = _clean_title(_text(title_el)) or None

    data_el = _first(doc, "div.article-data")
    published_at = None
    comment_count = None
    if data_el is not None:
        data_text = _text(data_el)
        published_at = parse_full_datetime(data_text)
        replies = re.search(r"评论\s*(\d+)", data_text)
        comment_count = int(replies.group(1)) if replies else None
        if user_id is None:
            author_link = _first(data_el, "a[href*='/blog/']")
            if author_link is not None:
                user_id = _blog_id(author_link.get("href"))
                user_name = user_name or _text(author_link)

    body_el = _first(doc, "div.article-text")
    content_text = _text(body_el) if body_el is not None else ""
    if not content_text and title:
        content_text = title
    if not content_text:
        return None, None

    post = NormalizedPost(
        source="tgb",
        source_entity_id=post_id,
        canonical_url=url,
        author_entity_id=user_id,
        title=title,
        content_text=content_text,
        language="zh",
        published_at=published_at,
        fetched_at=fetched_at,
        like_count=None,
        comment_count=comment_count,
        repost_count=None,
        raw_topic_ids=[board_code] if board_code else [],
    )

    author = None
    if user_id or user_name:
        author = NormalizedAuthor(
            source="tgb",
            source_entity_id=user_id or "unknown",
            username=None,
            display_name=user_name,
            profile_url=PROFILE_URL_TEMPLATE.format(user_id=user_id) if user_id else None,
            fetched_at=fetched_at,
        )
    return post, author


def parse_comments(
    html: str,
    post_id: str,
    canonical_url: str,
    fetched_at: datetime | None = None,
) -> list[NormalizedComment]:
    """Parse the first page of inline ``.comment-data`` reply blocks on a post page."""
    fetched_at = fetched_at or datetime.now(UTC)
    doc = lxml_html.fromstring(html)
    comments: list[NormalizedComment] = []
    seen: set[str] = set()
    for block in doc.cssselect("div.comment-data"):
        text_el = _first(block, "div.comment-data-text")
        if text_el is None:
            continue
        reply_match = _REPLY_ID_RE.search(text_el.get("id") or "")
        if reply_match is None:
            continue
        reply_id = reply_match.group(1)
        if reply_id in seen:
            continue
        seen.add(reply_id)

        content = _text(text_el)
        if not content:
            continue
        user_id = (block.get("ustr") or "").strip() or None
        date_el = _first(block, "span.pcyclspan")
        comments.append(
            NormalizedComment(
                source="tgb",
                source_entity_id=reply_id,
                post_entity_id=post_id,
                canonical_url=f"{canonical_url}/{reply_id}#{reply_id}",
                author_entity_id=user_id,
                parent_comment_entity_id=None,
                content_text=content,
                published_at=parse_full_datetime(_text(date_el)) if date_el is not None else None,
                fetched_at=fetched_at,
            )
        )
    return comments
