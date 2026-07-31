from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

from lxml import html as lxml_html

from alphapulse.pipeline.contracts import NormalizedAuthor, NormalizedPost
from alphapulse.sources.hupu.urls import extract_post_id


CN_TZ = timezone(timedelta(hours=8))
PROFILE_URL_TEMPLATE = "https://my.hupu.com/{user_id}"
_SHORT_DT_RE = re.compile(r"(\d{2})-(\d{2})\s+(\d{2}):(\d{2})")
_FULL_DT_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?"
)
_COUNTS_RE = re.compile(r"([\d,]+)\s*/\s*([\d,]+)")
_PROFILE_ID_RE = re.compile(r"my\.hupu\.com/(\d+)")
_INTEGER_RE = re.compile(r"(\d+)")


def _text(element) -> str:
    return re.sub(r"\s+", " ", element.text_content()).strip()


def _first(node, selector):
    matches = node.cssselect(selector)
    return matches[0] if matches else None


def _integer(value: str | None) -> int | None:
    match = _INTEGER_RE.search(value or "")
    return int(match.group(1)) if match else None


def parse_short_datetime(
    value: str | None,
    *,
    reference: datetime | None = None,
) -> datetime | None:
    if not value:
        return None
    match = _SHORT_DT_RE.search(value)
    if match is None:
        return None
    reference = reference or datetime.now(CN_TZ)
    month, day, hour, minute = match.groups()
    try:
        parsed = datetime(
            reference.year,
            int(month),
            int(day),
            int(hour),
            int(minute),
            tzinfo=CN_TZ,
        )
    except ValueError:
        return None
    if parsed - reference.astimezone(CN_TZ) > timedelta(days=1):
        try:
            parsed = parsed.replace(year=reference.year - 1)
        except ValueError:
            return None
    return parsed.astimezone(UTC)


def parse_full_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    match = _FULL_DT_RE.search(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = match.groups()
    try:
        parsed = datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second or 0),
            tzinfo=CN_TZ,
        )
    except ValueError:
        return None
    return parsed.astimezone(UTC)


@dataclass
class HupuListEntry:
    post_id: str
    title: str
    author_id: str | None
    author_name: str | None
    comment_count: int | None
    view_count: int | None
    published_at: datetime | None


def parse_list_page(
    html: str,
    *,
    reference: datetime | None = None,
) -> list[HupuListEntry]:
    doc = lxml_html.fromstring(html)
    entries: list[HupuListEntry] = []
    seen: set[str] = set()
    for row in doc.cssselect("li.bbs-sl-web-post-body"):
        title_link = _first(row, "a.p-title")
        if title_link is None:
            continue
        post_id = extract_post_id(title_link.get("href") or "")
        if not post_id or post_id in seen:
            continue
        seen.add(post_id)

        author_link = _first(row, ".post-auth a")
        author_href = author_link.get("href") if author_link is not None else ""
        author_match = _PROFILE_ID_RE.search(author_href or "")
        datum = _first(row, ".post-datum")
        counts = _COUNTS_RE.search(_text(datum)) if datum is not None else None
        time_cell = _first(row, ".post-time")
        entries.append(
            HupuListEntry(
                post_id=post_id,
                title=_text(title_link),
                author_id=author_match.group(1) if author_match else None,
                author_name=_text(author_link) if author_link is not None else None,
                comment_count=int(counts.group(1).replace(",", "")) if counts else None,
                view_count=int(counts.group(2).replace(",", "")) if counts else None,
                published_at=parse_short_datetime(
                    _text(time_cell) if time_cell is not None else None,
                    reference=reference,
                ),
            )
        )
    return entries


def infer_fixed_targets(
    title: str | None,
    content_text: str,
    fixed_targets: list[str],
    aliases: dict[str, list[str]],
) -> list[str]:
    haystack = f"{title or ''}\n{content_text}".casefold()
    return [
        target
        for target in fixed_targets
        if any(
            term and term.casefold() in haystack
            for term in [target, *aliases.get(target, [])]
        )
    ]


def parse_post_detail(
    html: str,
    url: str,
    fetched_at: datetime | None = None,
) -> tuple[NormalizedPost | None, NormalizedAuthor | None]:
    fetched_at = fetched_at or datetime.now(UTC)
    post_id = extract_post_id(url)
    if not post_id:
        return None, None

    doc = lxml_html.fromstring(html)
    title_el = _first(doc, "h1")
    title = _text(title_el) if title_el is not None else None

    body_el = _first(
        doc,
        "div[class*='post-content_main-post-info'] .thread-content-detail",
    )
    content_text = _text(body_el) if body_el is not None else ""
    if not content_text and title:
        content_text = title
    if not content_text:
        return None, None

    post_container = _first(doc, "div[class*='post-content_bbs-post-content']")
    if post_container is None:
        post_container = doc
    author_id = None
    author_name = None
    for link in post_container.cssselect("a[href*='my.hupu.com']"):
        match = _PROFILE_ID_RE.search(link.get("href") or "")
        name = _text(link)
        if match:
            author_id = author_id or match.group(1)
        if name:
            author_name = name
            break

    time_el = _first(post_container, "span[class*='post-user-comp-info-top-time']")
    reply_el = _first(doc, "span[class*='index_reply']")
    light_el = _first(doc, "span[class*='index_light']")
    post = NormalizedPost(
        source="hupu",
        source_entity_id=post_id,
        canonical_url=url,
        author_entity_id=author_id,
        title=title,
        content_text=content_text,
        language="zh",
        published_at=parse_full_datetime(_text(time_el) if time_el is not None else None),
        fetched_at=fetched_at,
        like_count=_integer(_text(light_el) if light_el is not None else None),
        comment_count=_integer(_text(reply_el) if reply_el is not None else None),
        repost_count=None,
        raw_topic_ids=[],
    )
    author = None
    if author_id or author_name:
        author = NormalizedAuthor(
            source="hupu",
            source_entity_id=author_id or "unknown",
            username=None,
            display_name=author_name,
            profile_url=(
                PROFILE_URL_TEMPLATE.format(user_id=author_id) if author_id else None
            ),
            fetched_at=fetched_at,
        )
    return post, author
