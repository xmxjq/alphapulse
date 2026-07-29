from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from alphapulse.pipeline.contracts import (
    CrawlTask,
    FetchOutcome,
    ItemReference,
    NormalizedComment,
    NormalizedPost,
    SeedDefinition,
)
from alphapulse.runtime.config import CrawlSettings, TgbSettings
from alphapulse.sources.tgb.api import TgbClient, TgbHttpResult, is_missing_page
from alphapulse.sources.tgb.parser import (
    TgbListEntry,
    parse_comments,
    parse_list_page,
    parse_post_detail,
    parse_stock_feed,
)
from alphapulse.sources.tgb.urls import (
    KIND_FEATURED,
    KIND_GENERAL,
    KIND_STOCK,
    featured_list_url,
    general_list_url,
    post_detail_url,
    stock_list_url,
)
from alphapulse.storage.rawstore import FetchRecord, RawResponseStore


logger = logging.getLogger(__name__)

# Board-tier priorities so that a post appearing in several feeds keeps the most
# specific attribution: featured > stock > general (the state claim gate keeps only
# the first fetch of a given post URL within the recrawl window).
_DISCOVER_PRIORITY = {KIND_FEATURED: 132, KIND_STOCK: 126, KIND_GENERAL: 120}
_FETCH_PRIORITY = {KIND_FEATURED: 152, KIND_STOCK: 151, KIND_GENERAL: 150}

_LIST_MARKER = "Nbbs-middle-list"
_STOCK_MARKER = "stockContent"
_DETAIL_MARKER = "article-content"


class TgbAdapter:
    source_name = "tgb"

    def __init__(
        self,
        settings: TgbSettings,
        crawl_settings: CrawlSettings,
        *,
        client: TgbClient | None = None,
        raw_store: RawResponseStore | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.client = client or TgbClient(settings, crawl_settings)
        self.raw_store = raw_store

    def _board_kind(self, code: str) -> str:
        if code == self.settings.featured_slug:
            return KIND_FEATURED
        if code == self.settings.general_slug:
            return KIND_GENERAL
        return KIND_STOCK

    def _list_url(self, kind: str, code: str, page: int) -> str:
        base = str(self.settings.base_url)
        if kind == KIND_FEATURED:
            return featured_list_url(base, code, page)
        if kind == KIND_STOCK:
            return stock_list_url(base, code)
        return general_list_url(base, code, page)

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        tasks: list[CrawlTask] = []
        for code in seed.tgb_board_codes:
            kind = self._board_kind(code)
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=self._list_url(kind, code, 1),
                    seed_name=seed.name,
                    priority=_DISCOVER_PRIORITY[kind],
                    metadata={"board_code": code, "board_kind": kind, "page": 1},
                )
            )
        return tasks

    def fetch_item(self, task: CrawlTask) -> FetchOutcome:
        kind = str(task.metadata.get("board_kind") or "")
        if task.kind == "discover":
            marker = _STOCK_MARKER if kind == KIND_STOCK else _LIST_MARKER
        else:
            marker = _DETAIL_MARKER
        response = self.client.get(str(task.url), expect_marker=marker)

        if response.status_code == 0:
            self._save_raw(response, task.kind, requested_url=str(task.url))
            outcome = FetchOutcome(blocked=False, status_code=None)
            outcome.errors.append(f"Fetch failed for {task.url}: {response.error_message}")
            return outcome

        if response.blocked:
            self._save_raw(response, task.kind, requested_url=str(task.url))
            outcome = FetchOutcome(blocked=True, status_code=response.status_code)
            outcome.errors.append(f"Blocked ({response.block_kind}) from {task.url}")
            return outcome

        if task.kind == "fetch_post" and is_missing_page(response.text):
            self._save_raw(
                response,
                task.kind,
                requested_url=str(task.url),
                block_kind="missing",
                meta={"post_id": task.metadata.get("post_id")},
            )
            outcome = FetchOutcome(blocked=False, status_code=response.status_code)
            outcome.errors.append(f"Post deleted or missing: {task.url}")
            return outcome

        if task.kind == "discover":
            if kind == KIND_STOCK:
                return self._handle_stock_feed(task, response)
            return self._handle_list_page(task, response)
        return self._handle_post_detail(task, response)

    def _day_start(self) -> datetime | None:
        if not self.settings.day_scoped:
            return None
        now = datetime.now(ZoneInfo(self.settings.ranking_timezone))
        return now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)

    def _post_task(self, task: CrawlTask, entry: TgbListEntry, kind: str, code: str) -> CrawlTask:
        base = str(self.settings.base_url)
        detail_url = post_detail_url(base, entry.post_id)
        return CrawlTask(
            source=self.source_name,
            kind="fetch_post",
            url=detail_url,
            seed_name=task.seed_name,
            priority=_FETCH_PRIORITY[kind],
            metadata={
                "post_id": entry.post_id,
                "board_code": code,
                "board_kind": kind,
                "canonical_url": detail_url,
            },
        )

    def _handle_list_page(self, task: CrawlTask, response: TgbHttpResult) -> FetchOutcome:
        outcome = FetchOutcome(status_code=response.status_code)
        code = str(task.metadata.get("board_code") or "")
        kind = str(task.metadata.get("board_kind") or KIND_GENERAL)
        page = int(task.metadata.get("page") or 1)

        entries = parse_list_page(response.text)
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"board_code": code, "board_kind": kind, "page": page, "entries": len(entries)},
        )
        if not entries:
            outcome.errors.append(f"No list entries parsed from {task.url}")
            return outcome

        day_start = self._day_start()
        for entry in entries:
            if day_start is not None and not (
                entry.publish_time is not None and entry.publish_time >= day_start
            ):
                continue
            outcome.discovered_tasks.append(self._post_task(task, entry, kind, code))

        # Feeds are sorted by post date descending, so keep paginating while a page
        # still holds a post published today, capped at max_list_pages per board.
        if day_start is not None:
            page_has_today = any(
                entry.publish_time is not None and entry.publish_time >= day_start
                for entry in entries
            )
            should_paginate = page < self.settings.max_list_pages and page_has_today
            if page_has_today and page >= self.settings.max_list_pages:
                logger.info(
                    "Tgb board hit day-scoped page cap; older same-day posts skipped",
                    extra={
                        "event": "tgb_day_page_cap",
                        "extra_data": {"board_code": code, "pages": page},
                    },
                )
        else:
            should_paginate = page < self.settings.max_list_pages

        if should_paginate:
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=self._list_url(kind, code, page + 1),
                    seed_name=task.seed_name,
                    priority=_DISCOVER_PRIORITY[kind] - 1,
                    metadata={"board_code": code, "board_kind": kind, "page": page + 1},
                )
            )
        return outcome

    def _handle_stock_feed(self, task: CrawlTask, response: TgbHttpResult) -> FetchOutcome:
        outcome = FetchOutcome(status_code=response.status_code)
        code = str(task.metadata.get("board_code") or "")

        entries = parse_stock_feed(response.text)
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"board_code": code, "board_kind": KIND_STOCK, "entries": len(entries)},
        )
        if not entries:
            # An empty mention feed is normal (a quiet stock), not an error.
            return outcome

        day_start = self._day_start()
        for entry in entries:
            if day_start is not None and not (
                entry.publish_time is not None and entry.publish_time >= day_start
            ):
                continue
            outcome.discovered_tasks.append(self._post_task(task, entry, KIND_STOCK, code))
        return outcome

    def _handle_post_detail(self, task: CrawlTask, response: TgbHttpResult) -> FetchOutcome:
        outcome = FetchOutcome(status_code=response.status_code)
        post_id = str(task.metadata.get("post_id") or "")
        board_code = str(task.metadata.get("board_code") or "") or None
        fetched_at = datetime.now(UTC)

        post, author = parse_post_detail(
            response.text, str(task.url), fetched_at, board_code=board_code
        )
        if post is None:
            self._save_raw(
                response,
                task.kind,
                requested_url=str(task.url),
                block_kind="empty_payload",
                parser_error=f"Could not parse post from {task.url}",
                meta={"post_id": post_id, "board_code": board_code},
            )
            outcome.errors.append(f"Could not parse post payload from {task.url}")
            return outcome

        self._save_raw(
            response, task.kind, requested_url=str(task.url),
            meta={"post_id": post_id, "board_code": board_code},
        )
        outcome.posts.append(post)
        if author is not None:
            outcome.authors.append(author)
        return outcome

    def refresh_comments(self, item_ref: ItemReference) -> list[NormalizedComment]:
        post_id = item_ref.source_entity_id
        if not post_id:
            return []
        canonical_url = str(item_ref.canonical_url)
        response = self.client.get(canonical_url, expect_marker=_DETAIL_MARKER)
        if response.status_code == 0 or response.blocked:
            self._save_raw(
                response,
                "refresh_comments",
                requested_url=canonical_url,
                meta={"post_id": post_id},
            )
            return []
        comments = parse_comments(response.text, post_id, canonical_url, datetime.now(UTC))
        self._save_raw(
            response,
            "refresh_comments",
            requested_url=canonical_url,
            meta={"post_id": post_id, "replies": len(comments)},
        )
        return comments

    def comment_task_for_post(self, post: NormalizedPost, seed_name: str) -> CrawlTask:
        return CrawlTask(
            source=self.source_name,
            kind="refresh_comments",
            url=post.canonical_url,
            seed_name=seed_name,
            priority=300,
            metadata={
                "post_id": post.source_entity_id,
                "canonical_url": str(post.canonical_url),
                "board_code": post.raw_topic_ids[0] if post.raw_topic_ids else "",
            },
        )

    def continue_after_blocked_task(self, task: CrawlTask) -> bool:
        return task.kind == "fetch_post" and (
            getattr(self.client, "agent_pool", None) is not None
            or getattr(self.client, "proxy_provider", None) is not None
        )

    def _save_raw(
        self,
        response: TgbHttpResult,
        task_kind: str,
        *,
        requested_url: str,
        method: str = "GET",
        block_kind: str | None = None,
        parser_error: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if self.raw_store is None:
            return
        record_meta = dict(meta or {})
        if response.url and response.url != requested_url:
            record_meta["final_url"] = response.url
        if response.error_message:
            record_meta["error_message"] = response.error_message
        try:
            self.raw_store.save(
                FetchRecord(
                    source=self.source_name,
                    url=requested_url,
                    method=method,
                    status_code=response.status_code or None,
                    duration_ms=response.duration_ms,
                    task_kind=task_kind,
                    block_kind=block_kind or response.block_kind,
                    parser_error=parser_error,
                    meta=record_meta,
                ),
                response.text or None,
            )
        except Exception:
            logger.exception(
                "Failed to persist raw response",
                extra={"event": "tgb_rawstore_error", "extra_data": {"url": requested_url}},
            )
