from __future__ import annotations

import logging
import time
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
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.guba.api import GubaClient, GubaHttpResult, GubaTransport
from alphapulse.sources.guba.browser import GubaBrowserClient
from alphapulse.sources.guba.parser import GubaListEntry, parse_article_list, parse_post_detail, parse_replies
from alphapulse.sources.guba.urls import (
    board_list_url,
    comment_refresh_url,
    extract_post_ref,
    normalize_board_code,
    post_detail_url,
)
from alphapulse.storage.rawstore import FetchRecord, RawResponseStore


logger = logging.getLogger(__name__)

LIST_PAGE_SIZE = 80


class GubaAdapter:
    source_name = "guba"

    def __init__(
        self,
        settings: GubaSettings,
        crawl_settings: CrawlSettings,
        *,
        client: GubaClient | None = None,
        browser_client: GubaBrowserClient | None = None,
        raw_store: RawResponseStore | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.client = client or GubaClient(settings, crawl_settings)
        self.browser_client = browser_client
        if self.browser_client is None and settings.browser.enabled:
            self.browser_client = GubaBrowserClient(settings.browser)
        self.raw_store = raw_store
        self._blocked_until = 0.0
        self._blocked_kind: str | None = None

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        tasks: list[CrawlTask] = []
        base = str(self.settings.base_url)
        for code in seed.guba_board_codes:
            code = normalize_board_code(code) or code
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=board_list_url(base, code),
                    seed_name=seed.name,
                    priority=160,
                    metadata={"seed_kind": "board", "board_code": code, "page": 1},
                )
            )
        for url in seed.post_urls:
            ref = extract_post_ref(str(url))
            if ref is None or "guba.eastmoney.com" not in str(url):
                continue
            board_code, post_id = ref
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=post_detail_url(base, board_code, post_id),
                    seed_name=seed.name,
                    priority=200,
                    metadata={"seed_kind": "post_url", "post_id": post_id, "board_code": board_code},
                )
            )
        return tasks

    def fetch_item(self, task: CrawlTask) -> FetchOutcome:
        return self._fetch_item(task, transport=None)

    def fetch_item_with_transport(
        self,
        task: CrawlTask,
        transport: GubaTransport,
    ) -> FetchOutcome:
        return self._fetch_item(task, transport=transport)

    def _fetch_item(
        self,
        task: CrawlTask,
        *,
        transport: GubaTransport | None,
    ) -> FetchOutcome:
        if self.is_circuit_open():
            return FetchOutcome(blocked=True)

        # Both page types embed a data payload on a healthy 200: list pages
        # carry `var article_list=`, post pages `var post_article=`. Its
        # absence on a 200 marks a WAF/soft-block page, so hand the marker to
        # the client and let it retry/rotate instead of surfacing downstream
        # as a parse error. Deleted posts redirect to `/error`, which the
        # client's soft-block check excludes, so they still fall through to
        # the `/error` handling in `_handle_post_detail`.
        expect_marker = "var article_list" if task.kind == "discover" else "var post_article"
        if task.kind == "fetch_post" and self.browser_client is not None:
            response = self.browser_client.get(str(task.url))
        elif transport is not None:
            response = self.client.get(
                str(task.url),
                expect_marker=expect_marker,
                transport=transport,
            )
        else:
            response = self.client.get(str(task.url), expect_marker=expect_marker)

        if response.status_code == 0:
            self._save_raw(response, task.kind, requested_url=str(task.url))
            outcome = FetchOutcome(blocked=False, status_code=None)
            outcome.errors.append(f"Fetch failed for {task.url}: {response.error_message}")
            return outcome

        if response.blocked:
            used_browser = task.kind == "fetch_post" and self.browser_client is not None
            self._trip_circuit(
                response.block_kind,
                transport_scoped=(
                    transport in {"agent", "existing"} and not used_browser
                ),
            )
            self._save_raw(response, task.kind, requested_url=str(task.url))
            outcome = FetchOutcome(blocked=True, status_code=response.status_code)
            outcome.errors.append(f"Blocked ({response.block_kind}) from {task.url}")
            return outcome

        if task.kind == "discover":
            return self._handle_list_page(task, response)
        return self._handle_post_detail(task, response)

    def _handle_list_page(self, task: CrawlTask, response: GubaHttpResult) -> FetchOutcome:
        outcome = FetchOutcome(status_code=response.status_code)
        board_code = normalize_board_code(
            str(task.metadata.get("board_code") or "")
        ) or ""
        page = int(task.metadata.get("page") or 1)

        article_list = parse_article_list(response.text)
        if article_list is None:
            self._save_raw(
                response,
                task.kind,
                requested_url=str(task.url),
                block_kind="empty_payload",
                parser_error=f"No article_list payload in {task.url}",
                meta={"board_code": board_code, "page": page},
            )
            outcome.errors.append(f"Could not parse article_list from {task.url}")
            return outcome

        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={
                "board_code": board_code,
                "page": page,
                "entries": len(article_list.entries),
                "total_count": article_list.total_count,
            },
        )

        base = str(self.settings.base_url)
        day_start = self._day_start()
        seen_post_ids: set[str] = set()
        for entry in article_list.entries:
            if entry.post_id in seen_post_ids:
                continue
            seen_post_ids.add(entry.post_id)
            request_code = str(entry.stockbar_code or board_code)
            code = normalize_board_code(request_code) or request_code
            if not request_code:
                continue
            # Day-scoped crawl: only publish today's posts. List pages are sorted
            # by last-reply time, so old posts bumped by fresh replies resurface
            # near the top; filter by publish time to keep only today's.
            if day_start is not None and not (
                entry.publish_time is not None and entry.publish_time >= day_start
            ):
                continue
            detail_url = post_detail_url(base, request_code, entry.post_id)
            pubdate_ts = self._entry_ts(entry)
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=detail_url,
                    seed_name=task.seed_name,
                    priority=150,
                    metadata={
                        "post_id": entry.post_id,
                        "board_code": code,
                        "pubdate_ts": pubdate_ts,
                        "comment_count": entry.comment_count,
                    },
                )
            )
            # Posts bumped by new replies resurface on early list pages sorted
            # by last-reply time; re-emitting the (stable-URL) refresh task
            # lets the state store's claim gate throttle actual refreshes.
            if entry.comment_count and self.settings.fetch_comments:
                outcome.discovered_tasks.append(
                    CrawlTask(
                        source=self.source_name,
                        kind="refresh_comments",
                        url=comment_refresh_url(base, code, entry.post_id),
                        seed_name=task.seed_name,
                        priority=100,
                        metadata={
                            "post_id": entry.post_id,
                            "canonical_url": detail_url,
                            "board_code": code,
                            "pubdate_ts": pubdate_ts,
                        },
                    )
                )

        if day_start is not None:
            # Keep paginating while this page still holds a post active today
            # (last_time >= day start). Because last_time >= publish_time, every
            # post published today sorts above the first all-stale page, so this
            # reaches the full day up to the max_list_pages per-board cap.
            page_has_today = any(
                entry.last_time is not None and entry.last_time >= day_start
                for entry in article_list.entries
            )
            should_paginate = page < self.settings.max_list_pages and page_has_today
            if page_has_today and page >= self.settings.max_list_pages:
                # Capped mid-day: today's board has more posts than we crawl.
                # Log it so the truncation is visible, not silent.
                logger.info(
                    "Guba board hit day-scoped page cap; older same-day posts skipped",
                    extra={
                        "event": "guba_day_page_cap",
                        "extra_data": {"board_code": board_code, "pages": page},
                    },
                )
        else:
            total = article_list.total_count or 0
            should_paginate = page < self.settings.max_list_pages and page * LIST_PAGE_SIZE < total

        if should_paginate:
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=board_list_url(base, board_code, page + 1),
                    seed_name=task.seed_name,
                    priority=119,
                    metadata={"seed_kind": "board", "board_code": board_code, "page": page + 1},
                )
            )
        return outcome

    def _day_start(self) -> datetime | None:
        """Start of the current day in the ranking timezone, or None if not day-scoped."""
        if not self.settings.day_scoped:
            return None
        now = datetime.now(ZoneInfo(self.settings.ranking_timezone))
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    def _handle_post_detail(self, task: CrawlTask, response: GubaHttpResult) -> FetchOutcome:
        outcome = FetchOutcome(status_code=response.status_code)
        post_id = str(task.metadata.get("post_id") or "")
        fetched_at = datetime.now(UTC)

        if "/error" in response.url:
            self._save_raw(
                response,
                task.kind,
                requested_url=str(task.url),
                block_kind="deleted",
                meta={"post_id": post_id},
            )
            outcome.errors.append(f"Post deleted or missing: {task.url}")
            return outcome

        post, author, post_meta = parse_post_detail(response.text, str(task.url), fetched_at)
        meta: dict[str, Any] = {"post_id": post_id}
        if post_meta is not None:
            meta.update(
                {
                    "post_mod_count": post_meta.mod_count,
                    "post_mod_time": post_meta.mod_time,
                    "post_state": post_meta.state,
                }
            )

        if post is None:
            self._save_raw(
                response,
                task.kind,
                requested_url=str(task.url),
                block_kind="empty_payload" if post_meta is None else None,
                parser_error=f"Could not parse post_article from {task.url}",
                meta=meta,
            )
            outcome.errors.append(f"Could not parse post payload from {task.url}")
            return outcome

        if post_meta is not None and post_meta.state not in (0, None):
            self._save_raw(
                response,
                task.kind,
                requested_url=str(task.url),
                block_kind="deleted",
                meta=meta,
            )
            outcome.errors.append(f"Post state {post_meta.state} (removed/hidden): {task.url}")
            return outcome

        self._save_raw(response, task.kind, requested_url=str(task.url), meta=meta)
        outcome.posts.append(post)
        if author is not None:
            outcome.authors.append(author)
        return outcome

    def refresh_comments(self, item_ref: ItemReference) -> list[NormalizedComment]:
        if self.is_circuit_open():
            return []

        post_id = item_ref.source_entity_id
        if not post_id:
            return []
        board_code = str(item_ref.metadata.get("board_code") or "")
        if not board_code:
            ref = extract_post_ref(str(item_ref.canonical_url))
            if ref is None:
                return []
            board_code = ref[0]

        comments: list[NormalizedComment] = []
        seen_ids: set[str] = set()
        total_count: int | None = None
        page = 1
        while page <= self.settings.max_reply_pages:
            response = self.client.post_replies(post_id=post_id, board_code=board_code, page=page)
            meta = {"post_id": post_id, "board_code": board_code, "page": page}
            if response.status_code == 0 or response.blocked:
                if response.blocked:
                    self._trip_circuit(response.block_kind)
                self._save_raw(
                    response,
                    "refresh_comments",
                    requested_url=str(item_ref.canonical_url),
                    method="POST",
                    meta=meta,
                )
                break

            payload = response.json()
            if payload is None:
                self._save_raw(
                    response,
                    "refresh_comments",
                    requested_url=str(item_ref.canonical_url),
                    method="POST",
                    block_kind="empty_payload",
                    parser_error=f"Reply payload is not JSON for post {post_id} page {page}",
                    meta=meta,
                )
                break

            page_comments, count = parse_replies(
                payload, post_id, str(item_ref.canonical_url), datetime.now(UTC)
            )
            total_count = count if count is not None else total_count
            self._save_raw(
                response,
                "refresh_comments",
                requested_url=str(item_ref.canonical_url),
                method="POST",
                meta={**meta, "replies": len(page_comments), "total_count": total_count},
            )

            fresh = [c for c in page_comments if c.source_entity_id not in seen_ids]
            if not fresh:
                break
            seen_ids.update(c.source_entity_id for c in fresh)
            comments.extend(fresh)

            raw_replies = payload.get("re") or []
            if len(raw_replies) < self.settings.reply_page_size:
                break
            if total_count is not None and page * self.settings.reply_page_size >= total_count:
                break
            page += 1
        return comments

    def is_circuit_open(self) -> bool:
        if time.monotonic() >= self._blocked_until:
            self._blocked_until = 0.0
            self._blocked_kind = None
            return False
        return True

    def _trip_circuit(
        self,
        block_kind: str | None,
        *,
        transport_scoped: bool = False,
    ) -> None:
        # soft_block is a heuristic (a 200 response missing its expected data
        # marker) rather than a confirmed block, so it still stops this
        # task/cycle via FetchOutcome.blocked but must not arm the long
        # cooldown the way a confirmed http_403/http_429/captcha/login
        # redirect does — that would turn a possibly IP-caused or
        # single-URL soft block into a multi-hour outage for the whole
        # source.
        if block_kind == "soft_block" or transport_scoped:
            return
        self._blocked_until = time.monotonic() + self.settings.block_cooldown_seconds
        self._blocked_kind = block_kind or "blocked"
        logger.warning(
            "Guba circuit opened after blocked response",
            extra={
                "event": "guba_circuit_open",
                "extra_data": {
                    "block_kind": self._blocked_kind,
                    "cooldown_seconds": self.settings.block_cooldown_seconds,
                },
            },
        )

    def comment_task_for_post(self, post: NormalizedPost, seed_name: str) -> CrawlTask | None:
        if not self.settings.fetch_comments:
            return None
        ref = extract_post_ref(str(post.canonical_url))
        board_code = ref[0] if ref else ""
        base = str(self.settings.base_url)
        pubdate_ts = int(post.published_at.timestamp()) if post.published_at else 0
        return CrawlTask(
            source=self.source_name,
            kind="refresh_comments",
            url=comment_refresh_url(base, board_code, post.source_entity_id),
            seed_name=seed_name,
            priority=100,
            metadata={
                "post_id": post.source_entity_id,
                "canonical_url": str(post.canonical_url),
                "board_code": board_code,
                "pubdate_ts": pubdate_ts,
            },
        )

    def available_agent_capacity(self) -> int:
        agent_pool = getattr(self.client, "agent_pool", None)
        if agent_pool is None:
            return 0
        return agent_pool.store.available_capacity("http")

    @staticmethod
    def _entry_ts(entry: GubaListEntry) -> int:
        moment = entry.last_time or entry.publish_time
        return int(moment.timestamp()) if moment else 0

    def _save_raw(
        self,
        response: GubaHttpResult,
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
                extra={"event": "guba_rawstore_error", "extra_data": {"url": requested_url}},
            )
