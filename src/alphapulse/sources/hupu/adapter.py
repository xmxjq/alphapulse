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
from alphapulse.runtime.config import CrawlSettings, HupuSettings
from alphapulse.sources.hupu.api import HupuClient, HupuHttpResult, HupuTransport
from alphapulse.sources.hupu.parser import (
    HupuListEntry,
    infer_fixed_targets,
    parse_list_page,
    parse_post_detail,
)
from alphapulse.sources.hupu.urls import latest_posts_url, post_detail_url
from alphapulse.storage.rawstore import FetchRecord, RawResponseStore


logger = logging.getLogger(__name__)
_LIST_MARKER = "bbs-sl-web-post-body"
_DETAIL_MARKER = "thread-content-detail"
_DISCOVER_PRIORITY = 180
_FETCH_PRIORITY = 160


class HupuAdapter:
    source_name = "hupu"

    def __init__(
        self,
        settings: HupuSettings,
        crawl_settings: CrawlSettings,
        *,
        client: HupuClient | None = None,
        raw_store: RawResponseStore | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.client = client or HupuClient(settings, crawl_settings)
        self.raw_store = raw_store

    def is_circuit_open(self) -> bool:
        local_day = datetime.now(ZoneInfo(self.settings.ranking_timezone)).date()
        return not self.settings.authorization_active(local_day)

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        if self.is_circuit_open() or self.settings.board_slug not in seed.hupu_board_codes:
            return []
        return [
            CrawlTask(
                source=self.source_name,
                kind="discover",
                url=latest_posts_url(
                    str(self.settings.base_url), self.settings.board_slug, 1
                ),
                seed_name=seed.name,
                priority=_DISCOVER_PRIORITY,
                metadata={"board_code": self.settings.board_slug, "page": 1},
            )
        ]

    def fetch_item(self, task: CrawlTask) -> FetchOutcome:
        return self._fetch_item(task, transport="auto")

    def fetch_item_with_transport(
        self,
        task: CrawlTask,
        transport: HupuTransport,
    ) -> FetchOutcome:
        return self._fetch_item(task, transport=transport)

    def _fetch_item(
        self,
        task: CrawlTask,
        *,
        transport: HupuTransport,
    ) -> FetchOutcome:
        if self.is_circuit_open():
            return FetchOutcome(
                errors=[
                    "Hupu authorization expired on "
                    f"{self.settings.authorization_expires_on}"
                ]
            )
        marker = _LIST_MARKER if task.kind == "discover" else _DETAIL_MARKER
        response = self.client.get(
            str(task.url), expect_marker=marker, transport=transport
        )
        base = self._base_outcome(task, response)
        if base is not None:
            return base
        if task.kind == "discover":
            return self._handle_list_page(task, response)
        return self._handle_post_detail(task, response)

    def _base_outcome(
        self, task: CrawlTask, response: HupuHttpResult
    ) -> FetchOutcome | None:
        if response.status_code == 0:
            self._save_raw(response, task.kind, requested_url=str(task.url))
            outcome = FetchOutcome(status_code=None)
            outcome.errors.append(f"Fetch failed for {task.url}: {response.error_message}")
            return outcome
        if response.blocked:
            self._save_raw(response, task.kind, requested_url=str(task.url))
            outcome = FetchOutcome(blocked=True, status_code=response.status_code)
            outcome.errors.append(f"Blocked ({response.block_kind}) from {task.url}")
            return outcome
        if response.status_code == 404:
            outcome = FetchOutcome(status_code=404)
            outcome.errors.append(f"Post deleted or missing: {task.url}")
            return outcome
        return None

    def _day_start(self) -> datetime | None:
        if not self.settings.day_scoped:
            return None
        now = datetime.now(ZoneInfo(self.settings.ranking_timezone))
        return now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)

    def _post_task(self, task: CrawlTask, entry: HupuListEntry) -> CrawlTask:
        detail_url = post_detail_url(str(self.settings.base_url), entry.post_id)
        return CrawlTask(
            source=self.source_name,
            kind="fetch_post",
            url=detail_url,
            seed_name=task.seed_name,
            priority=_FETCH_PRIORITY,
            metadata={
                "post_id": entry.post_id,
                "board_code": self.settings.board_slug,
                "canonical_url": detail_url,
                "pubdate_ts": (
                    int(entry.published_at.timestamp()) if entry.published_at else 0
                ),
            },
        )

    def _handle_list_page(
        self, task: CrawlTask, response: HupuHttpResult
    ) -> FetchOutcome:
        page = int(task.metadata.get("page") or 1)
        entries = parse_list_page(response.text)
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"page": page, "entries": len(entries)},
        )
        outcome = FetchOutcome(status_code=response.status_code)
        if not entries:
            outcome.errors.append(f"No Hupu list entries parsed from {task.url}")
            return outcome

        day_start = self._day_start()
        for entry in entries:
            if day_start is not None and not (
                entry.published_at is not None and entry.published_at >= day_start
            ):
                continue
            outcome.discovered_tasks.append(self._post_task(task, entry))

        page_has_today = day_start is None or any(
            entry.published_at is not None and entry.published_at >= day_start
            for entry in entries
        )
        if page < self.settings.max_list_pages and page_has_today:
            next_page = page + 1
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=latest_posts_url(
                        str(self.settings.base_url),
                        self.settings.board_slug,
                        next_page,
                    ),
                    seed_name=task.seed_name,
                    priority=_DISCOVER_PRIORITY - next_page,
                    metadata={
                        "board_code": self.settings.board_slug,
                        "page": next_page,
                    },
                )
            )
        elif page_has_today and page >= self.settings.max_list_pages:
            logger.warning(
                "Hupu list hit day-scoped page cap",
                extra={
                    "event": "hupu_day_page_cap",
                    "extra_data": {"pages": page},
                },
            )
        return outcome

    def _handle_post_detail(
        self, task: CrawlTask, response: HupuHttpResult
    ) -> FetchOutcome:
        post, author = parse_post_detail(
            response.text, str(task.url), fetched_at=datetime.now(UTC)
        )
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"post_id": task.metadata.get("post_id")},
        )
        outcome = FetchOutcome(status_code=response.status_code)
        if post is None:
            outcome.errors.append(f"Could not parse Hupu post payload from {task.url}")
            return outcome
        fixed_targets = infer_fixed_targets(
            post.title,
            post.content_text,
            self.settings.fixed_targets,
            self.settings.fixed_target_aliases,
        )
        post = post.model_copy(
            update={
                "raw_topic_ids": list(
                    dict.fromkeys([self.settings.board_slug, *fixed_targets])
                )
            }
        )
        outcome.posts.append(post)
        if author is not None:
            outcome.authors.append(author)
        return outcome

    def refresh_comments(self, item_ref: ItemReference) -> list[NormalizedComment]:
        return []

    def comment_task_for_post(
        self, post: NormalizedPost, seed_name: str
    ) -> CrawlTask | None:
        return None

    def continue_after_blocked_task(self, task: CrawlTask) -> bool:
        return (
            getattr(self.client, "agent_pool", None) is not None
            or getattr(self.client, "proxy_provider", None) is not None
        )

    def available_agent_capacity(self) -> int:
        agent_pool = getattr(self.client, "agent_pool", None)
        if agent_pool is None:
            return 0
        return agent_pool.store.available_capacity("http", source="hupu")

    def _save_raw(
        self,
        response: HupuHttpResult,
        task_kind: str,
        *,
        requested_url: str,
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
                    method="GET",
                    status_code=response.status_code or None,
                    duration_ms=response.duration_ms,
                    task_kind=task_kind,
                    block_kind=response.block_kind,
                    meta=record_meta,
                ),
                response.text or None,
            )
        except Exception:
            logger.exception(
                "Failed to persist Hupu raw response",
                extra={
                    "event": "hupu_rawstore_error",
                    "extra_data": {"url": requested_url},
                },
            )
