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
from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.jiuyan.api import JiuyanClient, JiuyanHttpResult
from alphapulse.sources.jiuyan.parser import parse_post_detail, parse_search_page
from alphapulse.sources.jiuyan.urls import (
    community_feed_url,
    post_detail_url,
    search_url,
)
from alphapulse.storage.rawstore import FetchRecord, RawResponseStore


logger = logging.getLogger(__name__)

COMMUNITY_FEED_LABELS = {
    "study": "研究优选",
    "square": "公社广场",
    "live": "生活区",
}


class JiuyanAdapter:
    source_name = "jiuyan"

    def __init__(
        self,
        settings: JiuyanSettings,
        crawl_settings: CrawlSettings,
        *,
        client: JiuyanClient | None = None,
        raw_store: RawResponseStore | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.client = client or JiuyanClient(settings, crawl_settings)
        self.raw_store = raw_store

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        tasks: list[CrawlTask] = []
        base_url = str(self.settings.base_url)
        if seed.jiuyan_target_codes:
            for rank, feed in enumerate(self.settings.community_feeds):
                tasks.append(
                    CrawlTask(
                        source=self.source_name,
                        kind="discover",
                        url=community_feed_url(base_url, feed),
                        seed_name=seed.name,
                        priority=180 - rank,
                        metadata={
                            "discovery_mode": "community",
                            "feed": feed,
                            "target_code": COMMUNITY_FEED_LABELS[feed],
                            "target_kind": "community",
                            "page": 1,
                        },
                    )
                )
        fixed = set(self.settings.fixed_targets)
        for rank, code in enumerate(seed.jiuyan_target_codes, start=1):
            is_fixed = code in fixed
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=search_url(base_url, code),
                    seed_name=seed.name,
                    priority=(170 if is_fixed else 165) - min(rank, 20),
                    metadata={
                        "target_code": code,
                        "target_kind": "fixed" if is_fixed else "hot",
                        "page": 1,
                    },
                )
            )
        return tasks

    def fetch_item(self, task: CrawlTask) -> FetchOutcome:
        if task.kind == "discover":
            if task.metadata.get("discovery_mode") == "community":
                feed = str(task.metadata.get("feed") or "")
                page = int(task.metadata.get("page") or 1)
                response = self.client.community_articles(
                    feed,
                    page,
                    page_size=self.settings.community_page_size,
                )
                return self._handle_community(task, response)
            keyword = str(task.metadata.get("target_code") or "")
            page = int(task.metadata.get("page") or 1)
            response = self.client.search_articles(keyword, page)
            return self._handle_search(task, response)
        article_id = str(task.metadata.get("article_id") or "")
        response = self.client.article_detail(article_id)
        return self._handle_detail(task, response)

    def _handle_search(
        self, task: CrawlTask, response: JiuyanHttpResult
    ) -> FetchOutcome:
        outcome = self._base_outcome(task, response)
        if outcome is not None:
            return outcome
        payload = response.json()
        page = parse_search_page(payload or {})
        target_code = str(task.metadata.get("target_code") or "")
        page_no = int(task.metadata.get("page") or 1)
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"target_code": target_code, "page": page_no},
        )
        outcome = FetchOutcome(status_code=response.status_code)
        if page is None:
            outcome.errors.append(f"Could not parse Jiuyan search payload from {task.url}")
            return outcome

        day_start = self._day_start()
        for entry in page.entries:
            if day_start is not None and not (
                entry.published_at is not None and entry.published_at >= day_start
            ):
                continue
            detail_url = post_detail_url(str(self.settings.base_url), entry.article_id)
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=detail_url,
                    seed_name=task.seed_name,
                    priority=150,
                    metadata={
                        "article_id": entry.article_id,
                        "target_code": target_code,
                        "target_kind": task.metadata.get("target_kind"),
                        "pubdate_ts": (
                            int(entry.published_at.timestamp())
                            if entry.published_at is not None
                            else 0
                        ),
                    },
                )
            )

        if day_start is not None:
            page_has_today = any(
                entry.published_at is not None and entry.published_at >= day_start
                for entry in page.entries
            )
            should_paginate = (
                page_no < self.settings.max_search_pages
                and page_has_today
                and page_no * page.page_size < page.total_count
            )
        else:
            should_paginate = (
                page_no < self.settings.max_search_pages
                and page_no * page.page_size < page.total_count
            )
        if should_paginate:
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=search_url(
                        str(self.settings.base_url), target_code, page_no + 1
                    ),
                    seed_name=task.seed_name,
                    priority=task.priority - 1,
                    metadata={**task.metadata, "page": page_no + 1},
                )
            )
        return outcome

    def _handle_community(
        self, task: CrawlTask, response: JiuyanHttpResult
    ) -> FetchOutcome:
        outcome = self._base_outcome(task, response)
        if outcome is not None:
            return outcome
        payload = response.json()
        page = parse_search_page(payload or {})
        feed = str(task.metadata.get("feed") or "")
        target_code = str(task.metadata.get("target_code") or "")
        page_no = int(task.metadata.get("page") or 1)
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"discovery_mode": "community", "feed": feed, "page": page_no},
        )
        outcome = FetchOutcome(status_code=response.status_code)
        if page is None:
            outcome.errors.append(
                f"Could not parse Jiuyan community payload from {task.url}"
            )
            return outcome

        day_start = self._day_start()
        for entry in page.entries:
            if day_start is not None and not (
                entry.published_at is not None and entry.published_at >= day_start
            ):
                continue
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=post_detail_url(
                        str(self.settings.base_url), entry.article_id
                    ),
                    seed_name=task.seed_name,
                    priority=149,
                    metadata={
                        "article_id": entry.article_id,
                        "target_code": target_code,
                        "target_kind": "community",
                        "feed": feed,
                        "pubdate_ts": (
                            int(entry.published_at.timestamp())
                            if entry.published_at is not None
                            else 0
                        ),
                    },
                )
            )

        if day_start is not None:
            page_has_today = any(
                entry.published_at is not None and entry.published_at >= day_start
                for entry in page.entries
            )
            should_paginate = (
                page_no < self.settings.max_community_pages and page_has_today
            )
        else:
            should_paginate = (
                page_no < self.settings.max_community_pages and bool(page.entries)
            )
        if should_paginate:
            outcome.discovered_tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=community_feed_url(
                        str(self.settings.base_url), feed, page_no + 1
                    ),
                    seed_name=task.seed_name,
                    priority=task.priority - 1,
                    metadata={**task.metadata, "page": page_no + 1},
                )
            )
        return outcome

    def _handle_detail(
        self, task: CrawlTask, response: JiuyanHttpResult
    ) -> FetchOutcome:
        outcome = self._base_outcome(task, response)
        if outcome is not None:
            return outcome
        payload = response.json()
        article_id = str(task.metadata.get("article_id") or "")
        target_code = str(task.metadata.get("target_code") or "") or None
        post, author = parse_post_detail(
            payload or {},
            str(task.url),
            target_code=target_code,
            fetched_at=datetime.now(UTC),
        )
        self._save_raw(
            response,
            task.kind,
            requested_url=str(task.url),
            meta={"article_id": article_id, "target_code": target_code},
        )
        outcome = FetchOutcome(status_code=response.status_code)
        if post is None:
            outcome.errors.append(f"Could not parse Jiuyan post payload from {task.url}")
            return outcome
        outcome.posts.append(post)
        if author is not None:
            outcome.authors.append(author)
        return outcome

    def _base_outcome(
        self, task: CrawlTask, response: JiuyanHttpResult
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
        payload = response.json()
        if payload is None:
            outcome = FetchOutcome(status_code=response.status_code)
            outcome.errors.append(f"Jiuyan response is not JSON for {task.url}")
            return outcome
        if str(payload.get("errCode") or "0") != "0":
            outcome = FetchOutcome(status_code=response.status_code)
            outcome.errors.append(
                f"Jiuyan API error {payload.get('errCode')}: {payload.get('msg') or task.url}"
            )
            return outcome
        return None

    def _day_start(self) -> datetime | None:
        if not self.settings.day_scoped:
            return None
        now = datetime.now(ZoneInfo(self.settings.ranking_timezone))
        return now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)

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

    def _save_raw(
        self,
        response: JiuyanHttpResult,
        task_kind: str,
        *,
        requested_url: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if self.raw_store is None:
            return
        record_meta = dict(meta or {})
        if response.error_message:
            record_meta["error_message"] = response.error_message
        try:
            self.raw_store.save(
                FetchRecord(
                    source=self.source_name,
                    url=requested_url,
                    method="POST",
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
                "Failed to persist Jiuyan raw response",
                extra={
                    "event": "jiuyan_rawstore_error",
                    "extra_data": {"url": requested_url},
                },
            )
