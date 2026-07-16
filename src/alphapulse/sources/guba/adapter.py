from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from alphapulse.pipeline.contracts import (
    CrawlTask,
    FetchOutcome,
    ItemReference,
    NormalizedComment,
    NormalizedPost,
    SeedDefinition,
)
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.guba.api import GubaClient, GubaHttpResult
from alphapulse.sources.guba.parser import GubaListEntry, parse_article_list, parse_post_detail, parse_replies
from alphapulse.sources.guba.urls import (
    board_list_url,
    comment_refresh_url,
    extract_post_ref,
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
        raw_store: RawResponseStore | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.client = client or GubaClient(settings, crawl_settings)
        self.raw_store = raw_store

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        tasks: list[CrawlTask] = []
        base = str(self.settings.base_url)
        for code in seed.guba_board_codes:
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=board_list_url(base, code),
                    seed_name=seed.name,
                    priority=120,
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
        response = self.client.get(str(task.url))

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

        if task.kind == "discover":
            return self._handle_list_page(task, response)
        return self._handle_post_detail(task, response)

    def _handle_list_page(self, task: CrawlTask, response: GubaHttpResult) -> FetchOutcome:
        outcome = FetchOutcome(status_code=response.status_code)
        board_code = str(task.metadata.get("board_code") or "")
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
        seen_post_ids: set[str] = set()
        for entry in article_list.entries:
            if entry.post_id in seen_post_ids:
                continue
            seen_post_ids.add(entry.post_id)
            code = entry.stockbar_code or board_code
            if not code:
                continue
            detail_url = post_detail_url(base, code, entry.post_id)
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
            if entry.comment_count:
                outcome.discovered_tasks.append(
                    CrawlTask(
                        source=self.source_name,
                        kind="refresh_comments",
                        url=comment_refresh_url(base, code, entry.post_id),
                        seed_name=task.seed_name,
                        priority=300,
                        metadata={
                            "post_id": entry.post_id,
                            "canonical_url": detail_url,
                            "board_code": code,
                            "pubdate_ts": pubdate_ts,
                        },
                    )
                )

        total = article_list.total_count or 0
        if page < self.settings.max_list_pages and page * LIST_PAGE_SIZE < total:
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

    def comment_task_for_post(self, post: NormalizedPost, seed_name: str) -> CrawlTask:
        ref = extract_post_ref(str(post.canonical_url))
        board_code = ref[0] if ref else ""
        base = str(self.settings.base_url)
        pubdate_ts = int(post.published_at.timestamp()) if post.published_at else 0
        return CrawlTask(
            source=self.source_name,
            kind="refresh_comments",
            url=comment_refresh_url(base, board_code, post.source_entity_id),
            seed_name=seed_name,
            priority=300,
            metadata={
                "post_id": post.source_entity_id,
                "canonical_url": str(post.canonical_url),
                "board_code": board_code,
                "pubdate_ts": pubdate_ts,
            },
        )

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
