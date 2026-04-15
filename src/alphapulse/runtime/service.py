from __future__ import annotations

import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from alphapulse.pipeline.contracts import CrawlTask, FetchOutcome, ItemReference, SeedDefinition
from alphapulse.runtime.config import Settings
from alphapulse.runtime.state import StateStore
from alphapulse.seeds.discovery import SeedDiscoveryManager
from alphapulse.sources.xueqiu.adapter import XueqiuAdapter
from alphapulse.storage.base import StorageStore
from alphapulse.storage.factory import build_store


logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    seeds_processed: int = 0
    tasks_enqueued: int = 0
    pages_fetched: int = 0
    posts_written: int = 0
    comments_written: int = 0
    authors_written: int = 0
    blocked_responses: int = 0
    errors: int = 0
    skipped_tasks: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "seeds_processed": self.seeds_processed,
            "tasks_enqueued": self.tasks_enqueued,
            "pages_fetched": self.pages_fetched,
            "posts_written": self.posts_written,
            "comments_written": self.comments_written,
            "authors_written": self.authors_written,
            "blocked_responses": self.blocked_responses,
            "errors": self.errors,
            "skipped_tasks": self.skipped_tasks,
        }


@dataclass
class AlphaPulseService:
    settings: Settings
    state: StateStore | None = None
    store: StorageStore | None = None
    xueqiu: XueqiuAdapter | None = None
    seed_discovery: SeedDiscoveryManager | None = None

    def __post_init__(self) -> None:
        if self.state is None:
            self.state = StateStore(self.settings.crawl.state_path)
        if self.store is None:
            self.store = build_store(self.settings)
        if self.xueqiu is None:
            self.xueqiu = XueqiuAdapter(self.settings.sources.xueqiu, self.settings.crawl)
        if self.seed_discovery is None:
            assert self.state is not None
            self.seed_discovery = SeedDiscoveryManager(self.settings.sources.xueqiu, self.state)

    def run_forever(self) -> None:
        logger.info("Starting AlphaPulse service loop", extra={"event": "service_start"})
        while True:
            self.run_cycle()
            time.sleep(self.settings.crawl.poll_interval_seconds)

    def run_cycle(self, seed_set_name: str | None = None) -> RunStats:
        assert self.state is not None
        assert self.store is not None
        assert self.xueqiu is not None
        assert self.seed_discovery is not None
        run_id = str(uuid.uuid4())
        started_at = datetime.now(UTC)
        stats = RunStats()
        self.state.start_run(run_id)
        try:
            queue: deque[CrawlTask] = deque()
            for seed in self._select_seeds(seed_set_name):
                stats.seeds_processed += 1
                for task in self.xueqiu.discover(seed):
                    self._enqueue_task(queue, task, stats)

            while queue:
                task = queue.popleft()
                if not self.state.should_fetch_url(
                    str(task.url),
                    self._min_age_for_task(task),
                ):
                    stats.skipped_tasks += 1
                    continue

                self.state.remember_url(
                    url=str(task.url),
                    source=task.source,
                    kind=task.kind,
                    seed_name=task.seed_name,
                )

                if task.kind == "refresh_comments":
                    comments = self.xueqiu.refresh_comments(
                        ItemReference(
                            source=task.source,
                            source_entity_id=task.metadata["post_id"],
                            canonical_url=task.metadata["canonical_url"],
                            metadata=task.metadata,
                        )
                    )
                    if comments:
                        self.store.upsert_comments(comments)
                        stats.comments_written += len(comments)
                        self.state.mark_comments_refreshed(task.source, task.metadata["post_id"])
                    self.state.mark_url_fetched(str(task.url), 200)
                    continue

                outcome = self.xueqiu.fetch_item(task)
                self._apply_outcome(task, outcome, queue, stats)
                self.state.mark_url_fetched(str(task.url), outcome.status_code)

            self.state.finish_run(run_id, "succeeded", stats.to_dict())
            self.store.insert_crawl_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                stats=stats.to_dict(),
                status="succeeded",
            )
            return stats
        except Exception:
            stats.errors += 1
            self.state.finish_run(run_id, "failed", stats.to_dict())
            self.store.insert_crawl_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                stats=stats.to_dict(),
                status="failed",
            )
            raise

    def _apply_outcome(
        self,
        task: CrawlTask,
        outcome: FetchOutcome,
        queue: deque[CrawlTask],
        stats: RunStats,
    ) -> None:
        stats.pages_fetched += 1

        if outcome.blocked:
            stats.blocked_responses += 1

        if outcome.errors:
            stats.errors += len(outcome.errors)
            for error in outcome.errors:
                self.store.insert_crawl_error(
                    source=task.source,
                    url=str(task.url),
                    error_message=error,
                )

        if outcome.authors:
            self.store.upsert_authors(outcome.authors)
            stats.authors_written += len(outcome.authors)

        if outcome.posts:
            self.store.upsert_posts(outcome.posts)
            stats.posts_written += len(outcome.posts)
            for post in outcome.posts:
                metadata = {"canonical_url": str(post.canonical_url)}
                self.state.upsert_item(post.source, post.source_entity_id, str(post.canonical_url), metadata)
                if self.state.should_refresh_comments(
                    post.source,
                    post.source_entity_id,
                    timedelta(minutes=self.settings.crawl.comment_refresh_minutes),
                ):
                    comment_task = self.xueqiu.comment_task_for_post(post, task.seed_name)
                    self._enqueue_task(queue, comment_task, stats)

        for discovered_task in outcome.discovered_tasks:
            self._enqueue_task(queue, discovered_task, stats)

    def _enqueue_task(self, queue: deque[CrawlTask], task: CrawlTask, stats: RunStats) -> None:
        queue.append(task)
        stats.tasks_enqueued += 1

    def _min_age_for_task(self, task: CrawlTask) -> timedelta:
        if task.kind == "discover":
            return timedelta(minutes=self.settings.crawl.comment_refresh_minutes)
        if task.kind == "refresh_comments":
            return timedelta(minutes=self.settings.crawl.comment_refresh_minutes)
        return timedelta(minutes=self.settings.crawl.post_recrawl_minutes)

    def _select_seeds(self, seed_set_name: str | None) -> list[SeedDefinition]:
        assert self.seed_discovery is not None
        return self.seed_discovery.ensure_compiled_seed_sets(seed_set_name)
