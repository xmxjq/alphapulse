from __future__ import annotations

import heapq
import itertools
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import Lock
from zoneinfo import ZoneInfo

from alphapulse.pipeline.contracts import (
    CrawlTask,
    FetchOutcome,
    ItemReference,
    SeedDefinition,
    SourceAdapter,
)
from alphapulse.runtime.config import Settings
from alphapulse.runtime.rqlite_state import RqliteStateStore
from alphapulse.runtime.state import StateStore
from alphapulse.runtime.state_factory import build_state_store
from alphapulse.seeds.discovery import SeedDiscoveryManager
from alphapulse.sources.bilibili.adapter import BilibiliAdapter
from alphapulse.sources.fetching import KuaidailiProxyPool
from alphapulse.sources.guba.adapter import GubaAdapter
from alphapulse.sources.guba.api import GubaClient
from alphapulse.sources.hupu.adapter import HupuAdapter
from alphapulse.sources.hupu.api import HupuClient
from alphapulse.sources.jiuyan.adapter import JiuyanAdapter
from alphapulse.sources.jiuyan.api import JiuyanClient
from alphapulse.sources.tgb.adapter import TgbAdapter
from alphapulse.sources.tgb.api import TgbClient
from alphapulse.sources.xueqiu.adapter import XueqiuAdapter
from alphapulse.storage.base import StorageStore
from alphapulse.storage.factory import build_store
from alphapulse.storage.rawstore import build_raw_store

logger = logging.getLogger(__name__)


def classify_crawl_error(message: str) -> str:
    """Bucket a free-text adapter error into a stable kind for the dashboard.

    Adapters emit human-readable error strings; grouping them by kind lets the
    dashboard tell a transient block from a genuine parse failure or a deleted
    post at a glance, without parsing prose. Order matters: the more specific
    prefixes are checked before the generic ones.
    """
    lowered = message.lower()
    if lowered.startswith("blocked"):
        return "blocked"
    if "deleted or missing" in lowered:
        return "deleted"
    if "(removed/hidden)" in lowered or lowered.startswith("post state "):
        return "removed"
    if lowered.startswith("fetch failed"):
        return "fetch_failed"
    if "could not parse" in lowered or "no article_list payload" in lowered:
        return "parse_error"
    return "other"


class TaskQueue:
    """Priority-ordered crawl queue.

    Higher ``task.priority`` wins. Within the same priority, tasks with a
    larger ``metadata['pubdate_ts']`` (newer content) come first, so bilibili
    space discovery fetches the newest videos before older ones. FIFO is the
    final tiebreaker.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, int, CrawlTask]] = []
        self._counter = itertools.count()

    def push(self, task: CrawlTask) -> None:
        pubdate_ts = int(task.metadata.get("pubdate_ts") or 0)
        seq = next(self._counter)
        heapq.heappush(self._heap, (-task.priority, -pubdate_ts, seq, task))

    def pop(self) -> CrawlTask:
        return heapq.heappop(self._heap)[3]

    def peek(self) -> CrawlTask:
        return self._heap[0][3]

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __len__(self) -> int:
        return len(self._heap)


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

    def merge(self, other: "RunStats") -> None:
        for name, value in other.to_dict().items():
            setattr(self, name, getattr(self, name) + value)


@dataclass
class AlphaPulseService:
    settings: Settings
    state: StateStore | RqliteStateStore | None = None
    store: StorageStore | None = None
    sources: dict[str, SourceAdapter] = field(default_factory=dict)
    seed_discovery: SeedDiscoveryManager | None = None
    seed_discovery_lock: Lock = field(default_factory=Lock)

    def __post_init__(self) -> None:
        if self.state is None:
            self.state = build_state_store(self.settings)
        if self.store is None:
            self.store = build_store(self.settings)
        if not self.sources:
            self.sources = self._build_sources()
        if self.seed_discovery is None:
            assert self.state is not None
            self.seed_discovery = SeedDiscoveryManager(
                self.settings.sources.xueqiu,
                self.state,
                guba_settings=self.settings.sources.guba,
                tgb_settings=self.settings.sources.tgb,
                jiuyan_settings=self.settings.sources.jiuyan,
                crawl_settings=self.settings.crawl,
            )

    def run_forever(self) -> None:
        logger.info(
            "Starting AlphaPulse independent source loops",
            extra={
                "event": "service_start",
                "extra_data": {
                    "sources": sorted(self.sources.keys()),
                    "poll_interval_seconds": self.settings.crawl.poll_interval_seconds,
                    "scheduling": "independent_source_loops",
                },
            },
        )
        with ThreadPoolExecutor(
            max_workers=max(1, len(self.sources)),
            thread_name_prefix="alphapulse-independent-source",
        ) as executor:
            for source_name in self.sources:
                executor.submit(self._run_source_forever, source_name)
            # Source workers own their own retry and sleep loops. Keeping the
            # supervisor alive here prevents a slow source from becoming the
            # scheduler for every other source.
            while True:
                time.sleep(3600)

    def _run_source_forever(self, source_name: str) -> None:
        poll_interval = self.settings.crawl.poll_interval_seconds
        while True:
            started = time.monotonic()
            try:
                self.run_source_cycle(source_name)
            except Exception:
                logger.exception(
                    "Source crawl cycle failed; worker will retry",
                    extra={
                        "event": "source_cycle_failed",
                        "extra_data": {"source": source_name},
                    },
                )
            elapsed = time.monotonic() - started
            delay = max(0.0, poll_interval - elapsed)
            logger.info(
                "Sleeping between source cycles",
                extra={
                    "event": "source_cycle_sleep",
                    "extra_data": {
                        "source": source_name,
                        "seconds": round(delay, 3),
                        "cycle_seconds": round(elapsed, 3),
                    },
                },
            )
            time.sleep(delay)

    def run_source_cycle(
        self,
        source_name: str,
        seed_set_name: str | None = None,
    ) -> RunStats:
        """Run one source without waiting for any other source.

        ``run_cycle`` remains the aggregate, one-shot API used by tests and
        manual runs. The long-running service uses this source-scoped variant
        so a large Guba queue cannot delay the next TGB, Jiuyan, or Hupu cycle.
        """
        assert self.state is not None
        assert self.store is not None
        assert self.seed_discovery is not None
        adapter = self.sources.get(source_name)
        if adapter is None:
            raise KeyError(f"Unknown source adapter: {source_name}")

        run_id = str(uuid.uuid4())
        started_at = datetime.now(UTC)
        stats = RunStats()
        logger.info(
            "Source crawl cycle started",
            extra={
                "event": "source_cycle_start",
                "extra_data": {
                    "run_id": run_id,
                    "source": source_name,
                    "seed_set": seed_set_name,
                },
            },
        )
        try:
            if source_name == "guba":
                self._prune_expired_guba_pending_tasks()
            queue = TaskQueue()
            for seed in self._select_seeds(seed_set_name):
                stats.seeds_processed += 1
                discovered = adapter.discover(seed)
                for task in discovered:
                    if task.source == source_name:
                        self._enqueue_task(queue, task, stats)
                logger.debug(
                    "Source seed expanded",
                    extra={
                        "event": "source_seed_expanded",
                        "extra_data": {
                            "source": source_name,
                            "seed": seed.name,
                            "tasks": len(discovered),
                        },
                    },
                )

            recovered = 0
            for task in self.state.load_pending_tasks(
                seed_set_name,
                source=source_name,
            ):
                self.state.release_url_claim(str(task.url))
                self._enqueue_task(queue, task, stats)
                recovered += 1
            if recovered:
                logger.info(
                    "Source pending tasks recovered",
                    extra={
                        "event": "source_pending_tasks_recovered",
                        "extra_data": {
                            "run_id": run_id,
                            "source": source_name,
                            "tasks": recovered,
                        },
                    },
                )

            logger.info(
                "Source seed discovery complete",
                extra={
                    "event": "source_seeds_discovered",
                    "extra_data": {
                        "run_id": run_id,
                        "source": source_name,
                        "seeds_processed": stats.seeds_processed,
                        "tasks_enqueued": stats.tasks_enqueued,
                    },
                },
            )
            source_stats = self._run_source_queue(source_name, queue)
            stats.merge(source_stats)
            self.store.insert_crawl_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                stats=stats.to_dict(),
                status="succeeded",
            )
            logger.info(
                "Source crawl cycle finished",
                extra={
                    "event": "source_cycle_done",
                    "extra_data": {
                        "run_id": run_id,
                        "source": source_name,
                        "status": "succeeded",
                        "duration_seconds": round(
                            (datetime.now(UTC) - started_at).total_seconds(), 3
                        ),
                        **stats.to_dict(),
                    },
                },
            )
            return stats
        except Exception:
            stats.errors += 1
            self.store.insert_crawl_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                stats=stats.to_dict(),
                status="failed",
            )
            logger.exception(
                "Source crawl cycle failed",
                extra={
                    "event": "source_cycle_failed",
                    "extra_data": {
                        "run_id": run_id,
                        "source": source_name,
                        **stats.to_dict(),
                    },
                },
            )
            raise

    def run_cycle(self, seed_set_name: str | None = None) -> RunStats:
        assert self.state is not None
        assert self.store is not None
        assert self.sources
        assert self.seed_discovery is not None
        run_id = str(uuid.uuid4())
        started_at = datetime.now(UTC)
        stats = RunStats()
        logger.info(
            "Crawl cycle started",
            extra={
                "event": "cycle_start",
                "extra_data": {"run_id": run_id, "seed_set": seed_set_name},
            },
        )
        try:
            self._prune_expired_guba_pending_tasks()
            queues = {source_name: TaskQueue() for source_name in self.sources}
            for seed in self._select_seeds(seed_set_name):
                stats.seeds_processed += 1
                for adapter in self.sources.values():
                    discovered = adapter.discover(seed)
                    for task in discovered:
                        self._enqueue_task(queues[task.source], task, stats)
                    logger.debug(
                        "Seed expanded",
                        extra={
                            "event": "seed_expanded",
                            "extra_data": {
                                "seed": seed.name,
                                "source": adapter.source_name,
                                "tasks": len(discovered),
                            },
                        },
                    )

            pending_tasks = self.state.load_pending_tasks(seed_set_name)
            recovered = 0
            for task in pending_tasks:
                queue = queues.get(task.source)
                if queue is None:
                    continue
                # try_claim_url records an optimistic timestamp before the
                # request starts. A crash can therefore leave a pending task
                # looking fresh even though it never completed.
                self.state.release_url_claim(str(task.url))
                self._enqueue_task(queue, task, stats)
                recovered += 1
            if recovered:
                logger.info(
                    "Pending tasks recovered",
                    extra={
                        "event": "pending_tasks_recovered",
                        "extra_data": {
                            "run_id": run_id,
                            "seed_set": seed_set_name,
                            "tasks": recovered,
                        },
                    },
                )

            logger.info(
                "Seed discovery complete",
                extra={
                    "event": "seeds_discovered",
                    "extra_data": {
                        "run_id": run_id,
                        "seeds_processed": stats.seeds_processed,
                        "tasks_enqueued": stats.tasks_enqueued,
                    },
                },
            )

            active_queues = {
                source_name: queue for source_name, queue in queues.items() if queue
            }
            max_workers = max(
                1,
                min(self.settings.crawl.concurrent_requests, len(active_queues)),
            )
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="alphapulse-source",
            ) as executor:
                futures = {
                    executor.submit(self._run_source_queue, source_name, queue): source_name
                    for source_name, queue in active_queues.items()
                }
                for future in as_completed(futures):
                    source_name = futures[future]
                    source_stats = future.result()
                    stats.merge(source_stats)
                    logger.info(
                        "Source queue finished",
                        extra={
                            "event": "source_queue_done",
                            "extra_data": {
                                "source": source_name,
                                **source_stats.to_dict(),
                            },
                        },
                    )

            duration = (datetime.now(UTC) - started_at).total_seconds()
            self.store.insert_crawl_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                stats=stats.to_dict(),
                status="succeeded",
            )
            logger.info(
                "Crawl cycle finished",
                extra={
                    "event": "cycle_done",
                    "extra_data": {
                        "run_id": run_id,
                        "status": "succeeded",
                        "duration_seconds": round(duration, 3),
                        **stats.to_dict(),
                    },
                },
            )
            return stats
        except Exception:
            stats.errors += 1
            self.store.insert_crawl_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                stats=stats.to_dict(),
                status="failed",
            )
            logger.exception(
                "Crawl cycle failed",
                extra={
                    "event": "cycle_failed",
                    "extra_data": {"run_id": run_id, **stats.to_dict()},
                },
            )
            raise

    def _run_source_queue(self, source_name: str, queue: TaskQueue) -> RunStats:
        if source_name == "guba" and self._guba_hybrid_enabled():
            return self._run_guba_hybrid_queue(queue)
        if source_name == "jiuyan" and self._jiuyan_hybrid_enabled():
            return self._run_jiuyan_hybrid_queue(queue)
        if source_name == "hupu" and self._hupu_hybrid_enabled():
            return self._run_hupu_hybrid_queue(queue)

        assert self.state is not None
        assert self.store is not None
        stats = RunStats()
        source_blocked = False
        guba_browser_posts_attempted = 0
        logger.info(
            "Source queue started",
            extra={
                "event": "source_queue_start",
                "extra_data": {"source": source_name, "tasks": len(queue)},
            },
        )

        while queue:
            task = queue.pop()
            if self._discard_task(task, stats):
                continue
            if source_blocked or self._source_circuit_open(task.source):
                source_blocked = True
                stats.skipped_tasks += 1
                logger.debug(
                    "Task skipped (source circuit open)",
                    extra={
                        "event": "task_skipped",
                        "extra_data": {
                            "source": task.source,
                            "kind": task.kind,
                            "url": str(task.url),
                            "reason": "source_circuit_open",
                        },
                    },
                )
                continue

            if (
                self._is_guba_browser_post(task)
                and guba_browser_posts_attempted
                >= self.settings.sources.guba.browser.max_posts_per_cycle
            ):
                stats.skipped_tasks += 1
                logger.debug(
                    "Task skipped (browser post cycle limit)",
                    extra={
                        "event": "task_skipped",
                        "extra_data": {
                            "source": task.source,
                            "kind": task.kind,
                            "url": str(task.url),
                            "reason": "browser_post_cycle_limit",
                            "limit": self.settings.sources.guba.browser.max_posts_per_cycle,
                        },
                    },
                )
                continue

            if not self.state.try_claim_url(
                url=str(task.url),
                source=task.source,
                kind=task.kind,
                seed_name=task.seed_name,
                min_age=self._min_age_for_task(task),
            ):
                stats.skipped_tasks += 1
                self.state.delete_pending_task(task.dedupe_key)
                logger.debug(
                    "Task skipped (claim lost or still fresh)",
                    extra={
                        "event": "task_skipped",
                        "extra_data": {
                            "source": task.source,
                            "kind": task.kind,
                            "url": str(task.url),
                        },
                    },
                )
                continue

            if self._is_guba_browser_post(task):
                guba_browser_posts_attempted += 1

            if task.kind == "refresh_comments":
                adapter = self._adapter_for_task(task)
                comments = adapter.refresh_comments(
                    ItemReference(
                        source=task.source,
                        source_entity_id=task.metadata["post_id"],
                        canonical_url=task.metadata["canonical_url"],
                        metadata=task.metadata,
                    )
                )
                if self._source_circuit_open(task.source):
                    source_blocked = True
                    stats.blocked_responses += 1
                    self.state.release_url_claim(str(task.url))
                    logger.warning(
                        "Source circuit opened during comment refresh",
                        extra={
                            "event": "source_circuit_open",
                            "extra_data": {
                                "source": task.source,
                                "kind": task.kind,
                                "url": str(task.url),
                            },
                        },
                    )
                    continue
                if comments:
                    self.store.upsert_comments(comments)
                    stats.comments_written += len(comments)
                    self.state.mark_comments_refreshed(
                        task.source, task.metadata["post_id"]
                    )
                self.state.mark_url_fetched(str(task.url), 200)
                self.state.delete_pending_task(task.dedupe_key)
                logger.info(
                    "Comments refreshed",
                    extra={
                        "event": "comments_refreshed",
                        "extra_data": {
                            "source": task.source,
                            "post_id": task.metadata["post_id"],
                            "comments": len(comments),
                        },
                    },
                )
                continue

            adapter = self._adapter_for_task(task)
            outcome = adapter.fetch_item(task)
            self._apply_outcome(task, outcome, queue, stats)
            if outcome.blocked:
                if self._record_jiuyan_captcha_block(task, outcome):
                    continue
                self.state.release_url_claim(str(task.url))
                continue_after_block = getattr(
                    adapter,
                    "continue_after_blocked_task",
                    lambda _task: False,
                )(task)
                if continue_after_block:
                    self.state.upsert_pending_tasks([task])
                    logger.warning(
                        "Blocked task isolated; rotating transports remain eligible",
                        extra={
                            "event": "task_blocked_isolated",
                            "extra_data": {
                                "source": task.source,
                                "kind": task.kind,
                                "url": str(task.url),
                            },
                        },
                    )
                else:
                    source_blocked = True
                    logger.warning(
                        "Source stopped for the rest of the cycle after blocked response",
                        extra={
                            "event": "source_circuit_open",
                            "extra_data": {
                                "source": task.source,
                                "kind": task.kind,
                                "url": str(task.url),
                            },
                        },
                    )
            elif outcome.status_code is None:
                self.state.release_url_claim(str(task.url))
            else:
                if task.source == "jiuyan" and task.kind == "fetch_post":
                    self.state.clear_task_failures(task.dedupe_key)
                self.state.mark_url_fetched(str(task.url), outcome.status_code)
                self.state.delete_pending_task(task.dedupe_key)

        return stats

    def _run_guba_hybrid_queue(self, queue: TaskQueue) -> RunStats:
        assert self.state is not None
        assert self.store is not None
        adapter = self._adapter_for_source("guba")
        stats = RunStats()
        source_blocked = False
        guba_browser_posts_attempted = 0
        max_workers = (
            self._guba_paid_slots()
            + self.settings.sources.guba.concurrent_agent_requests
        )
        logger.info(
            "Guba hybrid source queue started",
            extra={
                "event": "source_queue_start",
                "extra_data": {
                    "source": "guba",
                    "tasks": len(queue),
                    "routing": "hybrid",
                    "paid_slots": self._guba_paid_slots(),
                    "agent_slot_limit": self.settings.sources.guba.concurrent_agent_requests,
                },
            },
        )

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="alphapulse-guba",
        ) as executor:
            while queue:
                if source_blocked or self._source_circuit_open("guba"):
                    source_blocked = True
                    while queue:
                        task = queue.pop()
                        stats.skipped_tasks += 1
                        logger.debug(
                            "Task skipped (source circuit open)",
                            extra={
                                "event": "task_skipped",
                                "extra_data": {
                                    "source": task.source,
                                    "kind": task.kind,
                                    "url": str(task.url),
                                    "reason": "source_circuit_open",
                                },
                            },
                        )
                    break

                first = queue.peek()
                experiment_eligible = first.kind == "fetch_post"
                routes = self._guba_hybrid_routes(
                    adapter,
                    experiment_eligible=experiment_eligible,
                )
                if first.kind == "refresh_comments" or self._is_guba_browser_post(first):
                    routes = ["existing"]

                batch: list[tuple[CrawlTask, str]] = []
                for route in routes:
                    if not queue:
                        break
                    task = queue.peek()
                    if batch and (
                        task.kind == "refresh_comments"
                        or self._is_guba_browser_post(task)
                    ):
                        break
                    task = queue.pop()
                    if self._discard_task(task, stats):
                        continue
                    # Hybrid mode intentionally continues after a blocked
                    # transport. Persist every claimed task, including seed
                    # tasks, so the blocked member of a parallel batch remains
                    # recoverable while successful peers are removed.
                    self.state.upsert_pending_tasks([task])

                    if (
                        self._is_guba_browser_post(task)
                        and guba_browser_posts_attempted
                        >= self.settings.sources.guba.browser.max_posts_per_cycle
                    ):
                        stats.skipped_tasks += 1
                        logger.debug(
                            "Task skipped (browser post cycle limit)",
                            extra={
                                "event": "task_skipped",
                                "extra_data": {
                                    "source": task.source,
                                    "kind": task.kind,
                                    "url": str(task.url),
                                    "reason": "browser_post_cycle_limit",
                                    "limit": self.settings.sources.guba.browser.max_posts_per_cycle,
                                },
                            },
                        )
                        continue

                    if not self.state.try_claim_url(
                        url=str(task.url),
                        source=task.source,
                        kind=task.kind,
                        seed_name=task.seed_name,
                        min_age=self._min_age_for_task(task),
                    ):
                        stats.skipped_tasks += 1
                        self.state.delete_pending_task(task.dedupe_key)
                        logger.debug(
                            "Task skipped (claim lost or still fresh)",
                            extra={
                                "event": "task_skipped",
                                "extra_data": {
                                    "source": task.source,
                                    "kind": task.kind,
                                    "url": str(task.url),
                                },
                            },
                        )
                        continue

                    if self._is_guba_browser_post(task):
                        guba_browser_posts_attempted += 1
                        route = "existing"
                    batch.append((task, route))

                if not batch:
                    continue

                futures = [
                    (
                        task,
                        route,
                        executor.submit(
                            self._execute_guba_hybrid_task,
                            adapter,
                            task,
                            route,
                        ),
                    )
                    for task, route in batch
                ]
                for task, route, future in futures:
                    result_kind, payload = future.result()
                    if result_kind == "comments":
                        comments = payload
                        if self._source_circuit_open("guba"):
                            source_blocked = True
                            stats.blocked_responses += 1
                            self.state.release_url_claim(str(task.url))
                            logger.warning(
                                "Source circuit opened during comment refresh",
                                extra={
                                    "event": "source_circuit_open",
                                    "extra_data": {
                                        "source": task.source,
                                        "kind": task.kind,
                                        "url": str(task.url),
                                    },
                                },
                            )
                            continue
                        if comments:
                            self.store.upsert_comments(comments)
                            stats.comments_written += len(comments)
                            self.state.mark_comments_refreshed(
                                task.source,
                                task.metadata["post_id"],
                            )
                        self.state.mark_url_fetched(str(task.url), 200)
                        self.state.delete_pending_task(task.dedupe_key)
                        continue

                    outcome = payload
                    self._apply_outcome(task, outcome, queue, stats)
                    if outcome.blocked:
                        self.state.release_url_claim(str(task.url))
                        if route == "auto" or self._source_circuit_open("guba"):
                            source_blocked = True
                        logger.warning(
                            "Guba transport blocked; other pool remains eligible",
                            extra={
                                "event": "guba_transport_blocked",
                                "extra_data": {
                                    "source": task.source,
                                    "kind": task.kind,
                                    "url": str(task.url),
                                    "transport": route,
                                    "source_circuit_open": source_blocked,
                                },
                            },
                        )
                    elif outcome.status_code is None:
                        self.state.release_url_claim(str(task.url))
                    else:
                        self.state.mark_url_fetched(
                            str(task.url),
                            outcome.status_code,
                        )
                        self.state.delete_pending_task(task.dedupe_key)

        return stats

    @staticmethod
    def _execute_guba_hybrid_task(
        adapter: SourceAdapter,
        task: CrawlTask,
        route: str,
    ) -> tuple[str, object]:
        if task.kind == "refresh_comments":
            comments = adapter.refresh_comments(
                ItemReference(
                    source=task.source,
                    source_entity_id=task.metadata["post_id"],
                    canonical_url=task.metadata["canonical_url"],
                    metadata=task.metadata,
                )
            )
            return "comments", comments
        fetch_with_transport = getattr(adapter, "fetch_item_with_transport")
        return "outcome", fetch_with_transport(task, route)

    def _guba_hybrid_routes(
        self,
        adapter: SourceAdapter,
        *,
        experiment_eligible: bool = True,
    ) -> list[str]:
        paid_slots = self._guba_paid_slots(
            experiment_eligible=experiment_eligible,
        )
        capacity = getattr(adapter, "available_agent_capacity")()
        agent_slots = min(
            self.settings.sources.guba.concurrent_agent_requests,
            capacity,
        )
        if agent_slots == 0:
            return ["auto"] * paid_slots
        return ["existing"] * paid_slots + ["agent"] * agent_slots

    def _guba_paid_slots(self, *, experiment_eligible: bool = True) -> int:
        slots = self.settings.sources.guba.concurrent_paid_requests
        if experiment_eligible and self._guba_dual_endpoint_experiment_active():
            slots += 1
        return slots

    def _guba_dual_endpoint_experiment_active(self) -> bool:
        proxy = self.settings.crawl.proxy
        return bool(
            self.settings.sources.guba.proxy_dual_endpoint_experiment_active()
            and proxy.enabled
            and proxy.provider == "kuaidaili"
            and self.settings.crawl.kuaidaili.batch_size >= 2
            and self._proxy_enabled_for_source("guba")
        )

    def _guba_hybrid_enabled(self) -> bool:
        adapter = self.sources.get("guba")
        if adapter is None:
            return False
        client = getattr(adapter, "client", None)
        return bool(
            getattr(client, "agent_pool", None) is not None
            and getattr(client, "proxy_provider", None) is not None
        )

    def _run_jiuyan_hybrid_queue(self, queue: TaskQueue) -> RunStats:
        assert self.state is not None
        assert self.store is not None
        adapter = self._adapter_for_source("jiuyan")
        stats = RunStats()
        max_workers = (
            self.settings.sources.jiuyan.concurrent_paid_requests
            + self.settings.sources.jiuyan.concurrent_agent_requests
        )
        logger.info(
            "Jiuyan hybrid source queue started",
            extra={
                "event": "source_queue_start",
                "extra_data": {
                    "source": "jiuyan",
                    "tasks": len(queue),
                    "routing": "hybrid_detail_only",
                    "paid_slots": (
                        self.settings.sources.jiuyan.concurrent_paid_requests
                    ),
                    "agent_slot_limit": (
                        self.settings.sources.jiuyan.concurrent_agent_requests
                    ),
                },
            },
        )

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="alphapulse-jiuyan",
        ) as executor:
            while queue:
                first = queue.peek()
                routes = self._jiuyan_hybrid_routes(adapter, first)
                batch_kind = first.kind
                batch: list[tuple[CrawlTask, str]] = []

                for route in routes:
                    if not queue or queue.peek().kind != batch_kind:
                        break
                    task = queue.pop()
                    if self._discard_task(task, stats):
                        continue
                    self.state.upsert_pending_tasks([task])
                    if not self.state.try_claim_url(
                        url=str(task.url),
                        source=task.source,
                        kind=task.kind,
                        seed_name=task.seed_name,
                        min_age=self._min_age_for_task(task),
                    ):
                        stats.skipped_tasks += 1
                        self.state.delete_pending_task(task.dedupe_key)
                        continue
                    batch.append((task, route))

                if not batch:
                    continue

                futures = [
                    (
                        task,
                        route,
                        executor.submit(
                            getattr(adapter, "fetch_item_with_transport"),
                            task,
                            route,
                        ),
                    )
                    for task, route in batch
                ]
                for task, route, future in futures:
                    outcome = future.result()
                    self._apply_outcome(task, outcome, queue, stats)
                    if outcome.blocked:
                        if self._record_jiuyan_captcha_block(task, outcome):
                            continue
                        self.state.release_url_claim(str(task.url))
                        logger.warning(
                            "Jiuyan transport blocked; other pool remains eligible",
                            extra={
                                "event": "jiuyan_transport_blocked",
                                "extra_data": {
                                    "source": task.source,
                                    "kind": task.kind,
                                    "url": str(task.url),
                                    "transport": route,
                                },
                            },
                        )
                    elif outcome.status_code is None:
                        self.state.release_url_claim(str(task.url))
                    else:
                        self.state.clear_task_failures(task.dedupe_key)
                        self.state.mark_url_fetched(
                            str(task.url),
                            outcome.status_code,
                        )
                        self.state.delete_pending_task(task.dedupe_key)

        return stats

    def _jiuyan_hybrid_routes(
        self,
        adapter: SourceAdapter,
        task: CrawlTask,
    ) -> list[str]:
        paid_slots = self.settings.sources.jiuyan.concurrent_paid_requests
        if task.kind != "fetch_post":
            return ["existing"] * paid_slots
        capacity = getattr(adapter, "available_agent_capacity")()
        agent_slots = min(
            self.settings.sources.jiuyan.concurrent_agent_requests,
            capacity,
        )
        return ["existing"] * paid_slots + ["agent"] * agent_slots

    def _jiuyan_hybrid_enabled(self) -> bool:
        adapter = self.sources.get("jiuyan")
        if adapter is None:
            return False
        client = getattr(adapter, "client", None)
        return bool(
            getattr(client, "agent_pool", None) is not None
            and getattr(client, "proxy_provider", None) is not None
        )

    def _run_hupu_hybrid_queue(self, queue: TaskQueue) -> RunStats:
        assert self.state is not None
        assert self.store is not None
        adapter = self._adapter_for_source("hupu")
        stats = RunStats()
        max_workers = (
            self.settings.sources.hupu.concurrent_paid_requests
            + self.settings.sources.hupu.concurrent_agent_requests
        )
        logger.info(
            "Hupu hybrid source queue started",
            extra={
                "event": "source_queue_start",
                "extra_data": {
                    "source": "hupu",
                    "tasks": len(queue),
                    "routing": "hybrid_detail_only",
                    "paid_slots": self.settings.sources.hupu.concurrent_paid_requests,
                    "agent_slot_limit": self.settings.sources.hupu.concurrent_agent_requests,
                },
            },
        )

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="alphapulse-hupu",
        ) as executor:
            while queue:
                if self._source_circuit_open("hupu"):
                    while queue:
                        expired_task = queue.pop()
                        self.state.delete_pending_task(expired_task.dedupe_key)
                        stats.skipped_tasks += 1
                    logger.warning(
                        "Hupu source disabled because its authorization expired",
                        extra={
                            "event": "hupu_authorization_expired",
                            "extra_data": {
                                "authorization_expires_on": str(
                                    self.settings.sources.hupu.authorization_expires_on
                                )
                            },
                        },
                    )
                    break
                first = queue.peek()
                routes = self._hupu_hybrid_routes(adapter, first)
                batch_kind = first.kind
                batch: list[tuple[CrawlTask, str]] = []
                for route in routes:
                    if not queue or queue.peek().kind != batch_kind:
                        break
                    task = queue.pop()
                    self.state.upsert_pending_tasks([task])
                    if not self.state.try_claim_url(
                        url=str(task.url),
                        source=task.source,
                        kind=task.kind,
                        seed_name=task.seed_name,
                        min_age=self._min_age_for_task(task),
                    ):
                        stats.skipped_tasks += 1
                        self.state.delete_pending_task(task.dedupe_key)
                        continue
                    batch.append((task, route))

                if not batch:
                    continue

                futures = [
                    (
                        task,
                        route,
                        executor.submit(
                            getattr(adapter, "fetch_item_with_transport"),
                            task,
                            route,
                        ),
                    )
                    for task, route in batch
                ]
                for task, route, future in futures:
                    outcome = future.result()
                    self._apply_outcome(task, outcome, queue, stats)
                    if outcome.blocked:
                        self.state.release_url_claim(str(task.url))
                        logger.warning(
                            "Hupu transport blocked; other pool remains eligible",
                            extra={
                                "event": "hupu_transport_blocked",
                                "extra_data": {
                                    "source": task.source,
                                    "kind": task.kind,
                                    "url": str(task.url),
                                    "transport": route,
                                },
                            },
                        )
                    elif outcome.status_code is None:
                        self.state.release_url_claim(str(task.url))
                    else:
                        self.state.mark_url_fetched(str(task.url), outcome.status_code)
                        self.state.delete_pending_task(task.dedupe_key)
        return stats

    def _hupu_hybrid_routes(
        self,
        adapter: SourceAdapter,
        task: CrawlTask,
    ) -> list[str]:
        paid_slots = self.settings.sources.hupu.concurrent_paid_requests
        if task.kind != "fetch_post":
            return ["existing"] * paid_slots
        capacity = getattr(adapter, "available_agent_capacity")()
        agent_slots = min(
            self.settings.sources.hupu.concurrent_agent_requests,
            capacity,
        )
        return ["existing"] * paid_slots + ["agent"] * agent_slots

    def _hupu_hybrid_enabled(self) -> bool:
        adapter = self.sources.get("hupu")
        if adapter is None:
            return False
        client = getattr(adapter, "client", None)
        return bool(
            getattr(client, "agent_pool", None) is not None
            and getattr(client, "proxy_provider", None) is not None
        )

    def _apply_outcome(
        self,
        task: CrawlTask,
        outcome: FetchOutcome,
        queue: TaskQueue,
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
                    status_code=outcome.status_code,
                    task_kind=task.kind,
                    error_kind=classify_crawl_error(error),
                )

        if outcome.authors:
            self.store.upsert_authors(outcome.authors)
            stats.authors_written += len(outcome.authors)

        if outcome.posts:
            self.store.upsert_posts(outcome.posts)
            stats.posts_written += len(outcome.posts)
            comment_tasks: list[CrawlTask] = []
            for post in outcome.posts:
                metadata = {"canonical_url": str(post.canonical_url)}
                self.state.upsert_item(post.source, post.source_entity_id, str(post.canonical_url), metadata)
                if self.state.should_refresh_comments(
                    post.source,
                    post.source_entity_id,
                    timedelta(minutes=self.settings.crawl.comment_refresh_minutes),
                ):
                    comment_task = self._adapter_for_source(post.source).comment_task_for_post(post, task.seed_name)
                    if comment_task is not None:
                        comment_tasks.append(comment_task)
            self._enqueue_tasks(queue, comment_tasks, stats, persist=True)

        self._enqueue_tasks(queue, outcome.discovered_tasks, stats, persist=True)

        log_level = logging.WARNING if (outcome.blocked or outcome.errors) else logging.INFO
        logger.log(
            log_level,
            "Task fetched",
            extra={
                "event": "task_fetched",
                "extra_data": {
                    "source": task.source,
                    "kind": task.kind,
                    "url": str(task.url),
                    "status_code": outcome.status_code,
                    "blocked": outcome.blocked,
                    "posts": len(outcome.posts),
                    "authors": len(outcome.authors),
                    "discovered_tasks": len(outcome.discovered_tasks),
                    "errors": list(outcome.errors),
                },
            },
        )

    def _enqueue_task(self, queue: TaskQueue, task: CrawlTask, stats: RunStats) -> None:
        queue.push(task)
        stats.tasks_enqueued += 1

    def _enqueue_tasks(
        self,
        queue: TaskQueue,
        tasks: list[CrawlTask],
        stats: RunStats,
        *,
        persist: bool,
    ) -> None:
        if not tasks:
            return
        assert self.state is not None
        eligible_tasks: list[CrawlTask] = []
        for task in tasks:
            if self._discard_task(task, stats, check_failure_state=False):
                continue
            eligible_tasks.append(task)
        tasks = eligible_tasks
        if not tasks:
            return
        if persist:
            self.state.upsert_pending_tasks(tasks)
        for task in tasks:
            self._enqueue_task(queue, task, stats)

    def _min_age_for_task(self, task: CrawlTask) -> timedelta:
        if task.kind == "discover":
            if task.source == "bilibili" and task.metadata.get("seed_kind") == "space":
                return timedelta(minutes=self.settings.sources.bilibili.space_discovery_interval_minutes)
            if task.source == "guba":
                return timedelta(minutes=self.settings.sources.guba.list_recrawl_minutes)
            if task.source == "tgb":
                return timedelta(minutes=self.settings.sources.tgb.list_recrawl_minutes)
            if task.source == "jiuyan":
                return timedelta(
                    minutes=self.settings.sources.jiuyan.list_recrawl_minutes
                )
            if task.source == "hupu":
                return timedelta(minutes=self.settings.sources.hupu.list_recrawl_minutes)
            return timedelta(minutes=self.settings.crawl.comment_refresh_minutes)
        if task.kind == "refresh_comments":
            return timedelta(minutes=self.settings.crawl.comment_refresh_minutes)
        return timedelta(minutes=self.settings.crawl.post_recrawl_minutes)

    def _prune_expired_guba_pending_tasks(self) -> None:
        assert self.state is not None
        if not (
            self.settings.sources.guba.enabled
            and self.settings.sources.guba.day_scoped
        ):
            return
        start_ts, end_ts = self._guba_day_bounds()
        pruned = sum(
            self.state.prune_pending_tasks_outside_pubdate_range(
                source="guba",
                kind=kind,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            for kind in ("fetch_post", "refresh_comments")
        )
        if pruned:
            logger.info(
                "Expired Guba pending tasks pruned",
                extra={
                    "event": "guba_pending_pruned",
                    "extra_data": {
                        "tasks": pruned,
                        "day_start_ts": start_ts,
                        "day_end_ts": end_ts,
                    },
                },
            )

    def _guba_day_bounds(self, now: datetime | None = None) -> tuple[int, int]:
        timezone = ZoneInfo(self.settings.sources.guba.ranking_timezone)
        local_now = (now or datetime.now(UTC)).astimezone(timezone)
        start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=1)
        return int(start_local.timestamp()), int(end_local.timestamp())

    def _discard_task(
        self,
        task: CrawlTask,
        stats: RunStats,
        *,
        check_failure_state: bool = True,
    ) -> bool:
        assert self.state is not None
        reason: str | None = None
        if (
            task.source == "guba"
            and task.kind in {"fetch_post", "refresh_comments"}
            and self.settings.sources.guba.enabled
            and self.settings.sources.guba.day_scoped
        ):
            start_ts, end_ts = self._guba_day_bounds()
            pubdate_ts = int(task.metadata.get("pubdate_ts") or 0)
            if not start_ts <= pubdate_ts < end_ts:
                reason = "outside_current_guba_day"
        elif (
            check_failure_state
            and task.source == "jiuyan"
            and task.kind == "fetch_post"
            and self.state.task_failure_count(task.dedupe_key, "captcha")
            >= self.settings.sources.jiuyan.captcha_max_attempts
        ):
            reason = "jiuyan_captcha_attempt_limit"

        if reason is None:
            return False
        self.state.delete_pending_task(task.dedupe_key)
        stats.skipped_tasks += 1
        logger.info(
            "Task discarded",
            extra={
                "event": "task_discarded",
                "extra_data": {
                    "source": task.source,
                    "kind": task.kind,
                    "url": str(task.url),
                    "reason": reason,
                },
            },
        )
        return True

    def _record_jiuyan_captcha_block(
        self,
        task: CrawlTask,
        outcome: FetchOutcome,
    ) -> bool:
        assert self.state is not None
        if not (
            task.source == "jiuyan"
            and task.kind == "fetch_post"
            and outcome.blocked
            and any(error.lower().startswith("blocked (captcha)") for error in outcome.errors)
        ):
            return False
        attempts = self.state.record_task_failure(
            dedupe_key=task.dedupe_key,
            source=task.source,
            failure_kind="captcha",
        )
        limit = self.settings.sources.jiuyan.captcha_max_attempts
        if attempts < limit:
            return False
        self.state.delete_pending_task(task.dedupe_key)
        logger.warning(
            "Jiuyan detail abandoned after repeated captcha responses",
            extra={
                "event": "jiuyan_captcha_abandoned",
                "extra_data": {
                    "url": str(task.url),
                    "attempts": attempts,
                    "limit": limit,
                },
            },
        )
        return True

    def _is_guba_browser_post(self, task: CrawlTask) -> bool:
        return (
            task.source == "guba"
            and task.kind == "fetch_post"
            and self.settings.sources.guba.browser.enabled
        )

    def _source_circuit_open(self, source_name: str) -> bool:
        adapter = self._adapter_for_source(source_name)
        check = getattr(adapter, "is_circuit_open", None)
        return bool(check()) if callable(check) else False

    def _select_seeds(self, seed_set_name: str | None) -> list[SeedDefinition]:
        assert self.seed_discovery is not None
        with self.seed_discovery_lock:
            return self.seed_discovery.ensure_compiled_seed_sets(seed_set_name)

    def _build_sources(self) -> dict[str, SourceAdapter]:
        sources: dict[str, SourceAdapter] = {}
        shared_kuaidaili = self._shared_kuaidaili_pool()
        if self.settings.sources.xueqiu.enabled:
            sources["xueqiu"] = XueqiuAdapter(self.settings.sources.xueqiu, self.settings.crawl)
        if self.settings.sources.bilibili.enabled:
            sources["bilibili"] = BilibiliAdapter(self.settings.sources.bilibili, self.settings.crawl)
        if self.settings.sources.guba.enabled:
            guba_pool = shared_kuaidaili
            experiment = self.settings.sources.guba.proxy_dual_endpoint_experiment_enabled
            mobile_primary = self.settings.sources.guba.mobile_detail_api_enabled
            if (
                guba_pool is None
                and (experiment or mobile_primary)
                and self.settings.crawl.proxy.enabled
                and self.settings.crawl.proxy.provider == "kuaidaili"
                and self._proxy_enabled_for_source("guba")
            ):
                guba_pool = KuaidailiProxyPool(self.settings.crawl.kuaidaili)
            guba_client = (
                GubaClient(
                    self.settings.sources.guba,
                    self.settings.crawl,
                    proxy_provider=guba_pool.provider(
                        "guba",
                        experiment_active=(
                            self._guba_dual_endpoint_experiment_active
                            if experiment
                            else None
                        ),
                    ),
                    mobile_proxy_provider=(
                        guba_pool.provider("guba_mobile_primary")
                        if mobile_primary
                        else None
                    ),
                )
                if guba_pool is not None
                and self._proxy_enabled_for_source("guba")
                else None
            )
            sources["guba"] = GubaAdapter(
                self.settings.sources.guba,
                self.settings.crawl,
                client=guba_client,
                raw_store=build_raw_store(self.settings),
            )
        if self.settings.sources.tgb.enabled:
            tgb_client = (
                TgbClient(
                    self.settings.sources.tgb,
                    self.settings.crawl,
                    proxy_provider=shared_kuaidaili.provider("tgb"),
                )
                if shared_kuaidaili is not None
                and self._proxy_enabled_for_source("tgb")
                else None
            )
            sources["tgb"] = TgbAdapter(
                self.settings.sources.tgb,
                self.settings.crawl,
                client=tgb_client,
                raw_store=build_raw_store(self.settings),
            )
        if self.settings.sources.jiuyan.enabled:
            jiuyan_client = (
                JiuyanClient(
                    self.settings.sources.jiuyan,
                    self.settings.crawl,
                    proxy_provider=shared_kuaidaili.provider("jiuyan"),
                )
                if shared_kuaidaili is not None
                and self._proxy_enabled_for_source("jiuyan")
                else None
            )
            sources["jiuyan"] = JiuyanAdapter(
                self.settings.sources.jiuyan,
                self.settings.crawl,
                client=jiuyan_client,
                raw_store=build_raw_store(self.settings),
            )
        if self.settings.sources.hupu.enabled:
            hupu_client = (
                HupuClient(
                    self.settings.sources.hupu,
                    self.settings.crawl,
                    proxy_provider=shared_kuaidaili.provider("hupu"),
                )
                if shared_kuaidaili is not None
                and self._proxy_enabled_for_source("hupu")
                else None
            )
            sources["hupu"] = HupuAdapter(
                self.settings.sources.hupu,
                self.settings.crawl,
                client=hupu_client,
                raw_store=build_raw_store(self.settings),
            )
        return sources

    def _shared_kuaidaili_pool(self) -> KuaidailiProxyPool | None:
        proxy = self.settings.crawl.proxy
        kuaidaili = self.settings.crawl.kuaidaili
        if not (
            proxy.enabled
            and proxy.provider == "kuaidaili"
            and kuaidaili.share_across_sources
        ):
            return None
        return KuaidailiProxyPool(kuaidaili)

    def _proxy_enabled_for_source(self, source: str) -> bool:
        sources = self.settings.crawl.proxy.sources
        return not sources or source in sources

    def _adapter_for_task(self, task: CrawlTask) -> SourceAdapter:
        return self._adapter_for_source(task.source)

    def _adapter_for_source(self, source_name: str) -> SourceAdapter:
        adapter = self.sources.get(source_name)
        if adapter is None:
            raise KeyError(f"Unknown source adapter: {source_name}")
        return adapter
