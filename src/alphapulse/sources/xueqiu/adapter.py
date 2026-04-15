from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.parse import urlencode

from alphapulse.pipeline.contracts import CrawlTask, FetchOutcome, ItemReference, NormalizedPost, SeedDefinition
from alphapulse.runtime.config import CrawlSettings, XueqiuSettings
from alphapulse.sources.fetching import ScraplingClient
from alphapulse.sources.xueqiu.parser import discover_post_urls, parse_comments, parse_post
from alphapulse.sources.xueqiu.urls import extract_post_id, normalize_url, stock_url, topic_url, user_url


class XueqiuAdapter:
    source_name = "xueqiu"

    def __init__(self, settings: XueqiuSettings, crawl_settings: CrawlSettings) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.client = ScraplingClient(settings, crawl_settings)

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        tasks: list[CrawlTask] = []
        if seed.discover_homepage:
            for url in self.settings.homepage_discovery_urls:
                tasks.append(
                    CrawlTask(
                        source=self.source_name,
                        kind="discover",
                        url=normalize_url(url),
                        seed_name=seed.name,
                        priority=100,
                        metadata={"seed_kind": "homepage"},
                    )
                )

        for url in seed.post_urls:
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=normalize_url(url),
                    seed_name=seed.name,
                    priority=200,
                    metadata={"seed_kind": "post_url"},
                )
            )

        for stock_id in seed.stock_ids:
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=stock_url(str(self.settings.base_url), stock_id),
                    seed_name=seed.name,
                    priority=120,
                    metadata={"seed_kind": "stock", "stock_id": stock_id},
                )
            )

        for topic_id in seed.topic_ids:
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=topic_url(str(self.settings.base_url), topic_id),
                    seed_name=seed.name,
                    priority=120,
                    metadata={"seed_kind": "topic", "topic_id": topic_id},
                )
            )

        for user_id in seed.user_ids:
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="discover",
                    url=user_url(str(self.settings.base_url), user_id),
                    seed_name=seed.name,
                    priority=120,
                    metadata={"seed_kind": "user", "user_id": user_id},
                )
            )
        return tasks

    def fetch_item(self, task: CrawlTask) -> FetchOutcome:
        response = self.client.fetch(str(task.url))
        blocked = self._is_blocked(response.text, response.status_code)
        outcome = FetchOutcome(blocked=blocked, status_code=response.status_code)
        if blocked:
            outcome.errors.append(f"Blocked response from {task.url}")
            return outcome

        if task.kind == "discover":
            discovered = [
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=url,
                    seed_name=task.seed_name,
                    priority=150,
                    metadata={"discovered_from": str(task.url)},
                )
                for url in discover_post_urls(response.text)
            ]
            outcome.discovered_tasks.extend(discovered)
            return outcome

        post, author = parse_post(response.text, response.url, datetime.now(UTC))
        if post is None:
            outcome.errors.append(f"Could not parse post payload from {task.url}")
            return outcome

        outcome.posts.append(post)
        if author is not None:
            if post.author_entity_id and author.source_entity_id == "unknown":
                author.source_entity_id = post.author_entity_id
            outcome.authors.append(author)
        return outcome

    def refresh_comments(self, item_ref: ItemReference) -> list:
        post_id = item_ref.source_entity_id
        if not post_id:
            return []

        comments: list = []
        page = 1
        while True:
            url = self.settings.comments_api_template.format(
                post_id=post_id,
                page_size=20,
                page=page,
            )
            response = self.client.fetch(url)
            if self._is_blocked(response.text, response.status_code):
                break
            try:
                payload = json.loads(response.text)
            except json.JSONDecodeError:
                break
            page_comments = parse_comments(payload, post_id, datetime.now(UTC))
            if not page_comments:
                break
            comments.extend(page_comments)
            has_more = payload.get("next_max_id") or payload.get("has_more")
            if not has_more:
                break
            page += 1
        return comments

    def comment_task_for_post(self, post: NormalizedPost, seed_name: str) -> CrawlTask:
        comments_url = self.settings.comments_api_template.format(
            post_id=post.source_entity_id,
            page_size=20,
            page=1,
        )
        return CrawlTask(
            source=self.source_name,
            kind="refresh_comments",
            url=comments_url,
            seed_name=seed_name,
            priority=90,
            metadata={
                "post_id": post.source_entity_id,
                "canonical_url": str(post.canonical_url),
            },
        )

    @staticmethod
    def _is_blocked(text: str, status_code: int) -> bool:
        if status_code in {401, 403, 429, 503}:
            return True
        markers = ("aliyun_waf", "renderData", "_waf_", "captcha")
        return any(marker in text for marker in markers)

