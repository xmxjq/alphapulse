from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from alphapulse.pipeline.contracts import (
    CrawlTask,
    FetchOutcome,
    ItemReference,
    NormalizedAuthor,
    NormalizedComment,
    NormalizedPost,
    SeedDefinition,
)
from alphapulse.runtime.config import BilibiliSettings, CrawlSettings
from alphapulse.sources.bilibili.api import BilibiliApiClient, BilibiliApiResult
from alphapulse.sources.bilibili.ids import build_video_url, parse_video_target


class BilibiliAdapter:
    source_name = "bilibili"

    def __init__(self, settings: BilibiliSettings, crawl_settings: CrawlSettings) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.api = BilibiliApiClient(settings, crawl_settings)

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]:
        tasks: list[CrawlTask] = []
        for target in seed.bilibili_video_targets:
            parsed = parse_video_target(target, str(self.settings.web_base_url))
            if parsed is None:
                continue
            metadata: dict[str, Any] = {"video_target": target}
            if parsed.bvid is not None:
                metadata["bvid"] = parsed.bvid
            if parsed.aid is not None:
                metadata["aid"] = str(parsed.aid)
            tasks.append(
                CrawlTask(
                    source=self.source_name,
                    kind="fetch_post",
                    url=parsed.canonical_url,
                    seed_name=seed.name,
                    priority=200,
                    metadata=metadata,
                )
            )
        return tasks

    def fetch_item(self, task: CrawlTask) -> FetchOutcome:
        outcome = FetchOutcome()
        parsed = parse_video_target(task.metadata.get("video_target", str(task.url)), str(self.settings.web_base_url))
        if parsed is None:
            outcome.errors.append(f"Could not parse Bilibili video target from {task.url}")
            return outcome

        result = self.api.get_video_info(bvid=parsed.bvid, aid=parsed.aid)
        if result.error_message:
            return self._error_outcome(task, result)

        data = (result.payload or {}).get("data")
        if not isinstance(data, dict):
            outcome.errors.append(f"Could not parse video metadata from {task.url}")
            outcome.status_code = result.status_code
            return outcome

        fetched_at = datetime.now(UTC)
        post = self._normalize_post(data, fetched_at)
        if post is None:
            outcome.errors.append(f"Could not normalize video payload from {task.url}")
            outcome.status_code = result.status_code
            return outcome

        outcome.posts.append(post)
        author = self._normalize_author(data, fetched_at)
        if author is not None:
            outcome.authors.append(author)
        outcome.status_code = result.status_code
        return outcome

    def refresh_comments(self, item_ref: ItemReference) -> list[NormalizedComment]:
        try:
            aid = int(item_ref.source_entity_id)
        except ValueError:
            return []

        canonical_url = str(item_ref.canonical_url)
        owner_mid = item_ref.metadata.get("owner_mid") if item_ref.metadata else None
        comments: list[NormalizedComment] = []
        seen_root_ids: set[str] = set()
        next_cursor = 0
        page = 1

        while page <= self.settings.max_pages:
            result = self.api.get_comments(aid=aid, next_cursor=next_cursor, page=page)
            if result.error_message:
                break

            data = (result.payload or {}).get("data")
            if not isinstance(data, dict):
                break

            replies = data.get("replies") or []
            if not replies:
                break

            current_ids = {
                str(reply.get("rpid"))
                for reply in replies
                if isinstance(reply, dict) and reply.get("rpid") is not None
            }
            if current_ids and current_ids.issubset(seen_root_ids):
                break
            seen_root_ids.update(current_ids)

            root_tasks: list[int] = []
            for reply in replies:
                if not isinstance(reply, dict):
                    continue
                comment = self._normalize_comment(
                    reply,
                    aid=aid,
                    canonical_url=canonical_url,
                    is_reply=False,
                    owner_mid=owner_mid,
                )
                if comment is not None:
                    comments.append(comment)
                if int(reply.get("rcount") or 0) > 0 and reply.get("rpid") is not None:
                    root_tasks.append(int(reply["rpid"]))

            if root_tasks:
                comments.extend(self._fetch_replies_concurrently(aid, root_tasks, canonical_url, owner_mid))

            cursor = data.get("cursor") or {}
            is_end = bool(cursor.get("is_end", True))
            next_cursor = int(cursor.get("next") or 0)
            if is_end or next_cursor <= 0:
                break
            page += 1

        return comments

    def comment_task_for_post(self, post: NormalizedPost, seed_name: str) -> CrawlTask:
        query = urlencode(
            {
                "oid": post.source_entity_id,
                "type": 1,
                "mode": self.settings.sort_mode,
                "pn": 1,
                "ps": self.settings.page_size,
                "next": 0,
            }
        )
        metadata: dict[str, Any] = {
            "post_id": post.source_entity_id,
            "canonical_url": str(post.canonical_url),
        }
        if post.author_entity_id is not None:
            metadata["owner_mid"] = post.author_entity_id
        return CrawlTask(
            source=self.source_name,
            kind="refresh_comments",
            url=f"{str(self.settings.api_base_url).rstrip('/')}/x/v2/reply/main?{query}",
            seed_name=seed_name,
            priority=90,
            metadata=metadata,
        )

    def _fetch_replies_concurrently(
        self,
        aid: int,
        root_rpids: list[int],
        canonical_url: str,
        owner_mid: str | None = None,
    ) -> list[NormalizedComment]:
        max_workers = max(1, self.crawl_settings.concurrent_requests)
        comments: list[NormalizedComment] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._fetch_replies_for_root, aid, root_rpid, canonical_url, owner_mid): root_rpid
                for root_rpid in root_rpids
            }
            for future in as_completed(futures):
                try:
                    comments.extend(future.result())
                except Exception:
                    continue
        return comments

    def _fetch_replies_for_root(
        self,
        aid: int,
        root_rpid: int,
        canonical_url: str,
        owner_mid: str | None = None,
    ) -> list[NormalizedComment]:
        comments: list[NormalizedComment] = []
        page = 1
        while page <= self.settings.max_pages:
            result = self.api.get_replies(aid=aid, root_rpid=root_rpid, page=page)
            if result.error_message:
                break

            data = (result.payload or {}).get("data")
            if not isinstance(data, dict):
                break

            replies = data.get("replies") or []
            if not replies:
                break

            for reply in replies:
                if not isinstance(reply, dict):
                    continue
                comment = self._normalize_comment(
                    reply,
                    aid=aid,
                    canonical_url=canonical_url,
                    is_reply=True,
                    owner_mid=owner_mid,
                )
                if comment is not None:
                    comments.append(comment)

            cursor = data.get("cursor") or {}
            if bool(cursor.get("is_end", True)):
                break
            page += 1
        return comments

    def _normalize_post(self, payload: dict[str, Any], fetched_at: datetime) -> NormalizedPost | None:
        aid = payload.get("aid")
        bvid = payload.get("bvid")
        if aid is None or not bvid:
            return None

        owner = payload.get("owner") or {}
        stat = payload.get("stat") or {}
        title = payload.get("title")
        description = payload.get("desc") or title
        if not description:
            return None

        return NormalizedPost(
            source=self.source_name,
            source_entity_id=str(aid),
            canonical_url=build_video_url(str(self.settings.web_base_url), str(bvid)),
            author_entity_id=str(owner.get("mid")) if owner.get("mid") is not None else None,
            title=title,
            content_text=str(description),
            published_at=self._parse_timestamp(payload.get("pubdate")),
            fetched_at=fetched_at,
            like_count=self._optional_int(stat.get("like")),
            comment_count=self._optional_int(stat.get("reply")),
            repost_count=self._optional_int(stat.get("share")),
        )

    def _normalize_author(self, payload: dict[str, Any], fetched_at: datetime) -> NormalizedAuthor | None:
        owner = payload.get("owner") or {}
        mid = owner.get("mid")
        if mid is None:
            return None
        profile_url = f"https://space.bilibili.com/{mid}"
        name = owner.get("name")
        return NormalizedAuthor(
            source=self.source_name,
            source_entity_id=str(mid),
            username=name,
            display_name=name,
            profile_url=profile_url,
            fetched_at=fetched_at,
        )

    def _normalize_comment(
        self,
        reply: dict[str, Any],
        *,
        aid: int,
        canonical_url: str,
        is_reply: bool,
        owner_mid: str | None = None,
    ) -> NormalizedComment | None:
        rpid = reply.get("rpid")
        if rpid is None:
            return None
        content = reply.get("content") or {}
        message = (content.get("message") or "").strip()
        if not message:
            return None

        member = reply.get("member") or {}
        author_mid = str(member.get("mid")) if member.get("mid") is not None else None

        if owner_mid is not None and author_mid != owner_mid:
            return None

        parent_id = reply.get("parent")
        parent_comment_entity_id = None
        if is_reply and parent_id not in (None, 0, "0"):
            parent_comment_entity_id = str(parent_id)

        return NormalizedComment(
            source=self.source_name,
            source_entity_id=str(rpid),
            post_entity_id=str(aid),
            canonical_url=f"{canonical_url}#reply{rpid}",
            author_entity_id=author_mid,
            parent_comment_entity_id=parent_comment_entity_id,
            content_text=message,
            published_at=self._parse_timestamp(reply.get("ctime")),
            fetched_at=datetime.now(UTC),
            like_count=self._optional_int(reply.get("like")),
        )

    def _error_outcome(self, task: CrawlTask, result: BilibiliApiResult) -> FetchOutcome:
        outcome = FetchOutcome(blocked=result.blocked, status_code=result.status_code)
        error = f"Fetch failed for {task.url}: {result.error_message}"
        if result.proxy_url:
            error = f"{error} via {result.proxy_url}"
        outcome.errors.append(error)
        return outcome

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (TypeError, ValueError, OSError):
            return None
