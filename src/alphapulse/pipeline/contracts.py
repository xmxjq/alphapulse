from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


TaskKind = Literal["discover", "fetch_post", "refresh_comments"]


class SeedDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    discover_homepage: bool = False
    post_urls: list[HttpUrl] = Field(default_factory=list)
    bilibili_video_targets: list[str] = Field(default_factory=list)
    stock_ids: list[str] = Field(default_factory=list)
    topic_ids: list[str] = Field(default_factory=list)
    user_ids: list[str] = Field(default_factory=list)


class CrawlTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    kind: TaskKind
    url: HttpUrl
    seed_name: str
    priority: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)
    discovered_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def dedupe_key(self) -> str:
        return f"{self.source}:{self.kind}:{self.url}"


class NormalizedAuthor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    username: str | None = None
    display_name: str | None = None
    profile_url: HttpUrl | None = None
    bio: str | None = None
    followers: int | None = None
    following: int | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class NormalizedPost(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    canonical_url: HttpUrl
    author_entity_id: str | None = None
    title: str | None = None
    content_text: str
    language: str | None = None
    published_at: datetime | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    like_count: int | None = None
    comment_count: int | None = None
    repost_count: int | None = None
    raw_topic_ids: list[str] = Field(default_factory=list)


class NormalizedComment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    post_entity_id: str
    canonical_url: HttpUrl | None = None
    author_entity_id: str | None = None
    parent_comment_entity_id: str | None = None
    content_text: str
    published_at: datetime | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    like_count: int | None = None


class ItemReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    canonical_url: HttpUrl
    metadata: dict[str, Any] = Field(default_factory=dict)


class FetchOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")

    posts: list[NormalizedPost] = Field(default_factory=list)
    authors: list[NormalizedAuthor] = Field(default_factory=list)
    comments: list[NormalizedComment] = Field(default_factory=list)
    discovered_tasks: list[CrawlTask] = Field(default_factory=list)
    blocked: bool = False
    status_code: int | None = None
    errors: list[str] = Field(default_factory=list)


class SourceAdapter(Protocol):
    source_name: str

    def discover(self, seed: SeedDefinition) -> list[CrawlTask]: ...

    def fetch_item(self, task: CrawlTask) -> FetchOutcome: ...

    def refresh_comments(self, item_ref: ItemReference) -> list[NormalizedComment]: ...

    def comment_task_for_post(self, post: NormalizedPost, seed_name: str) -> CrawlTask: ...
