from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class CrawlRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    seeds_processed: int
    tasks_enqueued: int
    pages_fetched: int
    posts_written: int
    comments_written: int
    authors_written: int
    blocked_responses: int
    errors: int
    skipped_tasks: int


class CrawlError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    created_at: datetime
    source: str
    url: str
    error_message: str


class SeedSetSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    refreshed_at: datetime | None
    stock_count: int
    topic_count: int
    user_count: int
    bilibili_video_count: int
    bilibili_space_count: int
    post_url_count: int


class StatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latest_run: CrawlRun | None
    recent_runs: list[CrawlRun]
    recent_errors: list[CrawlError]
    in_flight_urls: int
    seed_sets: list[SeedSetSummary]


class PostSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    canonical_url: str
    author_entity_id: str | None
    title: str | None
    content_preview: str
    published_at: datetime | None
    fetched_at: datetime
    like_count: int | None
    comment_count: int | None


class PostDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    canonical_url: str
    author_entity_id: str | None
    title: str | None
    content_text: str
    language: str | None
    published_at: datetime | None
    fetched_at: datetime
    like_count: int | None
    comment_count: int | None
    repost_count: int | None
    raw_topic_ids: list[str]


class Comment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_entity_id: str
    post_entity_id: str
    parent_comment_entity_id: str | None
    author_entity_id: str | None
    content_text: str
    published_at: datetime | None
    fetched_at: datetime
    like_count: int | None


class PostDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    post: PostDetail
    comments: list[Comment]


class PostsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    posts: list[PostSummary]
    limit: int
    offset: int


class RunsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runs: list[CrawlRun]


class ErrorsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    errors: list[CrawlError]


class SeedsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seed_sets: list[SeedSetSummary]
