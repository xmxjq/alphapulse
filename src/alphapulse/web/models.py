from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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
    status_code: int | None = None
    task_kind: str | None = None
    error_kind: str | None = None


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


class GubaBoardSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    board_code: str
    seed_sets: list[str]
    post_count: int
    comment_count: int
    latest_published_at: datetime | None
    latest_fetched_at: datetime | None


class GubaBoardsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    boards: list[GubaBoardSummary]


class NextCrawlBoard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    board_code: str
    seed_name: str | None
    last_fetched_at: datetime | None
    eligible_at: datetime | None
    due_now: bool


class TaskKindForecast(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str
    tracked: int
    due_now: int
    next_eligible_at: datetime | None


class GubaNextCrawlResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime
    next_cycle_at: datetime | None
    poll_interval_seconds: int
    list_recrawl_minutes: int
    post_recrawl_minutes: int
    comment_refresh_minutes: int
    boards: list[NextCrawlBoard]
    task_forecasts: list[TaskKindForecast]


class StatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latest_run: CrawlRun | None
    recent_runs: list[CrawlRun]
    recent_errors: list[CrawlError]
    in_flight_urls: int
    seed_sets: list[SeedSetSummary]


class ProxyPoolNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    proxy_id: str
    status: str
    first_seen_at: datetime
    last_seen_at: datetime
    expires_at: datetime
    benched_until: datetime | None
    acquire_count: int
    success_count: int
    failure_count: int
    success_rate: float | None
    last_acquired_at: datetime | None
    last_success_at: datetime | None
    last_failure_at: datetime | None
    last_failure_reason: str | None


class ProxyPoolTrendPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hour: datetime
    extracted: int
    leases: int
    successes: int
    failures: int
    api_errors: int


class ProxyPoolEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    occurred_at: datetime
    event_type: str
    proxy_id: str | None
    count: int
    source: str
    detail: dict[str, Any] = Field(default_factory=dict)


class ProxyPoolSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    extracted: int
    leases: int
    successes: int
    failures: int
    api_errors: int
    pool_empty_events: int
    success_rate: float | None
    last_activity_at: datetime | None


class ProxyPoolResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    generated_at: datetime
    window_hours: int
    batch_size: int
    low_watermark: int
    lease_ttl_seconds: int
    use_api_expiry: bool
    expiry_safety_seconds: int
    active_nodes: int
    benched_nodes: int
    expired_nodes: int
    extracted: int
    unique_nodes: int
    leases: int
    successes: int
    failures: int
    api_errors: int
    pool_empty_events: int
    batches: int
    success_rate: float | None
    requests_per_proxy: float | None
    last_batch_at: datetime | None
    last_success_at: datetime | None
    last_failure_at: datetime | None
    sources: list[ProxyPoolSource] = Field(default_factory=list)
    nodes: list[ProxyPoolNode] = Field(default_factory=list)
    trend: list[ProxyPoolTrendPoint] = Field(default_factory=list)
    events: list[ProxyPoolEvent] = Field(default_factory=list)


class AgentHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(min_length=1, max_length=128)
    version: str = Field(default="unknown", max_length=64)
    os: str = Field(default="unknown", max_length=32)
    arch: str = Field(default="unknown", max_length=32)
    capabilities: list[str] = Field(default_factory=lambda: ["http"])
    max_concurrency: int = Field(default=1, ge=1, le=64)


class AgentLeaseRequest(AgentHeartbeatRequest):
    wait_seconds: int = Field(default=20, ge=0, le=30)


class AgentCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(min_length=1, max_length=128)
    status_code: int = Field(ge=0, le=999)
    final_url: str
    headers: dict[str, str] = Field(default_factory=dict)
    body_base64: str
    duration_ms: int | None = Field(default=None, ge=0)


class AgentFailRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(min_length=1, max_length=128)
    error_message: str = Field(min_length=1, max_length=2000)
    retryable: bool = True


class AgentPoolNode(BaseModel):
    model_config = ConfigDict(extra="ignore")

    agent_id: str
    version: str
    os: str
    arch: str
    last_ip_address: str | None
    capabilities: list[str]
    max_concurrency: int
    first_seen_at: datetime
    last_seen_at: datetime
    benched_until: datetime | None
    leased_count: int
    success_count: int
    failure_count: int
    blocked_count: int
    last_success_at: datetime | None
    last_failure_at: datetime | None
    last_failure_reason: str | None
    status: str
    success_rate: float | None
    source_health: list[dict[str, Any]] = Field(default_factory=list)


class AgentPoolSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    queued_jobs: int
    leased_jobs: int
    completed_jobs: int
    failed_jobs: int
    cancelled_jobs: int
    successes: int
    failures: int
    blocked: int
    last_activity_at: datetime | None


class AgentPoolJob(BaseModel):
    model_config = ConfigDict(extra="ignore")

    job_id: str
    source: str
    capability: str
    host: str
    status: str
    created_at: datetime
    leased_by: str | None
    attempts: int
    response_status: int | None
    duration_ms: int | None
    error_message: str | None
    outcome: str | None


class AgentPoolResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime
    enabled: bool
    routing_mode: str
    paid_slots: int
    agent_slot_limit: int
    online_capacity: int
    combined_capacity: int
    online_nodes: int
    offline_nodes: int
    benched_nodes: int
    queued_jobs: int
    leased_jobs: int
    completed_jobs: int
    failed_jobs: int
    cancelled_jobs: int
    sources: list[AgentPoolSource] = Field(default_factory=list)
    nodes: list[AgentPoolNode] = Field(default_factory=list)
    jobs: list[AgentPoolJob] = Field(default_factory=list)


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
    board_code: str | None = None


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


class ReportBoard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str  # "board" | "theme"
    rank: int | None
    code: str
    name: str | None
    url: str | None
    post_count: int
    comment_count: int
    posts: list[PostSummary] = Field(default_factory=list)
    # Populated only for theme entries: the boards making up the theme.
    members: list[ReportBoard] = Field(default_factory=list)


class ReportSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str
    title: str
    entries: list[ReportBoard] = Field(default_factory=list)


class ReportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    day: str
    timezone: str
    generated_at: datetime
    has_snapshot: bool
    total_posts: int
    total_comments: int
    sections: list[ReportSection] = Field(default_factory=list)
