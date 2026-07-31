from __future__ import annotations

import tomllib
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

FetchMode = Literal["static", "dynamic", "stealth"]
StorageBackend = Literal["clickhouse", "rqlite", "mongo"]
StateBackend = Literal["sqlite", "rqlite"]
ProxyProviderType = Literal["proxy_pool", "static_list", "kuaidaili"]
SpaceDiscoveryBackend = Literal["api", "cli"]
AgentPoolStrategy = Literal["agent_first"]


class StorageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: StorageBackend = "rqlite"


class ClickHouseSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = "http://localhost:8123"
    database: str = "alphapulse"
    username: str = "default"
    password: str = ""
    secure: bool = False


class RqliteSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = "http://localhost:4001"
    username: str | None = None
    password: str | None = None
    queue_writes: bool = False
    queue_timeout_seconds: int = 10


class MongoSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    uri: str = "mongodb://localhost:27017"
    database: str = "alphapulse"
    collection_prefix: str = ""
    authors_collection: str = "authors"
    posts_collection: str = "posts"
    comments_collection: str = "comments"
    crawl_runs_collection: str = "crawl_runs"
    crawl_errors_collection: str = "crawl_errors"
    server_selection_timeout_ms: int = Field(default=5000, ge=100)

    def resolved(self, collection: str) -> str:
        return f"{self.collection_prefix}{collection}"


class CrawlSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state_backend: StateBackend = "sqlite"
    state_path: Path = Path(".runtime/state.db")
    poll_interval_seconds: int = 300
    request_timeout_seconds: int = 30
    post_recrawl_minutes: int = 360
    comment_refresh_minutes: int = 60
    concurrent_requests: int = Field(default=4, ge=1)
    log_level: str = "INFO"
    user_agent: str = "AlphaPulseBot/0.1"
    proxy: "CrawlProxySettings" = Field(default_factory=lambda: CrawlProxySettings())
    proxy_pool: "CrawlProxyPoolSettings" = Field(default_factory=lambda: CrawlProxyPoolSettings())
    static_proxies: "CrawlStaticProxySettings" = Field(
        default_factory=lambda: CrawlStaticProxySettings()
    )
    kuaidaili: "CrawlKuaidailiSettings" = Field(
        default_factory=lambda: CrawlKuaidailiSettings()
    )
    agent_pool: "AgentPoolSettings" = Field(default_factory=lambda: AgentPoolSettings())
    raw_store: "RawStoreSettings" = Field(default_factory=lambda: RawStoreSettings())

    @model_validator(mode="after")
    def validate_static_proxy_urls(self) -> "CrawlSettings":
        if (
            self.proxy.enabled
            and self.proxy.provider == "static_list"
            and not self.static_proxies.urls
        ):
            raise ValueError(
                "crawl.static_proxies.urls must be non-empty when crawl.proxy.provider is 'static_list'"
            )
        return self


class CrawlProxySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    provider: ProxyProviderType | None = None
    max_attempts: int = Field(default=2, ge=1)
    fail_open: bool = False
    # Source names ("guba", "tgb", "jiuyan", "hupu", "xueqiu", "bilibili") that should use the proxy.
    # Empty means all sources. Scoping matters when a source carries an
    # authenticated cookie: routing it through rotating exits looks like
    # account sharing to the site.
    sources: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_enabled_provider(self) -> "CrawlProxySettings":
        if self.enabled and self.provider is None:
            raise ValueError("crawl.proxy.provider must be set when crawl.proxy.enabled is true")
        return self


class CrawlProxyPoolSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str = "http://proxy_pool:5010"
    https_only: bool = True
    acquire_timeout_seconds: int = Field(default=3, ge=1)
    report_bad_on_block: bool = True


class CrawlStaticProxySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Fixed upstream proxies (e.g. local xray tunnel inbounds), rotated
    # round-robin. Bare "host:port" entries are treated as http proxies.
    urls: list[str] = Field(default_factory=list)
    # How long a proxy sits out after a blocked/failed request before it
    # re-enters the rotation.
    cooldown_seconds: int = Field(default=300, ge=0)


class CrawlKuaidailiSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_url_file: Path = Path(".runtime/kuaidaili-api-url.txt")
    metrics_path: Path = Path(".runtime/proxy-metrics.db")
    batch_size: int = Field(default=5, ge=1, le=100)
    low_watermark: int = Field(default=2, ge=0, le=99)
    lease_ttl_seconds: int = Field(default=600, ge=30)
    cooldown_seconds: int = Field(default=600, ge=0)
    acquire_timeout_seconds: int = Field(default=20, ge=1)
    failure_threshold: int = Field(default=3, ge=1, le=10)
    share_across_sources: bool = False
    use_api_expiry: bool = False
    expiry_safety_seconds: int = Field(default=30, ge=0, le=300)

    @model_validator(mode="after")
    def validate_low_watermark(self) -> "CrawlKuaidailiSettings":
        if self.low_watermark >= self.batch_size:
            raise ValueError("crawl.kuaidaili.low_watermark must be less than batch_size")
        return self


class AgentPoolSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    db_path: Path = Path(".runtime/agent-pool.db")
    sources: list[str] = Field(default_factory=lambda: ["guba", "tgb", "jiuyan", "hupu"])
    strategy: AgentPoolStrategy = "agent_first"
    heartbeat_ttl_seconds: int = Field(default=90, ge=10, le=3600)
    lease_seconds: int = Field(default=180, ge=10, le=3600)
    queue_wait_seconds: int = Field(default=10, ge=1, le=30)
    job_wait_seconds: int = Field(default=120, ge=1, le=3600)
    result_poll_interval_seconds: float = Field(default=0.5, ge=0.05, le=10.0)
    blocked_cooldown_seconds: int = Field(default=21600, ge=0, le=86400)
    max_response_bytes: int = Field(default=8_000_000, ge=1024, le=50_000_000)
    max_pending_jobs: int = Field(default=10_000, ge=1, le=1_000_000)
    response_body_retention_hours: int = Field(default=24, ge=1, le=168)
    job_metadata_retention_days: int = Field(default=30, ge=1, le=365)
    fallback_to_existing_transport: bool = True
    allowed_hosts: list[str] = Field(
        default_factory=lambda: [
            "guba.eastmoney.com",
            "emappdata.eastmoney.com",
            "push2.eastmoney.com",
            "www.tgb.cn",
            "app.jiuyangongshe.com",
            "bbs.hupu.com",
        ]
    )


class RawStoreSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    root_path: Path = Path(".runtime/raw")
    compress: bool = True


class XueqiuSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    base_url: HttpUrl = "https://xueqiu.com"
    fetch_mode: FetchMode = "static"
    homepage_discovery_urls: list[HttpUrl] = Field(default_factory=lambda: ["https://xueqiu.com"])
    comments_api_template: str = "https://xueqiu.com/statuses/comments.json?id={post_id}&count={page_size}&page={page}"
    cookies: dict[str, str] = Field(default_factory=dict)
    seed_catalog_path: Path = Path("seed_catalog.toml")
    seed_refresh_minutes: int = 60
    generated_seed_ttl_minutes: int = 1440


class BilibiliSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    api_base_url: HttpUrl = "https://api.bilibili.com"
    web_base_url: HttpUrl = "https://www.bilibili.com"
    sort_mode: int = 3
    page_size: int = Field(default=30, ge=1, le=30)
    max_pages: int = Field(default=1000, ge=1)
    request_interval_min_seconds: float = Field(default=2.0, ge=0.0)
    request_interval_max_seconds: float = Field(default=5.0, ge=0.0)
    space_discovery_backend: SpaceDiscoveryBackend = "api"
    space_discovery_interval_minutes: int = Field(default=60, ge=1)
    space_discovery_max_videos: int = Field(default=50, ge=1)
    cookies: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_request_interval(self) -> "BilibiliSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.bilibili.request_interval_max_seconds must be >= request_interval_min_seconds"
            )
        return self


class GubaBrowserSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    cdp_url: str = "http://guba_browser:9223"
    navigation_timeout_seconds: int = Field(default=60, ge=1)
    settle_timeout_seconds: int = Field(default=15, ge=1)
    request_interval_min_seconds: float = Field(default=30.0, ge=0.0)
    request_interval_max_seconds: float = Field(default=90.0, ge=0.0)
    max_posts_per_cycle: int = Field(default=40, ge=1)

    @model_validator(mode="after")
    def validate_request_interval(self) -> "GubaBrowserSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.guba.browser.request_interval_max_seconds must be >= "
                "request_interval_min_seconds"
            )
        return self


class GubaSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    base_url: HttpUrl = "https://guba.eastmoney.com"
    # Per-board list-page cap (80 posts/page). In day-scoped mode a board stops
    # at whichever comes first: the day boundary (a page with no post still
    # active today) or this many pages. Keep this modest — one crawl cycle must
    # finish well within a day so seeds and the daily ranking snapshot refresh
    # each day (see GubaAdapter._handle_list_page). Raise it only alongside more
    # proxy throughput.
    max_list_pages: int = Field(default=3, ge=1)
    reply_page_size: int = Field(default=30, ge=1, le=100)
    max_reply_pages: int = Field(default=1, ge=1)
    # Post-only mode: skip reply/comment crawling entirely (no refresh_comments
    # tasks are emitted), cutting a cycle's request count when a full crawl
    # (posts + comments) can't finish within a day. Unlike max_reply_pages,
    # which only limits pagination depth on threads that are fetched, this
    # stops comment fetching outright.
    fetch_comments: bool = True
    list_recrawl_minutes: int = Field(default=30, ge=1)
    # Day-grouped, homepage-ranking-driven crawling.
    day_scoped: bool = True
    ranking_timezone: str = "Asia/Shanghai"
    hot_boards_per_section: int = Field(default=12, ge=1, le=100)
    # Deprecated/unused: 热门主题吧 entries are now whole boards, not themes with
    # member baskets. Kept so existing configs still validate.
    theme_member_cap: int = Field(default=8, ge=0, le=100)
    ranking_stock_url: HttpUrl = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
    ranking_concept_url: HttpUrl = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?ut=8dec03ba335b81bf4ebdf7b29ec27d15&dpt=gb.rmbk&fltt=2&invt=3"
        "&fields=f12,f14,f3&fs=m:90+t:3+e:3&np=1&pn=1&pz=30&po=1&fid=f3"
    )
    # 热门主题吧 CMS bulletin fragment (an HTML list of /list,{code}.html boards),
    # served over POST.
    ranking_theme_url: HttpUrl = (
        "https://guba.eastmoney.com/api/getBulletin?url=html%2FBase%2F1346.html&cachetime=600"
    )
    request_interval_min_seconds: float = Field(default=2.0, ge=0.0)
    request_interval_max_seconds: float = Field(default=6.0, ge=0.0)
    concurrent_paid_requests: int = Field(default=1, ge=1, le=16)
    concurrent_agent_requests: int = Field(default=4, ge=1, le=64)
    max_retries: int = Field(default=3, ge=1)
    block_cooldown_seconds: int = Field(default=21600, ge=1)
    user_agent: str | None = None
    cookies: dict[str, str] = Field(default_factory=dict)
    browser: GubaBrowserSettings = Field(default_factory=GubaBrowserSettings)

    @model_validator(mode="after")
    def validate_request_interval(self) -> "GubaSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.guba.request_interval_max_seconds must be >= request_interval_min_seconds"
            )
        return self


class TgbSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    base_url: HttpUrl = "https://www.tgb.cn"
    # Board slugs for the two fixed feeds. 精华 (featured) is the report's
    # "featured" section; 社区总版 (general) is the catch-all "general" tier.
    featured_slug: str = "jinghua"
    general_slug: str = "zongban"
    # Per-board list-page cap for the day-scoped feed crawl (~70 posts/page). Like
    # guba's cap, keep this modest so one crawl cycle finishes within a day and each
    # calendar day gets its own ranking snapshot (see TgbAdapter._handle_list_page).
    max_list_pages: int = Field(default=3, ge=1)
    # Hot-stock discussion boards (/quotes/{code}) render a single server-side page
    # of the mention feed; we crawl page 1 only by default.
    max_stock_pages: int = Field(default=1, ge=1)
    # Index boards crawled every day in addition to the dynamic hot-stock ranking.
    # Keys are tgb /quotes/{code} identifiers and values are report display names.
    fixed_boards: dict[str, str] = Field(
        default_factory=lambda: {
            "sh000001": "上证指数",
            "sz399006": "创业板指",
            "sh000688": "科创50",
            "sh000016": "上证50",
        }
    )
    # How many top 热门研股 (hot research stocks) to seed as "general" boards each day.
    hot_stocks_limit: int = Field(default=12, ge=1, le=100)
    list_recrawl_minutes: int = Field(default=30, ge=1)
    # Day-grouped crawling: each cycle re-derives "today" in ranking_timezone and
    # only crawls that day's posts (list feeds are sorted by post date descending).
    day_scoped: bool = True
    ranking_timezone: str = "Asia/Shanghai"
    # Homepage HTML carrying the 热门研股 widget; parsed for hot-stock discovery.
    ranking_hot_stock_url: HttpUrl = "https://www.tgb.cn/"
    request_interval_min_seconds: float = Field(default=2.0, ge=0.0)
    request_interval_max_seconds: float = Field(default=6.0, ge=0.0)
    max_retries: int = Field(default=3, ge=1)
    user_agent: str | None = None
    cookies: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_request_interval(self) -> "TgbSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.tgb.request_interval_max_seconds must be >= request_interval_min_seconds"
            )
        return self


class JiuyanSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    base_url: HttpUrl = "https://www.jiuyangongshe.com"
    api_base_url: HttpUrl = "https://app.jiuyangongshe.com/jystock-app"
    community_feeds: list[Literal["study", "square", "live"]] = Field(
        default_factory=lambda: ["study", "square", "live"]
    )
    community_page_size: int = Field(default=30, ge=1, le=100)
    max_community_pages: int = Field(default=12, ge=1, le=100)
    fixed_targets: list[str] = Field(
        default_factory=lambda: ["上证指数", "创业板指", "科创50", "上证50"]
    )
    fixed_target_aliases: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "上证指数": ["沪指", "上证综指"],
            "创业板指": ["创指", "创业板综指", "创业板ETF"],
            "科创50": ["科创50ETF"],
            "上证50": ["上证50ETF"],
        }
    )
    hot_targets_limit: int = Field(default=10, ge=1, le=100)
    max_search_pages: int = Field(default=3, ge=1, le=100)
    list_recrawl_minutes: int = Field(default=30, ge=1)
    day_scoped: bool = True
    ranking_timezone: str = "Asia/Shanghai"
    request_interval_min_seconds: float = Field(default=1.0, ge=0.0)
    request_interval_max_seconds: float = Field(default=3.0, ge=0.0)
    concurrent_paid_requests: int = Field(default=1, ge=1, le=16)
    concurrent_agent_requests: int = Field(default=2, ge=0, le=64)
    max_retries: int = Field(default=3, ge=1)
    fetch_comments: bool = False
    user_agent: str | None = None
    cookies: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_request_interval(self) -> "JiuyanSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.jiuyan.request_interval_max_seconds must be >= "
                "request_interval_min_seconds"
            )
        return self


class HupuSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    base_url: HttpUrl = "https://bbs.hupu.com"
    board_slug: str = "stock"
    authorization_expires_on: date | None = None
    max_list_pages: int = Field(default=6, ge=1, le=40)
    list_recrawl_minutes: int = Field(default=30, ge=1)
    day_scoped: bool = True
    ranking_timezone: str = "Asia/Shanghai"
    fixed_targets: list[str] = Field(
        default_factory=lambda: ["上证指数", "创业板指", "科创50", "上证50"]
    )
    fixed_target_aliases: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "上证指数": ["沪指", "上证综指"],
            "创业板指": ["创指", "创业板综指", "创业板ETF"],
            "科创50": ["科创50ETF"],
            "上证50": ["上证50ETF"],
        }
    )
    request_interval_min_seconds: float = Field(default=5.0, ge=0.0)
    request_interval_max_seconds: float = Field(default=9.0, ge=0.0)
    concurrent_paid_requests: int = Field(default=1, ge=1, le=8)
    concurrent_agent_requests: int = Field(default=1, ge=0, le=16)
    max_retries: int = Field(default=3, ge=1)
    fetch_comments: bool = False
    user_agent: str | None = None
    cookies: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_hupu_settings(self) -> "HupuSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.hupu.request_interval_max_seconds must be >= "
                "request_interval_min_seconds"
            )
        if self.enabled and self.authorization_expires_on is None:
            raise ValueError(
                "sources.hupu.authorization_expires_on is required when Hupu is enabled"
            )
        return self

    def authorization_active(self, on_date: date | None = None) -> bool:
        return bool(
            self.authorization_expires_on is not None
            and (on_date or date.today()) <= self.authorization_expires_on
        )


class SourcesSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    xueqiu: XueqiuSettings = Field(default_factory=XueqiuSettings)
    bilibili: BilibiliSettings = Field(default_factory=BilibiliSettings)
    guba: GubaSettings = Field(default_factory=GubaSettings)
    tgb: TgbSettings = Field(default_factory=TgbSettings)
    jiuyan: JiuyanSettings = Field(default_factory=JiuyanSettings)
    hupu: HupuSettings = Field(default_factory=HupuSettings)


class WebSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    storage: StorageSettings = Field(default_factory=StorageSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    rqlite: RqliteSettings = Field(default_factory=RqliteSettings)
    mongo: MongoSettings = Field(default_factory=MongoSettings)
    crawl: CrawlSettings = Field(default_factory=CrawlSettings)
    sources: SourcesSettings = Field(default_factory=SourcesSettings)
    web: WebSettings = Field(default_factory=WebSettings)

    @model_validator(mode="after")
    def validate_state_dir(self) -> "Settings":
        if self.crawl.state_backend == "sqlite":
            self.crawl.state_path.parent.mkdir(parents=True, exist_ok=True)
        return self


def load_settings(path: Path) -> Settings:
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    settings = Settings.model_validate(payload)
    config_dir = path.parent.resolve()
    settings.crawl.state_path = _resolve_path(config_dir, settings.crawl.state_path)
    settings.crawl.raw_store.root_path = _resolve_path(config_dir, settings.crawl.raw_store.root_path)
    settings.crawl.kuaidaili.api_url_file = _resolve_path(
        config_dir, settings.crawl.kuaidaili.api_url_file
    )
    settings.crawl.kuaidaili.metrics_path = _resolve_path(
        config_dir, settings.crawl.kuaidaili.metrics_path
    )
    settings.crawl.agent_pool.db_path = _resolve_path(
        config_dir, settings.crawl.agent_pool.db_path
    )
    settings.sources.xueqiu.seed_catalog_path = _resolve_path(config_dir, settings.sources.xueqiu.seed_catalog_path)
    if settings.crawl.state_backend == "sqlite":
        settings.crawl.state_path.parent.mkdir(parents=True, exist_ok=True)
    return settings


def _resolve_path(base_dir: Path, candidate: Path) -> Path:
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()
