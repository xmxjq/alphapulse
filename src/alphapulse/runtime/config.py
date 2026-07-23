from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

FetchMode = Literal["static", "dynamic", "stealth"]
StorageBackend = Literal["clickhouse", "rqlite", "mongo"]
StateBackend = Literal["sqlite", "rqlite"]
ProxyProviderType = Literal["proxy_pool", "static_list"]
SpaceDiscoveryBackend = Literal["api", "cli"]


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
    concurrent_requests: int = 4
    log_level: str = "INFO"
    user_agent: str = "AlphaPulseBot/0.1"
    proxy: "CrawlProxySettings" = Field(default_factory=lambda: CrawlProxySettings())
    proxy_pool: "CrawlProxyPoolSettings" = Field(default_factory=lambda: CrawlProxyPoolSettings())
    static_proxies: "CrawlStaticProxySettings" = Field(
        default_factory=lambda: CrawlStaticProxySettings()
    )
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
    # Source names ("guba", "xueqiu", "bilibili") that should use the proxy.
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
    max_reply_pages: int = Field(default=40, ge=1)
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
    max_retries: int = Field(default=3, ge=1)
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


class SourcesSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    xueqiu: XueqiuSettings = Field(default_factory=XueqiuSettings)
    bilibili: BilibiliSettings = Field(default_factory=BilibiliSettings)
    guba: GubaSettings = Field(default_factory=GubaSettings)
    tgb: TgbSettings = Field(default_factory=TgbSettings)


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
    settings.sources.xueqiu.seed_catalog_path = _resolve_path(config_dir, settings.sources.xueqiu.seed_catalog_path)
    if settings.crawl.state_backend == "sqlite":
        settings.crawl.state_path.parent.mkdir(parents=True, exist_ok=True)
    return settings


def _resolve_path(base_dir: Path, candidate: Path) -> Path:
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()
