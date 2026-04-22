from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

FetchMode = Literal["static", "dynamic", "stealth"]
StorageBackend = Literal["clickhouse", "rqlite"]
StateBackend = Literal["sqlite", "rqlite"]
ProxyProviderType = Literal["proxy_pool"]


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


class CrawlProxySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    provider: ProxyProviderType | None = None
    max_attempts: int = Field(default=2, ge=1)
    fail_open: bool = False

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
    cookies: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_request_interval(self) -> "BilibiliSettings":
        if self.request_interval_max_seconds < self.request_interval_min_seconds:
            raise ValueError(
                "sources.bilibili.request_interval_max_seconds must be >= request_interval_min_seconds"
            )
        return self


class SourcesSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    xueqiu: XueqiuSettings = Field(default_factory=XueqiuSettings)
    bilibili: BilibiliSettings = Field(default_factory=BilibiliSettings)


class WebSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    storage: StorageSettings = Field(default_factory=StorageSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    rqlite: RqliteSettings = Field(default_factory=RqliteSettings)
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
    settings.sources.xueqiu.seed_catalog_path = _resolve_path(config_dir, settings.sources.xueqiu.seed_catalog_path)
    if settings.crawl.state_backend == "sqlite":
        settings.crawl.state_path.parent.mkdir(parents=True, exist_ok=True)
    return settings


def _resolve_path(base_dir: Path, candidate: Path) -> Path:
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()
