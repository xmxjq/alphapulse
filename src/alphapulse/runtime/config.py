from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

FetchMode = Literal["static", "dynamic", "stealth"]
StorageBackend = Literal["clickhouse", "rqlite"]


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

    state_path: Path = Path(".runtime/state.db")
    poll_interval_seconds: int = 300
    request_timeout_seconds: int = 30
    post_recrawl_minutes: int = 360
    comment_refresh_minutes: int = 60
    concurrent_requests: int = 4
    log_level: str = "INFO"
    user_agent: str = "AlphaPulseBot/0.1"


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


class SourcesSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    xueqiu: XueqiuSettings = Field(default_factory=XueqiuSettings)


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    storage: StorageSettings = Field(default_factory=StorageSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    rqlite: RqliteSettings = Field(default_factory=RqliteSettings)
    crawl: CrawlSettings = Field(default_factory=CrawlSettings)
    sources: SourcesSettings = Field(default_factory=SourcesSettings)

    @model_validator(mode="after")
    def validate_state_dir(self) -> "Settings":
        self.crawl.state_path.parent.mkdir(parents=True, exist_ok=True)
        return self


def load_settings(path: Path) -> Settings:
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    settings = Settings.model_validate(payload)
    config_dir = path.parent.resolve()
    settings.crawl.state_path = _resolve_path(config_dir, settings.crawl.state_path)
    settings.sources.xueqiu.seed_catalog_path = _resolve_path(config_dir, settings.sources.xueqiu.seed_catalog_path)
    settings.crawl.state_path.parent.mkdir(parents=True, exist_ok=True)
    return settings


def _resolve_path(base_dir: Path, candidate: Path) -> Path:
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()
