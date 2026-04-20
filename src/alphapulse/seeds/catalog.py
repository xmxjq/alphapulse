from __future__ import annotations

import json
import tomllib
from datetime import date
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


SeedItemKind = Literal["discover_homepage", "post_url", "bilibili_video_target", "stock_id", "topic_id", "user_id"]


class GeneratedSeedItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: SeedItemKind
    value: str


class LogicalSeedSet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    generators: list[str] = Field(default_factory=list)


class ManualGeneratorDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["manual"] = "manual"
    discover_homepage: bool = False
    post_urls: list[HttpUrl] = Field(default_factory=list)
    bilibili_video_targets: list[str] = Field(default_factory=list)
    stock_ids: list[str] = Field(default_factory=list)
    topic_ids: list[str] = Field(default_factory=list)
    user_ids: list[str] = Field(default_factory=list)


class StockUniverseRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_id: str
    market: str | None = None
    board: str | None = None
    tags: list[str] = Field(default_factory=list)


class StockUniverseGeneratorDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["stock_universe"] = "stock_universe"
    dataset_path: Path | None = None
    stocks: list[StockUniverseRecord] = Field(default_factory=list)
    markets: list[str] = Field(default_factory=list)
    boards: list[str] = Field(default_factory=list)
    prefixes: list[str] = Field(default_factory=list)
    include_tags: list[str] = Field(default_factory=list)
    exclude_tags: list[str] = Field(default_factory=list)
    limit: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_source(self) -> "StockUniverseGeneratorDefinition":
        if self.dataset_path is None and not self.stocks:
            raise ValueError("stock_universe generators require dataset_path or inline stocks")
        return self


class LonghubangRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stock_id: str
    trade_date: date
    market: str | None = None
    ranking_mode: str | None = None
    rank: int | None = None


class LonghubangGeneratorDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["longhubang"] = "longhubang"
    source_url: HttpUrl | None = None
    dataset_path: Path | None = None
    entries: list[LonghubangRecord] = Field(default_factory=list)
    since_date: date | None = None
    days_window: int | None = Field(default=None, gt=0)
    markets: list[str] = Field(default_factory=list)
    ranking_modes: list[str] = Field(default_factory=list)
    top_n: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_source(self) -> "LonghubangGeneratorDefinition":
        if self.source_url is None and self.dataset_path is None and not self.entries:
            raise ValueError("longhubang generators require source_url, dataset_path, or inline entries")
        return self


GeneratorDefinition = Annotated[
    ManualGeneratorDefinition | StockUniverseGeneratorDefinition | LonghubangGeneratorDefinition,
    Field(discriminator="type"),
]


class SeedCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    logical_sets: list[LogicalSeedSet] = Field(default_factory=list)
    generators: list[GeneratorDefinition] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_references(self) -> "SeedCatalog":
        logical_set_names = [item.name for item in self.logical_sets]
        if len(logical_set_names) != len(set(logical_set_names)):
            raise ValueError("logical seed set names must be unique")

        generator_names = [item.name for item in self.generators]
        if len(generator_names) != len(set(generator_names)):
            raise ValueError("generator names must be unique")

        generator_index = set(generator_names)
        for logical_set in self.logical_sets:
            missing = [name for name in logical_set.generators if name not in generator_index]
            if missing:
                raise ValueError(f"logical seed set '{logical_set.name}' references unknown generators: {missing}")
        return self

    def generator_map(self) -> dict[str, GeneratorDefinition]:
        return {item.name: item for item in self.generators}


class SeedCatalogLoader:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> SeedCatalog:
        with self.path.open("rb") as handle:
            payload = tomllib.load(handle)
        catalog = SeedCatalog.model_validate(payload)
        self._resolve_paths(catalog)
        return catalog

    def _resolve_paths(self, catalog: SeedCatalog) -> None:
        base_dir = self.path.parent
        for generator in catalog.generators:
            if isinstance(generator, StockUniverseGeneratorDefinition) and generator.dataset_path is not None:
                generator.dataset_path = self._resolve_path(base_dir, generator.dataset_path)
            if isinstance(generator, LonghubangGeneratorDefinition) and generator.dataset_path is not None:
                generator.dataset_path = self._resolve_path(base_dir, generator.dataset_path)

    @staticmethod
    def _resolve_path(base_dir: Path, candidate: Path) -> Path:
        if candidate.is_absolute():
            return candidate
        return (base_dir / candidate).resolve()


def load_stock_dataset(path: Path) -> list[StockUniverseRecord]:
    payload = _load_dataset_payload(path)
    if isinstance(payload, dict):
        payload = payload.get("stocks", [])
    return [StockUniverseRecord.model_validate(item) for item in payload]


def load_longhubang_dataset(path: Path) -> list[LonghubangRecord]:
    payload = _load_dataset_payload(path)
    if isinstance(payload, dict):
        payload = payload.get("entries", [])
    return [LonghubangRecord.model_validate(item) for item in payload]


def _load_dataset_payload(path: Path) -> object:
    if path.suffix == ".json":
        return json.loads(path.read_text())
    if path.suffix == ".toml":
        with path.open("rb") as handle:
            return tomllib.load(handle)
    raise ValueError(f"Unsupported dataset file type: {path}")
