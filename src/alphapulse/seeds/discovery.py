from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from alphapulse.pipeline.contracts import SeedDefinition
from alphapulse.runtime.config import XueqiuSettings
from alphapulse.runtime.state import StateStore
from alphapulse.seeds.catalog import (
    GeneratedSeedItem,
    GeneratorDefinition,
    LonghubangGeneratorDefinition,
    LonghubangRecord,
    ManualGeneratorDefinition,
    SeedCatalog,
    SeedCatalogLoader,
    StockUniverseGeneratorDefinition,
    StockUniverseRecord,
    load_longhubang_dataset,
    load_stock_dataset,
)
from alphapulse.seeds.eastmoney import fetch_eastmoney_longhubang_page, parse_eastmoney_longhubang_page


class SeedGenerator(Protocol):
    def generate(self, definition: GeneratorDefinition, generated_at: datetime) -> list[GeneratedSeedItem]: ...


class StockUniverseProvider(Protocol):
    def list_stocks(self, definition: StockUniverseGeneratorDefinition) -> list[StockUniverseRecord]: ...


class LonghubangProvider(Protocol):
    def list_entries(self, definition: LonghubangGeneratorDefinition) -> list[LonghubangRecord]: ...


class CatalogStockUniverseProvider:
    def list_stocks(self, definition: StockUniverseGeneratorDefinition) -> list[StockUniverseRecord]:
        if definition.dataset_path is not None:
            return load_stock_dataset(definition.dataset_path)
        return definition.stocks


class CatalogLonghubangProvider:
    def list_entries(self, definition: LonghubangGeneratorDefinition) -> list[LonghubangRecord]:
        if definition.source_url is not None:
            return parse_eastmoney_longhubang_page(fetch_eastmoney_longhubang_page(str(definition.source_url)))
        if definition.dataset_path is not None:
            return load_longhubang_dataset(definition.dataset_path)
        return definition.entries


class ManualSeedGenerator:
    def generate(self, definition: GeneratorDefinition, generated_at: datetime) -> list[GeneratedSeedItem]:
        assert isinstance(definition, ManualGeneratorDefinition)
        items: list[GeneratedSeedItem] = []
        if definition.discover_homepage:
            items.append(GeneratedSeedItem(kind="discover_homepage", value="true"))
        items.extend(GeneratedSeedItem(kind="post_url", value=str(url)) for url in definition.post_urls)
        items.extend(
            GeneratedSeedItem(kind="bilibili_video_target", value=value)
            for value in definition.bilibili_video_targets
        )
        items.extend(
            GeneratedSeedItem(kind="bilibili_space_url", value=value)
            for value in definition.bilibili_space_urls
        )
        items.extend(GeneratedSeedItem(kind="stock_id", value=value) for value in definition.stock_ids)
        items.extend(GeneratedSeedItem(kind="topic_id", value=value) for value in definition.topic_ids)
        items.extend(GeneratedSeedItem(kind="user_id", value=value) for value in definition.user_ids)
        return items


class StockUniverseSeedGenerator:
    def __init__(self, provider: StockUniverseProvider | None = None) -> None:
        self.provider = provider or CatalogStockUniverseProvider()

    def generate(self, definition: GeneratorDefinition, generated_at: datetime) -> list[GeneratedSeedItem]:
        assert isinstance(definition, StockUniverseGeneratorDefinition)
        records = self.provider.list_stocks(definition)

        if definition.markets:
            allowed = set(definition.markets)
            records = [item for item in records if item.market in allowed]
        if definition.boards:
            allowed = set(definition.boards)
            records = [item for item in records if item.board in allowed]
        if definition.prefixes:
            prefixes = tuple(definition.prefixes)
            records = [item for item in records if item.stock_id.startswith(prefixes)]
        if definition.include_tags:
            required = set(definition.include_tags)
            records = [item for item in records if required.intersection(item.tags)]
        if definition.exclude_tags:
            blocked = set(definition.exclude_tags)
            records = [item for item in records if not blocked.intersection(item.tags)]

        ordered = sorted(records, key=lambda item: item.stock_id)
        if definition.limit is not None:
            ordered = ordered[: definition.limit]
        return [GeneratedSeedItem(kind="stock_id", value=item.stock_id) for item in ordered]


class LonghubangSeedGenerator:
    def __init__(self, provider: LonghubangProvider | None = None) -> None:
        self.provider = provider or CatalogLonghubangProvider()

    def generate(self, definition: GeneratorDefinition, generated_at: datetime) -> list[GeneratedSeedItem]:
        assert isinstance(definition, LonghubangGeneratorDefinition)
        entries = self.provider.list_entries(definition)

        if definition.since_date is not None:
            entries = [item for item in entries if item.trade_date >= definition.since_date]
        if definition.days_window is not None:
            window_start = generated_at.date() - timedelta(days=definition.days_window - 1)
            entries = [item for item in entries if item.trade_date >= window_start]
        if definition.markets:
            allowed = set(definition.markets)
            entries = [item for item in entries if item.market in allowed]
        if definition.ranking_modes:
            allowed = set(definition.ranking_modes)
            entries = [item for item in entries if item.ranking_mode in allowed]

        entries = sorted(
            entries,
            key=lambda item: (
                -item.trade_date.toordinal(),
                item.rank if item.rank is not None else 999999,
                item.stock_id,
            ),
        )

        seen: set[str] = set()
        stock_ids: list[str] = []
        for entry in entries:
            if entry.stock_id in seen:
                continue
            seen.add(entry.stock_id)
            stock_ids.append(entry.stock_id)
            if definition.top_n is not None and len(stock_ids) >= definition.top_n:
                break
        return [GeneratedSeedItem(kind="stock_id", value=item) for item in stock_ids]


class SeedCompiler:
    def compile(self, seed_name: str, items: list[GeneratedSeedItem]) -> SeedDefinition:
        buckets: dict[str, set[str]] = {
            "post_url": set(),
            "bilibili_video_target": set(),
            "bilibili_space_url": set(),
            "stock_id": set(),
            "topic_id": set(),
            "user_id": set(),
        }
        discover_homepage = False

        for item in items:
            if item.kind == "discover_homepage":
                discover_homepage = discover_homepage or item.value.lower() == "true"
                continue
            buckets[item.kind].add(item.value)

        return SeedDefinition(
            name=seed_name,
            discover_homepage=discover_homepage,
            post_urls=sorted(buckets["post_url"]),
            bilibili_video_targets=sorted(buckets["bilibili_video_target"]),
            bilibili_space_urls=sorted(buckets["bilibili_space_url"]),
            stock_ids=sorted(buckets["stock_id"]),
            topic_ids=sorted(buckets["topic_id"]),
            user_ids=sorted(buckets["user_id"]),
        )


@dataclass
class SeedRefreshResult:
    refreshed_at: datetime
    seed_sets: list[SeedDefinition]
    generator_runs: int
    generated_items: int

    def to_dict(self) -> dict[str, object]:
        return {
            "refreshed_at": self.refreshed_at.isoformat(),
            "generator_runs": self.generator_runs,
            "generated_items": self.generated_items,
            "seed_sets": [item.model_dump(mode="json") for item in self.seed_sets],
        }


class SeedDiscoveryManager:
    def __init__(
        self,
        settings: XueqiuSettings,
        state: StateStore,
        loader: SeedCatalogLoader | None = None,
        compiler: SeedCompiler | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.loader = loader or SeedCatalogLoader(settings.seed_catalog_path)
        self.compiler = compiler or SeedCompiler()
        self._generators: dict[str, SeedGenerator] = {
            "manual": ManualSeedGenerator(),
            "stock_universe": StockUniverseSeedGenerator(),
            "longhubang": LonghubangSeedGenerator(),
        }

    def load_catalog(self) -> SeedCatalog:
        return self.loader.load()

    def ensure_compiled_seed_sets(self, seed_set_name: str | None = None) -> list[SeedDefinition]:
        catalog = self.load_catalog()
        target_names = self._target_names(catalog, seed_set_name)
        if not target_names:
            return []
        if self._needs_refresh(target_names):
            return self.refresh(seed_set_name).seed_sets
        return self.state.load_compiled_seed_sets(seed_set_name)

    def refresh(self, seed_set_name: str | None = None) -> SeedRefreshResult:
        catalog = self.load_catalog()
        generated_at = datetime.now(UTC)
        generator_index = catalog.generator_map()
        target_sets = [
            item for item in catalog.logical_sets if seed_set_name is None or item.name == seed_set_name
        ]

        seed_sets: list[SeedDefinition] = []
        generator_runs = 0
        generated_items = 0

        for logical_set in target_sets:
            for generator_name in logical_set.generators:
                definition = generator_index[generator_name]
                started_at = datetime.now(UTC)
                run_id = str(uuid.uuid4())
                try:
                    items = self._generator_for(definition).generate(definition, generated_at)
                    self.state.upsert_generated_seed_items(logical_set.name, generator_name, items, generated_at)
                    self.state.record_generated_seed_run(
                        run_id=run_id,
                        logical_set_name=logical_set.name,
                        generator_name=generator_name,
                        started_at=started_at,
                        finished_at=datetime.now(UTC),
                        status="succeeded",
                        item_count=len(items),
                        error_message=None,
                    )
                    generated_items += len(items)
                except Exception as exc:
                    self.state.record_generated_seed_run(
                        run_id=run_id,
                        logical_set_name=logical_set.name,
                        generator_name=generator_name,
                        started_at=started_at,
                        finished_at=datetime.now(UTC),
                        status="failed",
                        item_count=0,
                        error_message=str(exc),
                    )
                    raise
                generator_runs += 1

            active_items = self.state.load_active_generated_seed_items(
                logical_set.name,
                ttl=timedelta(minutes=self.settings.generated_seed_ttl_minutes),
                as_of=generated_at,
            )
            compiled = self.compiler.compile(logical_set.name, active_items)
            self.state.store_compiled_seed_set(compiled, generated_at)
            seed_sets.append(compiled)

        return SeedRefreshResult(
            refreshed_at=generated_at,
            seed_sets=seed_sets,
            generator_runs=generator_runs,
            generated_items=generated_items,
        )

    def _needs_refresh(self, seed_set_names: list[str]) -> bool:
        if not seed_set_names:
            return False
        refresh_age = timedelta(minutes=self.settings.seed_refresh_minutes)
        now = datetime.now(UTC)
        compiled_names = set(self.state.list_compiled_seed_set_names())
        if any(name not in compiled_names for name in seed_set_names):
            return True
        for name in seed_set_names:
            refreshed_at = self.state.get_compiled_seed_set_refreshed_at(name)
            if refreshed_at is None or now - refreshed_at >= refresh_age:
                return True
        return False

    def _target_names(self, catalog: SeedCatalog, seed_set_name: str | None) -> list[str]:
        if seed_set_name is None:
            return [item.name for item in catalog.logical_sets]
        return [item.name for item in catalog.logical_sets if item.name == seed_set_name]

    def _generator_for(self, definition: GeneratorDefinition) -> SeedGenerator:
        return self._generators[definition.type]
