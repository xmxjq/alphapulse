from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from alphapulse.pipeline.contracts import SeedDefinition
from alphapulse.runtime.config import (
    CrawlSettings,
    GubaSettings,
    JiuyanSettings,
    TgbSettings,
    XueqiuSettings,
)
from alphapulse.runtime.state import StateStore
from alphapulse.seeds.catalog import (
    GeneratedSeedItem,
    GeneratorDefinition,
    GubaHotBoardsGeneratorDefinition,
    JiuyanHotTargetsGeneratorDefinition,
    LonghubangGeneratorDefinition,
    LonghubangRecord,
    ManualGeneratorDefinition,
    SeedCatalog,
    SeedCatalogLoader,
    StockUniverseGeneratorDefinition,
    StockUniverseRecord,
    TgbHotBoardsGeneratorDefinition,
    load_longhubang_dataset,
    load_stock_dataset,
)
from alphapulse.seeds.eastmoney import fetch_eastmoney_longhubang_page, parse_eastmoney_longhubang_page
from alphapulse.sources.guba.api import GubaClient
from alphapulse.sources.guba.rankings import (
    SECTION_HOT_CONCEPT,
    SECTION_HOT_STOCK,
    SECTION_HOT_THEME,
    HotRankings,
    fetch_hot_rankings,
)
from alphapulse.sources.jiuyan.api import JiuyanClient
from alphapulse.sources.jiuyan.rankings import fetch_hot_targets
from alphapulse.sources.jiuyan.urls import search_url as jiuyan_search_url
from alphapulse.sources.tgb.api import TgbClient
from alphapulse.sources.tgb.rankings import fetch_hot_stocks
from alphapulse.sources.tgb.urls import featured_list_url, general_list_url, stock_list_url


# tgb daily-report section keys (shared with web.queries TGB_REPORT_SECTIONS).
TGB_SECTION_FEATURED = "featured"
TGB_SECTION_GENERAL = "general"
JIUYAN_SECTION_FIXED = "fixed"
JIUYAN_SECTION_HOT = "hot"


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
        items.extend(
            GeneratedSeedItem(kind="guba_board_code", value=value)
            for value in definition.guba_board_codes
        )
        items.extend(
            GeneratedSeedItem(kind="tgb_board_code", value=value)
            for value in definition.tgb_board_codes
        )
        items.extend(
            GeneratedSeedItem(kind="jiuyan_target_code", value=value)
            for value in definition.jiuyan_target_codes
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


class GubaHotBoardsSeedGenerator:
    """Seed guba boards from the three homepage "hot" rankings and snapshot them.

    Emits ordinary ``guba_board_code`` items (consumed unchanged by the adapter)
    and, as a side effect, records the ordered ranking membership for the current
    Beijing day so the daily report can reproduce it.
    """

    def __init__(
        self,
        settings: GubaSettings | None,
        crawl_settings: CrawlSettings | None,
        state: StateStore | None,
        client: GubaClient | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.state = state
        self._client = client

    def _get_client(self) -> GubaClient:
        if self._client is not None:
            return self._client
        if self.settings is None or self.crawl_settings is None:
            raise RuntimeError(
                "guba_hot_boards generator requires guba/crawl settings to be wired"
            )
        ranking_settings = self.settings.model_copy(
            update={
                "request_interval_min_seconds": 0.0,
                "request_interval_max_seconds": 0.0,
                "max_retries": 3,
            }
        )
        ranking_crawl_settings = self.crawl_settings.model_copy(
            update={
                "proxy": self.crawl_settings.proxy.model_copy(
                    update={"enabled": False}
                )
            },
            deep=True,
        )
        self._client = GubaClient(ranking_settings, ranking_crawl_settings)
        return self._client

    def _get_proxy_client(self) -> GubaClient | None:
        if (
            self.settings is None
            or self.crawl_settings is None
            or not self.crawl_settings.proxy.enabled
        ):
            return None
        ranking_settings = self.settings.model_copy(
            update={
                "request_interval_min_seconds": 0.0,
                "request_interval_max_seconds": 0.0,
                "max_retries": 3,
            }
        )
        return GubaClient(ranking_settings, self.crawl_settings)

    def generate(
        self, definition: GeneratorDefinition, generated_at: datetime
    ) -> list[GeneratedSeedItem]:
        assert isinstance(definition, GubaHotBoardsGeneratorDefinition)
        if self.settings is None:
            raise RuntimeError("guba_hot_boards generator requires guba settings to be wired")
        sections = set(definition.sections)
        rankings = fetch_hot_rankings(
            self._get_client(),
            self.settings,
            sections=sections,
        )
        missing_sections = _missing_guba_ranking_sections(rankings, sections)
        proxy_client = self._get_proxy_client() if missing_sections else None
        if proxy_client is not None:
            fallback = fetch_hot_rankings(
                proxy_client,
                self.settings,
                sections=missing_sections,
            )
            rankings = _fill_missing_guba_rankings(
                rankings,
                fallback,
                missing_sections,
            )
        rows = _ranking_snapshot_rows(rankings, sections)

        if self.state is not None:
            day = (
                generated_at.astimezone(ZoneInfo(self.settings.ranking_timezone))
                .date()
                .isoformat()
            )
            rows = _merge_guba_ranking_rows(
                rows,
                self.state.get_guba_ranking(day),
                sections,
            )
            self.state.replace_guba_ranking(day, rows)

        codes: list[str] = []
        seen: set[str] = set()
        for row in rows:
            code = str(row["code"])
            if code not in seen:
                seen.add(code)
                codes.append(code)
        return [GeneratedSeedItem(kind="guba_board_code", value=code) for code in codes]


def _selected_boards(rankings: HotRankings, sections: set[str]) -> list:
    """Ordered boards across the requested sections (stock → concept → theme)."""
    by_section = {
        SECTION_HOT_STOCK: rankings.hot_stock,
        SECTION_HOT_CONCEPT: rankings.hot_concept,
        SECTION_HOT_THEME: rankings.hot_theme,
    }
    boards: list = []
    ordered_sections = (
        SECTION_HOT_STOCK,
        SECTION_HOT_CONCEPT,
        SECTION_HOT_THEME,
    )
    max_entries = max(
        (len(by_section[key]) for key in ordered_sections if key in sections),
        default=0,
    )
    for index in range(max_entries):
        for key in ordered_sections:
            entries = by_section[key]
            if key in sections and index < len(entries):
                boards.append(entries[index])
    return boards


def _ranking_snapshot_rows(
    rankings: HotRankings, sections: set[str]
) -> list[dict[str, object]]:
    # All three sections are now plain boards, so rows are uniform.
    return [
        {
            "section": board.section,
            "rank": board.rank,
            "code": board.board_code,
            "name": board.name,
            "url": board.url,
            "members": None,
        }
        for board in _selected_boards(rankings, sections)
    ]


def _merge_guba_ranking_rows(
    fetched_rows: list[dict[str, object]],
    existing_rows: list[dict[str, object]],
    sections: set[str],
) -> list[dict[str, object]]:
    ordered_sections = (
        SECTION_HOT_STOCK,
        SECTION_HOT_CONCEPT,
        SECTION_HOT_THEME,
    )
    rows_by_section: dict[str, list[dict[str, object]]] = {}
    for section in ordered_sections:
        if section not in sections:
            continue
        rows = [
            row for row in fetched_rows if str(row.get("section")) == section
        ]
        if not rows:
            rows = [
                row for row in existing_rows if str(row.get("section")) == section
            ]
        rows_by_section[section] = sorted(
            rows,
            key=lambda row: int(row.get("rank") or 0),
        )

    merged: list[dict[str, object]] = []
    max_entries = max((len(rows) for rows in rows_by_section.values()), default=0)
    for index in range(max_entries):
        for section in ordered_sections:
            rows = rows_by_section.get(section, [])
            if index < len(rows):
                merged.append(rows[index])
    return merged


def _missing_guba_ranking_sections(
    rankings: HotRankings,
    sections: set[str],
) -> set[str]:
    by_section = {
        SECTION_HOT_STOCK: rankings.hot_stock,
        SECTION_HOT_CONCEPT: rankings.hot_concept,
        SECTION_HOT_THEME: rankings.hot_theme,
    }
    return {section for section in sections if not by_section.get(section)}


def _fill_missing_guba_rankings(
    primary: HotRankings,
    fallback: HotRankings,
    missing_sections: set[str],
) -> HotRankings:
    if SECTION_HOT_STOCK in missing_sections and fallback.hot_stock:
        primary.hot_stock = fallback.hot_stock
    if SECTION_HOT_CONCEPT in missing_sections and fallback.hot_concept:
        primary.hot_concept = fallback.hot_concept
    if SECTION_HOT_THEME in missing_sections and fallback.hot_theme:
        primary.hot_theme = fallback.hot_theme
    return primary


def _order_guba_codes_by_ranking(
    codes: list[str],
    ranking_rows: list[dict[str, object]],
) -> list[str]:
    available = set(codes)
    sections = {
        str(row["section"])
        for row in ranking_rows
        if str(row.get("section") or "")
    }
    ordered_rows = _merge_guba_ranking_rows(ranking_rows, [], sections)
    ordered = [
        str(row["code"])
        for row in ordered_rows
        if str(row["code"]) in available
    ]
    seen = set(ordered)
    ordered.extend(code for code in codes if code not in seen)
    return ordered


def _order_jiuyan_codes_by_ranking(
    codes: list[str], ranking_rows: list[dict[str, object]]
) -> list[str]:
    available = set(codes)
    section_order = {JIUYAN_SECTION_FIXED: 0, JIUYAN_SECTION_HOT: 1}
    ordered_rows = sorted(
        ranking_rows,
        key=lambda row: (
            section_order.get(str(row["section"]), 99),
            int(row["rank"]),
        ),
    )
    ordered = [
        str(row["code"])
        for row in ordered_rows
        if str(row["code"]) in available
    ]
    seen = set(ordered)
    ordered.extend(code for code in codes if code not in seen)
    return ordered


class TgbHotBoardsSeedGenerator:
    """Seed the tgb 精华/社区总版 feeds plus self-discovered 热门研股 stock boards.

    Emits ordinary ``tgb_board_code`` items (a board code is either the featured slug,
    the general slug, or a stock code, disambiguated by the adapter) and, as a side
    effect, snapshots the ordered ranking membership for the current Beijing day so the
    daily report can reproduce the Featured/General sections.
    """

    def __init__(
        self,
        settings: TgbSettings | None,
        crawl_settings: CrawlSettings | None,
        state: StateStore | None,
        client: TgbClient | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.state = state
        self._client = client

    def _get_client(self) -> TgbClient:
        if self._client is not None:
            return self._client
        if self.settings is None or self.crawl_settings is None:
            raise RuntimeError("tgb_hot_boards generator requires tgb/crawl settings to be wired")
        self._client = TgbClient(self.settings, self.crawl_settings)
        return self._client

    def generate(
        self, definition: GeneratorDefinition, generated_at: datetime
    ) -> list[GeneratedSeedItem]:
        assert isinstance(definition, TgbHotBoardsGeneratorDefinition)
        if self.settings is None:
            raise RuntimeError("tgb_hot_boards generator requires tgb settings to be wired")
        base = str(self.settings.base_url)
        limit = definition.hot_stocks_limit or self.settings.hot_stocks_limit
        hot_stocks = fetch_hot_stocks(self._get_client(), self.settings)[:limit]

        rows: list[dict[str, object]] = []
        codes: list[str] = []
        seen_codes: set[str] = set()
        if definition.include_featured:
            codes.append(self.settings.featured_slug)
            seen_codes.add(self.settings.featured_slug)
            rows.append(
                {
                    "section": TGB_SECTION_FEATURED,
                    "rank": 1,
                    "code": self.settings.featured_slug,
                    "name": "精华",
                    "url": featured_list_url(base, self.settings.featured_slug),
                    "members": None,
                }
            )
        general_rank = 0
        if definition.include_general:
            general_rank += 1
            codes.append(self.settings.general_slug)
            seen_codes.add(self.settings.general_slug)
            rows.append(
                {
                    "section": TGB_SECTION_GENERAL,
                    "rank": general_rank,
                    "code": self.settings.general_slug,
                    "name": "社区总版",
                    "url": general_list_url(base, self.settings.general_slug),
                    "members": None,
                }
            )
        if definition.include_fixed_boards:
            for code, name in self.settings.fixed_boards.items():
                if code in seen_codes:
                    continue
                seen_codes.add(code)
                general_rank += 1
                codes.append(code)
                rows.append(
                    {
                        "section": TGB_SECTION_GENERAL,
                        "rank": general_rank,
                        "code": code,
                        "name": name,
                        "url": stock_list_url(base, code),
                        "members": None,
                    }
                )
        for stock in hot_stocks:
            if stock.code in seen_codes:
                continue
            seen_codes.add(stock.code)
            general_rank += 1
            codes.append(stock.code)
            rows.append(
                {
                    "section": TGB_SECTION_GENERAL,
                    "rank": general_rank,
                    "code": stock.code,
                    "name": stock.name,
                    "url": stock_list_url(base, stock.code),
                    "members": None,
                }
            )

        if self.state is not None:
            day = (
                generated_at.astimezone(ZoneInfo(self.settings.ranking_timezone))
                .date()
                .isoformat()
            )
            self.state.replace_tgb_ranking(day, rows)

        return [GeneratedSeedItem(kind="tgb_board_code", value=code) for code in codes]


class JiuyanHotTargetsSeedGenerator:
    def __init__(
        self,
        settings: JiuyanSettings | None,
        crawl_settings: CrawlSettings | None,
        state: StateStore | None,
        client: JiuyanClient | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.state = state
        self._client = client

    def _get_client(self) -> JiuyanClient:
        if self._client is not None:
            return self._client
        if self.settings is None or self.crawl_settings is None:
            raise RuntimeError(
                "jiuyan_hot_targets generator requires jiuyan/crawl settings"
            )
        self._client = JiuyanClient(self.settings, self.crawl_settings)
        return self._client

    def generate(
        self, definition: GeneratorDefinition, generated_at: datetime
    ) -> list[GeneratedSeedItem]:
        assert isinstance(definition, JiuyanHotTargetsGeneratorDefinition)
        if self.settings is None:
            raise RuntimeError(
                "jiuyan_hot_targets generator requires jiuyan settings"
            )
        limit = definition.hot_targets_limit or self.settings.hot_targets_limit
        hot_targets = fetch_hot_targets(self._get_client(), self.settings)[:limit]
        base = str(self.settings.base_url)
        rows: list[dict[str, object]] = []
        codes: list[str] = []
        seen: set[str] = set()

        if definition.include_fixed_targets:
            for rank, keyword in enumerate(self.settings.fixed_targets, start=1):
                if keyword in seen:
                    continue
                seen.add(keyword)
                codes.append(keyword)
                rows.append(
                    {
                        "section": JIUYAN_SECTION_FIXED,
                        "rank": rank,
                        "code": keyword,
                        "name": keyword,
                        "url": jiuyan_search_url(base, keyword),
                        "members": None,
                    }
                )

        hot_rank = 0
        for target in hot_targets:
            if target.keyword in seen:
                continue
            seen.add(target.keyword)
            hot_rank += 1
            codes.append(target.keyword)
            rows.append(
                {
                    "section": JIUYAN_SECTION_HOT,
                    "rank": hot_rank,
                    "code": target.keyword,
                    "name": target.keyword,
                    "url": jiuyan_search_url(base, target.keyword),
                    "members": None,
                }
            )

        if self.state is not None:
            day = (
                generated_at.astimezone(ZoneInfo(self.settings.ranking_timezone))
                .date()
                .isoformat()
            )
            self.state.replace_jiuyan_ranking(day, rows)
        return [
            GeneratedSeedItem(kind="jiuyan_target_code", value=code)
            for code in codes
        ]


class SeedCompiler:
    def compile(self, seed_name: str, items: list[GeneratedSeedItem]) -> SeedDefinition:
        buckets: dict[str, set[str]] = {
            "post_url": set(),
            "bilibili_video_target": set(),
            "bilibili_space_url": set(),
            "guba_board_code": set(),
            "tgb_board_code": set(),
            "jiuyan_target_code": set(),
            "stock_id": set(),
            "topic_id": set(),
            "user_id": set(),
        }
        ordered_guba_codes: list[str] = []
        seen_guba_codes: set[str] = set()
        ordered_jiuyan_codes: list[str] = []
        seen_jiuyan_codes: set[str] = set()
        discover_homepage = False

        for item in items:
            if item.kind == "discover_homepage":
                discover_homepage = discover_homepage or item.value.lower() == "true"
                continue
            if item.kind == "guba_board_code":
                if item.value not in seen_guba_codes:
                    seen_guba_codes.add(item.value)
                    ordered_guba_codes.append(item.value)
                continue
            if item.kind == "jiuyan_target_code":
                if item.value not in seen_jiuyan_codes:
                    seen_jiuyan_codes.add(item.value)
                    ordered_jiuyan_codes.append(item.value)
                continue
            buckets[item.kind].add(item.value)

        return SeedDefinition(
            name=seed_name,
            discover_homepage=discover_homepage,
            post_urls=sorted(buckets["post_url"]),
            bilibili_video_targets=sorted(buckets["bilibili_video_target"]),
            bilibili_space_urls=sorted(buckets["bilibili_space_url"]),
            guba_board_codes=ordered_guba_codes,
            tgb_board_codes=sorted(buckets["tgb_board_code"]),
            jiuyan_target_codes=ordered_jiuyan_codes,
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
        guba_settings: GubaSettings | None = None,
        tgb_settings: TgbSettings | None = None,
        jiuyan_settings: JiuyanSettings | None = None,
        crawl_settings: CrawlSettings | None = None,
    ) -> None:
        self.settings = settings
        self.guba_settings = guba_settings
        self.jiuyan_settings = jiuyan_settings
        self.state = state
        self.loader = loader or SeedCatalogLoader(settings.seed_catalog_path)
        self.compiler = compiler or SeedCompiler()
        self._generators: dict[str, SeedGenerator] = {
            "manual": ManualSeedGenerator(),
            "stock_universe": StockUniverseSeedGenerator(),
            "longhubang": LonghubangSeedGenerator(),
            "guba_hot_boards": GubaHotBoardsSeedGenerator(
                guba_settings, crawl_settings, state
            ),
            "tgb_hot_boards": TgbHotBoardsSeedGenerator(
                tgb_settings, crawl_settings, state
            ),
            "jiuyan_hot_targets": JiuyanHotTargetsSeedGenerator(
                jiuyan_settings, crawl_settings, state
            ),
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
            if self.guba_settings is not None and compiled.guba_board_codes:
                ranking_day = (
                    generated_at.astimezone(
                        ZoneInfo(self.guba_settings.ranking_timezone)
                    )
                    .date()
                    .isoformat()
                )
                compiled = compiled.model_copy(
                    update={
                        "guba_board_codes": _order_guba_codes_by_ranking(
                            compiled.guba_board_codes,
                            self.state.get_guba_ranking(ranking_day),
                        )
                    }
                )
            if self.jiuyan_settings is not None and compiled.jiuyan_target_codes:
                ranking_day = (
                    generated_at.astimezone(
                        ZoneInfo(self.jiuyan_settings.ranking_timezone)
                    )
                    .date()
                    .isoformat()
                )
                compiled = compiled.model_copy(
                    update={
                        "jiuyan_target_codes": _order_jiuyan_codes_by_ranking(
                            compiled.jiuyan_target_codes,
                            self.state.get_jiuyan_ranking(ranking_day),
                        )
                    }
                )
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
