from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from alphapulse.runtime.config import load_settings
from alphapulse.runtime.state import StateStore
from alphapulse.seeds.catalog import (
    GeneratedSeedItem,
    LonghubangGeneratorDefinition,
    LonghubangRecord,
    ManualGeneratorDefinition,
    SeedCatalogLoader,
    StockUniverseGeneratorDefinition,
    StockUniverseRecord,
)
from alphapulse.seeds.discovery import (
    LonghubangSeedGenerator,
    ManualSeedGenerator,
    SeedCompiler,
    SeedDiscoveryManager,
    StockUniverseSeedGenerator,
)
from alphapulse.seeds.eastmoney import parse_eastmoney_longhubang_page


def test_seed_catalog_loader_resolves_relative_dataset_paths(tmp_path: Path) -> None:
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir()
    (datasets_dir / "stocks.json").write_text('{"stocks":[{"stock_id":"SH600519"}]}')
    catalog_path = tmp_path / "seed_catalog.toml"
    catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["core-stocks"]

[[generators]]
name = "core-stocks"
type = "stock_universe"
dataset_path = "datasets/stocks.json"
""".strip()
    )

    catalog = SeedCatalogLoader(catalog_path).load()
    generator = catalog.generator_map()["core-stocks"]

    assert isinstance(generator, StockUniverseGeneratorDefinition)
    assert generator.dataset_path == (datasets_dir / "stocks.json").resolve()


def test_stock_universe_generator_filters_records() -> None:
    generator = StockUniverseSeedGenerator()
    definition = StockUniverseGeneratorDefinition(
        name="core-stocks",
        stocks=[
            StockUniverseRecord(stock_id="SH600519", market="CN", board="main", tags=["core", "consumer"]),
            StockUniverseRecord(stock_id="SZ000858", market="CN", board="main", tags=["core"]),
            StockUniverseRecord(stock_id="SZ300750", market="CN", board="gem", tags=["core"]),
            StockUniverseRecord(stock_id="SH688111", market="CN", board="star", tags=["core"]),
            StockUniverseRecord(stock_id="SZ000001", market="CN", board="main", tags=["bank"]),
        ],
        markets=["CN"],
        boards=["main"],
        prefixes=["SH60", "SZ00"],
        include_tags=["core"],
        limit=2,
    )

    items = generator.generate(definition, datetime.now(UTC))

    assert [item.value for item in items] == ["SH600519", "SZ000858"]


def test_longhubang_generator_filters_and_dedupes_entries() -> None:
    generator = LonghubangSeedGenerator()
    definition = LonghubangGeneratorDefinition(
        name="dragons",
        entries=[
            LonghubangRecord(
                stock_id="SH600519",
                trade_date=date(2026, 4, 15),
                market="CN",
                ranking_mode="buy",
                rank=1,
            ),
            LonghubangRecord(
                stock_id="SH600519",
                trade_date=date(2026, 4, 14),
                market="CN",
                ranking_mode="buy",
                rank=2,
            ),
            LonghubangRecord(
                stock_id="SZ002594",
                trade_date=date(2026, 4, 14),
                market="CN",
                ranking_mode="buy",
                rank=3,
            ),
            LonghubangRecord(
                stock_id="SZ300750",
                trade_date=date(2026, 4, 15),
                market="CN",
                ranking_mode="sell",
                rank=1,
            ),
        ],
        since_date=date(2026, 4, 14),
        markets=["CN"],
        ranking_modes=["buy"],
        top_n=2,
    )

    items = generator.generate(definition, datetime(2026, 4, 15, tzinfo=UTC))

    assert [item.value for item in items] == ["SH600519", "SZ002594"]


def test_parse_eastmoney_longhubang_page_extracts_live_shape() -> None:
    html = """
<script>
var pagedata={"sbgg_all":{"result":{"data":[
  {"SECURITY_CODE":"600519","MARKET_SUFFIX":"SH","TRADE_DATE":"2026-04-15 00:00:00","BILLBOARD_NET_AMT":123456.0},
  {"SECURITY_CODE":"002594","MARKET_SUFFIX":"SZ","TRADE_DATE":"2026-04-15 00:00:00","BILLBOARD_NET_AMT":-10.5}
]}}};
</script>
""".strip()

    entries = parse_eastmoney_longhubang_page(html)

    assert [
        (item.stock_id, item.market, item.ranking_mode, item.rank)
        for item in entries
    ] == [
        ("SH600519", "SH", "net_buy", 1),
        ("SZ002594", "SZ", "net_sell", 2),
    ]


def test_generated_seed_ttl_expires_items_and_compiled_snapshots_survive_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "state.db"
    state = StateStore(state_path)
    now = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    state.upsert_generated_seed_items(
        "cn-core",
        "manual",
        [GeneratedSeedItem(kind="stock_id", value="SH600519")],
        now - timedelta(minutes=20),
    )
    state.upsert_generated_seed_items(
        "cn-core",
        "manual",
        [GeneratedSeedItem(kind="topic_id", value="新能源")],
        now,
    )

    active_items = state.load_active_generated_seed_items(
        "cn-core",
        ttl=timedelta(minutes=10),
        as_of=now,
    )
    compiled = SeedCompiler().compile("cn-core", active_items)
    state.store_compiled_seed_set(compiled, now)

    restarted = StateStore(state_path)
    loaded = restarted.load_compiled_seed_sets("cn-core")

    assert compiled.stock_ids == []
    assert compiled.topic_ids == ["新能源"]
    assert loaded[0].topic_ids == ["新能源"]


def test_seed_discovery_reuses_fresh_snapshot_without_refreshing(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-core"]

[[generators]]
name = "manual-core"
type = "manual"
stock_ids = ["SH600519"]
""".strip()
    )

    state = StateStore(settings.crawl.state_path)
    manager = SeedDiscoveryManager(settings.sources.xueqiu, state)

    first = manager.ensure_compiled_seed_sets()
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-core"]

[[generators]]
name = "manual-core"
type = "manual"
stock_ids = ["SZ000858"]
""".strip()
    )
    second = manager.ensure_compiled_seed_sets()

    with state.connection() as conn:
        run_count = conn.execute("SELECT COUNT(*) AS count FROM generated_seed_runs").fetchone()["count"]

    assert first[0].stock_ids == ["SH600519"]
    assert second[0].stock_ids == ["SH600519"]
    assert run_count == 1


def test_manual_seed_generator_emits_bilibili_targets() -> None:
    generator = ManualSeedGenerator()
    items = generator.generate(
        ManualGeneratorDefinition(name="manual", bilibili_video_targets=["BV1xx411c7mu"]),
        datetime.now(UTC),
    )
    assert [(item.kind, item.value) for item in items] == [("bilibili_video_target", "BV1xx411c7mu")]


def test_manual_seed_generator_emits_bilibili_space_urls() -> None:
    generator = ManualSeedGenerator()
    items = generator.generate(
        ManualGeneratorDefinition(
            name="manual",
            bilibili_space_urls=["https://space.bilibili.com/7033507"],
        ),
        datetime.now(UTC),
    )
    assert [(item.kind, item.value) for item in items] == [
        ("bilibili_space_url", "https://space.bilibili.com/7033507")
    ]


def test_seed_compiler_preserves_bilibili_targets() -> None:
    compiled = SeedCompiler().compile(
        "cn-core",
        [
            GeneratedSeedItem(kind="bilibili_video_target", value="BV1xx411c7mu"),
            GeneratedSeedItem(kind="bilibili_space_url", value="https://space.bilibili.com/7033507"),
        ],
    )
    assert compiled.bilibili_video_targets == ["BV1xx411c7mu"]
    assert compiled.bilibili_space_urls == ["https://space.bilibili.com/7033507"]
