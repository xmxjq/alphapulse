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


def test_manual_seed_generator_emits_guba_board_codes() -> None:
    generator = ManualSeedGenerator()
    items = generator.generate(
        ManualGeneratorDefinition(name="manual", guba_board_codes=["zssh000001", "600519"]),
        datetime.now(UTC),
    )
    assert [(item.kind, item.value) for item in items] == [
        ("guba_board_code", "zssh000001"),
        ("guba_board_code", "600519"),
    ]


def test_seed_compiler_preserves_guba_board_codes() -> None:
    compiled = SeedCompiler().compile(
        "cn-core",
        [
            GeneratedSeedItem(kind="guba_board_code", value="zssh000001"),
            GeneratedSeedItem(kind="guba_board_code", value="600519"),
        ],
    )
    assert compiled.guba_board_codes == ["600519", "zssh000001"]


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


def test_guba_hot_boards_generator_emits_codes_and_snapshots(tmp_path: Path) -> None:
    from alphapulse.runtime.config import GubaSettings
    from alphapulse.sources.guba.api import GubaHttpResult
    from alphapulse.seeds.catalog import GubaHotBoardsGeneratorDefinition
    from alphapulse.seeds.discovery import GubaHotBoardsSeedGenerator

    fixtures = Path(__file__).parent.parent / "fixtures" / "guba"

    class FakeClient:
        def _match(self, url: str) -> GubaHttpResult:
            table = {
                "stockrank": "rank_hot_stock.json",
                "clist": "rank_hot_concept.json",
                "getBulletin": "rank_hot_theme.html",
            }
            for marker, name in table.items():
                if marker in url:
                    return GubaHttpResult(url=url, status_code=200, text=(fixtures / name).read_text(encoding="utf-8"))
            return GubaHttpResult(url=url, status_code=404, text="")

        def get(self, url, *, expect_marker=None):
            return self._match(url)

        def post_json(self, url, payload):
            return self._match(url)

    state = StateStore(tmp_path / "state.db")
    generator = GubaHotBoardsSeedGenerator(GubaSettings(enabled=True), None, state, client=FakeClient())
    definition = GubaHotBoardsGeneratorDefinition(name="guba-hot")

    generated_at = datetime(2026, 7, 21, 3, 0, 0, tzinfo=UTC)  # 11:00 Beijing
    items = generator.generate(definition, generated_at)

    assert all(item.kind == "guba_board_code" for item in items)
    codes = [item.value for item in items]
    # Stock (人气榜), concept (push2), and theme (bulletin board list) all seed codes.
    assert "600584" in codes and "BK1036" in codes and "gssz" in codes

    snapshot = state.get_guba_ranking("2026-07-21")
    sections = {row["section"] for row in snapshot}
    assert sections == {"hot_stock", "hot_concept", "hot_theme"}
    theme_rows = [r for r in snapshot if r["section"] == "hot_theme"]
    assert theme_rows[0]["code"] == "gssz"
    assert theme_rows[0]["members"] == []


def test_guba_ranking_client_bypasses_crawl_proxy(monkeypatch) -> None:
    from alphapulse.runtime.config import CrawlSettings, GubaSettings
    from alphapulse.seeds.discovery import GubaHotBoardsSeedGenerator

    captured = {}

    class FakeClient:
        def __init__(self, settings, crawl_settings):
            captured["settings"] = settings
            captured["crawl_settings"] = crawl_settings

    monkeypatch.setattr("alphapulse.seeds.discovery.GubaClient", FakeClient)
    generator = GubaHotBoardsSeedGenerator(
        GubaSettings(enabled=True),
        CrawlSettings.model_validate(
            {
                "proxy": {
                    "enabled": True,
                    "provider": "kuaidaili",
                    "sources": ["guba"],
                }
            }
        ),
        None,
    )

    generator._get_client()

    assert captured["settings"].max_retries == 3
    assert captured["settings"].request_interval_min_seconds == 0
    assert captured["settings"].request_interval_max_seconds == 0
    assert captured["crawl_settings"].proxy.enabled is False


def test_guba_hot_boards_preserves_failed_ranking_section(
    tmp_path: Path,
) -> None:
    from alphapulse.runtime.config import GubaSettings
    from alphapulse.sources.guba.api import GubaHttpResult
    from alphapulse.seeds.catalog import GubaHotBoardsGeneratorDefinition
    from alphapulse.seeds.discovery import GubaHotBoardsSeedGenerator

    fixtures = Path(__file__).parent.parent / "fixtures" / "guba"

    class PartialClient:
        def _match(self, url: str) -> GubaHttpResult:
            if "clist" in url:
                return GubaHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    error_message="temporary concept ranking failure",
                )
            table = {
                "stockrank": "rank_hot_stock.json",
                "getBulletin": "rank_hot_theme.html",
            }
            for marker, name in table.items():
                if marker in url:
                    return GubaHttpResult(
                        url=url,
                        status_code=200,
                        text=(fixtures / name).read_text(encoding="utf-8"),
                    )
            return GubaHttpResult(url=url, status_code=404, text="")

        def get(self, url, *, expect_marker=None):
            return self._match(url)

        def post_json(self, url, payload):
            return self._match(url)

    state = StateStore(tmp_path / "state.db")
    state.replace_guba_ranking(
        "2026-07-21",
        [
            {
                "section": "hot_concept",
                "rank": 1,
                "code": "BK9999",
                "name": "cached concept",
                "url": "https://guba.eastmoney.com/list,BK9999.html",
                "members": None,
            }
        ],
    )
    generator = GubaHotBoardsSeedGenerator(
        GubaSettings(enabled=True),
        None,
        state,
        client=PartialClient(),
    )

    items = generator.generate(
        GubaHotBoardsGeneratorDefinition(name="guba-hot"),
        datetime(2026, 7, 21, 3, 0, tzinfo=UTC),
    )

    codes = [item.value for item in items]
    assert "BK9999" in codes
    snapshot = state.get_guba_ranking("2026-07-21")
    concept_rows = [row for row in snapshot if row["section"] == "hot_concept"]
    assert [row["code"] for row in concept_rows] == ["BK9999"]
    assert {row["section"] for row in snapshot} == {
        "hot_stock",
        "hot_concept",
        "hot_theme",
    }


def test_guba_hot_boards_uses_proxy_fallback_for_missing_section(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from alphapulse.runtime.config import CrawlSettings, GubaSettings
    from alphapulse.sources.guba.api import GubaHttpResult
    from alphapulse.seeds.catalog import GubaHotBoardsGeneratorDefinition
    from alphapulse.seeds.discovery import GubaHotBoardsSeedGenerator

    fixtures = Path(__file__).parent.parent / "fixtures" / "guba"

    class DirectClient:
        def get(self, url, *, expect_marker=None):
            if "clist" in url:
                return GubaHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    error_message="direct failed",
                )
            return GubaHttpResult(url=url, status_code=404, text="")

        def post_json(self, url, payload):
            table = {
                "stockrank": "rank_hot_stock.json",
                "getBulletin": "rank_hot_theme.html",
            }
            for marker, name in table.items():
                if marker in url:
                    return GubaHttpResult(
                        url=url,
                        status_code=200,
                        text=(fixtures / name).read_text(encoding="utf-8"),
                    )
            return GubaHttpResult(url=url, status_code=404, text="")

    class ProxyClient:
        def get(self, url, *, expect_marker=None):
            return GubaHttpResult(
                url=url,
                status_code=200,
                text=(fixtures / "rank_hot_concept.json").read_text(
                    encoding="utf-8"
                ),
            )

        def post_json(self, url, payload):
            return GubaHttpResult(url=url, status_code=404, text="")

    state = StateStore(tmp_path / "state.db")
    generator = GubaHotBoardsSeedGenerator(
        GubaSettings(enabled=True),
        CrawlSettings.model_validate(
            {
                "proxy": {
                    "enabled": True,
                    "provider": "kuaidaili",
                    "sources": ["guba"],
                }
            }
        ),
        state,
        client=DirectClient(),
    )
    monkeypatch.setattr(generator, "_get_proxy_client", lambda: ProxyClient())

    items = generator.generate(
        GubaHotBoardsGeneratorDefinition(name="guba-hot"),
        datetime(2026, 7, 21, 3, 0, tzinfo=UTC),
    )

    assert "BK1036" in [item.value for item in items]
    snapshot = state.get_guba_ranking("2026-07-21")
    assert {row["section"] for row in snapshot} == {
        "hot_stock",
        "hot_concept",
        "hot_theme",
    }


def test_manual_seed_generator_emits_tgb_board_codes() -> None:
    generator = ManualSeedGenerator()
    items = generator.generate(
        ManualGeneratorDefinition(name="manual", tgb_board_codes=["jinghua", "sz000938"]),
        datetime.now(UTC),
    )
    assert [(item.kind, item.value) for item in items] == [
        ("tgb_board_code", "jinghua"),
        ("tgb_board_code", "sz000938"),
    ]


def test_seed_compiler_preserves_tgb_board_codes() -> None:
    compiled = SeedCompiler().compile(
        "tgb-daily",
        [
            GeneratedSeedItem(kind="tgb_board_code", value="jinghua"),
            GeneratedSeedItem(kind="tgb_board_code", value="sz000938"),
        ],
    )
    assert compiled.tgb_board_codes == ["jinghua", "sz000938"]


def test_tgb_hot_boards_generator_emits_codes_and_snapshots(tmp_path: Path) -> None:
    from alphapulse.runtime.config import TgbSettings
    from alphapulse.sources.tgb.api import TgbHttpResult
    from alphapulse.seeds.catalog import TgbHotBoardsGeneratorDefinition
    from alphapulse.seeds.discovery import TgbHotBoardsSeedGenerator

    fixtures = Path(__file__).parent.parent / "fixtures" / "tgb"

    class FakeClient:
        def get(self, url, *, expect_marker=None):
            return TgbHttpResult(
                url=url, status_code=200,
                text=(fixtures / "home_rankings.html").read_text(encoding="utf-8"),
            )

    state = StateStore(tmp_path / "state.db")
    generator = TgbHotBoardsSeedGenerator(TgbSettings(enabled=True), None, state, client=FakeClient())
    definition = TgbHotBoardsGeneratorDefinition(name="tgb-hot", hot_stocks_limit=3)

    generated_at = datetime(2026, 7, 22, 3, 0, 0, tzinfo=UTC)  # 11:00 Beijing
    items = generator.generate(definition, generated_at)

    assert all(item.kind == "tgb_board_code" for item in items)
    codes = [item.value for item in items]
    # Always-on featured + general feeds, then discovered hot-stock boards.
    assert codes[:2] == ["jinghua", "zongban"]
    assert "sz000938" in codes

    snapshot = state.get_tgb_ranking("2026-07-22")
    by_section: dict[str, list[str]] = {}
    for row in snapshot:
        by_section.setdefault(row["section"], []).append(row["code"])
    assert by_section["featured"] == ["jinghua"]
    assert by_section["general"][0] == "zongban"
    assert "sz000938" in by_section["general"]


def test_guba_hot_boards_generator_respects_section_filter(tmp_path: Path) -> None:
    from alphapulse.runtime.config import GubaSettings
    from alphapulse.sources.guba.api import GubaHttpResult
    from alphapulse.seeds.catalog import GubaHotBoardsGeneratorDefinition
    from alphapulse.seeds.discovery import GubaHotBoardsSeedGenerator

    fixtures = Path(__file__).parent.parent / "fixtures" / "guba"

    class FakeClient:
        def _match(self, url: str) -> GubaHttpResult:
            table = {"stockrank": "rank_hot_stock.json", "clist": "rank_hot_concept.json", "HomePageListRead": "rank_hot_theme.js"}
            for marker, name in table.items():
                if marker in url:
                    return GubaHttpResult(url=url, status_code=200, text=(fixtures / name).read_text(encoding="utf-8"))
            return GubaHttpResult(url=url, status_code=404, text="")

        def get(self, url, *, expect_marker=None):
            return self._match(url)

        def post_json(self, url, payload):
            return self._match(url)

    state = StateStore(tmp_path / "state.db")
    generator = GubaHotBoardsSeedGenerator(GubaSettings(enabled=True), None, state, client=FakeClient())
    definition = GubaHotBoardsGeneratorDefinition(name="guba-hot", sections=["hot_stock"])

    items = generator.generate(definition, datetime(2026, 7, 21, 3, 0, 0, tzinfo=UTC))
    codes = {item.value for item in items}
    # Only stock codes; no concept/theme-member codes.
    assert "600584" in codes
    assert "BK1036" not in codes
    snapshot_sections = {row["section"] for row in state.get_guba_ranking("2026-07-21")}
    assert snapshot_sections == {"hot_stock"}
