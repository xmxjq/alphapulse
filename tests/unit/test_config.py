from pathlib import Path

from alphapulse.runtime.config import load_settings
from alphapulse.seeds.catalog import SeedCatalogLoader


def test_load_settings_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    assert settings.storage.backend == "rqlite"
    assert settings.rqlite.url == "http://remote-rqlite.example.com:4001"
    assert settings.mongo.uri == "mongodb://localhost:27017"
    assert settings.mongo.database == "alphapulse"
    assert settings.mongo.posts_collection == "posts"
    assert settings.crawl.proxy.enabled is False
    assert settings.crawl.proxy.provider is None
    assert settings.crawl.proxy_pool.base_url == "http://proxy_pool:5010"
    assert settings.crawl.kuaidaili.api_url_file.name == "kuaidaili-api-url.txt"
    assert settings.crawl.kuaidaili.metrics_path.name == "proxy-metrics.db"
    assert settings.crawl.kuaidaili.batch_size == 5
    assert settings.crawl.kuaidaili.low_watermark == 2
    assert settings.crawl.kuaidaili.lease_ttl_seconds == 600
    assert settings.crawl.kuaidaili.failure_threshold == 3
    assert settings.crawl.kuaidaili.share_across_sources is False
    assert settings.crawl.agent_pool.enabled is False
    assert settings.crawl.agent_pool.db_path.name == "agent-pool.db"
    assert settings.crawl.agent_pool.db_path.is_absolute()
    assert settings.crawl.agent_pool.sources == ["guba", "tgb", "jiuyan"]
    assert "www.tgb.cn" in settings.crawl.agent_pool.allowed_hosts
    assert "app.jiuyangongshe.com" in settings.crawl.agent_pool.allowed_hosts
    assert settings.crawl.agent_pool.queue_wait_seconds == 10
    assert settings.crawl.agent_pool.lease_seconds == 180
    assert settings.crawl.agent_pool.job_wait_seconds == 120
    assert settings.crawl.agent_pool.blocked_cooldown_seconds == 21600
    assert settings.crawl.agent_pool.max_response_bytes == 8_000_000
    assert settings.crawl.agent_pool.response_body_retention_hours == 24
    assert settings.crawl.agent_pool.job_metadata_retention_days == 30
    assert settings.sources.xueqiu.seed_catalog_path.name == "seed_catalog.example.toml"
    assert settings.sources.xueqiu.seed_refresh_minutes == 60
    assert settings.sources.xueqiu.generated_seed_ttl_minutes == 1440
    assert str(settings.sources.bilibili.api_base_url) == "https://api.bilibili.com/"
    assert settings.sources.bilibili.page_size == 30
    assert settings.sources.bilibili.max_pages == 1000
    assert settings.sources.bilibili.space_discovery_backend == "api"
    assert settings.sources.bilibili.space_discovery_interval_minutes == 60
    assert settings.sources.bilibili.space_discovery_max_videos == 50
    assert settings.sources.guba.enabled is False
    assert str(settings.sources.guba.base_url) == "https://guba.eastmoney.com/"
    assert settings.sources.guba.max_list_pages == 3
    assert settings.sources.guba.reply_page_size == 30
    assert settings.sources.guba.max_reply_pages == 1
    assert settings.sources.guba.fetch_comments is True
    assert settings.sources.guba.list_recrawl_minutes == 30
    assert settings.sources.guba.day_scoped is True
    assert settings.sources.guba.hot_boards_per_section == 12
    assert settings.sources.guba.request_interval_min_seconds == 2.0
    assert settings.sources.guba.request_interval_max_seconds == 6.0
    assert settings.sources.guba.concurrent_paid_requests == 1
    assert settings.sources.guba.concurrent_agent_requests == 4
    assert settings.sources.guba.max_retries == 3
    assert settings.sources.guba.block_cooldown_seconds == 21600
    assert settings.sources.guba.browser.enabled is False
    assert settings.sources.guba.browser.cdp_url == "http://guba_browser:9223"
    assert settings.sources.guba.browser.navigation_timeout_seconds == 60
    assert settings.sources.guba.browser.settle_timeout_seconds == 15
    assert settings.sources.guba.browser.request_interval_min_seconds == 30.0
    assert settings.sources.guba.browser.request_interval_max_seconds == 90.0
    assert settings.sources.guba.browser.max_posts_per_cycle == 40
    assert settings.sources.tgb.fixed_boards == {
        "sh000001": "上证指数",
        "sz399006": "创业板指",
        "sh000688": "科创50",
        "sh000016": "上证50",
    }
    assert settings.sources.jiuyan.fixed_targets == [
        "上证指数",
        "创业板指",
        "科创50",
        "上证50",
    ]
    assert settings.sources.jiuyan.hot_targets_limit == 10
    assert settings.sources.jiuyan.max_search_pages == 3
    assert settings.sources.jiuyan.fetch_comments is False
    assert settings.crawl.raw_store.enabled is False
    assert settings.crawl.raw_store.root_path.name == "raw"
    assert settings.crawl.raw_store.root_path.is_absolute()
    assert settings.crawl.raw_store.compress is True


def test_load_seed_catalog_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    catalog = SeedCatalogLoader(settings.sources.xueqiu.seed_catalog_path).load()
    assert [item.name for item in catalog.logical_sets] == [
        "cn-core",
        "tgb-daily",
        "jiuyan-daily",
    ]
    assert catalog.logical_sets[0].generators == ["cn-core-manual", "guba-hot"]
    manual = catalog.generator_map()["cn-core-manual"]
    assert manual.bilibili_video_targets == ["BV1xx411c7mu"]
    assert manual.guba_board_codes == []
    guba_hot = catalog.generator_map()["guba-hot"]
    assert guba_hot.type == "guba_hot_boards"
    assert guba_hot.sections == ["hot_stock", "hot_concept", "hot_theme"]
    tgb_hot = catalog.generator_map()["tgb-hot"]
    assert tgb_hot.type == "tgb_hot_boards"
    assert tgb_hot.include_featured and tgb_hot.include_general
    assert tgb_hot.include_fixed_boards
    jiuyan_hot = catalog.generator_map()["jiuyan-hot"]
    assert jiuyan_hot.type == "jiuyan_hot_targets"
    assert jiuyan_hot.include_fixed_targets


def test_load_settings_with_proxy_enabled(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.toml"
    config_path.write_text(
        """
[storage]
backend = "rqlite"

[rqlite]
url = "http://remote-rqlite.example.com:4001"

[crawl]
state_path = ".runtime/state.db"

[crawl.proxy]
enabled = true
provider = "proxy_pool"
max_attempts = 3
fail_open = false

[crawl.proxy_pool]
base_url = "http://proxy_pool:5010"
https_only = true
acquire_timeout_seconds = 5
report_bad_on_block = true
""".strip()
    )

    settings = load_settings(config_path)

    assert settings.crawl.proxy.enabled is True
    assert settings.crawl.proxy.provider == "proxy_pool"
    assert settings.crawl.proxy.max_attempts == 3
    assert settings.crawl.proxy_pool.acquire_timeout_seconds == 5
