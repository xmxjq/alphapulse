from pathlib import Path

from alphapulse.runtime.config import load_settings
from alphapulse.seeds.catalog import SeedCatalogLoader


def test_load_settings_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    assert settings.storage.backend == "rqlite"
    assert settings.rqlite.url == "http://remote-rqlite.example.com:4001"
    assert settings.crawl.proxy.enabled is False
    assert settings.crawl.proxy.provider is None
    assert settings.crawl.proxy_pool.base_url == "http://proxy_pool:5010"
    assert settings.sources.xueqiu.seed_catalog_path.name == "seed_catalog.example.toml"
    assert settings.sources.xueqiu.seed_refresh_minutes == 60
    assert settings.sources.xueqiu.generated_seed_ttl_minutes == 1440
    assert str(settings.sources.bilibili.api_base_url) == "https://api.bilibili.com/"
    assert settings.sources.bilibili.page_size == 30
    assert settings.sources.bilibili.max_pages == 1000
    assert settings.sources.bilibili.space_discovery_backend == "api"
    assert settings.sources.bilibili.space_discovery_interval_minutes == 60
    assert settings.sources.bilibili.space_discovery_max_videos == 50


def test_load_seed_catalog_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    catalog = SeedCatalogLoader(settings.sources.xueqiu.seed_catalog_path).load()
    assert [item.name for item in catalog.logical_sets] == ["cn-core"]
    assert catalog.logical_sets[0].generators == ["cn-core-manual"]
    manual = catalog.generator_map()["cn-core-manual"]
    assert manual.bilibili_video_targets == ["BV1xx411c7mu"]


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
