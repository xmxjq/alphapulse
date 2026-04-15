from pathlib import Path

from alphapulse.runtime.config import load_settings
from alphapulse.seeds.catalog import SeedCatalogLoader


def test_load_settings_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    assert settings.storage.backend == "rqlite"
    assert settings.rqlite.url == "http://remote-rqlite.example.com:4001"
    assert settings.sources.xueqiu.seed_catalog_path.name == "seed_catalog.example.toml"
    assert settings.sources.xueqiu.seed_refresh_minutes == 60
    assert settings.sources.xueqiu.generated_seed_ttl_minutes == 1440


def test_load_seed_catalog_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    catalog = SeedCatalogLoader(settings.sources.xueqiu.seed_catalog_path).load()
    assert [item.name for item in catalog.logical_sets] == ["cn-core"]
    assert catalog.logical_sets[0].generators == ["cn-core-manual"]
