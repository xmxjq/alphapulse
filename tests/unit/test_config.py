from pathlib import Path

from alphapulse.runtime.config import load_settings


def test_load_settings_example() -> None:
    settings = load_settings(Path("settings.example.toml"))
    assert settings.storage.backend == "rqlite"
    assert settings.rqlite.url == "http://remote-rqlite.example.com:4001"
    assert settings.sources.xueqiu.seed_sets[0].stock_ids == ["SH600519", "SZ000858"]
