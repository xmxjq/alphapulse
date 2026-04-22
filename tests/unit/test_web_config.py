from pathlib import Path

import pytest
from pydantic import ValidationError

from alphapulse.runtime.config import WebSettings, load_settings


def test_web_settings_defaults() -> None:
    settings = WebSettings()
    assert settings.host == "127.0.0.1"
    assert settings.port == 8000


def test_web_settings_rejects_invalid_port() -> None:
    with pytest.raises(ValidationError):
        WebSettings(port=0)
    with pytest.raises(ValidationError):
        WebSettings(port=70_000)


def test_load_settings_example_exposes_web_section() -> None:
    settings = load_settings(Path("settings.example.toml"))
    assert settings.web.host == "127.0.0.1"
    assert settings.web.port == 8000
