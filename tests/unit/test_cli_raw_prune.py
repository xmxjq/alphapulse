from __future__ import annotations

import argparse

import pytest

from alphapulse.cli.app import build_parser, main


def _parse(*args: str) -> argparse.Namespace:
    return build_parser().parse_args(["raw-prune", *args])


def test_raw_prune_defaults_to_settings() -> None:
    args = _parse()
    assert args.days is None
    assert args.fetch_log_days is None
    assert args.dry_run is False
    assert args.force is False


def test_raw_prune_accepts_inclusive_bounds() -> None:
    assert _parse("--days", "1").days == 1
    assert _parse("--days", "365").days == 365
    assert _parse("--fetch-log-days", "1").fetch_log_days == 1
    assert _parse("--fetch-log-days", "3650").fetch_log_days == 3650


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--days", "0"),
        ("--days", "-1"),
        ("--days", "366"),
        ("--days", "abc"),
        ("--fetch-log-days", "0"),
        ("--fetch-log-days", "3651"),
    ],
)
def test_raw_prune_rejects_invalid_ranges(flag: str, value: str) -> None:
    with pytest.raises(SystemExit) as excinfo:
        _parse(flag, value)
    assert excinfo.value.code == 2


def test_raw_prune_rejects_before_loading_runtime(monkeypatch) -> None:
    def fail(*args, **kwargs):
        raise AssertionError("runtime must not load for an invalid request")

    monkeypatch.setattr("alphapulse.cli.app._load_runtime", fail)
    with pytest.raises(SystemExit) as excinfo:
        main(["raw-prune", "--days", "0"])
    assert excinfo.value.code == 2
