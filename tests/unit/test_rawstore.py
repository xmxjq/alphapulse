from __future__ import annotations

import gzip
from datetime import UTC, datetime

from alphapulse.storage.rawstore import FetchRecord, RawResponseStore


def _record(**overrides):
    defaults = {
        "source": "guba",
        "url": "https://guba.eastmoney.com/list,600519.html",
        "method": "GET",
        "status_code": 200,
        "fetched_at": datetime(2026, 7, 15, 10, 0, 0, tzinfo=UTC),
        "duration_ms": 1200,
        "task_kind": "discover",
        "meta": {"board_code": "600519", "page": 1},
    }
    defaults.update(overrides)
    return FetchRecord(**defaults)


def test_save_and_load_roundtrip(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    sha = store.save(_record(), "<html>var article_list={};</html>")

    assert sha is not None
    assert store.load_body(sha) == b"<html>var article_list={};</html>"
    blob_path = tmp_path / "raw" / "blobs" / sha[:2] / f"{sha}.gz"
    assert blob_path.exists()
    assert gzip.decompress(blob_path.read_bytes()).startswith(b"<html>")


def test_identical_bodies_stored_once(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    sha_one = store.save(_record(), "same body")
    sha_two = store.save(_record(url="https://guba.eastmoney.com/list,600519_2.html"), "same body")

    assert sha_one == sha_two
    blob_files = list((tmp_path / "raw" / "blobs").rglob("*.gz"))
    assert len(blob_files) == 1

    with store.connection() as conn:
        rows = conn.execute("SELECT url, content_sha256 FROM fetch_log ORDER BY id").fetchall()
    assert len(rows) == 2
    assert {row["content_sha256"] for row in rows} == {sha_one}


def test_fetch_log_row_contents(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    store.save(
        _record(
            status_code=403,
            block_kind="http_403",
            parser_error="Could not parse article_list",
        ),
        None,
    )

    with store.connection() as conn:
        row = conn.execute("SELECT * FROM fetch_log").fetchone()
    assert row["source"] == "guba"
    assert row["method"] == "GET"
    assert row["task_kind"] == "discover"
    assert row["status_code"] == 403
    assert row["block_kind"] == "http_403"
    assert row["parser_error"] == "Could not parse article_list"
    assert row["content_sha256"] is None
    assert row["content_length"] is None
    assert '"board_code": "600519"' in row["meta_json"]
    assert row["fetched_at"] == "2026-07-15T10:00:00+00:00"


def test_uncompressed_mode(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw", compress=False)
    sha = store.save(_record(), b"\x00\x01binary")
    assert store.load_body(sha) == b"\x00\x01binary"
    assert (tmp_path / "raw" / "blobs" / sha[:2] / f"{sha}.bin").exists()


def test_load_missing_body_returns_none(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    assert store.load_body("0" * 64) is None
