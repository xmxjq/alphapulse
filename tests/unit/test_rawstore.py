from __future__ import annotations

import gzip
import os
from datetime import UTC, datetime, timedelta

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

def _ago(days: float) -> datetime:
    return datetime.now(UTC) - timedelta(days=days)


def _set_age(path, days: float) -> None:
    stamp = _ago(days).timestamp()
    os.utime(path, (stamp, stamp))


def _blob_path(tmp_path, sha: str):
    return tmp_path / "raw" / "blobs" / sha[:2] / f"{sha}.gz"


def test_prune_drops_expired_rows_and_orphan_blobs(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    stale_sha = store.save(_record(fetched_at=_ago(40)), "stale body")
    fresh_sha = store.save(
        _record(url="https://guba.eastmoney.com/list,600519_2.html", fetched_at=_ago(1)),
        "fresh body",
    )
    assert stale_sha is not None and fresh_sha is not None
    stale_blob = _blob_path(tmp_path, stale_sha)
    _set_age(stale_blob, 40)

    stats = store.prune(retention_days=7, fetch_log_retention_days=30)

    assert stats.skipped is False
    assert stats.fetch_log_rows_deleted == 1
    assert stats.blobs_deleted == 1
    assert stats.bytes_deleted > 0
    assert not stale_blob.exists()
    assert store.load_body(fresh_sha) == b"fresh body"


def test_prune_keeps_log_row_after_body_expires(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    sha = store.save(_record(fetched_at=_ago(3)), "kept body")
    assert sha is not None
    blob = _blob_path(tmp_path, sha)
    _set_age(blob, 30)

    stats = store.prune(retention_days=7, fetch_log_retention_days=90)

    assert stats.fetch_log_rows_deleted == 0
    assert stats.blobs_deleted == 1
    assert not blob.exists()
    with store.connection() as conn:
        rows = conn.execute("SELECT content_sha256 FROM fetch_log").fetchall()
    assert [row["content_sha256"] for row in rows] == [sha]


def test_prune_dry_run_deletes_nothing(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    sha = store.save(_record(fetched_at=_ago(40)), "stale body")
    assert sha is not None
    blob = _blob_path(tmp_path, sha)
    _set_age(blob, 40)

    stats = store.prune(retention_days=7, fetch_log_retention_days=30, dry_run=True)

    assert stats.fetch_log_rows_deleted == 1
    assert stats.blobs_deleted == 1
    assert blob.exists()
    with store.connection() as conn:
        remaining = conn.execute("SELECT COUNT(*) AS total FROM fetch_log").fetchone()
    assert remaining["total"] == 1

    real = store.prune(retention_days=7, fetch_log_retention_days=30)
    assert real.skipped is False
    assert real.fetch_log_rows_deleted == 1
    assert not blob.exists()


def test_prune_skips_within_min_interval(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    sha = store.save(_record(fetched_at=_ago(40)), "stale body")
    assert sha is not None
    blob = _blob_path(tmp_path, sha)
    _set_age(blob, 40)

    first = store.prune(retention_days=7, fetch_log_retention_days=30)
    assert first.skipped is False

    second = store.prune(retention_days=7, fetch_log_retention_days=30)
    assert second.skipped is True
    assert second.blobs_deleted == 0
    assert second.fetch_log_rows_deleted == 0

    forced = store.prune(
        retention_days=7,
        fetch_log_retention_days=30,
        min_interval=timedelta(),
    )
    assert forced.skipped is False


def test_prune_removes_emptied_shard_dir(tmp_path) -> None:
    store = RawResponseStore(tmp_path / "raw")
    sha = store.save(_record(fetched_at=_ago(40)), "only body")
    assert sha is not None
    blob = _blob_path(tmp_path, sha)
    _set_age(blob, 40)

    stats = store.prune(retention_days=7, fetch_log_retention_days=30)

    assert stats.shards_removed == 1
    assert not blob.exists()
    assert not blob.parent.exists()
