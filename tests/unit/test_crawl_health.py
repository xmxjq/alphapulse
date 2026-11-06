from datetime import UTC, datetime, timedelta
import json
import sqlite3
from pathlib import Path

from alphapulse.runtime.config import Settings
from alphapulse.runtime.state import StateStore
from alphapulse.runtime.proxy_metrics import ProxyMetricsStore
from alphapulse.storage.rawstore import FetchRecord, RawResponseStore
from alphapulse.web.crawl_health import crawl_health


NOW = datetime(2026, 11, 6, 10, tzinfo=UTC)


def setup(tmp_path):
    s = Settings()
    s.crawl.state_path = tmp_path / "state.db"
    s.crawl.raw_store.root_path = tmp_path / "raw"
    s.crawl.raw_store.enabled = True
    s.crawl.proxy.enabled = True
    s.crawl.proxy.provider = "kuaidaili"
    s.crawl.kuaidaili.metrics_path = tmp_path / "proxy.db"
    for name in ("guba", "tgb", "jiuyan", "hupu", "bilibili", "xueqiu"):
        getattr(s.sources, name).enabled = name in {"guba", "jiuyan"}
    StateStore(s.crawl.state_path)
    ProxyMetricsStore(s.crawl.kuaidaili.metrics_path)
    raw = RawResponseStore(s.crawl.raw_store.root_path)
    return s, raw


def test_success_excludes_blocks_and_parser_failures(tmp_path):
    s, raw = setup(tmp_path)
    good = NOW - timedelta(minutes=8)
    for when, block, error in [(good, None, None), (NOW, "captcha", None), (NOW, None, "private-error")]:
        raw.save(FetchRecord("guba", "https://example.invalid/private", "GET", 200,
                             fetched_at=when, block_kind=block, parser_error=error), None)
    d = crawl_health(s, now=NOW)
    row = next(x for x in d["sources"] if x["name"] == "guba")
    assert row["last_success_at"] == good.isoformat()
    assert row["last_attempt_at"] == NOW.isoformat()
    assert "private" not in json.dumps(d)
    assert d["probe_errors"] == []


def test_missing_files_do_not_get_created(tmp_path):
    s, _ = setup(tmp_path)
    s.crawl.state_path = tmp_path / "missing-state.db"
    s.crawl.raw_store.root_path = tmp_path / "missing-raw"
    s.crawl.kuaidaili.metrics_path = tmp_path / "missing-proxy.db"
    before = set(tmp_path.rglob("*"))
    d = crawl_health(s, now=NOW, cycle_readable=False)
    assert set(tmp_path.rglob("*")) == before
    assert set(d["probe_errors"]) == {"state_store_unavailable", "fetch_store_unavailable",
                                       "proxy_store_unavailable", "cycle_store_unavailable"}


def test_auth_marker_and_shared_budgets(tmp_path):
    s, _ = setup(tmp_path)
    k = s.crawl.kuaidaili
    k.max_extracted_per_hour = 3
    k.max_extracted_per_day = 3
    k.max_extracted_without_success = 3
    marker = k.metrics_path.with_name(k.metrics_path.name + ".auth-blocked")
    marker.write_text("private-do-not-read")
    with sqlite3.connect(k.metrics_path) as conn:
        conn.execute("INSERT INTO proxy_events(occurred_at,provider,event_type,count) VALUES(?,?,?,?)",
                     (NOW.isoformat(), "kuaidaili", "extraction_reserved", 3))
    d = crawl_health(s, now=NOW)
    assert d["proxy"]["auth_blocked"] is True
    assert d["proxy"]["budget_guards"] == ["hourly", "daily", "no_success"]
    assert "private" not in json.dumps(d)


def test_pending_source_isolation_and_disabled_sources(tmp_path):
    s, _ = setup(tmp_path)
    with sqlite3.connect(s.crawl.state_path) as conn:
        conn.execute("INSERT INTO pending_tasks VALUES('a','guba','seed',1,0,?,'{}',?)",
                     (NOW.isoformat(), NOW.isoformat()))
    d = crawl_health(s, now=NOW, last_cycle_at=NOW)
    assert {x["name"]: x["pending_tasks"] for x in d["sources"]} == {"guba": 1, "jiuyan": 0}
    assert d["last_cycle_at"] == NOW.isoformat()


def test_old_reservations_and_success_reset(tmp_path):
    s, _ = setup(tmp_path)
    k = s.crawl.kuaidaili
    k.max_extracted_without_success = 1
    with sqlite3.connect(k.metrics_path) as conn:
        for stamp, event in [(NOW - timedelta(days=2), "extraction_reserved"),
                             (NOW, "request_success")]:
            conn.execute("INSERT INTO proxy_events(occurred_at,provider,event_type,count) VALUES(?,?,?,1)",
                         (stamp.isoformat(), "kuaidaili", event))
    assert crawl_health(s, now=NOW)["proxy"]["budget_guards"] == []


def test_rejected_batch_near_limit_is_visible(tmp_path):
    s, raw = setup(tmp_path)
    raw.save(FetchRecord("guba", "https://example.invalid", "GET", None, fetched_at=NOW,
                         meta={"reason": "Paid proxy hourly extraction budget exhausted"}), None)
    assert crawl_health(s, now=NOW)["proxy"]["budget_guards"] == ["hourly"]


def test_api_is_no_store_and_reader_failure_is_sanitized(tmp_path):
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from alphapulse.web.app import create_app
    s, _ = setup(tmp_path)
    s.crawl.agent_pool.db_path = tmp_path / "agents.db"

    def failed():
        raise RuntimeError("fake-private-error-do-not-return")

    reader = SimpleNamespace(latest_run=failed)
    with TestClient(create_app(s, queries=SimpleNamespace(reader=reader))) as client:
        response = client.get("/api/crawl-health")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "cycle_store_unavailable" in response.json()["probe_errors"]
    assert "fake-private-error" not in response.text
