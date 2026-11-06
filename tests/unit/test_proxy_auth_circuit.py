import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from alphapulse.runtime.config import CrawlKuaidailiSettings
from alphapulse.sources.fetching import KuaidailiProxyPool


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return b"198.51.100.1:8080,600\n198.51.100.2:8080,600"


@pytest.fixture
def paid_pool(tmp_path, monkeypatch):
    api = tmp_path / "api.txt"
    api.write_text("https://example.invalid/getdps?num=2", encoding="utf-8")
    settings = CrawlKuaidailiSettings(
        api_url_file=api, metrics_path=tmp_path / "metrics.db",
        batch_size=2, low_watermark=0, use_api_expiry=True,
    )
    calls = []

    def extract(*args, **kwargs):
        calls.append(1)
        return Response()

    monkeypatch.setattr("alphapulse.sources.fetching.request.urlopen", extract)
    return KuaidailiProxyPool(settings), calls


@pytest.mark.parametrize("reason", [
    "curl: (56) CONNECT tunnel failed, response 407",
    "HTTP 407", "HTTP Error 407", "blocked status=407",
    "status_code: 407", "Proxy Authentication Required",
])
def test_auth_failure_blocks_all_sources_and_extraction(paid_pool, reason):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, reason, "guba")
    for source in ("guba", "guba_mobile_primary", "jiuyan", "tgb", "hupu"):
        with pytest.raises(RuntimeError, match="authentication circuit is open"):
            pool.acquire(source)
    with pytest.raises(RuntimeError, match="authentication circuit is open"):
        pool._refresh("tgb")
    assert calls == [1]
    assert json.loads(pool.auth_block_path.read_text())['reason'] == 'proxy_auth_407'


def test_marker_blocks_new_pool_after_restart(paid_pool):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, "HTTP 407", "guba")
    restarted = KuaidailiProxyPool(pool.settings)
    with pytest.raises(RuntimeError, match="manual verification"):
        restarted.acquire("tgb", experiment=True)
    assert calls == [1]


def test_existing_marker_blocks_without_reading_api_credentials(paid_pool):
    pool, calls = paid_pool
    pool.auth_block_path.write_text("manual emergency hold", encoding="utf-8")
    pool.settings.api_url_file = pool.settings.api_url_file.with_name("does-not-exist")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("jiuyan")
    assert calls == []


def test_success_does_not_clear_auth_circuit(paid_pool):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, "HTTP 407", "guba")
    pool.report_success(lease, "tgb")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("tgb")
    assert calls == [1]


def test_concurrent_retries_never_extract_more_ips(paid_pool):
    pool, calls = paid_pool
    pool.report_bad(pool.acquire("guba"), "HTTP 407", "guba")

    def retry(_):
        with pytest.raises(RuntimeError, match="authentication circuit"):
            pool.acquire("jiuyan")

    with ThreadPoolExecutor(max_workers=8) as threads:
        list(threads.map(retry, range(64)))
    assert calls == [1]


@pytest.mark.parametrize("reason", ["HTTP 403", "HTTP 429", "HTTP 544", "curl: (28) timed out", "post id 407", "https://example.invalid/407"])
def test_non_auth_errors_preserve_existing_source_isolation(paid_pool, reason):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    pool.report_bad(lease, reason, "guba")
    assert pool.acquire("tgb") is not None
    assert not pool.auth_block_path.exists()
    assert calls == [1]


def test_memory_latch_survives_marker_write_failure(paid_pool, monkeypatch):
    pool, calls = paid_pool
    lease = pool.acquire("guba")
    original = type(pool.auth_block_path).open

    def fail_marker(path, *args, **kwargs):
        if path == pool.auth_block_path:
            raise PermissionError("fixture")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(type(pool.auth_block_path), "open", fail_marker)
    with pytest.raises(PermissionError):
        pool.report_bad(lease, "HTTP 407", "guba")
    with pytest.raises(RuntimeError, match="authentication circuit"):
        pool.acquire("tgb")
    assert calls == [1]
