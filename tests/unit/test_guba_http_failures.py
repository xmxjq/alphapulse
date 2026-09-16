from io import BytesIO
from urllib.error import HTTPError

import pytest

from alphapulse.pipeline.contracts import CrawlTask
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.fetching import ProxyLease
from alphapulse.sources.guba.adapter import GubaAdapter
from alphapulse.sources.guba.api import GubaClient, GubaHttpResult


URL = "https://guba.eastmoney.com/news,600519,42.html"


class MetricsProxy:
    def __init__(self):
        self.successes = 0
        self.failures = []

    def acquire(self):
        return ProxyLease("http://127.0.0.1:9999", "fixture", "test")

    def report_bad(self, lease, reason):
        self.failures.append(reason)

    def report_success(self, lease):
        self.successes += 1


@pytest.mark.parametrize("status", [500, 502, 503, 544])
@pytest.mark.parametrize("raise_http_error", [False, True])
def test_server_failure_retries_without_false_proxy_success(monkeypatch, status, raise_http_error):
    proxy = MetricsProxy()
    client = GubaClient(GubaSettings(max_retries=2), CrawlSettings(), proxy_provider=proxy)
    monkeypatch.setattr(client, "_adaptive_sleep", lambda **kwargs: None)
    calls = []

    def dispatch(*args):
        calls.append(args)
        if len(calls) == 1:
            if raise_http_error:
                raise HTTPError(URL, status, "temporary", {}, BytesIO(b""))
            return status, "", URL
        return 200, "<script>var post_article={};</script>", URL

    monkeypatch.setattr(client, "_dispatch", dispatch)
    result = client.get(URL, expect_marker="var post_article")
    assert result.status_code == 200
    assert len(calls) == 2
    assert proxy.failures == [f"HTTP {status}"]
    assert proxy.successes == 1


def test_exhausted_544_is_failure_not_success_or_global_block(monkeypatch):
    proxy = MetricsProxy()
    client = GubaClient(GubaSettings(max_retries=2), CrawlSettings(), proxy_provider=proxy)
    monkeypatch.setattr(client, "_adaptive_sleep", lambda **kwargs: None)
    monkeypatch.setattr(client, "_dispatch", lambda *args: (544, "", URL))
    result = client.get(URL, expect_marker="var post_article")
    assert result.error_message == "HTTP 544"
    assert not result.blocked
    assert proxy.successes == 0
    assert proxy.failures == ["HTTP 544", "HTTP 544"]


def test_adapter_keeps_server_failure_retryable(monkeypatch):
    adapter = GubaAdapter(GubaSettings(enabled=True), CrawlSettings())
    monkeypatch.setattr(adapter.client, "get", lambda *args, **kwargs: GubaHttpResult(
        url=URL, status_code=544, text="", block_kind="http_5xx", error_message="HTTP 544",
    ))
    task = CrawlTask(source="guba", kind="fetch_post", url=URL, seed_name="test", metadata={"post_id": "42"})
    result = adapter.fetch_item(task)
    assert result.status_code is None
    assert result.errors == [f"Fetch failed for {URL}: HTTP 544"]
    assert not result.posts
    assert not adapter.is_circuit_open()
