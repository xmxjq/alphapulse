from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol
from urllib import parse, request
from urllib.parse import urlparse

from alphapulse.runtime.config import CrawlSettings, CrawlProxyPoolSettings, FetchMode, XueqiuSettings


def _response_text(response: Any) -> str:
    if hasattr(response, "text"):
        text = response.text
        if callable(text):
            return text()
        rendered = str(text)
        if rendered:
            return rendered
    if hasattr(response, "body"):
        body = response.body
        if isinstance(body, bytes):
            decoded = body.decode("utf-8", errors="ignore")
            if decoded:
                return decoded
        elif body is not None:
            rendered = str(body)
            if rendered:
                return rendered
    if hasattr(response, "html_content"):
        html_content = response.html_content
        if callable(html_content):
            html_content = html_content()
        if html_content is not None:
            rendered = str(html_content)
            if rendered:
                return rendered
    return str(response)


def _browser_cookies(source_settings: XueqiuSettings) -> list[dict[str, Any]] | None:
    if not source_settings.cookies:
        return None
    hostname = urlparse(str(source_settings.base_url)).hostname or "xueqiu.com"
    return [
        {
            "name": name,
            "value": value,
            "domain": hostname,
            "path": "/",
        }
        for name, value in source_settings.cookies.items()
    ]


def _proxy_delete_key(raw_proxy: str) -> str:
    if "://" not in raw_proxy:
        return raw_proxy
    parsed = urlparse(raw_proxy)
    return parsed.netloc or raw_proxy


def _proxy_url(raw_proxy: str) -> str:
    if "://" in raw_proxy:
        return raw_proxy
    return f"http://{raw_proxy}"


def _is_likely_blocked_response(text: str, status_code: int) -> bool:
    if not text.strip():
        return True
    if status_code in {401, 403, 429, 503}:
        return True
    markers = ("aliyun_waf", "renderData", "_waf_", "captcha")
    lowered = text.lower()
    return any(marker in lowered for marker in markers)


@dataclass
class FetchResult:
    url: str
    status_code: int
    text: str
    headers: dict[str, Any]
    error_message: str | None = None
    proxy_url: str | None = None

    def json(self) -> dict[str, Any]:
        return json.loads(self.text)


@dataclass(frozen=True)
class ProxyLease:
    proxy_url: str
    delete_key: str
    provider_name: str


class ProxyProvider(Protocol):
    def acquire(self) -> ProxyLease | None: ...

    def report_bad(self, lease: ProxyLease, reason: str) -> None: ...


class ProxyPoolProvider:
    provider_name = "proxy_pool"

    def __init__(self, settings: CrawlProxyPoolSettings) -> None:
        self.settings = settings

    def acquire(self) -> ProxyLease | None:
        query = parse.urlencode({"type": "https"}) if self.settings.https_only else ""
        url = self._build_url("/get/", query)
        with request.urlopen(url, timeout=self.settings.acquire_timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        proxy = payload.get("proxy")
        if not proxy:
            return None
        return ProxyLease(
            proxy_url=_proxy_url(str(proxy)),
            delete_key=_proxy_delete_key(str(proxy)),
            provider_name=self.provider_name,
        )

    def report_bad(self, lease: ProxyLease, reason: str) -> None:
        del reason
        query = parse.urlencode({"proxy": lease.delete_key})
        url = self._build_url("/delete/", query)
        with request.urlopen(url, timeout=self.settings.acquire_timeout_seconds):
            return None

    def _build_url(self, path: str, query: str = "") -> str:
        base = self.settings.base_url.rstrip("/")
        url = f"{base}{path}"
        if query:
            url = f"{url}?{query}"
        return url


class ScraplingClient:
    def __init__(self, source_settings: XueqiuSettings, crawl_settings: CrawlSettings) -> None:
        self.source_settings = source_settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = _build_proxy_provider(crawl_settings)

    def fetch(self, url: str) -> FetchResult:
        attempts = self.crawl_settings.proxy.max_attempts if self.proxy_provider is not None else 1
        last_error: str | None = None

        for attempt in range(attempts):
            lease: ProxyLease | None = None
            proxy_url: str | None = None

            if self.proxy_provider is not None:
                try:
                    lease = self.proxy_provider.acquire()
                except Exception as exc:
                    if not self.crawl_settings.proxy.fail_open:
                        return self._error_result(url, f"Failed to acquire proxy: {exc}")
                    last_error = f"Failed to acquire proxy: {exc}"
                else:
                    if lease is None and not self.crawl_settings.proxy.fail_open:
                        return self._error_result(url, "No proxy available from proxy provider")
                    if lease is not None:
                        proxy_url = lease.proxy_url

            try:
                response = self._dispatch_fetch(url, proxy_url)
            except Exception as exc:
                last_error = str(exc)
                if lease is not None:
                    self._report_bad_proxy(lease, last_error)
                if attempt + 1 < attempts:
                    continue
                return self._error_result(url, last_error, proxy_url=proxy_url)

            result = self._build_result(response, url, proxy_url)
            if (
                lease is not None
                and self._should_retry_result(result)
                and attempt + 1 < attempts
            ):
                self._report_bad_proxy(lease, f"blocked status={result.status_code}")
                continue
            return result

        return self._error_result(url, last_error or "Fetch failed")

    def _dispatch_fetch(self, url: str, proxy_url: str | None) -> Any:
        mode: FetchMode = self.source_settings.fetch_mode
        browser_cookies = _browser_cookies(self.source_settings)
        if mode == "dynamic":
            from scrapling.fetchers import DynamicFetcher

            return DynamicFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
                timeout=self.crawl_settings.request_timeout_seconds * 1000,
                cookies=browser_cookies,
                extra_headers={"user-agent": self.crawl_settings.user_agent},
                proxy=proxy_url,
            )
        if mode == "stealth":
            from scrapling.fetchers import StealthyFetcher

            return StealthyFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
                timeout=self.crawl_settings.request_timeout_seconds * 1000,
                cookies=browser_cookies,
                extra_headers={"user-agent": self.crawl_settings.user_agent},
                proxy=proxy_url,
            )
        from scrapling.fetchers import Fetcher

        return Fetcher.get(
            url,
            timeout=self.crawl_settings.request_timeout_seconds,
            headers={"user-agent": self.crawl_settings.user_agent},
            cookies=self.source_settings.cookies,
            proxy=proxy_url,
        )

    def _build_result(self, response: Any, request_url: str, proxy_url: str | None) -> FetchResult:
        headers = dict(getattr(response, "headers", {}) or {})
        return FetchResult(
            url=getattr(response, "url", request_url),
            status_code=getattr(response, "status", getattr(response, "status_code", 0)),
            text=_response_text(response),
            headers=headers,
            proxy_url=proxy_url,
        )

    def _should_retry_result(self, result: FetchResult) -> bool:
        return _is_likely_blocked_response(result.text, result.status_code)

    def _report_bad_proxy(self, lease: ProxyLease, reason: str) -> None:
        if not self.crawl_settings.proxy_pool.report_bad_on_block:
            return
        try:
            self.proxy_provider.report_bad(lease, reason)
        except Exception:
            return

    @staticmethod
    def _error_result(url: str, error_message: str, proxy_url: str | None = None) -> FetchResult:
        return FetchResult(
            url=url,
            status_code=0,
            text="",
            headers={},
            error_message=error_message,
            proxy_url=proxy_url,
        )


def _build_proxy_provider(crawl_settings: CrawlSettings) -> ProxyProvider | None:
    if not crawl_settings.proxy.enabled:
        return None
    if crawl_settings.proxy.provider == "proxy_pool":
        return ProxyPoolProvider(crawl_settings.proxy_pool)
    return None
