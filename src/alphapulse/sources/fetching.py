from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib import parse, request
from urllib.parse import urlparse

from alphapulse.runtime.config import (
    CrawlSettings,
    CrawlProxyPoolSettings,
    CrawlStaticProxySettings,
    CrawlKuaidailiSettings,
    FetchMode,
    XueqiuSettings,
)
from alphapulse.runtime.proxy_metrics import ProxyMetricsStore


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

    def report_success(self, lease: ProxyLease) -> None: ...


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

    def report_success(self, lease: ProxyLease) -> None:
        del lease

    def _build_url(self, path: str, query: str = "") -> str:
        base = self.settings.base_url.rstrip("/")
        url = f"{base}{path}"
        if query:
            url = f"{url}?{query}"
        return url


class StaticListProxyProvider:
    """Rotates round-robin over a fixed proxy list (e.g. local xray tunnels).

    A proxy reported bad sits out ``cooldown_seconds`` before re-entering the
    rotation; if every proxy is cooling down, the one closest to recovery is
    used anyway so a small pool never stalls the crawl.
    """

    provider_name = "static_list"

    def __init__(self, settings: CrawlStaticProxySettings) -> None:
        self.settings = settings
        self._urls = [_proxy_url(url) for url in settings.urls]
        self._benched_until: dict[str, float] = {}
        self._next_index = 0
        self._lock = threading.Lock()

    def acquire(self) -> ProxyLease | None:
        with self._lock:
            if not self._urls:
                return None
            now = time.monotonic()
            for _ in range(len(self._urls)):
                url = self._urls[self._next_index % len(self._urls)]
                self._next_index += 1
                if self._benched_until.get(url, 0.0) <= now:
                    return self._lease(url)
            return self._lease(min(self._urls, key=lambda url: self._benched_until.get(url, 0.0)))

    def report_bad(self, lease: ProxyLease, reason: str) -> None:
        del reason
        with self._lock:
            self._benched_until[lease.proxy_url] = (
                time.monotonic() + self.settings.cooldown_seconds
            )

    def _lease(self, url: str) -> ProxyLease:
        return ProxyLease(
            proxy_url=url,
            delete_key=_proxy_delete_key(url),
            provider_name=self.provider_name,
        )


class KuaidailiProxyProvider:
    """Caches short-lived private proxies extracted from Kuaidaili GetDPS."""

    provider_name = "kuaidaili"

    def __init__(self, settings: CrawlKuaidailiSettings) -> None:
        self.settings = settings
        self.metrics = ProxyMetricsStore(settings.metrics_path)
        self._urls: list[str] = []
        self._expires_at: dict[str, float] = {}
        self._benched_until: dict[str, float] = {}
        self._failure_streaks: dict[str, int] = {}
        self._next_index = 0
        self._lock = threading.Lock()

    def acquire(self) -> ProxyLease | None:
        with self._lock:
            now = time.monotonic()
            self._prune_expired(now)
            if len(self._available(now)) <= self.settings.low_watermark:
                self._refresh(now)
            available = set(self._available(now))
            if not available:
                self.metrics.record_pool_empty(self.provider_name)
                return None
            for _ in range(len(self._urls)):
                url = self._urls[self._next_index % len(self._urls)]
                self._next_index += 1
                if url in available:
                    lease = self._lease(url)
                    self.metrics.record_acquire(self.provider_name, lease.proxy_url)
                    return lease
            self.metrics.record_pool_empty(self.provider_name)
            return None

    def report_bad(self, lease: ProxyLease, reason: str) -> None:
        with self._lock:
            streak = self._failure_streaks.get(lease.proxy_url, 0) + 1
            self._failure_streaks[lease.proxy_url] = streak
            should_bench = self._is_hard_failure(reason) or (
                streak >= self.settings.failure_threshold
            )
            benched_until = None
            if should_bench:
                self._benched_until[lease.proxy_url] = (
                    time.monotonic() + self.settings.cooldown_seconds
                )
                self._failure_streaks.pop(lease.proxy_url, None)
                benched_until = datetime.now(UTC) + timedelta(
                    seconds=self.settings.cooldown_seconds
                )
            self.metrics.record_failure(
                self.provider_name,
                lease.proxy_url,
                reason=reason,
                benched_until=benched_until,
            )

    def report_success(self, lease: ProxyLease) -> None:
        with self._lock:
            self._failure_streaks.pop(lease.proxy_url, None)
        self.metrics.record_success(self.provider_name, lease.proxy_url)

    def _refresh(self, now: float) -> None:
        try:
            api_url = self.settings.api_url_file.read_text(encoding="utf-8").strip()
            if not api_url.startswith(("http://", "https://")):
                raise RuntimeError("Kuaidaili API URL file is empty or invalid")
            parts = parse.urlsplit(api_url)
            query = parse.parse_qsl(parts.query, keep_blank_values=True)
            query = [
                (key, str(self.settings.batch_size) if key == "num" else value)
                for key, value in query
            ]
            if not any(key == "num" for key, _ in query):
                query.append(("num", str(self.settings.batch_size)))
            request_url = parse.urlunsplit(
                (
                    parts.scheme,
                    parts.netloc,
                    parts.path,
                    parse.urlencode(query),
                    parts.fragment,
                )
            )
            with request.urlopen(
                request_url,
                timeout=self.settings.acquire_timeout_seconds,
            ) as response:
                payload = response.read().decode("utf-8", errors="replace")
            proxies = self._parse_proxies(payload)
            if not proxies:
                raise RuntimeError("Kuaidaili API returned no proxy addresses")
        except Exception as exc:
            self.metrics.record_api_error(
                self.provider_name,
                f"{type(exc).__name__}: {exc}",
            )
            raise
        expires_at = now + self.settings.lease_ttl_seconds
        expires_at_wall = datetime.now(UTC) + timedelta(
            seconds=self.settings.lease_ttl_seconds
        )
        proxy_urls = [_proxy_url(proxy) for proxy in proxies]
        self.metrics.record_batch(
            self.provider_name,
            proxy_urls,
            expires_at=expires_at_wall,
        )
        for proxy in proxies:
            url = _proxy_url(proxy)
            if url not in self._urls:
                self._urls.append(url)
            self._expires_at[url] = expires_at

    def _available(self, now: float) -> list[str]:
        return [
            url
            for url in self._urls
            if self._expires_at.get(url, 0.0) > now
            and self._benched_until.get(url, 0.0) <= now
        ]

    def _prune_expired(self, now: float) -> None:
        self._urls = [
            url for url in self._urls if self._expires_at.get(url, 0.0) > now
        ]
        live = set(self._urls)
        self._expires_at = {
            url: expires_at for url, expires_at in self._expires_at.items() if url in live
        }
        self._benched_until = {
            url: until for url, until in self._benched_until.items() if url in live
        }
        self._failure_streaks = {
            url: streak for url, streak in self._failure_streaks.items() if url in live
        }
        if self._urls:
            self._next_index %= len(self._urls)
        else:
            self._next_index = 0

    @staticmethod
    def _parse_proxies(payload: str) -> list[str]:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            candidates = re.split(r"[\s,;]+", payload)
        else:
            if isinstance(parsed, dict) and parsed.get("code") not in (None, 0):
                raise RuntimeError(
                    f"Kuaidaili API error code {parsed.get('code')}"
                )
            data = parsed.get("data") if isinstance(parsed, dict) else None
            entries = data.get("proxy_list") if isinstance(data, dict) else None
            candidates = list(entries or [])

        proxies: list[str] = []
        for entry in candidates:
            if isinstance(entry, dict):
                value = entry.get("proxy") or entry.get("server") or entry.get("ip_port")
                if value is None and (entry.get("ip") or entry.get("host")) and entry.get("port"):
                    value = f"{entry.get('ip') or entry.get('host')}:{entry['port']}"
                entry = value
            if entry is None:
                continue
            rendered = str(entry).strip()
            if re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}", rendered):
                proxies.append(rendered)
        return list(dict.fromkeys(proxies))

    def _lease(self, url: str) -> ProxyLease:
        return ProxyLease(
            proxy_url=url,
            delete_key=_proxy_delete_key(url),
            provider_name=self.provider_name,
        )

    @staticmethod
    def _is_hard_failure(reason: str) -> bool:
        lowered = reason.lower()
        return (
            (lowered.startswith("blocked:") and "soft_block" not in lowered)
            or bool(re.search(r"\bhttp (?:403|407|418|429)\b", lowered))
            or "proxy setup failed" in lowered
        )


class ScraplingClient:
    def __init__(self, source_settings: XueqiuSettings, crawl_settings: CrawlSettings) -> None:
        self.source_settings = source_settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = _build_proxy_provider(crawl_settings, source="xueqiu")

    def fetch(self, url: str) -> FetchResult:
        attempts = self.crawl_settings.proxy.max_attempts if self.proxy_provider is not None else 1
        last_error: str | None = None

        for attempt in range(attempts):
            lease: ProxyLease | None = None
            proxy_url: str | None = None
            acquire_failed = False

            if self.proxy_provider is not None:
                try:
                    lease = self.proxy_provider.acquire()
                except Exception as exc:
                    last_error = f"Failed to acquire proxy: {exc}"
                    acquire_failed = not self.crawl_settings.proxy.fail_open
                else:
                    if lease is None and not self.crawl_settings.proxy.fail_open:
                        last_error = "No proxy available from proxy provider"
                        acquire_failed = True
                    elif lease is not None:
                        proxy_url = lease.proxy_url

            if acquire_failed:
                if attempt + 1 < attempts:
                    continue
                return self._error_result(url, last_error or "Failed to acquire proxy")

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


def _build_proxy_provider(
    crawl_settings: CrawlSettings, source: str | None = None
) -> ProxyProvider | None:
    proxy = crawl_settings.proxy
    if not proxy.enabled:
        return None
    if source is not None and proxy.sources and source not in proxy.sources:
        return None
    if proxy.provider == "proxy_pool":
        return ProxyPoolProvider(crawl_settings.proxy_pool)
    if proxy.provider == "static_list":
        return StaticListProxyProvider(crawl_settings.static_proxies)
    if proxy.provider == "kuaidaili":
        return KuaidailiProxyProvider(crawl_settings.kuaidaili)
    return None
