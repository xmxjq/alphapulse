from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Protocol
from urllib import parse, request
from urllib.parse import urlparse

from alphapulse.runtime.config import (
    CrawlKuaidailiSettings,
    CrawlProxyPoolSettings,
    CrawlSettings,
    CrawlStaticProxySettings,
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
    cohort: str | None = None
    channel: str | None = None


@dataclass(frozen=True)
class ExtractedProxy:
    address: str
    ttl_seconds: int | None = None


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


class KuaidailiProxyPool:
    """Shares paid proxy extraction while isolating health by source."""

    provider_name = "kuaidaili"

    def __init__(self, settings: CrawlKuaidailiSettings) -> None:
        self.settings = settings
        self.metrics = ProxyMetricsStore(settings.metrics_path)
        self._urls: list[str] = []
        self._expires_at: dict[str, float] = {}
        self._benched_until: dict[tuple[str, str | None], float] = {}
        self._failure_streaks: dict[tuple[str, str | None], int] = {}
        self._next_indexes: dict[str | None, int] = {}
        self._experiment_roles: dict[str, str] = {}
        self._dual_channel_indexes: dict[str, int] = {}
        self._lock = threading.Lock()

    def provider(
        self,
        source: str | None = None,
        *,
        experiment_active: Callable[[], bool] | None = None,
    ) -> KuaidailiProxyProvider:
        return KuaidailiProxyProvider(
            self.settings,
            source=source,
            pool=self,
            experiment_active=experiment_active,
        )

    def acquire(
        self,
        source: str | None = None,
        *,
        experiment: bool = False,
    ) -> ProxyLease | None:
        with self._lock:
            now = time.monotonic()
            self._prune_expired(now)
            if len(self._available(now, source)) <= self.settings.low_watermark:
                self._refresh(source)
            available = set(self._available(now, source))
            if experiment and not self._experiment_batch_complete():
                metric_source = (
                    f"{source}_ab_incomplete_batch" if source else source
                )
                self.metrics.record_pool_empty(
                    self.provider_name,
                    source=metric_source,
                )
                return None
            if not available:
                self.metrics.record_pool_empty(
                    self.provider_name, source=source
                )
                return None
            next_index = self._next_indexes.get(source, 0)
            for _ in range(len(self._urls)):
                url = self._urls[next_index % len(self._urls)]
                next_index += 1
                if url in available:
                    self._next_indexes[source] = next_index
                    lease = self._experiment_lease(url) if experiment else self._lease(url)
                    self.metrics.record_acquire(
                        self.provider_name,
                        lease.proxy_url,
                        source=self._metrics_source(source, lease),
                    )
                    return lease
            self.metrics.record_pool_empty(
                self.provider_name, source=source
            )
            return None

    def report_bad(
        self,
        lease: ProxyLease,
        reason: str,
        source: str | None = None,
    ) -> None:
        with self._lock:
            bench_key = (lease.proxy_url, source)
            streak_key = self._failure_streak_key(lease, source)
            streak = self._failure_streaks.get(streak_key, 0) + 1
            self._failure_streaks[streak_key] = streak
            should_bench = self._is_hard_failure(reason) or (
                streak >= self.settings.failure_threshold
            )
            benched_until = None
            if should_bench:
                now = time.monotonic()
                remaining_lifetime = max(
                    0.0,
                    self._expires_at.get(lease.proxy_url, now) - now,
                )
                bench_seconds = max(
                    float(self.settings.cooldown_seconds),
                    remaining_lifetime,
                )
                self._benched_until[bench_key] = now + bench_seconds
                self._failure_streaks.pop(streak_key, None)
                benched_until = datetime.now(UTC) + timedelta(
                    seconds=bench_seconds
                )
            self.metrics.record_failure(
                self.provider_name,
                lease.proxy_url,
                reason=reason,
                benched_until=benched_until,
                source=self._metrics_source(source, lease),
            )

    def report_success(
        self,
        lease: ProxyLease,
        source: str | None = None,
    ) -> None:
        with self._lock:
            self._failure_streaks.pop(
                self._failure_streak_key(lease, source),
                None,
            )
        self.metrics.record_success(
            self.provider_name,
            lease.proxy_url,
            source=self._metrics_source(source, lease),
        )

    def _refresh(self, source: str | None) -> None:
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
            if self.settings.use_api_expiry:
                query = [
                    (key, "1" if key == "f_et" else value)
                    for key, value in query
                ]
                if not any(key == "f_et" for key, _ in query):
                    query.append(("f_et", "1"))
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
            extracted = self._parse_extracted_proxies(payload)
            if not extracted:
                raise RuntimeError("Kuaidaili API returned no proxy addresses")
        except Exception as exc:
            self.metrics.record_api_error(
                self.provider_name,
                f"{type(exc).__name__}: {exc}",
                source=source,
            )
            raise

        refreshed_at = time.monotonic()
        refreshed_at_wall = datetime.now(UTC)
        expiry_by_url: dict[str, float] = {}
        expiry_wall_by_url: dict[str, datetime] = {}
        reported_ttls: list[int] = []
        effective_ttls: list[int] = []
        api_ttl_count = 0
        fallback_ttl_count = 0
        for item in extracted:
            if self.settings.use_api_expiry and item.ttl_seconds is not None:
                reported_ttls.append(item.ttl_seconds)
                ttl_seconds = item.ttl_seconds - self.settings.expiry_safety_seconds
                if ttl_seconds <= 0:
                    continue
                api_ttl_count += 1
            else:
                ttl_seconds = self.settings.lease_ttl_seconds
                fallback_ttl_count += 1
            url = _proxy_url(item.address)
            effective_ttls.append(ttl_seconds)
            expiry_by_url[url] = refreshed_at + ttl_seconds
            expiry_wall_by_url[url] = refreshed_at_wall + timedelta(
                seconds=ttl_seconds
            )

        if not expiry_by_url:
            reason = "Kuaidaili API returned no proxy with usable lifetime"
            self.metrics.record_api_error(
                self.provider_name,
                reason,
                source=source,
            )
            raise RuntimeError(reason)

        proxy_urls = list(expiry_by_url)
        for index, url in enumerate(proxy_urls):
            self._experiment_roles[url] = (
                "dual" if len(proxy_urls) >= 2 and index % 2 == 0 else "control"
            )
            self._dual_channel_indexes.pop(url, None)
        self.metrics.record_batch(
            self.provider_name,
            proxy_urls,
            expires_at_by_proxy=expiry_wall_by_url,
            source=source,
            detail={
                "ttl_mode": (
                    "api"
                    if api_ttl_count and not fallback_ttl_count
                    else "mixed"
                    if api_ttl_count
                    else "fallback"
                ),
                "reported_ttl_min": min(reported_ttls) if reported_ttls else None,
                "reported_ttl_max": max(reported_ttls) if reported_ttls else None,
                "effective_ttl_min": min(effective_ttls),
                "effective_ttl_max": max(effective_ttls),
                "expiry_safety_seconds": (
                    self.settings.expiry_safety_seconds
                    if api_ttl_count
                    else 0
                ),
            },
        )
        for url, expires_at in expiry_by_url.items():
            if url not in self._urls:
                self._urls.append(url)
            self._expires_at[url] = expires_at

    def _available(self, now: float, source: str | None) -> list[str]:
        return [
            url
            for url in self._urls
            if self._expires_at.get(url, 0.0) > now
            and self._benched_until.get((url, source), 0.0) <= now
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
            key: until for key, until in self._benched_until.items() if key[0] in live
        }
        self._failure_streaks = {
            key: streak
            for key, streak in self._failure_streaks.items()
            if key[0] in live
        }
        self._experiment_roles = {
            url: role for url, role in self._experiment_roles.items() if url in live
        }
        self._dual_channel_indexes = {
            url: index for url, index in self._dual_channel_indexes.items() if url in live
        }
        if self._urls:
            self._next_indexes = {
                source: index % len(self._urls)
                for source, index in self._next_indexes.items()
            }
        else:
            self._next_indexes.clear()

    @staticmethod
    def _parse_extracted_proxies(payload: str) -> list[ExtractedProxy]:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            candidates: list[Any] = [payload]
        else:
            if isinstance(parsed, dict) and parsed.get("code") not in (None, 0):
                raise RuntimeError(
                    f"Kuaidaili API error code {parsed.get('code')}"
                )
            data = parsed.get("data") if isinstance(parsed, dict) else None
            entries = data.get("proxy_list") if isinstance(data, dict) else None
            candidates = list(entries or [])

        proxies: dict[str, int | None] = {}

        def add_proxy(address: str, ttl_seconds: int | None) -> None:
            current = proxies.get(address)
            if current is None or (
                ttl_seconds is not None and ttl_seconds > current
            ):
                proxies[address] = ttl_seconds

        for entry in candidates:
            ttl_seconds: int | None = None
            if isinstance(entry, dict):
                value = entry.get("proxy") or entry.get("server") or entry.get("ip_port")
                if value is None and (entry.get("ip") or entry.get("host")) and entry.get("port"):
                    value = f"{entry.get('ip') or entry.get('host')}:{entry['port']}"
                raw_ttl = (
                    entry.get("ttl")
                    or entry.get("ttl_seconds")
                    or entry.get("valid_time")
                    or entry.get("expire_seconds")
                )
                try:
                    ttl_seconds = int(raw_ttl) if raw_ttl is not None else None
                except (TypeError, ValueError):
                    ttl_seconds = None
                entry = value
            if entry is None:
                continue
            rendered = str(entry).strip()
            for match in re.finditer(
                r"(?P<proxy>(?:\d{1,3}\.){3}\d{1,3}:\d{1,5})"
                r"(?:,(?P<ttl>\d+))?",
                rendered,
            ):
                matched_ttl = match.group("ttl")
                add_proxy(
                    match.group("proxy"),
                    int(matched_ttl) if matched_ttl is not None else ttl_seconds,
                )
        return [
            ExtractedProxy(address=address, ttl_seconds=ttl_seconds)
            for address, ttl_seconds in proxies.items()
        ]

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

    def _experiment_lease(self, url: str) -> ProxyLease:
        cohort = self._experiment_roles.get(url, "control")
        channel = "desktop"
        if cohort == "dual":
            index = self._dual_channel_indexes.get(url, 0)
            channel = "desktop" if index % 2 == 0 else "mobile"
            self._dual_channel_indexes[url] = index + 1
        return ProxyLease(
            proxy_url=url,
            delete_key=_proxy_delete_key(url),
            provider_name=self.provider_name,
            cohort=cohort,
            channel=channel,
        )

    def _experiment_batch_complete(self) -> bool:
        live_roles = {
            self._experiment_roles[url]
            for url in self._urls
            if url in self._experiment_roles
        }
        return {"control", "dual"}.issubset(live_roles)

    @staticmethod
    def _failure_streak_key(
        lease: ProxyLease,
        source: str | None,
    ) -> tuple[str, str | None]:
        if source is None or lease.channel is None:
            return lease.proxy_url, source
        return lease.proxy_url, f"{source}:{lease.channel}"

    @staticmethod
    def _metrics_source(source: str | None, lease: ProxyLease) -> str | None:
        if source is None or lease.cohort is None:
            return source
        return f"{source}_ab_{lease.cohort}_{lease.channel or 'desktop'}"


class KuaidailiProxyProvider:
    """Source-scoped view over a short-lived Kuaidaili proxy pool."""

    provider_name = "kuaidaili"

    def __init__(
        self,
        settings: CrawlKuaidailiSettings,
        *,
        source: str | None = None,
        pool: KuaidailiProxyPool | None = None,
        experiment_active: Callable[[], bool] | None = None,
    ) -> None:
        self.settings = settings
        self.source = source
        self.pool = pool or KuaidailiProxyPool(settings)
        self.metrics = self.pool.metrics
        self.experiment_active = experiment_active

    def acquire(self) -> ProxyLease | None:
        return self.acquire_for_experiment(eligible=False)

    def acquire_for_experiment(self, *, eligible: bool) -> ProxyLease | None:
        return self.pool.acquire(
            self.source,
            experiment=bool(
                eligible
                and self.experiment_active
                and self.experiment_active()
            ),
        )

    def report_bad(self, lease: ProxyLease, reason: str) -> None:
        self.pool.report_bad(lease, reason, self.source)

    def report_success(self, lease: ProxyLease) -> None:
        self.pool.report_success(lease, self.source)


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
        return KuaidailiProxyProvider(crawl_settings.kuaidaili, source=source)
    return None
