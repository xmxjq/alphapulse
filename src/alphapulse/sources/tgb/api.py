from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from urllib import error, request

from alphapulse.runtime.config import CrawlSettings, TgbSettings
from alphapulse.sources.fetching import ProxyLease, _build_proxy_provider


logger = logging.getLogger(__name__)


REQUEST_BACKOFF_MULTIPLIER_MAX = 16.0
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
)

BLOCKED_KINDS = {"http_403", "http_429", "captcha", "login_redirect", "soft_block"}


def classify_block(status_code: int, text: str, final_url: str) -> str | None:
    if status_code == 403:
        return "http_403"
    if status_code == 429:
        return "http_429"
    if status_code >= 500:
        return "http_5xx"
    lowered_url = final_url.lower()
    if "sso.tgb.cn" in lowered_url or "/login" in lowered_url:
        return "login_redirect"
    if len(text) < 5000 and ("验证码" in text or "captcha" in lowered_url):
        return "captcha"
    return None


@dataclass
class TgbHttpResult:
    url: str
    status_code: int
    text: str
    duration_ms: int | None = None
    error_message: str | None = None
    blocked: bool = False
    block_kind: str | None = None


class TgbClient:
    """Minimal GET client for tgb.cn (server-rendered HTML), copied from GubaClient.

    tgb.cn has no JSON list/reply APIs, so this client only needs GET plus the same
    retry/backoff, proxy leasing, soft-block detection and gb18030 decode fallback
    guba uses.
    """

    def __init__(self, settings: TgbSettings, crawl_settings: CrawlSettings) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = _build_proxy_provider(crawl_settings, source="tgb")
        self._backoff_multiplier = 1.0

    def get(self, url: str, *, expect_marker: str | None = None) -> TgbHttpResult:
        attempts = max(1, self.settings.max_retries)
        last_result: TgbHttpResult | None = None

        for attempt in range(attempts):
            lease: ProxyLease | None = None
            proxy_url: str | None = None
            was_rate_limited = attempt > 0

            if self.proxy_provider is not None:
                try:
                    lease = self.proxy_provider.acquire()
                except Exception as exc:
                    if not self.crawl_settings.proxy.fail_open:
                        return TgbHttpResult(
                            url=url,
                            status_code=0,
                            text="",
                            error_message=f"Failed to acquire proxy: {exc}",
                        )
                if lease is not None:
                    proxy_url = lease.proxy_url

            started = time.monotonic()
            try:
                self._adaptive_sleep(was_rate_limited=was_rate_limited)
                status_code, text, final_url = self._dispatch(url, proxy_url)
            except error.HTTPError as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                body = exc.read().decode("utf-8", errors="ignore")
                block_kind = classify_block(exc.code, body, url)
                blocked = block_kind in BLOCKED_KINDS
                last_result = TgbHttpResult(
                    url=url,
                    status_code=exc.code,
                    text=body,
                    duration_ms=duration_ms,
                    error_message=f"HTTP {exc.code}",
                    blocked=blocked,
                    block_kind=block_kind,
                )
                logger.warning(
                    "Tgb HTTP error",
                    extra={
                        "event": "tgb_http_error",
                        "extra_data": {
                            "url": url,
                            "status_code": exc.code,
                            "block_kind": block_kind,
                            "attempt": attempt + 1,
                        },
                    },
                )
                if lease is not None and blocked:
                    self._report_bad_proxy(lease, f"HTTP {exc.code}")
                if blocked and attempt + 1 < attempts:
                    continue
                return last_result
            except (error.URLError, TimeoutError, OSError) as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                last_result = TgbHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    duration_ms=duration_ms,
                    error_message=str(exc),
                )
                logger.warning(
                    "Tgb request error",
                    extra={
                        "event": "tgb_request_error",
                        "extra_data": {"url": url, "error": str(exc), "attempt": attempt + 1},
                    },
                )
                if lease is not None:
                    self._report_bad_proxy(lease, str(exc))
                if attempt + 1 < attempts:
                    time.sleep(2**attempt)
                    continue
                return last_result

            duration_ms = int((time.monotonic() - started) * 1000)
            block_kind = classify_block(status_code, text, final_url)
            if (
                block_kind is None
                and expect_marker is not None
                and expect_marker not in text
            ):
                # An HTTP 200 missing its expected DOM marker is a WAF/soft-block page
                # classify_block cannot recognize; flag it blocked to engage retries,
                # backoff and proxy rotation instead of surfacing as a parse error.
                block_kind = "soft_block"
            blocked = block_kind in BLOCKED_KINDS
            result = TgbHttpResult(
                url=final_url,
                status_code=status_code,
                text=text,
                duration_ms=duration_ms,
                blocked=blocked,
                block_kind=block_kind,
                error_message=f"Blocked response ({block_kind})" if blocked else None,
            )
            if blocked:
                logger.warning(
                    "Tgb blocked response",
                    extra={
                        "event": "tgb_blocked",
                        "extra_data": {
                            "url": url,
                            "final_url": final_url,
                            "block_kind": block_kind,
                            "attempt": attempt + 1,
                        },
                    },
                )
                if lease is not None:
                    self._report_bad_proxy(lease, f"blocked: {block_kind}")
                last_result = result
                if attempt + 1 < attempts:
                    continue
            return result

        return last_result or TgbHttpResult(url=url, status_code=0, text="", error_message="Request failed")

    def _dispatch(self, url: str, proxy_url: str | None) -> tuple[int, str, str]:
        opener = request.build_opener()
        if proxy_url is not None:
            opener = request.build_opener(
                request.ProxyHandler({"http": proxy_url, "https": proxy_url})
            )
        req = request.Request(url, headers=self._headers(), method="GET")
        with opener.open(req, timeout=self.crawl_settings.request_timeout_seconds) as response:
            raw = response.read()
            charset = response.headers.get_content_charset()
            text = self._decode(raw, charset)
            return response.status, text, response.geturl()

    @staticmethod
    def _decode(raw: bytes, charset: str | None) -> str:
        if charset:
            try:
                return raw.decode(charset, errors="replace")
            except LookupError:
                pass
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("gb18030", errors="replace")

    def _headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": self.settings.user_agent or DEFAULT_USER_AGENT,
            "Referer": f"{str(self.settings.base_url).rstrip('/')}/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if self.settings.cookies:
            headers["Cookie"] = "; ".join(
                f"{name}={value}" for name, value in self.settings.cookies.items()
            )
        return headers

    def _adaptive_sleep(self, *, was_rate_limited: bool) -> None:
        if was_rate_limited:
            self._backoff_multiplier = min(self._backoff_multiplier * 2, REQUEST_BACKOFF_MULTIPLIER_MAX)
        else:
            self._backoff_multiplier = max(self._backoff_multiplier * 0.5, 1.0)
        base = random.uniform(
            self.settings.request_interval_min_seconds,
            self.settings.request_interval_max_seconds,
        )
        time.sleep(base * self._backoff_multiplier)

    def _report_bad_proxy(self, lease: ProxyLease, reason: str) -> None:
        if not self.crawl_settings.proxy_pool.report_bad_on_block:
            return
        try:
            self.proxy_provider.report_bad(lease, reason)
        except Exception:
            return
