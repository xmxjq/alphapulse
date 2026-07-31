from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Literal

from curl_cffi import requests as curl_requests

from alphapulse.runtime.agent_pool import (
    AgentJobFailed,
    AgentPoolClient,
    AgentPoolUnavailable,
)
from alphapulse.runtime.config import CrawlSettings, HupuSettings
from alphapulse.sources.fetching import ProxyLease, ProxyProvider, _build_proxy_provider


logger = logging.getLogger(__name__)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
BLOCKED_KINDS = {"http_403", "http_429", "captcha", "login_redirect", "soft_block"}
HupuTransport = Literal["auto", "agent", "existing"]


def classify_block(status_code: int, text: str, final_url: str) -> str | None:
    if status_code == 403:
        return "http_403"
    if status_code == 429:
        return "http_429"
    if status_code >= 500:
        return "http_5xx"
    lowered_url = final_url.lower()
    if "passport.hupu.com" in lowered_url or "/login" in lowered_url:
        return "login_redirect"
    if len(text) < 10_000 and any(
        marker in text.lower() for marker in ("captcha", "安全验证", "请输入验证码")
    ):
        return "captcha"
    return None


@dataclass
class HupuHttpResult:
    url: str
    status_code: int
    text: str
    duration_ms: int | None = None
    error_message: str | None = None
    blocked: bool = False
    block_kind: str | None = None


class HupuClient:
    def __init__(
        self,
        settings: HupuSettings,
        crawl_settings: CrawlSettings,
        *,
        proxy_provider: ProxyProvider | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = proxy_provider or _build_proxy_provider(
            crawl_settings, source="hupu"
        )
        self.agent_pool = (
            AgentPoolClient(crawl_settings.agent_pool)
            if crawl_settings.agent_pool.enabled
            and (
                not crawl_settings.agent_pool.sources
                or "hupu" in crawl_settings.agent_pool.sources
            )
            else None
        )
        self._backoff_multiplier = 1.0

    def get(
        self,
        url: str,
        *,
        expect_marker: str | None = None,
        transport: HupuTransport = "auto",
    ) -> HupuHttpResult:
        attempts = max(1, self.settings.max_retries)
        last_result: HupuHttpResult | None = None
        for attempt in range(attempts):
            self._adaptive_sleep(was_rate_limited=attempt > 0)
            lease: ProxyLease | None = None
            proxy_url: str | None = None
            agent_job_id: str | None = None
            remote_response = None
            remote_error: str | None = None

            if self.agent_pool is not None and transport != "existing":
                try:
                    remote_response = self.agent_pool.fetch(
                        source="hupu",
                        method="GET",
                        url=url,
                        headers=self._headers(include_cookies=False),
                        body=None,
                        timeout_seconds=self.crawl_settings.request_timeout_seconds,
                        priority=100,
                    )
                    agent_job_id = remote_response.job_id
                except (AgentPoolUnavailable, AgentJobFailed, ValueError) as exc:
                    remote_error = str(exc)
                    logger.info(
                        "Hupu remote agent unavailable; using existing transport",
                        extra={
                            "event": "hupu_agent_fallback",
                            "extra_data": {"url": url, "reason": remote_error},
                        },
                    )

            if remote_response is None:
                if (
                    self.agent_pool is not None
                    and transport != "existing"
                    and not self.crawl_settings.agent_pool.fallback_to_existing_transport
                ):
                    return HupuHttpResult(
                        url=url,
                        status_code=0,
                        text="",
                        error_message=remote_error or "No remote agent available",
                    )
                if self.proxy_provider is not None:
                    try:
                        lease = self.proxy_provider.acquire()
                    except Exception as exc:
                        if not self.crawl_settings.proxy.fail_open:
                            last_result = HupuHttpResult(
                                url=url,
                                status_code=0,
                                text="",
                                error_message=f"Failed to acquire proxy: {exc}",
                            )
                            continue
                    else:
                        if lease is not None:
                            proxy_url = lease.proxy_url
                        elif not self.crawl_settings.proxy.fail_open:
                            last_result = HupuHttpResult(
                                url=url,
                                status_code=0,
                                text="",
                                error_message="No proxy available from proxy provider",
                            )
                            continue

            started = time.monotonic()
            try:
                if remote_response is not None:
                    status_code = remote_response.status_code
                    text = remote_response.text
                    final_url = remote_response.final_url
                else:
                    status_code, text, final_url = self._dispatch(url, proxy_url)
            except OSError as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                last_result = HupuHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    duration_ms=duration_ms,
                    error_message=str(exc),
                )
                if lease is not None:
                    self._report_bad_proxy(lease, str(exc))
                if attempt + 1 < attempts:
                    time.sleep(2**attempt)
                    continue
                return last_result

            duration_ms = int((time.monotonic() - started) * 1000)
            if remote_response is not None and remote_response.duration_ms is not None:
                duration_ms = remote_response.duration_ms
            block_kind = classify_block(status_code, text, final_url)
            if block_kind is None and expect_marker and expect_marker not in text:
                block_kind = "soft_block"
            blocked = block_kind in BLOCKED_KINDS
            result = HupuHttpResult(
                url=final_url,
                status_code=status_code,
                text=text,
                duration_ms=duration_ms,
                blocked=blocked,
                block_kind=block_kind,
                error_message=f"Blocked response ({block_kind})" if blocked else None,
            )
            if blocked:
                if lease is not None:
                    self._report_bad_proxy(lease, f"blocked: {block_kind}")
                if agent_job_id is not None:
                    self.agent_pool.store.record_outcome(
                        agent_job_id, "blocked", f"blocked: {block_kind}"
                    )
                last_result = result
                if attempt + 1 < attempts:
                    continue
            elif lease is not None:
                self._report_success_proxy(lease)
            elif agent_job_id is not None:
                self.agent_pool.store.record_outcome(agent_job_id, "success")
            return result

        return last_result or HupuHttpResult(
            url=url, status_code=0, text="", error_message="Request failed"
        )

    def _dispatch(self, url: str, proxy_url: str | None) -> tuple[int, str, str]:
        try:
            response = curl_requests.get(
                url,
                headers=self._headers(),
                proxy=proxy_url,
                impersonate="chrome",
                timeout=self.crawl_settings.request_timeout_seconds,
                allow_redirects=True,
            )
        except Exception as exc:
            raise OSError(str(exc)) from exc
        return response.status_code, response.text, str(response.url)

    def _headers(self, *, include_cookies: bool = True) -> dict[str, str]:
        headers = {
            "User-Agent": self.settings.user_agent or DEFAULT_USER_AGENT,
            "Referer": f"{str(self.settings.base_url).rstrip('/')}/stock",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
        }
        if include_cookies and self.settings.cookies:
            headers["Cookie"] = "; ".join(
                f"{name}={value}" for name, value in self.settings.cookies.items()
            )
        return headers

    def _adaptive_sleep(self, *, was_rate_limited: bool) -> None:
        if was_rate_limited:
            self._backoff_multiplier = min(self._backoff_multiplier * 2, 16.0)
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

    def _report_success_proxy(self, lease: ProxyLease) -> None:
        try:
            self.proxy_provider.report_success(lease)
        except Exception:
            return
