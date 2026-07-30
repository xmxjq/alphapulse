from __future__ import annotations

import hashlib
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from curl_cffi import requests as curl_requests

from alphapulse.runtime.agent_pool import (
    AgentJobFailed,
    AgentPoolClient,
    AgentPoolUnavailable,
)
from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.fetching import ProxyLease, ProxyProvider, _build_proxy_provider

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
)
SIGNING_PREFIX = "Uu0KfOB8iUP69d3c:"
BLOCKED_KINDS = {"http_403", "http_429", "captcha", "login"}


def classify_block(status_code: int, text: str) -> str | None:
    if status_code == 403:
        return "http_403"
    if status_code == 429:
        return "http_429"
    if status_code >= 500:
        return "http_5xx"
    lowered = text.lower()
    if "captcha" in lowered or "验证码" in text:
        return "captcha"
    if '"errCode":"1"' in text and "登录" in text:
        return "login"
    return None


@dataclass
class JiuyanHttpResult:
    url: str
    status_code: int
    text: str
    duration_ms: int | None = None
    error_message: str | None = None
    blocked: bool = False
    block_kind: str | None = None

    def json(self) -> dict[str, Any] | None:
        try:
            payload = json.loads(self.text)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None


class JiuyanClient:
    def __init__(
        self,
        settings: JiuyanSettings,
        crawl_settings: CrawlSettings,
        *,
        proxy_provider: ProxyProvider | None = None,
    ) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = proxy_provider or _build_proxy_provider(
            crawl_settings, source="jiuyan"
        )
        self.agent_pool = (
            AgentPoolClient(crawl_settings.agent_pool)
            if crawl_settings.agent_pool.enabled
            and (
                not crawl_settings.agent_pool.sources
                or "jiuyan" in crawl_settings.agent_pool.sources
            )
            else None
        )
        self._page_times: dict[str, str] = {}

    def hot_rankings(self) -> JiuyanHttpResult:
        return self.post("/api/v1/article/rank-board", {})

    def search_articles(
        self, keyword: str, page: int, *, page_size: int = 15
    ) -> JiuyanHttpResult:
        return self.post(
            "/api/v2/article/search",
            {
                "back_garden": 0,
                "keyword": keyword,
                "order": 1,
                "limit": page_size,
                "start": page,
                "type": "1",
            },
        )

    def article_detail(self, article_id: str) -> JiuyanHttpResult:
        return self.post(
            f"/api/v2/article/detail?articleId={article_id}",
            {"article_id": article_id},
        )

    def post(self, path: str, payload: dict[str, Any]) -> JiuyanHttpResult:
        attempts = max(1, self.settings.max_retries)
        last_result: JiuyanHttpResult | None = None
        for attempt in range(attempts):
            self._sleep(attempt)
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
            headers = self._headers(path, payload)
            url = f"{str(self.settings.api_base_url).rstrip('/')}{path}"
            lease: ProxyLease | None = None
            proxy_url: str | None = None
            agent_job_id: str | None = None
            remote_response = None
            remote_error: str | None = None

            if self.agent_pool is not None:
                try:
                    remote_response = self.agent_pool.fetch(
                        source="jiuyan",
                        method="POST",
                        url=url,
                        headers=headers,
                        body=body,
                        timeout_seconds=self.crawl_settings.request_timeout_seconds,
                        priority=100,
                    )
                    agent_job_id = remote_response.job_id
                except (AgentPoolUnavailable, AgentJobFailed, ValueError) as exc:
                    remote_error = str(exc)
                    logger.info(
                        "Jiuyan remote agent unavailable; using existing transport",
                        extra={
                            "event": "jiuyan_agent_fallback",
                            "extra_data": {"url": url, "reason": remote_error},
                        },
                    )

            if remote_response is None and self.proxy_provider is not None:
                try:
                    lease = self.proxy_provider.acquire()
                except Exception as exc:
                    if not self.crawl_settings.proxy.fail_open:
                        return JiuyanHttpResult(
                            url=url,
                            status_code=0,
                            text="",
                            error_message=f"Failed to acquire proxy: {exc}",
                        )
                else:
                    proxy_url = lease.proxy_url if lease is not None else None
                    if lease is None and not self.crawl_settings.proxy.fail_open:
                        return JiuyanHttpResult(
                            url=url,
                            status_code=0,
                            text="",
                            error_message="No proxy available from proxy provider",
                        )

            if (
                remote_response is None
                and self.agent_pool is not None
                and not self.crawl_settings.agent_pool.fallback_to_existing_transport
            ):
                return JiuyanHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    error_message=remote_error or "No remote agent available",
                )

            started = time.monotonic()
            try:
                if remote_response is not None:
                    status_code = remote_response.status_code
                    text = remote_response.text
                    final_url = remote_response.final_url
                    duration_ms = remote_response.duration_ms
                else:
                    status_code, text, final_url = self._dispatch(
                        url, headers, body, proxy_url
                    )
                    duration_ms = int((time.monotonic() - started) * 1000)
            except error.HTTPError as exc:
                text = exc.read().decode("utf-8", errors="replace")
                status_code = exc.code
                final_url = url
                duration_ms = int((time.monotonic() - started) * 1000)
            except (error.URLError, TimeoutError, OSError) as exc:
                last_result = JiuyanHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    duration_ms=int((time.monotonic() - started) * 1000),
                    error_message=str(exc),
                )
                if lease is not None:
                    self._report_bad_proxy(lease, str(exc))
                if attempt + 1 < attempts:
                    continue
                return last_result

            block_kind = classify_block(status_code, text)
            blocked = block_kind in BLOCKED_KINDS
            result = JiuyanHttpResult(
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
            else:
                self._remember_page_time(path, payload, result)
                if lease is not None:
                    self._report_success_proxy(lease)
                if agent_job_id is not None:
                    self.agent_pool.store.record_outcome(agent_job_id, "success")
            return result
        return last_result or JiuyanHttpResult(
            url=path, status_code=0, text="", error_message="Request failed"
        )

    def _headers(self, path: str, payload: dict[str, Any]) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        headers = {
            "User-Agent": self.settings.user_agent or DEFAULT_USER_AGENT,
            "Referer": f"{str(self.settings.base_url).rstrip('/')}/",
            "Origin": str(self.settings.base_url).rstrip("/"),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "platform": "3",
            "timestamp": timestamp,
            "token": hashlib.md5(f"{SIGNING_PREFIX}{timestamp}".encode()).hexdigest(),
        }
        if int(payload.get("start") or 1) > 1 and path in self._page_times:
            headers["Page-Time"] = self._page_times[path]
        if self.settings.cookies:
            headers["Cookie"] = "; ".join(
                f"{name}={value}" for name, value in self.settings.cookies.items()
            )
        return headers

    def _dispatch(
        self, url: str, headers: dict[str, str], body: bytes, proxy_url: str | None
    ) -> tuple[int, str, str]:
        if proxy_url is not None:
            try:
                response = curl_requests.post(
                    url,
                    headers=headers,
                    data=body,
                    proxy=proxy_url,
                    impersonate="chrome",
                    timeout=self.crawl_settings.request_timeout_seconds,
                    allow_redirects=True,
                )
            except Exception as exc:
                raise OSError(str(exc)) from exc
            return response.status_code, response.text, str(response.url)
        req = request.Request(url, data=body, headers=headers, method="POST")
        with request.urlopen(
            req, timeout=self.crawl_settings.request_timeout_seconds
        ) as response:
            return (
                response.status,
                response.read().decode("utf-8", errors="replace"),
                response.geturl(),
            )

    def _remember_page_time(
        self, path: str, request_payload: dict[str, Any], result: JiuyanHttpResult
    ) -> None:
        if int(request_payload.get("start") or 1) != 1:
            return
        payload = result.json()
        if payload is not None and payload.get("serverTime") is not None:
            self._page_times[path] = str(payload["serverTime"])

    def _sleep(self, attempt: int) -> None:
        delay = random.uniform(
            self.settings.request_interval_min_seconds,
            self.settings.request_interval_max_seconds,
        )
        time.sleep(delay * (2**attempt))

    def _report_bad_proxy(self, lease: ProxyLease, reason: str) -> None:
        if not self.crawl_settings.proxy_pool.report_bad_on_block:
            return
        try:
            self.proxy_provider.report_bad(lease, reason)
        except Exception:
            pass

    def _report_success_proxy(self, lease: ProxyLease) -> None:
        try:
            self.proxy_provider.report_success(lease)
        except Exception:
            pass
