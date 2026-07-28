from __future__ import annotations

import json
import http.client
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Literal
from urllib import error, parse, request

from curl_cffi import requests as curl_requests

from alphapulse.runtime.agent_pool import (
    AgentJobFailed,
    AgentPoolClient,
    AgentPoolUnavailable,
)
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.fetching import ProxyLease, _build_proxy_provider
from alphapulse.sources.guba.parser import extract_embedded_json
from alphapulse.sources.guba.urls import getdata_url


logger = logging.getLogger(__name__)


REPLY_LIST_API_PATH = "reply/api/Reply/ArticleNewReplyList"
REQUEST_BACKOFF_MULTIPLIER_MAX = 16.0
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
)

BLOCKED_KINDS = {"http_403", "http_429", "captcha", "login_redirect", "soft_block"}
GubaTransport = Literal["auto", "agent", "existing"]


def classify_block(status_code: int, text: str, final_url: str) -> str | None:
    if status_code == 403:
        return "http_403"
    if status_code == 429:
        return "http_429"
    if status_code >= 500:
        return "http_5xx"
    lowered_url = final_url.lower()
    if "passport" in lowered_url or "/login" in lowered_url:
        return "login_redirect"
    # em_capt.js is present-but-dormant on every normal page, so only treat a
    # page as a captcha interstitial when it is interstitial-shaped: short and
    # carrying an active verification marker.
    if len(text) < 5000 and ("验证码" in text or "captcha" in lowered_url):
        return "captcha"
    return None


@dataclass
class GubaHttpResult:
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


class GubaClient:
    def __init__(self, settings: GubaSettings, crawl_settings: CrawlSettings) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = _build_proxy_provider(crawl_settings, source="guba")
        self.agent_pool = (
            AgentPoolClient(crawl_settings.agent_pool)
            if crawl_settings.agent_pool.enabled
            and (
                not crawl_settings.agent_pool.sources
                or "guba" in crawl_settings.agent_pool.sources
            )
            else None
        )
        self._backoff_multiplier = 1.0

    def get(
        self,
        url: str,
        *,
        expect_marker: str | None = None,
        transport: GubaTransport = "auto",
    ) -> GubaHttpResult:
        return self._request(
            "GET",
            url,
            form=None,
            expect_marker=expect_marker,
            transport=transport,
        )

    def post_json(self, url: str, payload: dict[str, Any]) -> GubaHttpResult:
        """POST a JSON body (used by ranking APIs that reject form-encoded input)."""
        return self._request(
            "POST",
            url,
            form=None,
            json_body=json.dumps(payload),
            transport="auto",
        )

    def post_replies(
        self,
        *,
        post_id: str,
        board_code: str,
        page: int,
        transport: GubaTransport = "auto",
    ) -> GubaHttpResult:
        base = str(self.settings.base_url)
        param = (
            f"postid={post_id}&sort=1&sorttype=1&p={page}&ps={self.settings.reply_page_size}"
        )
        form = {
            "param": param,
            "path": REPLY_LIST_API_PATH,
            "env": "2",
        }
        referer = f"{base.rstrip('/')}/news,{board_code},{post_id}.html"
        return self._request(
            "POST",
            getdata_url(base),
            form=form,
            referer=referer,
            transport=transport,
        )

    def _request(
        self,
        method: str,
        url: str,
        *,
        form: dict[str, str] | None,
        referer: str | None = None,
        expect_marker: str | None = None,
        json_body: str | None = None,
        transport: GubaTransport = "auto",
    ) -> GubaHttpResult:
        attempts = max(1, self.settings.max_retries)
        last_result: GubaHttpResult | None = None
        backoff_for_block = False

        for attempt in range(attempts):
            lease: ProxyLease | None = None
            proxy_url: str | None = None
            acquire_error: str | None = None
            agent_job_id: str | None = None
            self._adaptive_sleep(was_rate_limited=backoff_for_block)

            remote_response = None
            remote_error: str | None = None
            if self.agent_pool is not None and transport != "existing":
                headers, body = self._request_parts(
                    form=form,
                    referer=referer,
                    json_body=json_body,
                    include_cookies=False,
                )
                try:
                    remote_response = self.agent_pool.fetch(
                        source="guba",
                        method=method,
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
                        "Guba remote agent unavailable; using existing transport",
                        extra={
                            "event": "guba_agent_fallback",
                            "extra_data": {
                                "url": url,
                                "reason": remote_error,
                                "attempt": attempt + 1,
                            },
                        },
                    )

            if remote_response is None:
                if (
                    self.agent_pool is not None
                    and transport != "existing"
                    and not self.crawl_settings.agent_pool.fallback_to_existing_transport
                ):
                    return GubaHttpResult(
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
                            acquire_error = f"Failed to acquire proxy: {exc}"
                    else:
                        if lease is not None:
                            proxy_url = lease.proxy_url
                        elif not self.crawl_settings.proxy.fail_open:
                            acquire_error = "No proxy available from proxy provider"

            if acquire_error is not None:
                last_result = GubaHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    error_message=acquire_error,
                )
                if attempt + 1 < attempts:
                    backoff_for_block = False
                    continue
                return last_result

            started = time.monotonic()
            try:
                if remote_response is not None:
                    status_code = remote_response.status_code
                    text = remote_response.text
                    final_url = remote_response.final_url
                else:
                    status_code, text, final_url = self._dispatch(
                        method, url, form, referer, proxy_url, json_body
                    )
            except error.HTTPError as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                body = exc.read().decode("utf-8", errors="ignore")
                block_kind = classify_block(exc.code, body, url)
                blocked = block_kind in BLOCKED_KINDS
                last_result = GubaHttpResult(
                    url=url,
                    status_code=exc.code,
                    text=body,
                    duration_ms=duration_ms,
                    error_message=f"HTTP {exc.code}",
                    blocked=blocked,
                    block_kind=block_kind,
                )
                logger.warning(
                    "Guba HTTP error",
                    extra={
                        "event": "guba_http_error",
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
                    backoff_for_block = True
                    continue
                return last_result
            except (error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                last_result = GubaHttpResult(
                    url=url,
                    status_code=0,
                    text="",
                    duration_ms=duration_ms,
                    error_message=str(exc),
                )
                logger.warning(
                    "Guba request error",
                    extra={
                        "event": "guba_request_error",
                        "extra_data": {"url": url, "error": str(exc), "attempt": attempt + 1},
                    },
                )
                if lease is not None:
                    self._report_bad_proxy(lease, str(exc))
                if attempt + 1 < attempts:
                    backoff_for_block = False
                    time.sleep(2**attempt)
                    continue
                return last_result

            duration_ms = int((time.monotonic() - started) * 1000)
            if remote_response is not None and remote_response.duration_ms is not None:
                duration_ms = remote_response.duration_ms
            block_kind = classify_block(status_code, text, final_url)
            if (
                block_kind is None
                and expect_marker is not None
                and expect_marker not in text
                and "/error" not in final_url.lower()
            ):
                # An HTTP 200 page missing its expected data payload is a
                # WAF/soft-block page classify_block cannot recognize by
                # status or shape; flagging it blocked engages retries,
                # adaptive backoff, and proxy rotation instead of letting it
                # surface downstream as a parse error.
                block_kind = "soft_block"
            blocked = block_kind in BLOCKED_KINDS
            result = GubaHttpResult(
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
                    "Guba blocked response",
                    extra={
                        "event": "guba_blocked",
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
                if agent_job_id is not None:
                    self.agent_pool.store.record_outcome(
                        agent_job_id,
                        "blocked",
                        f"blocked: {block_kind}",
                    )
                last_result = result
                if attempt + 1 < attempts:
                    backoff_for_block = True
                    continue
            elif lease is not None:
                self._report_success_proxy(lease)
            elif agent_job_id is not None:
                self.agent_pool.store.record_outcome(agent_job_id, "success")
            return result

        return last_result or GubaHttpResult(url=url, status_code=0, text="", error_message="Request failed")

    def _dispatch(
        self,
        method: str,
        url: str,
        form: dict[str, str] | None,
        referer: str | None,
        proxy_url: str | None,
        json_body: str | None = None,
    ) -> tuple[int, str, str]:
        if proxy_url is not None:
            return self._dispatch_with_curl(
                method,
                url,
                form,
                referer,
                proxy_url,
                json_body,
            )

        opener = request.build_opener()
        headers, data = self._request_parts(
            form=form,
            referer=referer,
            json_body=json_body,
            include_cookies=True,
        )
        req = request.Request(url, data=data, headers=headers, method=method)
        with opener.open(req, timeout=self.crawl_settings.request_timeout_seconds) as response:
            charset = response.headers.get_content_charset()
            raw = self._read_body(response, url, charset)
            text = self._decode(raw, charset)
            return response.status, text, response.geturl()

    def _dispatch_with_curl(
        self,
        method: str,
        url: str,
        form: dict[str, str] | None,
        referer: str | None,
        proxy_url: str,
        json_body: str | None,
    ) -> tuple[int, str, str]:
        headers, data = self._request_parts(
            form=form,
            referer=referer,
            json_body=json_body,
            include_cookies=True,
        )
        try:
            response = curl_requests.request(
                method,
                url,
                headers=headers,
                data=data,
                proxy=proxy_url,
                impersonate="chrome",
                timeout=self.crawl_settings.request_timeout_seconds,
                allow_redirects=True,
            )
        except Exception as exc:
            raise OSError(str(exc)) from exc
        return response.status_code, response.text, str(response.url)

    def _read_body(self, response: Any, url: str, charset: str | None) -> bytes:
        var_name = self._embedded_var_name(url)
        if var_name is None:
            return response.read()

        chunks: list[bytes] = []
        while True:
            try:
                chunk = response.read(16 * 1024)
            except http.client.IncompleteRead as exc:
                chunks.append(exc.partial)
                raw = b"".join(chunks)
                if self._has_complete_embedded_json(raw, charset, var_name):
                    return raw
                repaired = self._repair_one_byte_truncation(
                    raw,
                    charset,
                    var_name,
                    exc.expected,
                )
                if repaired is not None:
                    return repaired
                raise http.client.IncompleteRead(raw, exc.expected) from exc
            if not chunk:
                raw = b"".join(chunks)
                if self._has_complete_embedded_json(raw, charset, var_name):
                    return raw
                raise http.client.IncompleteRead(raw, 1)
            chunks.append(chunk)
            raw = b"".join(chunks)
            if self._has_complete_embedded_json(raw, charset, var_name):
                return raw

    def _has_complete_embedded_json(
        self,
        raw: bytes,
        charset: str | None,
        var_name: str,
    ) -> bool:
        return extract_embedded_json(self._decode(raw, charset), var_name) is not None

    def _repair_one_byte_truncation(
        self,
        raw: bytes,
        charset: str | None,
        var_name: str,
        expected: int | None,
    ) -> bytes | None:
        if expected != 1:
            return None
        for suffix in (b"}", b"]"):
            candidate = raw + suffix
            if self._has_complete_embedded_json(candidate, charset, var_name):
                return candidate
        return None

    @staticmethod
    def _embedded_var_name(url: str) -> str | None:
        if "/list," in url:
            return "article_list"
        if "/news," in url:
            return "post_article"
        return None

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

    def _request_parts(
        self,
        *,
        form: dict[str, str] | None,
        referer: str | None,
        json_body: str | None,
        include_cookies: bool,
    ) -> tuple[dict[str, str], bytes | None]:
        headers = self._headers(referer, include_cookies=include_cookies)
        data: bytes | None = None
        if json_body is not None:
            data = json_body.encode("utf-8")
            headers["Content-Type"] = "application/json"
        elif form is not None:
            data = parse.urlencode(form).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        return headers, data

    def _headers(
        self,
        referer: str | None,
        *,
        include_cookies: bool = True,
    ) -> dict[str, str]:
        headers = {
            "User-Agent": self.settings.user_agent or DEFAULT_USER_AGENT,
            "Referer": referer or f"{str(self.settings.base_url).rstrip('/')}/",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        }
        if include_cookies and self.settings.cookies:
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

    def _report_success_proxy(self, lease: ProxyLease) -> None:
        try:
            self.proxy_provider.report_success(lease)
        except Exception:
            return
