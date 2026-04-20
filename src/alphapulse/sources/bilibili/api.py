from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request

from alphapulse.runtime.config import BilibiliSettings, CrawlSettings
from alphapulse.sources.fetching import ProxyLease, _build_proxy_provider


COMMENT_API_PATH = "/x/v2/reply/main"
REPLY_API_PATH = "/x/v2/reply/reply"
VIDEO_INFO_API_PATH = "/x/web-interface/view"

REQUEST_DELAY_MIN = 0.1
REQUEST_DELAY_MAX = 2.0
REQUEST_DELAY_DEFAULT = 0.15


@dataclass
class BilibiliApiResult:
    payload: dict[str, Any] | None
    status_code: int
    error_message: str | None = None
    blocked: bool = False
    proxy_url: str | None = None


class BilibiliApiClient:
    def __init__(self, settings: BilibiliSettings, crawl_settings: CrawlSettings) -> None:
        self.settings = settings
        self.crawl_settings = crawl_settings
        self.proxy_provider = _build_proxy_provider(crawl_settings)
        self._current_delay = REQUEST_DELAY_DEFAULT

    def get_video_info(self, *, bvid: str | None = None, aid: int | None = None) -> BilibiliApiResult:
        params: dict[str, Any] = {}
        if bvid is not None:
            params["bvid"] = bvid
        elif aid is not None:
            params["aid"] = aid
        else:
            return BilibiliApiResult(payload=None, status_code=0, error_message="Missing Bilibili video identifier")
        return self._request_json(VIDEO_INFO_API_PATH, params=params)

    def get_comments(
        self,
        *,
        aid: int,
        next_cursor: int = 0,
        page: int = 1,
    ) -> BilibiliApiResult:
        return self._request_json(
            COMMENT_API_PATH,
            params={
                "oid": aid,
                "type": 1,
                "mode": self.settings.sort_mode,
                "pn": page,
                "ps": self.settings.page_size,
                "next": next_cursor,
            },
        )

    def get_replies(self, *, aid: int, root_rpid: int, page: int = 1) -> BilibiliApiResult:
        return self._request_json(
            REPLY_API_PATH,
            params={
                "oid": aid,
                "type": 1,
                "root": root_rpid,
                "pn": page,
                "ps": self.settings.page_size,
            },
        )

    def _request_json(self, path: str, *, params: dict[str, Any]) -> BilibiliApiResult:
        attempts = max(1, self.crawl_settings.proxy.max_attempts)
        last_error: str | None = None

        for attempt in range(attempts):
            lease: ProxyLease | None = None
            proxy_url: str | None = None
            was_rate_limited = attempt > 0

            if self.proxy_provider is not None:
                try:
                    lease = self.proxy_provider.acquire()
                except Exception as exc:
                    if not self.crawl_settings.proxy.fail_open:
                        return BilibiliApiResult(
                            payload=None,
                            status_code=0,
                            error_message=f"Failed to acquire proxy: {exc}",
                        )
                    last_error = f"Failed to acquire proxy: {exc}"
                else:
                    if lease is None and not self.crawl_settings.proxy.fail_open:
                        return BilibiliApiResult(
                            payload=None,
                            status_code=0,
                            error_message="No proxy available from proxy provider",
                        )
                    if lease is not None:
                        proxy_url = lease.proxy_url

            try:
                self._adaptive_sleep(was_rate_limited=was_rate_limited)
                status_code, body = self._dispatch_request(path, params, proxy_url)
                payload = json.loads(body)
            except error.HTTPError as exc:
                status_code = exc.code
                body = exc.read().decode("utf-8", errors="ignore")
                blocked = status_code in {403, 412, 429}
                last_error = f"HTTP {status_code}"
                if lease is not None and blocked:
                    self._report_bad_proxy(lease, last_error)
                if blocked and attempt + 1 < attempts:
                    continue
                return BilibiliApiResult(
                    payload=None,
                    status_code=status_code,
                    error_message=last_error,
                    blocked=blocked,
                    proxy_url=proxy_url,
                )
            except (error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
                last_error = str(exc)
                if lease is not None:
                    self._report_bad_proxy(lease, last_error)
                if attempt + 1 < attempts:
                    time.sleep(2**attempt)
                    continue
                return BilibiliApiResult(
                    payload=None,
                    status_code=0,
                    error_message=last_error,
                    proxy_url=proxy_url,
                )

            code = payload.get("code", -1)
            if code == 0:
                self._current_delay = max(self._current_delay * 0.8, REQUEST_DELAY_MIN)
                return BilibiliApiResult(payload=payload, status_code=status_code, proxy_url=proxy_url)

            blocked = code == -412
            message = str(payload.get("message") or payload.get("msg") or f"API error {code}")
            last_error = f"API code {code}: {message}"
            if lease is not None and blocked:
                self._report_bad_proxy(lease, last_error)
            if blocked and attempt + 1 < attempts:
                continue
            return BilibiliApiResult(
                payload=payload,
                status_code=status_code,
                error_message=last_error,
                blocked=blocked,
                proxy_url=proxy_url,
            )

        return BilibiliApiResult(payload=None, status_code=0, error_message=last_error or "Request failed")

    def _dispatch_request(self, path: str, params: dict[str, Any], proxy_url: str | None) -> tuple[int, str]:
        query = parse.urlencode(params)
        url = f"{str(self.settings.api_base_url).rstrip('/')}{path}?{query}"
        opener = request.build_opener()
        if proxy_url is not None:
            opener = request.build_opener(
                request.ProxyHandler(
                    {
                        "http": proxy_url,
                        "https": proxy_url,
                    }
                )
            )
        req = request.Request(url, headers=self._headers())
        with opener.open(req, timeout=self.crawl_settings.request_timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="ignore")
            return response.status, body

    def _headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": self.crawl_settings.user_agent,
            "Referer": f"{str(self.settings.web_base_url).rstrip('/')}/",
            "Accept": "application/json, text/plain, */*",
            "Origin": str(self.settings.web_base_url).rstrip("/"),
        }
        if self.settings.cookies:
            headers["Cookie"] = "; ".join(f"{name}={value}" for name, value in self.settings.cookies.items())
        return headers

    def _adaptive_sleep(self, *, was_rate_limited: bool) -> None:
        if was_rate_limited:
            self._current_delay = min(self._current_delay * 2, REQUEST_DELAY_MAX)
        else:
            self._current_delay = max(self._current_delay * 0.8, REQUEST_DELAY_MIN)
        time.sleep(self._current_delay)

    def _report_bad_proxy(self, lease: ProxyLease, reason: str) -> None:
        if not self.crawl_settings.proxy_pool.report_bad_on_block:
            return
        try:
            self.proxy_provider.report_bad(lease, reason)
        except Exception:
            return
