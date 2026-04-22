from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request

from alphapulse.runtime.config import BilibiliSettings, CrawlSettings
from alphapulse.sources.fetching import ProxyLease, _build_proxy_provider


logger = logging.getLogger(__name__)


COMMENT_API_PATH = "/x/v2/reply/main"
REPLY_API_PATH = "/x/v2/reply/reply"
VIDEO_INFO_API_PATH = "/x/web-interface/view"
USER_VIDEOS_API_PATH = "/x/space/wbi/arc/search"
NAV_API_PATH = "/x/web-interface/nav"

REQUEST_BACKOFF_MULTIPLIER_MAX = 16.0
WBI_KEYS_TTL_SECONDS = 300

# Fixed permutation used by Bilibili's WBI signing scheme: concatenate
# img_key + sub_key (64 chars), reorder by this index table, take the first 32.
_WBI_MIXIN_KEY_ENC_TAB = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
)
_WBI_KEY_STEM_RE = re.compile(r"/([0-9a-f]{32})\.png", re.IGNORECASE)
_WBI_FORBIDDEN_CHARS = "!'()*"


def _extract_wbi_key(url: str | None) -> str | None:
    if not url:
        return None
    match = _WBI_KEY_STEM_RE.search(url)
    return match.group(1) if match else None


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
        self._backoff_multiplier = 1.0
        self._wbi_keys: tuple[str, str] | None = None
        self._wbi_keys_fetched_at: float = 0.0

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

    def get_user_videos(
        self,
        *,
        mid: str | int,
        page: int = 1,
        page_size: int = 30,
        order: str = "pubdate",
    ) -> BilibiliApiResult:
        return self._request_json(
            USER_VIDEOS_API_PATH,
            params={
                "mid": str(mid),
                "ps": page_size,
                "pn": page,
                "order": order,
                "platform": "web",
                "web_location": "1550101",
                # Browser-fingerprint params bilibili expects alongside the WBI
                # signature; without these the server returns -352 风控校验失败.
                "dm_img_list": "[]",
                "dm_img_str": "V2ViR0wgMS4wIChPcGVuR0wgRVMgMi4wIENocm9taXVtKQ",
                "dm_cover_img_str": "QU5HTEUgKEludGVsKQ",
                "dm_img_inter": '{"ds":[],"wh":[0,0,0],"of":[0,0,0]}',
            },
            sign_wbi=True,
        )

    def _request_json(
        self,
        path: str,
        *,
        params: dict[str, Any],
        sign_wbi: bool = False,
    ) -> BilibiliApiResult:
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
                logger.debug(
                    "Bilibili API request",
                    extra={
                        "event": "bilibili_request",
                        "extra_data": {
                            "path": path,
                            "params": params,
                            "attempt": attempt + 1,
                            "proxy": proxy_url,
                        },
                    },
                )
                request_params = self._sign_wbi_params(params) if sign_wbi else params
                status_code, body = self._dispatch_request(path, request_params, proxy_url)
                payload = json.loads(body)
            except error.HTTPError as exc:
                status_code = exc.code
                body = exc.read().decode("utf-8", errors="ignore")
                blocked = status_code in {403, 412, 429}
                last_error = f"HTTP {status_code}"
                logger.warning(
                    "Bilibili HTTP error",
                    extra={
                        "event": "bilibili_http_error",
                        "extra_data": {
                            "path": path,
                            "status_code": status_code,
                            "blocked": blocked,
                            "attempt": attempt + 1,
                            "proxy": proxy_url,
                        },
                    },
                )
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
                logger.warning(
                    "Bilibili request error",
                    extra={
                        "event": "bilibili_request_error",
                        "extra_data": {
                            "path": path,
                            "error": last_error,
                            "attempt": attempt + 1,
                            "proxy": proxy_url,
                        },
                    },
                )
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
                logger.debug(
                    "Bilibili API ok",
                    extra={
                        "event": "bilibili_response",
                        "extra_data": {
                            "path": path,
                            "status_code": status_code,
                            "proxy": proxy_url,
                        },
                    },
                )
                return BilibiliApiResult(payload=payload, status_code=status_code, proxy_url=proxy_url)

            blocked = code == -412
            message = str(payload.get("message") or payload.get("msg") or f"API error {code}")
            last_error = f"API code {code}: {message}"
            logger.warning(
                "Bilibili API error code",
                extra={
                    "event": "bilibili_api_error",
                    "extra_data": {
                        "path": path,
                        "code": code,
                        "message": message,
                        "blocked": blocked,
                        "attempt": attempt + 1,
                        "proxy": proxy_url,
                    },
                },
            )
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

    def _sign_wbi_params(self, params: dict[str, Any]) -> dict[str, Any]:
        img_key, sub_key = self._get_wbi_keys()
        mixin_source = img_key + sub_key
        mixin_key = "".join(mixin_source[i] for i in _WBI_MIXIN_KEY_ENC_TAB)[:32]
        cleaned = {
            key: "".join(ch for ch in str(value) if ch not in _WBI_FORBIDDEN_CHARS)
            for key, value in params.items()
        }
        cleaned["wts"] = str(int(time.time()))
        ordered = dict(sorted(cleaned.items()))
        query = parse.urlencode(ordered)
        ordered["w_rid"] = hashlib.md5((query + mixin_key).encode("utf-8")).hexdigest()
        return ordered

    def _get_wbi_keys(self) -> tuple[str, str]:
        now = time.monotonic()
        if self._wbi_keys is not None and now - self._wbi_keys_fetched_at < WBI_KEYS_TTL_SECONDS:
            return self._wbi_keys

        url = f"{str(self.settings.api_base_url).rstrip('/')}{NAV_API_PATH}"
        req = request.Request(url, headers=self._headers())
        with request.build_opener().open(req, timeout=self.crawl_settings.request_timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="ignore")
        payload = json.loads(body)
        wbi_img = (payload.get("data") or {}).get("wbi_img") or {}
        img_key = _extract_wbi_key(wbi_img.get("img_url"))
        sub_key = _extract_wbi_key(wbi_img.get("sub_url"))
        if img_key is None or sub_key is None:
            raise RuntimeError("Failed to parse Bilibili WBI keys from nav response")
        self._wbi_keys = (img_key, sub_key)
        self._wbi_keys_fetched_at = now
        return self._wbi_keys

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
