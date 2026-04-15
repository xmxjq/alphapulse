from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from alphapulse.runtime.config import CrawlSettings, FetchMode, XueqiuSettings


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


@dataclass
class FetchResult:
    url: str
    status_code: int
    text: str
    headers: dict[str, Any]

    def json(self) -> dict[str, Any]:
        return json.loads(self.text)


class ScraplingClient:
    def __init__(self, source_settings: XueqiuSettings, crawl_settings: CrawlSettings) -> None:
        self.source_settings = source_settings
        self.crawl_settings = crawl_settings

    def fetch(self, url: str) -> FetchResult:
        mode: FetchMode = self.source_settings.fetch_mode
        browser_cookies = _browser_cookies(self.source_settings)
        if mode == "dynamic":
            from scrapling.fetchers import DynamicFetcher

            response = DynamicFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
                timeout=self.crawl_settings.request_timeout_seconds * 1000,
                cookies=browser_cookies,
                extra_headers={"user-agent": self.crawl_settings.user_agent},
            )
        elif mode == "stealth":
            from scrapling.fetchers import StealthyFetcher

            response = StealthyFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
                timeout=self.crawl_settings.request_timeout_seconds * 1000,
                cookies=browser_cookies,
                extra_headers={"user-agent": self.crawl_settings.user_agent},
            )
        else:
            from scrapling.fetchers import Fetcher

            response = Fetcher.get(
                url,
                timeout=self.crawl_settings.request_timeout_seconds,
                headers={"user-agent": self.crawl_settings.user_agent},
                cookies=self.source_settings.cookies,
            )

        headers = dict(getattr(response, "headers", {}) or {})
        return FetchResult(
            url=getattr(response, "url", url),
            status_code=getattr(response, "status", 0),
            text=_response_text(response),
            headers=headers,
        )
