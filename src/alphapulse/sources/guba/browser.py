from __future__ import annotations

import logging
import random
import time
from typing import Any

from alphapulse.runtime.config import GubaBrowserSettings
from alphapulse.sources.guba.api import GubaHttpResult
from alphapulse.sources.guba.parser import extract_embedded_json


logger = logging.getLogger(__name__)

POST_MARKER = "var post_article"
BLOCKED_RESOURCE_TYPES = {"font", "image", "media"}
READY_EXPRESSION = """
() => (
    typeof window.post_article === "object"
    || location.pathname.startsWith("/error")
    || [...document.scripts].some(script => script.src.includes("fd_guba_validate"))
    || !!document.querySelector(
        'iframe[src*="websitecaptcha"], iframe[src*="slidervalid"]'
    )
)
"""


class GubaBrowserClient:
    """Fetch guba post pages through a persistent, manually authenticated browser."""

    def __init__(self, settings: GubaBrowserSettings) -> None:
        self.settings = settings
        self._playwright: Any | None = None
        self._browser: Any | None = None
        self._page: Any | None = None
        self._last_request_finished_at: float | None = None

    def get(self, url: str) -> GubaHttpResult:
        started = time.monotonic()
        try:
            self._wait_for_request_slot()
            page = self._ensure_page()
            response = page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=self.settings.navigation_timeout_seconds * 1000,
            )
            self._wait_until_resolved(page)
            final_url = page.url
            html = page.content()
            payload = page.evaluate(
                "() => window.post_article ? JSON.stringify(window.post_article) : null"
            )
            embedded_payload = extract_embedded_json(html, "post_article")
            if payload and embedded_payload is None:
                html = f"{html}<script>{POST_MARKER}={payload};</script>"

            status_code = response.status if response is not None else 200
            block_kind = self._block_kind(
                final_url,
                html,
                has_payload=payload is not None or embedded_payload is not None,
            )
            blocked = block_kind is not None
            return GubaHttpResult(
                url=final_url,
                status_code=status_code,
                text=html,
                duration_ms=int((time.monotonic() - started) * 1000),
                error_message=(
                    f"Browser blocked response ({block_kind})" if blocked else None
                ),
                blocked=blocked,
                block_kind=block_kind,
            )
        except Exception as exc:
            self._discard_page()
            logger.warning(
                "Guba browser request failed",
                extra={
                    "event": "guba_browser_error",
                    "extra_data": {"url": url, "error": str(exc)},
                },
            )
            return GubaHttpResult(
                url=url,
                status_code=0,
                text="",
                duration_ms=int((time.monotonic() - started) * 1000),
                error_message=str(exc),
            )
        finally:
            self._last_request_finished_at = time.monotonic()

    def _ensure_page(self) -> Any:
        if self._page is not None and not self._page.is_closed():
            return self._page

        from patchright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.connect_over_cdp(
            self.settings.cdp_url,
            timeout=self.settings.navigation_timeout_seconds * 1000,
        )
        contexts = self._browser.contexts
        if not contexts:
            raise RuntimeError("The guba browser has no persistent context")
        self._page = contexts[0].new_page()
        self._page.route("**/*", self._route_request)
        self._page.set_default_navigation_timeout(
            self.settings.navigation_timeout_seconds * 1000
        )
        return self._page

    def _wait_for_request_slot(self) -> None:
        if self._last_request_finished_at is None:
            return
        interval = random.uniform(
            self.settings.request_interval_min_seconds,
            self.settings.request_interval_max_seconds,
        )
        remaining = interval - (time.monotonic() - self._last_request_finished_at)
        if remaining > 0:
            time.sleep(remaining)

    @staticmethod
    def _route_request(route: Any) -> None:
        if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
            route.abort()
            return
        route.continue_()

    def _wait_until_resolved(self, page: Any) -> None:
        from patchright.sync_api import TimeoutError as PlaywrightTimeoutError

        try:
            page.wait_for_function(
                READY_EXPRESSION,
                timeout=self.settings.settle_timeout_seconds * 1000,
            )
        except PlaywrightTimeoutError:
            return

    @staticmethod
    def _block_kind(final_url: str, html: str, *, has_payload: bool) -> str | None:
        lowered = html.lower()
        if "fd_guba_validate" in lowered or "websitecaptcha/slidervalid" in lowered:
            return "captcha"
        if not has_payload and "/error" not in final_url.lower():
            return "soft_block"
        return None

    def _discard_page(self) -> None:
        if self._page is not None:
            try:
                self._page.close()
            except Exception:
                pass
        self._page = None
        self._browser = None
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:
                pass
        self._playwright = None
