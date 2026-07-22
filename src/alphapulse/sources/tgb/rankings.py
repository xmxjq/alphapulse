"""Discover 淘股吧 hot-stock boards from the homepage 热门研股 widget.

tgb.cn is server-rendered, so unlike guba (whose ranking widgets are encrypted/SPA and
need side APIs) we read the 热门研股 (hot research stocks) list straight out of the
homepage HTML. Each entry is a stock code whose per-stock board is ``/quotes/{code}``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lxml import html as lxml_html

from alphapulse.runtime.config import TgbSettings
from alphapulse.sources.tgb.api import TgbClient


logger = logging.getLogger(__name__)


@dataclass
class HotStock:
    rank: int
    code: str
    name: str


def parse_hot_stocks(html: str, limit: int) -> list[HotStock]:
    """Parse the homepage 热门研股 block into ranked (code, name) hot stocks."""
    doc = lxml_html.fromstring(html)
    # Pick the sidebar block whose title is 热门研股 (there are several such widgets).
    container = None
    for block in doc.cssselect("div.defaultContainerRight-guba"):
        title = block.cssselect("div.middle-right-tittle")
        if title and "热门研股" in title[0].text_content():
            container = block
            break
    if container is None:
        return []

    stocks: list[HotStock] = []
    seen: set[str] = set()
    for item in container.cssselect("div.defaultContainerRight-stock-item"):
        code = (item.get("id") or "").strip()
        if not code or code in seen:
            continue
        name_el = item.cssselect("a.defaultContainerRight-stock-name p")
        name = name_el[0].text_content().strip() if name_el else code
        seen.add(code)
        stocks.append(HotStock(rank=len(stocks) + 1, code=code, name=name))
        if len(stocks) >= limit:
            break
    return stocks


def fetch_hot_stocks(client: TgbClient, settings: TgbSettings) -> list[HotStock]:
    """Fetch the homepage and extract the top-N 热门研股 hot stocks."""
    result = client.get(str(settings.ranking_hot_stock_url), expect_marker="热门研股")
    if result.status_code == 0 or result.blocked or not result.text:
        logger.warning(
            "Tgb hot-stock fetch failed",
            extra={
                "event": "tgb_ranking_fetch_failed",
                "extra_data": {
                    "url": str(settings.ranking_hot_stock_url),
                    "status_code": result.status_code,
                    "block_kind": result.block_kind,
                    "error": result.error_message,
                },
            },
        )
        return []
    return parse_hot_stocks(result.text, settings.hot_stocks_limit)
