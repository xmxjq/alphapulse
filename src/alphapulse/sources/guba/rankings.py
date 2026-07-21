"""Fetch the three guba homepage "hot" rankings and resolve them to crawlable boards.

The guba homepage renders three ranking widgets — 热门个股吧 (hot stock boards),
热门主题吧 (hot theme boards) and 热门概念吧 (hot concept boards). The widgets
themselves are React-rendered from partly-encrypted / CMS-hosted internal endpoints,
so instead we read the same underlying data from stable public East Money APIs:

* 热门个股吧 → the 人气榜 popularity API (``emappdata`` ``getAllCurrentList``), which
  returns ranked security ids we map to 6-digit guba board codes.
* 热门概念吧 → the push2 quote ``clist`` for concept sectors (``dpt=gb.rmbk``), whose
  ``f12`` field is the ``BK`` board code.
* 热门主题吧 → the theme list (``HomePageListRead`` JSONP). A theme is a topic on a
  separate SPA rather than a board, so each theme is expanded into the **concept
  (``BK``) boards** in its ``StockListNew`` basket (individual stocks are dropped).

Every crawlable identifier is an ordinary ``/list,{code}.html`` board code, so the
existing board crawler consumes them unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from alphapulse.runtime.config import GubaSettings
from alphapulse.sources.guba.api import GubaClient
from alphapulse.sources.guba.urls import board_list_url, topic_url


logger = logging.getLogger(__name__)

SECTION_HOT_STOCK = "hot_stock"
SECTION_HOT_CONCEPT = "hot_concept"
SECTION_HOT_THEME = "hot_theme"

# StockListNew tokens are "{marketPrefix}_{code}". Theme expansion keeps only the
# concept/board members (market 90 → "BK...."); individual stocks and foreign
# markets are dropped.
_BOARD_MARKET_PREFIX = "90"
# emappdata security ids look like "SH600584" / "SZ001309" / "BJ430047".
_SECID_RE = re.compile(r"^(?P<mkt>[A-Za-z]{2})(?P<code>[0-9A-Za-z]+)$")


@dataclass
class HotBoard:
    section: str
    rank: int
    board_code: str
    name: str
    url: str


@dataclass
class HotTheme:
    rank: int
    htid: str
    name: str
    url: str
    member_board_codes: list[str] = field(default_factory=list)


@dataclass
class HotRankings:
    hot_stock: list[HotBoard] = field(default_factory=list)
    hot_concept: list[HotBoard] = field(default_factory=list)
    hot_theme: list[HotTheme] = field(default_factory=list)

    def board_codes(self) -> list[str]:
        """Ordered, de-duplicated crawlable board codes across every section."""
        ordered: list[str] = []
        seen: set[str] = set()
        for board in (*self.hot_stock, *self.hot_concept):
            if board.board_code not in seen:
                seen.add(board.board_code)
                ordered.append(board.board_code)
        for theme in self.hot_theme:
            for code in theme.member_board_codes:
                if code not in seen:
                    seen.add(code)
                    ordered.append(code)
        return ordered


def _loads(text: str) -> Any | None:
    """Parse a ranking response as JSON, tolerating a ``var x=...;//comment`` wrapper."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = min(
        (i for i in (text.find("{"), text.find("[")) if i != -1),
        default=-1,
    )
    if start == -1:
        return None
    opener = text[start]
    closer = "}" if opener == "{" else "]"
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : index + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _fetch_json(client: GubaClient, url: str, *, payload: dict[str, Any] | None = None) -> Any | None:
    result = client.post_json(url, payload) if payload is not None else client.get(url)
    if result.status_code == 0 or result.blocked or not result.text:
        logger.warning(
            "Guba ranking fetch failed",
            extra={
                "event": "guba_ranking_fetch_failed",
                "extra_data": {
                    "url": url,
                    "status_code": result.status_code,
                    "block_kind": result.block_kind,
                    "error": result.error_message,
                },
            },
        )
        return None
    return _loads(result.text)


def _board_code_from_secid(secid: str) -> str | None:
    match = _SECID_RE.match(secid.strip())
    if match is None:
        return None
    return match.group("code")


def _concept_code_from_token(token: str) -> str | None:
    """Map a ``StockListNew`` token to a concept board code, or None if not a board.

    Only market-90 tokens ("90_BK1036") are concept/sector boards; individual
    stocks ("1_688981") and foreign markets are dropped.
    """
    prefix, _, code = token.partition("_")
    if prefix != _BOARD_MARKET_PREFIX or not code:
        return None
    return code


def _fetch_hot_stock(client: GubaClient, settings: GubaSettings) -> list[HotBoard]:
    payload = {"marketType": "", "pageNo": 1, "pageSize": settings.hot_boards_per_section}
    data = _fetch_json(client, settings.ranking_stock_url, payload=payload)
    if not isinstance(data, dict):
        return []
    entries = data.get("data") or []
    base = str(settings.base_url)
    boards: list[HotBoard] = []
    for rank, entry in enumerate(entries[: settings.hot_boards_per_section], start=1):
        if not isinstance(entry, dict):
            continue
        code = _board_code_from_secid(str(entry.get("sc") or ""))
        if code is None:
            continue
        boards.append(
            HotBoard(
                section=SECTION_HOT_STOCK,
                rank=int(entry.get("rk") or rank),
                board_code=code,
                name=str(entry.get("name") or code),
                url=board_list_url(base, code),
            )
        )
    return boards


def _fetch_hot_concept(client: GubaClient, settings: GubaSettings) -> list[HotBoard]:
    data = _fetch_json(client, settings.ranking_concept_url)
    if not isinstance(data, dict):
        return []
    diff = ((data.get("data") or {}) or {}).get("diff") or []
    base = str(settings.base_url)
    boards: list[HotBoard] = []
    for rank, entry in enumerate(diff[: settings.hot_boards_per_section], start=1):
        if not isinstance(entry, dict):
            continue
        code = str(entry.get("f12") or "").strip()
        if not code:
            continue
        boards.append(
            HotBoard(
                section=SECTION_HOT_CONCEPT,
                rank=rank,
                board_code=code,
                name=str(entry.get("f14") or code),
                url=board_list_url(base, code),
            )
        )
    return boards


def _fetch_hot_theme(client: GubaClient, settings: GubaSettings) -> list[HotTheme]:
    data = _fetch_json(client, settings.ranking_theme_url)
    if not isinstance(data, dict):
        return []
    entries = data.get("re") or []
    themes: list[HotTheme] = []
    for rank, entry in enumerate(entries[: settings.hot_boards_per_section], start=1):
        if not isinstance(entry, dict):
            continue
        htid = entry.get("htid")
        if htid is None:
            continue
        members: list[str] = []
        for token in entry.get("StockListNew") or []:
            code = _concept_code_from_token(str(token))
            if code is not None and code not in members:
                members.append(code)
            if len(members) >= settings.theme_member_cap:
                break
        themes.append(
            HotTheme(
                rank=rank,
                htid=str(htid),
                name=str(entry.get("nickname") or f"话题{htid}"),
                url=topic_url(htid),
                member_board_codes=members,
            )
        )
    return themes


def fetch_hot_rankings(client: GubaClient, settings: GubaSettings) -> HotRankings:
    """Fetch all three homepage rankings. A failing section yields an empty list."""
    return HotRankings(
        hot_stock=_fetch_hot_stock(client, settings),
        hot_concept=_fetch_hot_concept(client, settings),
        hot_theme=_fetch_hot_theme(client, settings),
    )
