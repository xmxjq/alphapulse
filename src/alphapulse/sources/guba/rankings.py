"""Fetch the three guba homepage "hot" rankings and resolve them to crawlable boards.

The guba homepage renders three ranking widgets — 热门个股吧 (hot stock boards),
热门主题吧 (hot theme boards) and 热门概念吧 (hot concept boards). The widgets
themselves are React-rendered from partly-encrypted / CMS-hosted internal endpoints,
so instead we read the same underlying data from stable public East Money APIs:

* 热门个股吧 → the 人气榜 popularity API (``emappdata`` ``getAllCurrentList``), which
  returns ranked security ids we map to 6-digit guba board codes.
* 热门概念吧 → the push2 quote ``clist`` for concept sectors (``dpt=gb.rmbk``), whose
  ``f12`` field is the ``BK`` board code.
* 热门主题吧 → the CMS "bulletin" fragment (``/api/getBulletin`` POST) that the
  homepage widget renders. It is an HTML ``<li><a href="/list,{code}.html">`` list
  of theme boards (股市实战吧, 财经评论吧, index/market boards, …), each a normal board.

Every result is an ordinary ``/list,{code}.html`` board code, so the existing board
crawler consumes all three sections unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from alphapulse.runtime.config import GubaSettings
from alphapulse.sources.guba.api import GubaClient
from alphapulse.sources.guba.urls import board_list_url


logger = logging.getLogger(__name__)

SECTION_HOT_STOCK = "hot_stock"
SECTION_HOT_CONCEPT = "hot_concept"
SECTION_HOT_THEME = "hot_theme"

# emappdata security ids look like "SH600584" / "SZ001309" / "BJ430047".
_SECID_RE = re.compile(r"^(?P<mkt>[A-Za-z]{2})(?P<code>[0-9A-Za-z]+)$")
# Theme-board bulletin links: <a href="...list,gssz.html ...">股市实战吧</a>
_THEME_LINK_RE = re.compile(r"list,([0-9a-zA-Z]+)\.html[^>]*>\s*(.*?)\s*</a>", re.S)


@dataclass
class HotBoard:
    section: str
    rank: int
    board_code: str
    name: str
    url: str


@dataclass
class HotRankings:
    hot_stock: list[HotBoard] = field(default_factory=list)
    hot_concept: list[HotBoard] = field(default_factory=list)
    hot_theme: list[HotBoard] = field(default_factory=list)

    def board_codes(self) -> list[str]:
        """Ordered, de-duplicated crawlable board codes across every section."""
        ordered: list[str] = []
        seen: set[str] = set()
        for board in (*self.hot_stock, *self.hot_concept, *self.hot_theme):
            if board.board_code not in seen:
                seen.add(board.board_code)
                ordered.append(board.board_code)
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


def _fetch_text(client: GubaClient, url: str, *, payload: dict[str, Any] | None = None) -> str | None:
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
    return result.text


def _fetch_json(client: GubaClient, url: str, *, payload: dict[str, Any] | None = None) -> Any | None:
    text = _fetch_text(client, url, payload=payload)
    return _loads(text) if text is not None else None


def _board_code_from_secid(secid: str) -> str | None:
    match = _SECID_RE.match(secid.strip())
    if match is None:
        return None
    return match.group("code")


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


def _fetch_hot_theme(client: GubaClient, settings: GubaSettings) -> list[HotBoard]:
    # The bulletin fragment is served only over POST; the body is ignored.
    text = _fetch_text(client, str(settings.ranking_theme_url), payload={})
    if not text:
        return []
    base = str(settings.base_url)
    boards: list[HotBoard] = []
    seen: set[str] = set()
    for match in _THEME_LINK_RE.finditer(text):
        code = match.group(1)
        if code in seen:
            continue
        seen.add(code)
        name = re.sub(r"<[^>]+>", "", match.group(2)).strip() or code
        boards.append(
            HotBoard(
                section=SECTION_HOT_THEME,
                rank=len(boards) + 1,
                board_code=code,
                name=name,
                url=board_list_url(base, code),
            )
        )
        if len(boards) >= settings.hot_boards_per_section:
            break
    return boards


def fetch_hot_rankings(
    client: GubaClient,
    settings: GubaSettings,
    *,
    sections: set[str] | None = None,
) -> HotRankings:
    """Fetch all three homepage rankings. A failing section yields an empty list."""
    selected = sections or {
        SECTION_HOT_STOCK,
        SECTION_HOT_CONCEPT,
        SECTION_HOT_THEME,
    }
    return HotRankings(
        hot_stock=(
            _fetch_hot_stock(client, settings)
            if SECTION_HOT_STOCK in selected
            else []
        ),
        hot_concept=(
            _fetch_hot_concept(client, settings)
            if SECTION_HOT_CONCEPT in selected
            else []
        ),
        hot_theme=(
            _fetch_hot_theme(client, settings)
            if SECTION_HOT_THEME in selected
            else []
        ),
    )
