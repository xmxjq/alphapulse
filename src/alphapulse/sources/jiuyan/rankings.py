from __future__ import annotations

import logging
from dataclasses import dataclass

from alphapulse.runtime.config import JiuyanSettings
from alphapulse.sources.jiuyan.api import JiuyanClient


logger = logging.getLogger(__name__)


@dataclass
class HotTarget:
    rank: int
    keyword: str


def fetch_hot_targets(
    client: JiuyanClient, settings: JiuyanSettings
) -> list[HotTarget]:
    result = client.hot_rankings()
    payload = result.json()
    if result.status_code == 0 or result.blocked or payload is None:
        logger.warning(
            "Jiuyan hot-target fetch failed",
            extra={
                "event": "jiuyan_ranking_fetch_failed",
                "extra_data": {
                    "status_code": result.status_code,
                    "block_kind": result.block_kind,
                    "error": result.error_message,
                },
            },
        )
        return []
    data = payload.get("data")
    rows = data.get("hot_search_list") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    targets: list[HotTarget] = []
    seen: set[str] = set()
    for row in rows:
        keyword = (
            str(row.get("keyword") or "").strip() if isinstance(row, dict) else ""
        )
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        targets.append(HotTarget(rank=len(targets) + 1, keyword=keyword))
        if len(targets) >= settings.hot_targets_limit:
            break
    return targets
