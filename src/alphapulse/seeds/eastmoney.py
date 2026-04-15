from __future__ import annotations

import json
from datetime import datetime
from urllib.request import Request, urlopen

from alphapulse.seeds.catalog import LonghubangRecord


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)


def fetch_eastmoney_longhubang_page(url: str, timeout_seconds: int = 20) -> str:
    request = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8")


def parse_eastmoney_longhubang_page(html: str) -> list[LonghubangRecord]:
    marker = "var pagedata="
    start = html.find(marker)
    if start < 0:
        raise ValueError("Could not locate Eastmoney pagedata payload")
    start += len(marker)

    end_marker = "};\n</script>"
    end = html.find(end_marker, start)
    if end < 0:
        raise ValueError("Could not locate Eastmoney pagedata terminator")

    payload = json.loads(html[start : end + 1])
    rows = (((payload.get("sbgg_all") or {}).get("result") or {}).get("data") or [])

    entries: list[LonghubangRecord] = []
    for index, row in enumerate(rows, start=1):
        security_code = row.get("SECURITY_CODE")
        market_suffix = row.get("MARKET_SUFFIX")
        trade_date = row.get("TRADE_DATE")
        if not security_code or not market_suffix or not trade_date:
            continue

        net_amount = row.get("BILLBOARD_NET_AMT")
        ranking_mode = "net_buy" if net_amount is not None and net_amount >= 0 else "net_sell"

        entries.append(
            LonghubangRecord(
                stock_id=f"{market_suffix}{security_code}",
                trade_date=datetime.strptime(trade_date, "%Y-%m-%d %H:%M:%S").date(),
                market=market_suffix,
                ranking_mode=ranking_mode,
                rank=index,
            )
        )

    return entries
