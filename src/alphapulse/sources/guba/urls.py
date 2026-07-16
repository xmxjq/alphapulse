from __future__ import annotations

import re


POST_DETAIL_RE = re.compile(r"/news,([0-9a-zA-Z]+),(\d+)\.html")


def board_list_url(base_url: str, code: str, page: int = 1) -> str:
    base = base_url.rstrip("/")
    if page <= 1:
        return f"{base}/list,{code}.html"
    return f"{base}/list,{code}_{page}.html"


def post_detail_url(base_url: str, code: str, post_id: str) -> str:
    return f"{base_url.rstrip('/')}/news,{code},{post_id}.html"


def comment_refresh_url(base_url: str, code: str, post_id: str) -> str:
    """Stable synthetic URL used as the state-store claim key for reply refreshes.

    The actual request is a POST to /interface/GetData.aspx; this URL only
    needs to be unique and stable per post so claim gating works.
    """
    return f"{base_url.rstrip('/')}/interface/GetData.aspx?path=reply&code={code}&postid={post_id}"


def getdata_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/interface/GetData.aspx"


def extract_post_ref(url: str) -> tuple[str, str] | None:
    """Return (board_code, post_id) from a post detail URL."""
    match = POST_DETAIL_RE.search(url)
    if match is None:
        return None
    return match.group(1), match.group(2)
