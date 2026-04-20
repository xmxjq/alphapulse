from __future__ import annotations

import re
from dataclasses import dataclass


BV_RE = re.compile(r"(BV[a-zA-Z0-9]{10})", re.IGNORECASE)
AV_RE = re.compile(r"(?:^|/)(av\d+)(?:$|[/?#])", re.IGNORECASE)
VIDEO_URL_RE = re.compile(r"bilibili\.com/video/([^/?#]+)", re.IGNORECASE)


@dataclass(frozen=True)
class VideoTarget:
    raw_input: str
    canonical_url: str
    bvid: str | None = None
    aid: int | None = None


def extract_bvid(value: str) -> str | None:
    match = BV_RE.search(value.strip())
    if match is None:
        return None
    return match.group(1)


def extract_aid(value: str) -> int | None:
    candidate = value.strip()
    if candidate.lower().startswith("av") and candidate[2:].isdigit():
        return int(candidate[2:])

    video_match = VIDEO_URL_RE.search(candidate)
    if video_match is not None:
        tail = video_match.group(1)
        if tail.lower().startswith("av") and tail[2:].isdigit():
            return int(tail[2:])

    match = AV_RE.search(candidate)
    if match is None:
        return None
    avid = match.group(1)
    if not avid.lower().startswith("av") or not avid[2:].isdigit():
        return None
    return int(avid[2:])


def build_video_url(web_base_url: str, bvid: str) -> str:
    return f"{web_base_url.rstrip('/')}/video/{bvid}"


def parse_video_target(value: str, web_base_url: str) -> VideoTarget | None:
    raw = value.strip()
    if not raw:
        return None

    bvid = extract_bvid(raw)
    if bvid is not None:
        return VideoTarget(raw_input=raw, canonical_url=build_video_url(web_base_url, bvid), bvid=bvid)

    aid = extract_aid(raw)
    if aid is not None:
        return VideoTarget(
            raw_input=raw,
            canonical_url=f"{web_base_url.rstrip('/')}/video/av{aid}",
            aid=aid,
        )

    return None
