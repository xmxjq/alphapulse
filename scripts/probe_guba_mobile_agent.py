from __future__ import annotations

import argparse
import json
import random
import signal
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib import parse, request

from alphapulse.runtime.agent_pool import (
    AgentJobFailed,
    AgentPoolClient,
    AgentPoolUnavailable,
    host_http_capability,
)
from alphapulse.runtime.config import load_settings


API_HOST = "mguba.eastmoney.com"
ARTICLE_API = f"https://{API_HOST}/api/getArticle"
LIST_API = "https://gbapi.eastmoney.com/webarticlelist/api/Article/Articlelist"
USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36"
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def write_event(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = {"at": utc_now().isoformat(), **event}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(rendered, ensure_ascii=False, separators=(",", ":")))
        handle.write("\n")
    print(json.dumps(rendered, ensure_ascii=False), flush=True)


def discover_post_ids(board_code: str, page_size: int = 80) -> list[str]:
    query = parse.urlencode(
        {
            "code": board_code,
            "type": "1",
            "p": "1",
            "ps": str(page_size),
            "sorttype": "1",
            "plat": "Web",
            "version": "2022",
            "product": "Guba",
            "deviceid": "1",
        }
    )
    opener = request.build_opener(request.ProxyHandler({}))
    req = request.Request(
        f"{LIST_API}?{query}",
        headers={"User-Agent": USER_AGENT, "Referer": "https://guba.eastmoney.com/"},
    )
    with opener.open(req, timeout=20) as response:
        payload = json.load(response)
    return [
        str(record["post_id"])
        for record in payload.get("re") or []
        if isinstance(record, dict) and record.get("post_id") is not None
    ]


def article_request(post_id: str) -> tuple[dict[str, str], bytes]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://mguba.eastmoney.com",
        "Referer": "https://mguba.eastmoney.com/",
    }
    body = parse.urlencode(
        {
            "deviceid": "ugc",
            "version": "200",
            "plat": "wap",
            "product": "guba",
            "ctoken": "",
            "utoken": "",
            "postid": post_id,
            "type": "0",
            "cutword": "true",
            "paytext": "true",
            "location": "",
            "env": "prod",
            "bizfrom": "ugc",
        }
    ).encode("utf-8")
    return headers, body


def parse_article_response(body: bytes, post_id: str) -> tuple[bool, dict[str, object]]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return False, {"validation": "invalid_json", "error": str(exc)}
    post = payload.get("post") if isinstance(payload, dict) else None
    content = post.get("post_content") if isinstance(post, dict) else None
    valid = bool(
        isinstance(payload, dict)
        and payload.get("rc") == 1
        and isinstance(post, dict)
        and str(post.get("post_id")) == post_id
        and content
    )
    return valid, {
        "rc": payload.get("rc") if isinstance(payload, dict) else None,
        "returned_post_id": post.get("post_id") if isinstance(post, dict) else None,
        "post_type": post.get("post_type") if isinstance(post, dict) else None,
        "content_length": len(content or ""),
        "validation": "ok" if valid else "incomplete_post",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Long-running Guba mobile API agent probe")
    parser.add_argument("--config", type=Path, default=Path("settings.toml"))
    parser.add_argument("--duration-hours", type=float, default=48.0)
    parser.add_argument("--interval-min-seconds", type=float, default=180.0)
    parser.add_argument("--interval-max-seconds", type=float, default=360.0)
    parser.add_argument("--board-code", default="600519")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(".runtime/guba-mobile-agent-probe.jsonl"),
    )
    parser.add_argument("--stop-after-consecutive-failures", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration_hours <= 0:
        raise SystemExit("--duration-hours must be positive")
    if args.interval_min_seconds <= 0 or args.interval_max_seconds < args.interval_min_seconds:
        raise SystemExit("invalid probe interval")

    settings = load_settings(args.config)
    client = AgentPoolClient(settings.crawl.agent_pool)
    capability = host_http_capability(API_HOST)
    deadline = utc_now() + timedelta(hours=args.duration_hours)
    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    post_ids: list[str] = []
    last_discovery = datetime.min.replace(tzinfo=UTC)
    consecutive_failures = 0
    write_event(
        args.output,
        {
            "event": "probe_started",
            "deadline": deadline.isoformat(),
            "capability": capability,
            "interval_seconds": [args.interval_min_seconds, args.interval_max_seconds],
        },
    )

    while not stopping and utc_now() < deadline:
        now = utc_now()
        if not post_ids or now - last_discovery >= timedelta(hours=1):
            try:
                post_ids = discover_post_ids(args.board_code)
                last_discovery = now
                write_event(
                    args.output,
                    {"event": "posts_discovered", "count": len(post_ids)},
                )
            except Exception as exc:
                write_event(
                    args.output,
                    {
                        "event": "discovery_failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )

        if not post_ids:
            time.sleep(min(300.0, args.interval_max_seconds))
            continue

        post_id = random.choice(post_ids)
        headers, body = article_request(post_id)
        try:
            response = client.fetch(
                source="guba",
                capability=capability,
                method="POST",
                url=f"{ARTICLE_API}?postid={post_id}",
                headers=headers,
                body=body,
                timeout_seconds=30,
                priority=-100,
            )
        except AgentPoolUnavailable as exc:
            write_event(
                args.output,
                {"event": "probe_skipped", "reason": str(exc)},
            )
        except AgentJobFailed as exc:
            consecutive_failures += 1
            write_event(
                args.output,
                {
                    "event": "probe_failed",
                    "post_id": post_id,
                    "error": str(exc),
                    "consecutive_failures": consecutive_failures,
                },
            )
        else:
            valid, details = parse_article_response(response.body, post_id)
            blocked = response.status_code in {403, 429}
            outcome = "success" if response.status_code == 200 and valid else "failure"
            if blocked:
                outcome = "blocked"
            client.store.record_outcome(
                response.job_id,
                outcome,
                None if outcome == "success" else str(details.get("validation")),
            )
            consecutive_failures = 0 if outcome == "success" else consecutive_failures + 1
            write_event(
                args.output,
                {
                    "event": "probe_result",
                    "outcome": outcome,
                    "agent_id": response.agent_id,
                    "post_id": post_id,
                    "status_code": response.status_code,
                    "response_bytes": len(response.body),
                    "duration_ms": response.duration_ms,
                    "consecutive_failures": consecutive_failures,
                    **details,
                },
            )

        if consecutive_failures >= args.stop_after_consecutive_failures:
            write_event(
                args.output,
                {
                    "event": "probe_stopped",
                    "reason": "consecutive_failures",
                    "count": consecutive_failures,
                },
            )
            return 1
        delay = random.uniform(args.interval_min_seconds, args.interval_max_seconds)
        time.sleep(min(delay, max(0.0, (deadline - utc_now()).total_seconds())))

    write_event(
        args.output,
        {"event": "probe_finished", "reason": "signal" if stopping else "deadline"},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
