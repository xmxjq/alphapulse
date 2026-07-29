from __future__ import annotations

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from alphapulse.runtime.config import CrawlSettings, JiuyanSettings
from alphapulse.sources.jiuyan.api import JiuyanClient
from alphapulse.sources.jiuyan.parser import parse_post_detail, parse_search_page
from alphapulse.sources.jiuyan.rankings import fetch_hot_targets
from alphapulse.sources.jiuyan.urls import post_detail_url


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Jiuyan API smoke test")
    parser.add_argument("--pages", type=int, default=1)
    args = parser.parse_args()

    settings = JiuyanSettings(
        enabled=True,
        request_interval_min_seconds=0,
        request_interval_max_seconds=0,
    )
    client = JiuyanClient(settings, CrawlSettings())
    hot = fetch_hot_targets(client, settings)
    print("hot_targets", [item.keyword for item in hot])

    today = datetime.now(ZoneInfo(settings.ranking_timezone)).date()
    first_article_id: str | None = None
    for keyword in settings.fixed_targets:
        today_count = 0
        status_codes: list[int] = []
        for page_no in range(1, max(1, args.pages) + 1):
            response = client.search_articles(keyword, page_no)
            status_codes.append(response.status_code)
            page = parse_search_page(response.json() or {})
            if page is None:
                continue
            today_entries = [
                entry
                for entry in page.entries
                if entry.published_at is not None
                and entry.published_at.astimezone(
                    ZoneInfo(settings.ranking_timezone)
                ).date()
                == today
            ]
            today_count += len(today_entries)
            if first_article_id is None and today_entries:
                first_article_id = today_entries[0].article_id
        print(keyword, "statuses", status_codes, "today_posts", today_count)

    if first_article_id is None:
        print("detail", "skipped: no current-day fixed-target article")
        return
    response = client.article_detail(first_article_id)
    url = post_detail_url(str(settings.base_url), first_article_id)
    post, _ = parse_post_detail(
        response.json() or {},
        url,
        target_code=settings.fixed_targets[0],
    )
    print(
        "detail",
        response.status_code,
        first_article_id,
        "parsed",
        post is not None,
        "text_chars",
        len(post.content_text) if post is not None else 0,
    )


if __name__ == "__main__":
    main()
