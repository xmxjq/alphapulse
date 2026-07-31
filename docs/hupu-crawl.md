# Hupu Stock Board Crawl

The `hupu` source crawls the server-rendered Hupu stock board under a written,
time-limited authorization. Set the agreement's actual expiry date in
`sources.hupu.authorization_expires_on`; discovery and pending work stop after
that date while stored reports remain readable.

Discovery uses the newest-first feed:

```text
https://bbs.hupu.com/stock-postdate
https://bbs.hupu.com/stock-postdate-2
```

The adapter follows pages until the current Beijing-day boundary, then fetches
each unique `/{post_id}.html` detail page. Details are queued newest-first.
Comments are disabled by default. Reported reply and light counts are retained.

All posts belong to the `stock` board. Full title and body text are also matched
against the configured aliases for 上证指数、创业板指、科创50 and 上证50. A post
may therefore carry several `board_codes`, but storage and LLM output deduplicate
it by Hupu post ID.

```toml
[sources.hupu]
enabled = true
authorization_expires_on = 2029-07-31
max_list_pages = 6
request_interval_min_seconds = 5.0
request_interval_max_seconds = 9.0
concurrent_paid_requests = 1
concurrent_agent_requests = 1
fetch_comments = false
```

When Kuaidaili and the agent pool are both enabled for `hupu`, list discovery
stays on the paid route. Detail work uses one paid slot plus at most one
currently available agent slot. Capacity races fall back to the paid route.
