# Jiuyan Gongshe Crawl

The `jiuyan` source crawls public content from [韭研公社](https://www.jiuyangongshe.com/)
into the same normalized post storage and daily-report pipeline used by `guba` and
`tgb`.

## Discovery

Every crawl starts with the three public latest-publish feeds:

- 研究优选 (`study`)
- 公社广场 (`square`)
- 生活区 (`live`)

These feeds are the completeness path: they discover current-day posts even when a
post does not match a configured keyword. Each feed is paginated until a page no
longer contains a post from the current Beijing day.

The crawl also includes four fixed search targets:

- 上证指数
- 创业板指
- 科创50
- 上证50

Each fixed target can define low-ambiguity search aliases. Alias results are
normalized back to the canonical target, and fetched post bodies are checked for
all fixed names and aliases. A market roundup that discusses several indices can
therefore appear under several fixed report boards without being fetched twice.

It also reads the public 公社热榜 and adds the top `hot_targets_limit` search
keywords. Fixed targets are ranked before dynamic targets, and duplicates are removed.

## Fetch strategy

The PC site is Nuxt-rendered, but its public JSON API is more stable and smaller than
scraping the DOM. Requests use the same timestamp and MD5 validation headers as the
site's web client.

- Ranking: `POST /api/v1/article/rank-board`
- Latest-publish feeds: `POST /api/v2/article/community`
- Search: `POST /api/v2/article/search`
- Detail: `POST /api/v2/article/detail?articleId={id}`

Search pages are requested newest-first. In `day_scoped` mode, only posts published on
the current Beijing day are emitted, and pagination stops after the first page without
today's posts or `max_search_pages`, whichever comes first.

Comments are disabled by default because they require another request per post.

## Enable

```toml
[sources.jiuyan]
enabled = true
fixed_targets = ["上证指数", "创业板指", "科创50", "上证50"]
fixed_target_aliases = { "上证指数" = ["沪指", "上证综指"], "创业板指" = ["创指", "创业板综指", "创业板ETF"], "科创50" = ["科创50ETF"], "上证50" = ["上证50ETF"] }
community_feeds = ["study", "square", "live"]
community_page_size = 30
max_community_pages = 12
hot_targets_limit = 10
max_search_pages = 3
day_scoped = true
fetch_comments = false
```

Add the source to rotating transports when desired:

```toml
[crawl.proxy]
sources = ["guba", "tgb", "jiuyan"]

[crawl.agent_pool]
sources = ["guba", "tgb", "jiuyan"]
allowed_hosts = [
  "guba.eastmoney.com",
  "emappdata.eastmoney.com",
  "push2.eastmoney.com",
  "www.tgb.cn",
  "app.jiuyangongshe.com",
]
```

The daily pages and LLM endpoint are:

```text
/report/jiuyan/{YYYY-MM-DD}
/api/jiuyan/report/{YYYY-MM-DD}
/api/llm/jiuyan/report/{YYYY-MM-DD}
```

Run a public API smoke test with:

```bash
uv run python scripts/jiuyan_live_check.py --pages 1
```
