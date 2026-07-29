# 淘股吧 (tgb.cn) Crawl

A day-scoped crawler for the 淘股吧 stock forum that produces a daily "newspaper"
report, mirroring the [Guba crawl](guba-crawl.md). It is a second `SourceAdapter`
(`source = "tgb"`) wired into the same runtime, storage, and web dashboard.

## What It Supports

- **精华 (featured)** and **社区总版 (general)** feeds, always crawled.
- **Self-discovered hot-stock boards**: the homepage 热门研股 (hot research stocks)
  ranking is parsed for the top-N stock codes, and each stock's `/quotes/{code}`
  discussion feed is crawled.
- **First-page comments** for each post (the reply list is server-rendered inline).
- A **daily report** at `/report/tgb/{date}` splitting the day into **Featured** vs
  **General** (hot-stock boards + general feed), grouped by board.

## How It Works

tgb.cn is server-rendered HTML (unlike guba's SPA), so parsing is DOM-based
(`lxml` + CSS selectors) rather than embedded-JSON extraction. Code lives under
`src/alphapulse/sources/tgb/` (`urls`, `api`, `parser`, `rankings`, `adapter`).

- **Discovery.** The `tgb_hot_boards` seed generator emits the featured slug
  (`jinghua`), the general slug (`zongban`), and the top-N 热门研股 stock codes as
  `tgb_board_code` seed items, and snapshots that day's ordered membership into the
  `tgb_daily_ranking` state table (section `featured` for jinghua, `general` for
  zongban + the stock boards).
- **List crawl.** `/zongban/{page}/{flag}` and `/jinghua/{page}-{flag}` (note the
  differing separators) are fetched with `flag=1` (post-date-descending sort) so the
  crawler can day-scope: it emits `fetch_post` tasks only for posts published today
  and keeps paginating while a page still holds a today post, capped at
  `max_list_pages`. Per-stock `/quotes/{code}` feeds are `#forumRow_*` mention blocks
  whose underlying `/a/{id}` posts are de-duplicated and day-scoped by their timestamp.
- **Board attribution.** A post's `/a/{id}` URL carries no board, so the board code is
  threaded through task metadata into `raw_topic_ids[0]`. Featured discovery has the
  highest priority so a post appearing in several feeds keeps its featured attribution
  (the state claim gate keeps only the first fetch of a post URL per recrawl window).
- **Comments.** Post detail (`/a/{id}`) yields the post + author; the first page of
  `.comment-data` reply blocks is captured via `refresh_comments` (re-fetching the post
  page), matching guba's two-request-per-post model.

## 1. Enable tgb in `settings.toml`

```toml
[sources.tgb]
enabled = true
base_url = "https://www.tgb.cn"
featured_slug = "jinghua"
general_slug = "zongban"
max_list_pages = 3
hot_stocks_limit = 12
day_scoped = true
ranking_timezone = "Asia/Shanghai"
```

tgb.cn returns **HTTP 502 to datacenter IPs**, so from a cloud/dev box you must crawl
through a proxy — configure `[crawl.proxy]` / `[crawl.static_proxies]` (see
[xray-proxy.md](xray-proxy.md)). The daily report also requires the **mongo** storage
backend.

For the production worker, include `tgb` in both rotating transport scopes:

```toml
[crawl.proxy]
sources = ["guba", "tgb"]

[crawl.agent_pool]
sources = ["guba", "tgb"]
allowed_hosts = [
  "guba.eastmoney.com",
  "emappdata.eastmoney.com",
  "push2.eastmoney.com",
  "www.tgb.cn",
]
```

TGB requests prefer an online self-hosted Agent and fall back to the configured
paid proxy pool. A blocked detail page is isolated while another rotating exit
remains available; a blocked list page still stops the source for that cycle.

## 2. Add the seed set

`seed_catalog.example.toml` ships a `tgb-daily` logical set:

```toml
[[logical_sets]]
name = "tgb-daily"
generators = ["tgb-hot"]

[[generators]]
name = "tgb-hot"
type = "tgb_hot_boards"
# include_featured = true
# include_general = true
# hot_stocks_limit = 12
```

## 3. Run

```bash
uv run alphapulse --config settings.toml refresh-seeds    # discover + snapshot ranking
uv run alphapulse --config settings.toml run --once       # crawl today's posts
uv run alphapulse --config settings.toml web              # dashboard + report
# then browse http://127.0.0.1:8000/report/tgb/2026-07-22
```

The long-lived `run` loop re-derives "today" each cycle; keep `max_list_pages` modest
so a cycle finishes within a day and each day gets its own ranking snapshot.

## Daily report page

`/report/tgb/{date}` (and `/api/tgb/report/{date}`) render the day's posts under the
Featured (精华) and General (热门个股 / 综合) sections, grouped by board, with the day's
ranking snapshot. Comments load lazily per post. A source switcher toggles between the
tgb and guba reports. The report models and query machinery are shared between sources
(`WebQueries._daily_report`).

For LLM integrations, `GET /api/llm/tgb/report/{date}` returns the same day's
full stored post bodies and comments in compact TOON format. See
[llm-report-api.md](llm-report-api.md).

## Live Smoke Test

```bash
docker compose --profile xray up -d xray
# hot-stock discovery only:
uv run python scripts/tgb_live_check.py --hot-stocks --proxy http://127.0.0.1:10809
# full list -> detail -> comments:
uv run python scripts/tgb_live_check.py \
    --boards jinghua,zongban,sz000938 --details 15 --comment-posts 5 \
    --proxy http://127.0.0.1:10809
```

Prints success rate, status/block distribution, parser errors, latency, and a proxy
verdict. See the `tgb-crawl-structure` project memory for the reverse-engineered page
structure the parsers rely on.
