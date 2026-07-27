# Eastmoney Guba Crawl

This guide shows how to enable and run the Eastmoney Guba (东方财富股吧) crawler in AlphaPulse.

## What It Supports

- **Day-grouped, ranking-driven crawling** — each day's seed boards come from the
  three guba homepage rankings, and the day's posts feed a newspaper-style report
  (see [Day Crawl & Daily Report](#day-crawl--daily-report))
- Stock boards (`600519`) and index boards (`zssh000001`) via `guba_board_codes` seeds
- Direct post URLs (`https://guba.eastmoney.com/news,600519,<post_id>.html`) via `post_urls` seeds
- Post lists, post details, and the newest reply page by default (nested child replies flattened with parent links)
- Original timestamps preserved: Guba's Beijing wall-clock times are converted to UTC
- Duplicate posts, edits, deletions, and resurfaced posts (see mechanics below)
- Raw-response archival plus a per-fetch log for debugging and parser reprocessing

## How It Works

- **List pages** (`/list,{code}.html`, page N `/list,{code}_{N}.html`) embed a
  `var article_list={...}` JSON payload with 80 posts per page, including
  `post_publish_time` and `post_last_time` (last-reply time). No login,
  cookies, or JavaScript execution is required.
- **Post details** (`/news,{code},{post_id}.html`) embed `var post_article={...}`
  with the full content, `post_mod_count`/`post_mod_time` (edit tracking), and
  `post_state`. Deleted/missing posts redirect to `/error?type=2` and are
  recorded with `block_kind = "deleted"`.
- **Soft blocks**: some egress IPs get served an HTTP 200 page without the
  `article_list` payload. The client detects the missing payload on list
  pages, records `block_kind = "soft_block"`, and retries with adaptive
  backoff and proxy rotation (up to `max_retries`). If every retry fails,
  the run records `Blocked (soft_block)` — switch egress (see
  `docs/xray-proxy.md`) or wait out the block.
- **Replies** come from a form-encoded POST to `/interface/GetData.aspx`
  (`path=reply/api/Reply/ArticleNewReplyList`), returning plain JSON. Daily
  crawling requests page 1 only by default; raise `max_reply_pages` only for
  an explicit historical backfill that needs complete reply threads. If a
  cycle still can't finish within a day, set `fetch_comments = false` to skip
  reply crawling entirely (post-only mode) — no `refresh_comments` tasks are
  emitted at all, unlike `max_reply_pages`, which still fetches replies just
  fewer pages of them.
- **Resurfacing**: posts bumped by new replies reappear at the top of list
  pages (sorted by `post_last_time`). The adapter re-emits a stable-URL
  `refresh_comments` task for every listed post with comments (unless
  `fetch_comments = false`); the state store's claim gate
  (`comment_refresh_minutes`) turns this into an incremental refresh instead
  of a re-crawl.
- **Duplicates**: the state store's URL claim (`post_recrawl_minutes`) plus
  `source:source_entity_id` upsert keys in storage dedupe re-listed posts.
- **Edits**: storage keeps the latest version; the fetch log records
  `content_sha256` and `post_mod_count` per fetch, and prior versions remain
  reconstructable from the raw blob archive.

## 1. Enable Guba In `settings.toml`

```toml
[sources.guba]
enabled = true
base_url = "https://guba.eastmoney.com"
max_list_pages = 3            # per-board page cap; keep modest so cycles finish daily
reply_page_size = 30
max_reply_pages = 1            # newest page only; raise for explicit backfills
fetch_comments = true          # set false for post-only crawling (no reply fetches at all)
list_recrawl_minutes = 30
day_scoped = true             # crawl the current day's posts (set false for classic)
ranking_timezone = "Asia/Shanghai"
hot_boards_per_section = 12
theme_member_cap = 8
request_interval_min_seconds = 2.0
request_interval_max_seconds = 6.0
max_retries = 3

[crawl.raw_store]
enabled = true
root_path = ".runtime/raw"
compress = true
```

Every request sleeps `uniform(min, max)` seconds times an adaptive backoff
multiplier (doubles on rate-limit responses, halves back down on success).
Keep the defaults conservative: live testing showed no blocking at these
rates from a single fixed IP, so no proxy pool is needed.

## 2. Add Boards To The Seed Catalog

Seed boards automatically from the homepage rankings (recommended):

```toml
[[logical_sets]]
name = "cn-core"
generators = ["guba-hot"]

[[generators]]
name = "guba-hot"
type = "guba_hot_boards"
# sections = ["hot_stock", "hot_concept", "hot_theme"]  # optional subset
```

Or pin boards manually (works alongside `guba_hot_boards`):

```toml
[[generators]]
name = "cn-core-manual"
type = "manual"
guba_board_codes = ["zssh000001", "600519"]
```

## 3. Run

```bash
uv run alphapulse run --once
```

## Day Crawl & Daily Report

By default (`day_scoped = true`) the guba crawl is organized around the calendar
day in `ranking_timezone` (Asia/Shanghai):

1. **Seeds from the homepage rankings.** The `guba_hot_boards` seed generator reads
   the three homepage rankings and seeds their boards:
   - **热门个股吧** — the 人气榜 popularity API (`emappdata` `getAllCurrentList`);
     ranked security ids map to 6-digit board codes.
   - **热门概念吧** — the push2 concept-sector list (`dpt=gb.rmbk`); `f12` is the
     `BK` board code.
   - **热门主题吧** — the CMS bulletin fragment (`/api/getBulletin`, POST) the
     homepage widget renders: an HTML list of theme boards (股市实战吧, 财经评论吧,
     index/market boards, …), each a normal `/list,{code}.html` board.

   Only the top `hot_boards_per_section` (~12, "the render part") of each ranking
   are taken. Sourcing uses stable public East Money endpoints rather than the
   homepage's encrypted/CMS widgets, so ordering may differ slightly from the live
   page but does not break when East Money rotates its client-side code.

2. **Day-scoped pagination.** For each seed board the adapter emits `fetch_post`
   tasks only for posts *published today*, and keeps paginating while a list page
   still holds a post active today (`post_last_time >= today 00:00`), up to the
   `max_list_pages` per-board cap. Because `last_time >= publish_time`, every post
   published today sorts above the first all-stale page. Keep `max_list_pages`
   modest: one crawl cycle refreshes seeds and writes the daily snapshot only at
   its **start**, so a cycle that runs longer than a day (e.g. uncapped full-day
   crawls across many boards through one proxy) never produces the next day's
   snapshot. Hitting the cap mid-day logs `guba_day_page_cap` (not silent).
   Page-1 discovery for all ranked boards runs ahead of post details, so hot
   stock, concept, and theme sections receive broad daily coverage before any
   single busy board can consume the detail queue.

3. **Ranking snapshot.** Every generator refresh records the day's ordered ranking
   membership (per section, plus each theme's member boards) in the crawler state
   DB, so the report can reproduce it even after the live rankings change. Because
   this happens at crawl-cycle start, cycles must finish within a day (hence the
   modest `max_list_pages`) for each day to get its own snapshot.

### Daily report page

Run the web dashboard and open `/report/<YYYY-MM-DD>` (defaults to today):

```bash
uv run alphapulse --config settings.toml web
# then browse http://127.0.0.1:8000/report/2026-07-21
```

The report is a light, print-friendly "newspaper": one section per ranking →
ranked boards (themes show their member boards) → the day's posts, with comment
threads loaded on demand. Backed by `GET /api/guba/report/{date}` (requires the
mongo storage backend). Snapshots come from the crawler state DB and posts from
storage; a day with no snapshot falls back to grouping crawled posts by board.

For feeding this same day's data to an LLM instead of a browser, see
[llm-report-api.md](llm-report-api.md) (`GET /api/llm/guba/report/{date}`).

## Live Smoke Test

`scripts/guba_live_check.py` drives the real adapter against one index board
and one stock board and prints a report (success rate, status distribution,
block events, latency, proxy verdict). Exit code 1 on success rate < 90% or
any captcha/login redirect.

```bash
uv run python scripts/guba_live_check.py --boards zssh000001,600519 --list-pages 2 --details 15 --reply-posts 5
```

To test through an xray tunnel (or any HTTP proxy) using the same
`static_list` provider the crawler uses in production, pass `--proxy`
(repeatable for a rotating pool):

```bash
uv run python scripts/guba_live_check.py --boards 600900 --list-pages 1 --details 4 --proxy http://127.0.0.1:10809
```

## Raw Store Layout

- Blobs: `{root_path}/blobs/{sha256[:2]}/{sha256}.gz` — content-addressed, so
  identical bodies are stored once. Load with
  `RawResponseStore(root).load_body(sha)`.
- Fetch log: `{root_path}/fetch_log.db` (SQLite) — one row per HTTP request
  with `fetched_at`, `url`, `method`, `task_kind`, `status_code`,
  `duration_ms`, `content_sha256`, `block_kind`
  (`http_403 | http_429 | http_5xx | captcha | login_redirect | soft_block | empty_payload | deleted`),
  `parser_error`, and a `meta_json` blob (`post_id`, `board_code`, `page`,
  `post_mod_count`, ...).
