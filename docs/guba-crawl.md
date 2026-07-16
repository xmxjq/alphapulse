# Eastmoney Guba Crawl

This guide shows how to enable and run the Eastmoney Guba (东方财富股吧) crawler in AlphaPulse.

## What It Supports

- Stock boards (`600519`) and index boards (`zssh000001`) via `guba_board_codes` seeds
- Direct post URLs (`https://guba.eastmoney.com/news,600519,<post_id>.html`) via `post_urls` seeds
- Post lists, post details, and full reply threads (nested child replies flattened with parent links)
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
- **Replies** come from a form-encoded POST to `/interface/GetData.aspx`
  (`path=reply/api/Reply/ArticleNewReplyList`), returning plain JSON.
- **Resurfacing**: posts bumped by new replies reappear at the top of list
  pages (sorted by `post_last_time`). The adapter re-emits a stable-URL
  `refresh_comments` task for every listed post with comments; the state
  store's claim gate (`comment_refresh_minutes`) turns this into an
  incremental refresh instead of a re-crawl.
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
max_list_pages = 3
reply_page_size = 30
max_reply_pages = 40
list_recrawl_minutes = 30
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

```toml
[[logical_sets]]
name = "cn-core"
generators = ["cn-core-manual"]

[[generators]]
name = "cn-core-manual"
type = "manual"
guba_board_codes = ["zssh000001", "600519"]
```

## 3. Run

```bash
uv run alphapulse run --once
```

## Live Smoke Test

`scripts/guba_live_check.py` drives the real adapter against one index board
and one stock board and prints a report (success rate, status distribution,
block events, latency, proxy verdict). Exit code 1 on success rate < 90% or
any captcha/login redirect.

```bash
uv run python scripts/guba_live_check.py --boards zssh000001,600519 --list-pages 2 --details 15 --reply-posts 5
```

## Raw Store Layout

- Blobs: `{root_path}/blobs/{sha256[:2]}/{sha256}.gz` — content-addressed, so
  identical bodies are stored once. Load with
  `RawResponseStore(root).load_body(sha)`.
- Fetch log: `{root_path}/fetch_log.db` (SQLite) — one row per HTTP request
  with `fetched_at`, `url`, `method`, `task_kind`, `status_code`,
  `duration_ms`, `content_sha256`, `block_kind`
  (`http_403 | http_429 | http_5xx | captcha | login_redirect | empty_payload | deleted`),
  `parser_error`, and a `meta_json` blob (`post_id`, `board_code`, `page`,
  `post_mod_count`, ...).
