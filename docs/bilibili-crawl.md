# Bilibili Crawl

This guide shows how to enable and run the Bilibili video comment crawler in AlphaPulse.

## What It Supports

- Manual video targets only
- Accepted target formats:
  - Canonical video URL: `https://www.bilibili.com/video/BV...`
  - BVID: `BV...`
  - AVID: `av...`
- Video metadata normalization into the shared `posts` and `authors` tables
- Full comment thread refresh into the shared `comments` table

## 1. Enable Bilibili In `settings.toml`

Add or update this section in your runtime config:

```toml
[sources.bilibili]
enabled = true
api_base_url = "https://api.bilibili.com"
web_base_url = "https://www.bilibili.com"
sort_mode = 3
page_size = 30
max_pages = 1000

[sources.bilibili.cookies]
# SESSDATA = "..."
```

Notes:

- `sort_mode = 3` means sort by time.
- `page_size = 30` is the Bilibili API maximum.
- Cookies are optional. Start in guest mode and only add cookies if Bilibili starts rate-limiting or restricting responses.

## 2. Add Video Seeds In `seed_catalog.toml`

The current implementation uses manual seed targets.

Example:

```toml
[[logical_sets]]
name = "bili-core"
generators = ["bili-manual"]

[[generators]]
name = "bili-manual"
type = "manual"
bilibili_video_targets = [
  "BV1xx411c7mu",
  "https://www.bilibili.com/video/BV1xx411c7mu",
  "av123456",
]
```

## 3. Validate Config

Before running the crawler:

```bash
uv run alphapulse --config settings.toml validate-config
```

This checks the TOML structure and the shared seed catalog.

## 4. Run The Crawler

Run one cycle locally:

```bash
uv run alphapulse --config settings.toml run --once
```

Run the long-lived service:

```bash
uv run alphapulse --config settings.toml run
```

Backfill one logical seed set:

```bash
uv run alphapulse --config settings.toml backfill --seed-set bili-core
```

Refresh compiled seeds only:

```bash
uv run alphapulse --config settings.toml refresh-seeds --seed-set bili-core
```

## 5. Docker / Prod Usage

In this repo’s Docker setup, the crawler runs with:

```bash
uv run alphapulse --config /app/settings.toml run
```

Because `docker-compose.yml` mounts the repo into `/app`, update the host files:

- `settings.toml`
- the seed catalog file referenced by `sources.xueqiu.seed_catalog_path`

Then restart the service:

```bash
docker compose restart crawler
```

## Behavior Notes

- Bilibili support is a first-class source, not a standalone crawler command.
- The service discovers tasks from enabled adapters. Bilibili only emits tasks for `bilibili_video_targets`.
- Posts are stored with `source = "bilibili"` and `source_entity_id = aid`.
- Comments are refreshed after post metadata is written.
- Main comment pages are fetched sequentially. Sub-replies are fetched concurrently, capped by `crawl.concurrent_requests`.

## Troubleshooting

If Bilibili crawl is not running:

- Confirm `[sources.bilibili].enabled = true`
- Confirm the seed catalog actually contains `bilibili_video_targets`
- Run `validate-config` and check the normalized output
- Run `run --once` to verify one cycle can resolve video metadata and comments
- If guest mode starts failing, add cookies under `[sources.bilibili.cookies]`
