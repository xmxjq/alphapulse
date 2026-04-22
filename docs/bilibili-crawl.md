# Bilibili Crawl

This guide shows how to enable and run the Bilibili video comment crawler in AlphaPulse.

## What It Supports

- Manual video targets
- Bilibili space discovery via `bilibili_space_urls`
- Accepted target formats:
  - Canonical video URL: `https://www.bilibili.com/video/BV...`
  - BVID: `BV...`
  - AVID: `av...`
- Accepted space target formats:
  - Canonical space URL: `https://space.bilibili.com/<mid>`
  - Numeric `mid`
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
space_discovery_backend = "cli"
space_discovery_interval_minutes = 60
space_discovery_max_videos = 50

[sources.bilibili.cookies]
# SESSDATA = "..."
```

Notes:

- `sort_mode = 3` means sort by time.
- `page_size = 30` is the Bilibili API maximum.
- `space_discovery_backend = "api"` uses AlphaPulse's built-in API client for space discovery.
- `space_discovery_backend = "cli"` uses `bilibili-cli` for space discovery and can reuse browser cookies or QR-login credentials managed by that tool.
- `space_discovery_interval_minutes` controls how often one `bilibili_space_urls` seed may be refreshed.
- `space_discovery_max_videos` caps how many recent videos AlphaPulse will ingest from one space discovery call.
- Direct video metadata and comment refresh still use AlphaPulse's built-in Bilibili client.

## 2. Add Seeds In `seed_catalog.toml`

You can mix direct video targets and space discovery in the same logical set.

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
bilibili_space_urls = [
  "https://space.bilibili.com/3493130286402061",
  "7033507",
]
```

## 3. Optional: Login With `bilibili-cli`

If you use `space_discovery_backend = "cli"`, log in with `bilibili-cli` before running the crawler.

Local environment:

```bash
uv run bili login
```

Docker Compose:

```bash
docker compose run --rm crawler uv run bili login
```

This stores credentials for `bilibili-cli`, which AlphaPulse reuses when it asks the tool for recent videos from a user space.

## 4. Validate Config

Before running the crawler:

```bash
uv run alphapulse --config settings.toml validate-config
```

This checks the TOML structure and the shared seed catalog.

## 5. Run The Crawler

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

## 6. Docker / Prod Usage

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

If you changed Python dependencies or switched to `space_discovery_backend = "cli"`, rebuild the image first:

```bash
docker compose build crawler web
```

## Behavior Notes

- Bilibili support is a first-class source, not a standalone crawler command.
- The service discovers tasks from enabled adapters.
- `bilibili_video_targets` emit direct `fetch_post` tasks.
- `bilibili_space_urls` emit `discover` tasks, which then expand into one `fetch_post` task per discovered recent video.
- Posts are stored with `source = "bilibili"` and `source_entity_id = aid`.
- Comments are refreshed after post metadata is written.
- Main comment pages are fetched sequentially. Sub-replies are fetched concurrently, capped by `crawl.concurrent_requests`.

## Troubleshooting

If Bilibili crawl is not running:

- Confirm `[sources.bilibili].enabled = true`
- Confirm the seed catalog contains either `bilibili_video_targets` or `bilibili_space_urls`
- Run `validate-config` and check the normalized output
- Run `backfill --seed-set bili-core` to test one logical set in isolation
- If `space_discovery_backend = "cli"`, confirm `uv run bili status` or `docker compose run --rm crawler uv run bili status` shows a valid login
- If `space_discovery_backend = "api"` starts returning `HTTP 412`, switch to the `cli` backend or refresh credentials
- If direct video or comment fetches start failing, refresh cookies under `[sources.bilibili.cookies]`
