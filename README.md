# AlphaPulse

AlphaPulse is a Python-first crawling platform for finance data collection. The current scaffold supports `xueqiu.com`, Bilibili video comments, and Eastmoney Guba (股吧) boards, uses `Scrapling` where browser-style fetching is needed, supports `rqlite`, `ClickHouse`, and `MongoDB` storage backends, and runs as a long-lived crawl service.

## What is included

- Pluggable source adapter contract for future sites
- `XueqiuAdapter` with:
  - generated seed snapshots compiled from a seed catalog
  - homepage discovery
  - post extraction
  - full comment thread refresh via API template
- `BilibiliAdapter` with:
  - manual video seed support via canonical URL, `BV...`, or `av...`
  - UP主 space discovery via `bilibili_space_urls`
  - video metadata normalization into the shared post/author models
  - full video comment thread refresh via Bilibili JSON APIs
- `GubaAdapter` (Eastmoney 股吧) with:
  - stock and index board seeds via `guba_board_codes`
  - post lists, details, and full reply threads without cookies or JS
  - edit/deletion/resurfacing tracking (see `docs/guba-crawl.md`)
- Configurable storage backends:
  - `rqlite` for lightweight remote persistence
  - `ClickHouse` for analytical storage
  - `MongoDB` for document storage (managed or via `docker-compose.mongo.yml`)
- Durable local crawl state in SQLite
- Optional raw-response archive: sha256-addressed gzip blobs plus a SQLite fetch log (`[crawl.raw_store]`)
- Proxy providers: `proxy_pool` sidecar or `static_list` for self-hosted xray tunnels (see `docs/xray-proxy.md`)
- CLI commands for config validation, DB init, health checks, run loop, and backfill
- Read-only web dashboard for crawl status, seed-set inventory, posts, and comments
- Docker Compose for the crawler; storage is external (rqlite/ClickHouse/Mongo) via `settings.toml`

## Quick start

1. Create a local config from the examples:

   ```bash
   cp settings.example.toml settings.toml
   cp seed_catalog.example.toml seed_catalog.toml
   ```

2. Install dependencies:

   ```bash
   uv sync
   ```

3. Initialize the configured storage backend:

   ```bash
   uv run alphapulse --config settings.toml init-db
   ```

4. Run one crawl cycle:

   ```bash
   uv run alphapulse --config settings.toml run --once
   ```

   Or refresh generated seed snapshots without crawling:

   ```bash
   uv run alphapulse --config settings.toml refresh-seeds
   ```

5. Start the interactive SQL shell against the configured storage backend (`rqlite` and `clickhouse` only — for the `mongo` backend use `mongosh` instead):

   ```bash
   uv run alphapulse --config settings.toml sql --pretty
   ```

   Or use the standalone wrapper:

   ```bash
   uv run python scripts/sql_cli.py --config settings.toml --pretty
   ```

   You can still run one-shot SQL:

   ```bash
   uv run alphapulse --config settings.toml sql --pretty "SELECT * FROM posts LIMIT 5"
   ```

6. Start the crawler container:

   ```bash
   docker compose up --build
   ```

   Start the crawler and web dashboard together:

   ```bash
   docker compose up --build crawler web
   ```

   Then open `http://localhost:8000`.

   If you want to initialize the remote `rqlite` schema from inside the container first:

   ```bash
   docker compose run --rm crawler uv run alphapulse --config /app/settings.toml init-db
   ```

7. Optional: run the dashboard directly on your workstation:

   ```bash
   uv run alphapulse --config settings.toml web --host 127.0.0.1 --port 8000
   ```

8. Optional: route crawl traffic through proxies when a source blocks your crawler IP. Two providers are available:

   `proxy_pool` — free rotating public proxies as a sidecar:

   ```bash
   docker compose --profile proxy up -d
   curl http://localhost:5010/count/   # verify it is serving proxies
   ```

   ```toml
   [crawl.proxy]
   enabled = true
   provider = "proxy_pool"
   max_attempts = 2
   fail_open = false

   [crawl.proxy_pool]
   base_url = "http://proxy_pool:5010"
   https_only = true
   acquire_timeout_seconds = 3
   report_bad_on_block = true
   ```

   `static_list` — your own fixed upstreams, e.g. xray tunnels run by the `xray` compose profile. Generate the xray config from share links with `scripts/xray_config_from_links.py`; full walkthrough in `docs/xray-proxy.md`:

   ```toml
   [crawl.proxy]
   enabled = true
   provider = "static_list"
   sources = ["guba"]   # keep cookie-authenticated sources on the direct path

   [crawl.static_proxies]
   urls = ["http://xray:10809", "http://xray:10810"]
   cooldown_seconds = 300
   ```

## Storage

- `rqlite` is the default backend in `settings.example.toml` and is intended for lighter remote persistence.
- The Docker Compose file assumes you are pointing the crawler and dashboard at an external `rqlite`, ClickHouse, or MongoDB instance via `settings.toml`; it does not start a database container.
- `ClickHouse` support remains available by setting `storage.backend = "clickhouse"` and pointing the crawler at an existing ClickHouse instance.
- `MongoDB` support is enabled with `storage.backend = "mongo"` and a `[mongo]` URI. To self-host, `docker-compose.mongo.yml` runs Mongo 7 as a standalone compose project with auth from `.env`; the crawler containers reach it at `host.docker.internal:27017`. `scripts/mongo_backup.sh` plus the launchd plist in `scripts/` provide scheduled `mongodump` backups with retention.

## Notes

- The dashboard is read-only. It serves a small static UI plus JSON endpoints from `alphapulse web`.
- Posts, comments, runs, and errors are read from the configured storage backend. Seed-set summaries and recent URL activity are read from the local SQLite state file at `crawl.state_path`, so the crawler and dashboard must share the same mounted `.runtime` directory.
- `xueqiu.com` is behind WAF protection. The default config runs in guest mode, but the config already supports cookie injection and fetch-mode selection.
- Bilibili video comments use public JSON APIs in guest mode by default. Optional cookies can be supplied under `[sources.bilibili.cookies]` when rate limits or access restrictions appear.
- Guba requires no cookies or JS; request pacing is randomized with adaptive backoff, and blocked responses are classified into the fetch log. See `docs/guba-crawl.md` for mechanics and the live smoke test.
- Proxy support is provider-based: the `proxy_pool` sidecar (free public proxies, unstable but zero-setup) or `static_list` (fixed upstreams such as self-hosted xray tunnels, see `docs/xray-proxy.md`). `crawl.proxy.sources` scopes proxying per source.
- The proxy abstraction is intentionally generic so stronger external pools, including paid residential or mobile providers, can be added later without rewriting the adapters.
- The comments API template is configurable because Xueqiu can change its endpoints and payload shapes.
- Seed generation is configured in `seed_catalog.toml`; `settings.toml` now points at the catalog and controls refresh cadence plus TTL.
- V1 seed generator types are `manual`, `stock_universe`, and `longhubang`. The built-in providers are deterministic and file-backed.
- `manual` generators can now include `bilibili_video_targets = ["https://www.bilibili.com/video/BV...", "BV...", "av..."]`, `bilibili_space_urls = ["https://space.bilibili.com/<mid>"]`, and `guba_board_codes = ["600519", "zssh000001"]`.
- Storage backends hold normalized records; raw HTTP responses can additionally be archived to disk by enabling `[crawl.raw_store]` (content-addressed gzip blobs plus a per-fetch SQLite log).
