# AlphaPulse

AlphaPulse is a Python-first crawling platform for finance data collection. The v1 scaffold targets `xueqiu.com`, uses `Scrapling` for fetching, supports `rqlite` and `ClickHouse` storage backends, and runs as a long-lived crawl service.

## What is included

- Pluggable source adapter contract for future sites
- First `XueqiuAdapter` with:
  - generated seed snapshots compiled from a seed catalog
  - homepage discovery
  - post extraction
  - full comment thread refresh via API template
- Configurable storage backends:
  - `rqlite` for lightweight remote persistence
  - `ClickHouse` for analytical storage
- Durable local crawl state in SQLite
- CLI commands for config validation, DB init, health checks, run loop, and backfill
- Docker Compose for the crawler, configured to write to remote `rqlite`

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

5. Start the interactive SQL shell against the configured storage backend:

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

   If you want to initialize the remote `rqlite` schema from inside the container first:

   ```bash
   docker compose run --rm crawler uv run alphapulse --config /app/settings.toml init-db
   ```

## Storage

- `rqlite` is the default backend in `settings.example.toml` and is intended for lighter remote persistence.
- The Docker Compose file assumes you are pointing the crawler at an external `rqlite` node via `settings.toml`; it does not start a database container.
- `ClickHouse` support remains available by setting `storage.backend = "clickhouse"` and pointing the crawler at an existing ClickHouse instance.

## Notes

- `xueqiu.com` is behind WAF protection. The default config runs in guest mode, but the config already supports cookie injection and fetch-mode selection.
- The comments API template is configurable because Xueqiu can change its endpoints and payload shapes.
- Seed generation is configured in `seed_catalog.toml`; `settings.toml` now points at the catalog and controls refresh cadence plus TTL.
- V1 seed generator types are `manual`, `stock_universe`, and `longhubang`. The built-in providers are deterministic and file-backed.
- V1 stores normalized records only. Raw HTML retention is intentionally deferred.
