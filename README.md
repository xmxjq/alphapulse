# AlphaPulse

AlphaPulse is a Python-first crawling platform for finance data collection. The v1 scaffold targets `xueqiu.com`, uses `Scrapling` for fetching, supports `rqlite` and `ClickHouse` storage backends, and runs as a long-lived crawl service.

## What is included

- Pluggable source adapter contract for future sites
- First `XueqiuAdapter` with:
  - configured seeds
  - homepage discovery
  - post extraction
  - full comment thread refresh via API template
- Configurable storage backends:
  - `rqlite` for lightweight remote persistence
  - `ClickHouse` for analytical storage
- Durable local crawl state in SQLite
- CLI commands for config validation, DB init, health checks, run loop, and backfill
- Docker Compose for local ClickHouse + crawler

## Quick start

1. Create a local config from the example:

   ```bash
   cp settings.example.toml settings.toml
   ```

2. Install dependencies:

   ```bash
   uv sync
   ```

3. Initialize the configured storage backend:

   ```bash
   uv run alphapulse init-db --config settings.toml
   ```

4. Run one crawl cycle:

   ```bash
   uv run alphapulse run --config settings.toml --once
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

6. Start the local stack:

   ```bash
   docker compose up --build
   ```

## Storage

- `rqlite` is the default backend in `settings.example.toml` and is intended for lighter remote persistence.
- `ClickHouse` support remains available by setting `storage.backend = "clickhouse"`.
- The local Docker Compose file still includes a `clickhouse` service for the analytical path; it is not required when you point the crawler at a remote `rqlite` node.

## Notes

- `xueqiu.com` is behind WAF protection. The default config runs in guest mode, but the config already supports cookie injection and fetch-mode selection.
- The comments API template is configurable because Xueqiu can change its endpoints and payload shapes.
- V1 stores normalized records only. Raw HTML retention is intentionally deferred.
