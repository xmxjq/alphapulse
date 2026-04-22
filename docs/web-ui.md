# Web UI

This guide covers the built-in AlphaPulse dashboard exposed by `alphapulse web`.

## What It Shows

- Latest crawl run summary
- Recent crawl runs and recent crawl errors
- Compiled seed-set inventory, including Bilibili-specific targets
- Post list with source filtering and pagination
- Post detail view with full content and associated comments

The dashboard is read-only. It does not trigger crawls or mutate storage.

## Runtime Model

The web process reads from two places:

- The configured storage backend for crawl runs, posts, comments, and crawl errors
- The local SQLite state file at `crawl.state_path` for compiled seed sets and recent URL activity

That split matters when you run the crawler and dashboard in separate processes or containers. They must share the same `settings.toml` and the same mounted `.runtime` directory if you want the seed-set and activity panels to stay accurate.

## 1. Configure `settings.toml`

Add or confirm this section:

```toml
[web]
host = "127.0.0.1"
port = 8000
```

Notes:

- `host = "127.0.0.1"` keeps the dashboard local by default
- In Docker, bind to `0.0.0.0` so the container port can be published
- The dashboard supports both storage backends already supported by AlphaPulse: `rqlite` and `ClickHouse`

## 2. Run Locally

Start the crawler if you want fresh data:

```bash
uv run alphapulse --config settings.toml run
```

Start the dashboard in another terminal:

```bash
uv run alphapulse --config settings.toml web --host 127.0.0.1 --port 8000
```

Then open `http://127.0.0.1:8000`.

## 3. Run With Docker Compose

This repo now includes a `web` service in `docker-compose.yml`.

Start both services:

```bash
docker compose up --build crawler web
```

Or just the dashboard:

```bash
docker compose up --build web
```

The compose service:

- Reuses `docker/crawler/Dockerfile`
- Runs `alphapulse web --host 0.0.0.0 --port 8000`
- Publishes `localhost:8000`
- Mounts the repo at `/app`, which lets it read `settings.toml` and the shared `.runtime/state.db`

Open `http://localhost:8000` after the container starts.

## 4. JSON API Surface

The UI is backed by these read-only endpoints:

- `GET /api/status`
- `GET /api/runs?limit=20`
- `GET /api/errors?limit=50&source=bilibili`
- `GET /api/seeds`
- `GET /api/posts?limit=50&offset=0&source=xueqiu`
- `GET /api/posts/{source}/{entity_id}`

Allowed `source` values are `bilibili` and `xueqiu`.

## Troubleshooting

If the page loads but looks empty:

- Confirm the crawler has already written data to the configured storage backend
- Confirm the dashboard is using the same `settings.toml` as the crawler
- Confirm the dashboard can read the same local `crawl.state_path` file as the crawler

If the container starts but `localhost:8000` is unreachable:

- Confirm the `web` service is running: `docker compose ps`
- Confirm nothing else is already bound to port `8000`
- Confirm the process is started with `--host 0.0.0.0` inside the container
