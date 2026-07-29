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
- `GET /api/guba/report/{date}`
- `GET /api/llm/guba/report/{date}` (`text/toon`)
- `GET /api/tgb/report/{date}`
- `GET /api/llm/tgb/report/{date}` (`text/toon`)

The LLM report endpoints return the day's full post bodies and the comments
already stored for each post in [TOON](https://github.com/toon-format/toon)
format. Empty ranking boards are omitted to reduce tokens. Each source has its
own schema identifier, while both payloads use four flat tables so TOON can
apply tabular encoding:

- `sections`: report section metadata
- `boards`: boards keyed by `code` and linked through `section_key`
- `posts`: posts linked through `board_code`
- `comments`: comments linked through `post_id`

Optional query parameters:

- `limit=500` limits the number of daily posts (maximum 5000)
- `include_comments=false` omits comments for a lighter prompt
- `max_comments_per_post=100` caps included comments per post

Example:

```bash
curl -H "Accept: text/toon" \
  "http://127.0.0.1:8000/api/llm/guba/report/2026-07-24?limit=200"

curl -H "Accept: text/toon" \
  "http://127.0.0.1:8000/api/llm/tgb/report/2026-07-24?limit=200"
```

Allowed `source` values are `bilibili`, `xueqiu`, `guba`, and `tgb`.

## Troubleshooting

If the page loads but looks empty:

- Confirm the crawler has already written data to the configured storage backend
- Confirm the dashboard is using the same `settings.toml` as the crawler
- Confirm the dashboard can read the same local `crawl.state_path` file as the crawler

If the container starts but `localhost:8000` is unreachable:

- Confirm the `web` service is running: `docker compose ps`
- Confirm nothing else is already bound to port `8000`
- Confirm the process is started with `--host 0.0.0.0` inside the container
