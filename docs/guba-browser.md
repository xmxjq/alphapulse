# Authenticated Guba Browser Worker

Use a persistent Chromium session when guba post-detail pages require interactive
identity verification. List discovery and reply APIs remain on the normal HTTP
client; only `fetch_post` uses the browser.

## Start the browser

```bash
docker compose --profile guba-browser up -d --build guba_browser
```

The browser profile persists under `.runtime/guba-browser-profile`.

## Open the login UI

The noVNC listener binds to worker loopback only. From the operator machine:

```bash
ssh -L 6080:127.0.0.1:6080 <worker>
```

Keep that SSH session open, then visit:

```text
http://127.0.0.1:6080/vnc.html?autoconnect=1&resize=scale
```

Log in to Eastmoney inside Chromium and verify that a guba post page opens
without the slider challenge.

## Enable browser fetching

Add this to `settings.toml`:

```toml
[sources.guba.browser]
enabled = true
cdp_url = "http://guba_browser:9223"
navigation_timeout_seconds = 60
settle_timeout_seconds = 15
```

Then restart the crawler:

```bash
docker compose restart crawler
```

Do not copy browser cookies into `settings.toml`. The authenticated state stays
inside the persistent Chromium profile.

When the browser returns a rendered captcha, the fetch is recorded as blocked.
Reopen noVNC and complete any required interactive verification before resuming
the crawler.
