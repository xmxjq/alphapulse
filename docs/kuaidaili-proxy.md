# Kuaidaili Private Proxy

AlphaPulse can extract and cache short-lived private proxies from a Kuaidaili
GetDPS API URL. The generated URL is a credential and must stay outside Git.

## Configure the API URL

Save the generated GetDPS URL in:

```text
.runtime/kuaidaili-api-url.txt
```

Authorize the worker's public IPv4 in the Kuaidaili console. IP whitelist
authentication is preferred because every guba request uses HTTPS CONNECT.

## Configure AlphaPulse

```toml
[crawl.proxy]
enabled = true
provider = "kuaidaili"
sources = ["guba"]
max_attempts = 2
fail_open = false

[crawl.kuaidaili]
api_url_file = ".runtime/kuaidaili-api-url.txt"
batch_size = 5
low_watermark = 2
lease_ttl_seconds = 600
cooldown_seconds = 600
acquire_timeout_seconds = 20
failure_threshold = 3

[sources.guba.browser]
enabled = false
```

The provider overrides the `num` parameter in the saved URL with `batch_size`,
accepts both text and JSON API responses, rotates cached proxies round-robin,
and benches proxies immediately after explicit blocks or proxy setup failures.
Transient transport failures keep using the same proxy until
`failure_threshold` consecutive failures; any success resets that streak.

For a package billed per extracted IP with a stated 5-10 minute lifetime, use
one IP at a time and retire it conservatively before the minimum lifetime:

```toml
[crawl.kuaidaili]
batch_size = 1
low_watermark = 0
lease_ttl_seconds = 240
cooldown_seconds = 300
failure_threshold = 3

[sources.guba]
request_interval_min_seconds = 4.0
request_interval_max_seconds = 8.0
```

`fail_open = false` is required for guba. If extraction fails or every cached
proxy is unavailable, the request fails without falling back to the worker's
direct public IP.

## Recovery

Keep both `crawler` and `guba_browser` stopped while changing proxy settings.
Run a one-off list and post-detail check first. Start only `crawler` after both
responses contain their expected embedded payloads; the persistent browser is
not needed when direct detail requests work through the private proxies.
