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
sources = ["guba", "tgb", "jiuyan"]
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
share_across_sources = true
use_api_expiry = true
expiry_safety_seconds = 30

[sources.guba.browser]
enabled = false
```

The provider overrides the `num` parameter in the saved URL with `batch_size`,
accepts both text and JSON API responses, rotates cached proxies round-robin,
and benches proxies immediately after explicit blocks (HTTP 403/407/418/429)
or proxy setup failures. A `soft_block` classification (a 200 response
missing its expected data marker — the only signal `classify_block` has for
a WAF/interstitial page) is a heuristic, not a confirmed block, so it goes
through the same `failure_threshold` streak as ordinary transient transport
failures instead of an instant bench: with `batch_size=1`/`low_watermark=0`,
an instant bench empties the pool and forces an immediate re-extraction, so
treating `soft_block` as hard would turn one blocked request into up to
`max_retries` paid extractions instead of one. Any success resets the streak.

Guba and TGB requests with an acquired proxy use the browser-impersonating
`curl_cffi` transport because some private proxy exits truncate urllib's
HTTP/1 response stream even when the upstream page is complete.

The WebUI `Proxy pool` tab attributes leases, successes, failures, API errors,
and empty-pool events to Guba, TGB, or Jiuyan. Events recorded before source
attribution was introduced remain visible as `unknown`.

A failure to acquire a proxy at all (Kuaidaili GetDPS API error, or an empty
pool) is retried within the same request up to `sources.guba.max_retries` /
`crawl.proxy.max_attempts`, the same as any other transient failure — it does
not fail the request outright.

For a package billed per extracted IP with a stated 5-10 minute lifetime, use
one IP at a time and retire it conservatively before the minimum lifetime:

```toml
[crawl.kuaidaili]
batch_size = 1
low_watermark = 0
lease_ttl_seconds = 240
cooldown_seconds = 300
failure_threshold = 3
share_across_sources = true
use_api_expiry = true
expiry_safety_seconds = 30

[sources.guba]
request_interval_min_seconds = 4.0
request_interval_max_seconds = 8.0
```

`fail_open = false` is required for guba. If extraction fails or every cached
proxy is unavailable after retries, the request fails without falling back to
the worker's direct public IP.

`share_across_sources = true` makes the crawler's Guba, TGB, and Jiuyan
clients share one extracted-IP cache. Proxy health remains source-scoped: a
Guba block benches that exit for Guba without wasting its remaining paid life
on TGB or Jiuyan. This is most effective because source queues run in parallel
and target unrelated domains.

For an IP-count-billed order, `use_api_expiry = true` adds `f_et=1` to GetDPS.
Kuaidaili then appends the remaining lifetime in seconds to every extracted
proxy. AlphaPulse subtracts `expiry_safety_seconds` and stores the resulting
per-proxy expiry. `lease_ttl_seconds` remains the fallback when the API omits
the lifetime. The separate GetDpsValidTime endpoint does not support
IP-count-billed orders, so it is not used for this package.

For Kuaidaili, every bench lasts until at least that proxy's recorded expiry,
even when its API-reported lifetime is longer than `cooldown_seconds`. This
makes a hard block or repeated soft block terminal for that source while the
same shared IP remains available to other sources that have not blocked it.
`cooldown_seconds` is still the minimum bench duration when it extends beyond
the proxy lifetime.

Because `soft_block` no longer benches instantly, a request that keeps
hitting the same problematic proxy exit can now exhaust all of
`sources.guba.max_retries` on that one IP before the whole request comes back
blocked, instead of getting a fresh IP on every attempt. `GubaAdapter` accounts
for this: only a confirmed block (`http_403`/`http_429`/`captcha`/
`login_redirect`) arms the long `sources.guba.block_cooldown_seconds` circuit
breaker for the whole source. `soft_block` still marks that one
task/cycle as blocked (and is still retried within the request and counted in
proxy-pool metrics) but never triggers the multi-hour source-wide cooldown on
its own — only a genuinely confirmed block does.

## Recovery

Keep both `crawler` and `guba_browser` stopped while changing proxy settings.
Run a one-off list and post-detail check first. Start only `crawler` after both
responses contain their expected embedded payloads; the persistent browser is
not needed when direct detail requests work through the private proxies.
