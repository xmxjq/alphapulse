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
and benches proxies immediately after explicit blocks (HTTP 403/418/429)
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

## Authentication circuit breaker

An explicit proxy authentication failure (HTTP 407, including a curl CONNECT
tunnel response) immediately opens a persistent, pool-wide circuit. No cached
proxy can be leased by normal tasks, including after a crawler restart.
Ordinary source-specific blocks and timeouts keep the existing source-isolated
retry policy. A bounded half-open probe is the only automatic exception.

The marker is next to `metrics_path`, with `.auth-blocked` appended to its
filename. Operators may create this marker to hold paid traffic during an
incident. Keep `crawl.proxy.enabled = true` and `fail_open = false`: disabling
the proxy is not a safe substitute because it can enable direct requests.

Automatic 407 holds are probed after five minutes, then with 10, 20, 40 and
60 minute delays (capped at 60 minutes, plus up to 30 seconds of jitter).
Probes run at the next source cycle after the due time. SQLite reserves one
shared slot before network access, so parallel sources and restarts cannot
multiply attempts. Each probe reuses a valid, unbenched cached route or requests
exactly one paid IP, then issues one HTTPS HEAD to `https://www.baidu.com/`
with a 10-second timeout, TLS verification, no redirects and no direct fallback.
Only a 2xx/3xx response releases the hold; its marker is archived without
overwriting. Failures retain the hold and back off. All extraction budgets
still apply, and a neutral probe never resets the no-success crawl budget.
If that budget is exhausted, automatic probes cannot purchase another IP.

Manual or malformed holds never auto-release. After independently verifying
the provider account and the worker's public-IP whitelist, operators may stop
the crawler, move the exact marker to a timestamped recovery location without
overwriting, and restart the crawler. Self-hosted agent paths remain eligible
throughout a paid hold; pending paid work resumes only after recovery.

## Extraction budgets

Paid extractions also have durable, pool-wide limits:

```toml
[crawl.kuaidaili]
max_extracted_per_hour = 60
max_extracted_per_day = 600
max_extracted_without_success = 10
```

These are the defaults and use rolling one-hour and 24-hour windows, not
calendar boundaries. Each API call reserves its requested IP count in SQLite
before any network access. Failed, empty, or timed-out calls still consume the
reservation because their billing outcome may be unknown. Reservations are
atomic across sources, pool instances and processes, and survive restarts.
Accounting starts when this protection is deployed; historical `batch_fetched`
events are not retroactively charged to the new reservation ledger.

After ten reserved IPs without any successful paid request, further extraction
stops until a paid request succeeds. Cached valid IPs may still be used, but
the HTTP 407 circuit blocks normal cached leases and extractions; only the
bounded half-open probe above may run. Self-hosted agent successes do not reset
the paid no-success counter. Hourly and daily
limits expire naturally; restarting the crawler does not reset any budget.
If authentication has been repaired while the no-success limit is exhausted,
keep the persistent hold in place and use a separately bounded, single-IP
recovery probe. Record success only after validating the response. Do not clear
the reservation ledger to resume, or automatically change the provider whitelist.

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

## Guba dual-endpoint A/B experiment

With `batch_size = 2`, a time-bounded experiment can keep one extracted IP as
the desktop-only control and assign the other IP two alternating channels:
desktop post HTML and the mobile `getArticle` API. The crawler adds one paid
slot only while the experiment is active, so the expected allocation is one
control request plus two dual-cohort requests.

```toml
[sources.guba]
concurrent_paid_requests = 2
proxy_dual_endpoint_experiment_enabled = true
proxy_dual_endpoint_experiment_until = "2026-08-03T10:30:00+08:00"
```

The deadline must include a timezone. After it passes, the extra slot and
mobile routing turn off without a restart. Proxy metrics are split into
`guba_ab_control_desktop`, `guba_ab_dual_desktop`, and
`guba_ab_dual_mobile`. Health remains shared by IP: a hard block on either
dual channel benches that IP for all Guba traffic.

## Adaptive Extraction

For a shared pool, adaptive batching reduces purchases at low demand without
lowering the configured peak pool size or changing source crawl settings:

```toml
[crawl.kuaidaili]
batch_size = 2
low_watermark = 0
adaptive_batching = true
adaptive_request_limit_per_minute = 16
adaptive_recovery_seconds = 600
```

An empty healthy pool initially requests one IP. If all usable IPs reach the
rolling one-minute acquisition threshold, it buys only the missing capacity up
to `batch_size` purchases in that replenishment round. If one IP subsequently
expires, it does not repeatedly top up the pool: surviving usable IPs are reused
until none remain, as in the original policy. This threshold is an expansion
trigger, not a throttle. Already-paid usable IPs are retained until expiry.

An IP bench or an elevated recent failure ratio temporarily restores the normal
batch size on the next required refill, without topping up healthy cached IPs.
The failure guard uses up to 50 results, requires at least 20
results and three failures, and triggers above 5% failures. The recovery period
does not shorten source-scoped benches. Optional expansion errors retain a
still-valid cached route and back off extraction for 30 seconds; an empty or
expired pool still fails closed rather than falling through to direct egress.

Adaptive batching is opt-in, requires `batch_size >= 2` and `low_watermark = 0`,
and cannot be combined with the paired-IP Guba experiment. It does not change
source lists, refresh intervals, source concurrency, agent schedules, or the IP
expiry safety margin. Batch metrics add `requested` and `allocation`
(`single`, `pressure`, `recovery`, or `configured`) for independent readback.
Lease events also record `wait_ms` for time spent waiting to acquire a proxy.
Savings depend on demand and failure patterns and should be measured, not
assumed to be 50%.

## Recovery

Keep both `crawler` and `guba_browser` stopped while changing proxy settings.
Run a one-off list and post-detail check first. Start only `crawler` after both
responses contain their expected embedded payloads; the persistent browser is
not needed when direct detail requests work through the private proxies.
