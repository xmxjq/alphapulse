# Xray Tunnels As A Crawler Proxy Pool

Route crawler traffic through one or more of your own xray tunnels instead of
public proxies. Each tunnel is exposed as a local HTTP-proxy inbound; the
crawler's `static_list` proxy provider rotates across them round-robin and
benches an endpoint for a cooldown when it returns a blocked response.

## 1. Configure Xray

If you have share links (`vless://`, `vmess://`, `trojan://`), generate the
whole config from them — one inbound/outbound pair per link:

```bash
python3 scripts/xray_config_from_links.py 'vless://...' 'vless://...' > xray/config.json
```

It also prints the matching `[crawl.static_proxies] urls` list for
`settings.toml`. Quote each link — they contain `?` and `&`.

Or start from the example and fill servers in by hand:

```bash
cp xray/config.example.json xray/config.json   # untracked, like settings.toml
```

Edit `xray/config.json`: one `http` inbound per tunnel (ports 10809, 10810,
...), one outbound per upstream server (VLESS/VMess/Trojan — whatever your
servers speak), and a routing rule pairing each `inboundTag` with its
`outboundTag`. Add or remove inbound/outbound/rule triplets to change the
number of tunnels.

The inbounds listen on `0.0.0.0` **inside the container**, which is only
reachable from the compose network (no host ports are published).

## 2. Start The Xray Service

```bash
docker compose --profile xray up -d xray
docker compose logs xray   # should show the inbounds starting, no config errors
```

## 3. Point The Crawler At The Tunnels

In `settings.toml`:

```toml
[crawl.proxy]
enabled = true
provider = "static_list"
max_attempts = 2
fail_open = false
# Only proxy cookie-less sources. Routing an authenticated session (bilibili
# SESSDATA, xueqiu tokens) through rotating exit IPs is a fast way to get the
# account flagged.
sources = ["guba"]

[crawl.static_proxies]
urls = ["http://xray:10809", "http://xray:10810"]
cooldown_seconds = 300
```

Then `docker compose restart crawler`.

## Behavior

- Requests rotate round-robin across `urls`; each request carries exactly one
  tunnel.
- A request classified as blocked (403/429/captcha) or failed reports the
  tunnel bad; it sits out `cooldown_seconds`, and traffic continues on the
  remaining tunnels. If everything is benched, the endpoint closest to
  recovery is used anyway — a small pool never stalls the crawl.
- `max_attempts` controls how many different tunnels a single request tries
  before giving up; `fail_open = true` would fall back to a direct (unproxied)
  request when no proxy is available.

## Verify

```bash
# A request through one tunnel, from inside the compose network:
docker compose run --rm crawler sh -c \
  'uv run python -c "
from urllib import request
proxy = request.ProxyHandler({\"http\": \"http://xray:10809\", \"https\": \"http://xray:10809\"})
opener = request.build_opener(proxy)
print(opener.open(\"https://api.ipify.org\", timeout=10).read())"'
```

Run it once per inbound port — each should print its tunnel's exit IP.
After enabling, confirm crawl traffic stays clean via the fetch log:

```bash
sqlite3 .runtime/raw/fetch_log.db \
  "SELECT block_kind, COUNT(*) FROM fetch_log WHERE block_kind IS NOT NULL GROUP BY 1"
```
