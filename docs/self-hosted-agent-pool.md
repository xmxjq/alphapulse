# Self-hosted fetch agent pool

AlphaPulse can use VPS and home-network machines as controlled outbound fetch
nodes. Each node actively long-polls the worker over HTTPS. No inbound port,
public IP, Python runtime, or Docker installation is required on the node.

The lightweight agent is a static Go binary. It supports:

- Linux amd64, arm64, and armv7
- Windows amd64 and arm64
- macOS amd64 and arm64
- concurrent HTTP workers configurable per machine
- Cloudflare Access service-token headers
- exact target-host allowlists
- DNS and private-address rejection
- redirect, response-size, and timeout limits

The lightweight agent intentionally does not embed Chromium. Machines that need
an authenticated browser session should run the existing browser container or a
future browser-capable agent separately.

## Worker configuration

Add this to `settings.toml`:

```toml
[crawl.agent_pool]
enabled = true
db_path = ".runtime/agent-pool.db"
sources = ["guba", "tgb"]
strategy = "agent_first"
heartbeat_ttl_seconds = 90
lease_seconds = 180
queue_wait_seconds = 10
job_wait_seconds = 120
result_poll_interval_seconds = 0.5
blocked_cooldown_seconds = 21600
max_response_bytes = 8000000
max_pending_jobs = 10000
response_body_retention_hours = 24
job_metadata_retention_days = 30
fallback_to_existing_transport = true
allowed_hosts = [
  "guba.eastmoney.com",
  "emappdata.eastmoney.com",
  "push2.eastmoney.com",
  "www.tgb.cn",
  "app.jiuyangongshe.com",
]
```

The crawler and WebUI containers must share the same `.runtime` directory. This
is already true in the project Compose file.

Guba can run paid and Agent slots concurrently:

```toml
[sources.guba]
concurrent_paid_requests = 1
concurrent_agent_requests = 4
```

These pools run together on different queued tasks. The effective number of
Agent slots is capped by online nodes' reported `max-concurrency`; the paid
slots continue working when all Agent nodes are offline or benched.
Blocked cooldowns are tracked per Agent and source, so a node benched for TGB
can continue serving guba, and vice versa.

Jiuyan keeps ranking, community-feed, and search discovery on paid proxies,
then distributes independent post-detail requests across paid and currently
online Agent slots:

```toml
[sources.jiuyan]
concurrent_paid_requests = 1
concurrent_agent_requests = 2
```

Restart `web` after enabling the pool so the Agent API is available. Restart
`crawler` to enable agent-first request routing.

## Create a node credential

Run on the worker:

```bash
uv run alphapulse --config settings.toml agent-token create --agent-id home-arm-1
```

The command prints the token once. Store only the token value in a root-readable
file on that node:

```bash
install -m 600 /dev/null /etc/alphapulse-agent.token
```

List or revoke credentials:

```bash
uv run alphapulse --config settings.toml agent-token list
uv run alphapulse --config settings.toml agent-token revoke --agent-id home-arm-1
```

Creating a token again for the same agent id rotates the credential.

## Build binaries

With Go 1.22 or newer:

```bash
./scripts/build_agent_binaries.sh
```

On Windows:

```powershell
.\scripts\build_agent_binaries.ps1
```

Outputs are written to `dist/agents/`. Builds use `CGO_ENABLED=0`, `-trimpath`,
and stripped symbols, so the resulting executables have no external runtime
library requirement. The current Git revision is embedded in the binary and is
reported in node heartbeats.

## Run a node

For the existing Cloudflare API hostname and one-header service token:

```bash
./alphapulse-agent-linux-arm64 \
  --server https://alphapulse-api.sanae.edu.kg \
  --id home-arm-1 \
  --token-file /etc/alphapulse-agent.token \
  --cloudflare-authorization-file /etc/alphapulse-cloudflare.token \
  --max-concurrency 1
```

For the standard Cloudflare Access client-id/client-secret headers:

```bash
./alphapulse-agent-linux-amd64 \
  --server https://alphapulse-api.sanae.edu.kg \
  --id vps-sg-1 \
  --token-file /etc/alphapulse-agent.token \
  --cf-access-client-id-file /etc/alphapulse-cf-client-id \
  --cf-access-client-secret-file /etc/alphapulse-cf-client-secret \
  --max-concurrency 3
```

The built-in target allowlist matches the worker defaults. Repeat
`--allow-host hostname` to supply an explicit local allowlist.

Use `--max-concurrency 1` on routers, single-board computers, and home
connections. Start with `2` or `3` on small VPS instances and raise it only
after observing latency and block rates in WebUI.

Stable home-network IPs should also enable Agent-local pacing:

```bash
--max-concurrency 1 \
--request-interval-min 30s \
--request-interval-max 60s
```

The interval is enforced inside the Agent across its worker loop, independently
of crawler threads and processes. When interval pacing is enabled, the Agent
rejects `max-concurrency` values other than `1`.

The `mac-mini-tv` worker runs an especially conservative local Agent because
its direct exit is also reserved for browser-based exploration. Its checked-in
launchd configuration is:

```text
scripts/com.alphapulse.worker-agent.plist
```

That node uses one worker, a shared three-to-five-minute request interval, and
three one-hour daily windows with inward jitter. It is intended as low-volume
fallback capacity rather than a backlog worker.

Home and residential exits should not advertise fetch capacity around the
clock. Configure one or more daily local-time windows:

```bash
--active-timezone Asia/Shanghai \
--active-window 08:30-12:00 \
--active-window 14:00-18:00 \
--active-window 20:00-23:00 \
--active-window-jitter 20m
```

Outside these windows the process stays alive but stops both heartbeats and job
leases. The worker therefore marks it offline after
`heartbeat_ttl_seconds` and continues through paid proxies or other agents.
It resumes automatically at the next window.

Jitter is deterministic for each node and local date, so process restarts do
not reset it. Start boundaries move later and end boundaries move earlier by
up to the configured duration; the Agent never fetches beyond the configured
outer windows. Overnight windows such as `22:00-02:00` are supported.

## systemd service

```ini
[Unit]
Description=AlphaPulse fetch agent
After=network-online.target
Wants=network-online.target

[Service]
User=alphapulse
ExecStart=/usr/local/bin/alphapulse-agent \
  --server https://alphapulse-api.sanae.edu.kg \
  --id home-arm-1 \
  --token-file /etc/alphapulse-agent.token \
  --cloudflare-authorization-file /etc/alphapulse-cloudflare.token \
  --max-concurrency 1
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

## Synology DSM

Synology systems can run the static binary from `/volume1` and use an
`/usr/local/etc/rc.d` script for boot persistence. The project includes the
deployed template:

```bash
scripts/alphapulse-agent-synology.sh
```

Install it after placing the binary and credentials in
`/volume1/alphapulse-agent`:

```bash
sudo install -o root -g root -m 755 \
  scripts/alphapulse-agent-synology.sh \
  /usr/local/etc/rc.d/alphapulse-agent.sh

sudo /usr/local/etc/rc.d/alphapulse-agent.sh start
sudo /usr/local/etc/rc.d/alphapulse-agent.sh status
```

The service process runs as the unprivileged `xmxjq` user. The directory and
credential files should be mode `700` and `600` respectively. The DS120j used
by this project reports `aarch64`, so it uses the `linux-arm64` binary even
though the hardware is resource-constrained.

## QNAP QTS

The project's `xmxjq-NAS` is an x86_64 QNAP system. Install the
`linux-amd64` binary and credentials under:

```text
/share/CACHEDEV1_DATA/alphapulse-agent
```

The project includes its service script:

```bash
scripts/alphapulse-agent-qnap.sh
```

Install the script as
`/share/CACHEDEV1_DATA/alphapulse-agent/alphapulse-agent.sh`, then add this
idempotent start command to QNAP's persistent autorun script:

```bash
/share/CACHEDEV1_DATA/alphapulse-agent/alphapulse-agent.sh start
```

Manage the process manually with:

```bash
/share/CACHEDEV1_DATA/alphapulse-agent/alphapulse-agent.sh start
/share/CACHEDEV1_DATA/alphapulse-agent/alphapulse-agent.sh stop
/share/CACHEDEV1_DATA/alphapulse-agent/alphapulse-agent.sh restart
/share/CACHEDEV1_DATA/alphapulse-agent/alphapulse-agent.sh status
```

## Request routing and failure behavior

When eligible agents are online, guba consumes different queued tasks through
the paid proxy and Agent pools concurrently. If an Agent disappears after
advertising capacity, its task waits up to `queue_wait_seconds` for a lease,
then returns to the existing paid proxy/direct transport when
`fallback_to_existing_transport = true`.

After guba classifies a response as blocked, that agent is benched for
`blocked_cooldown_seconds`; paid proxies continue rotating independently.
Likewise, a blocked paid proxy does not stop healthy Agent nodes. Pending
crawler tasks remain in the normal durable `pending_tasks` state and are not
owned by the agent pool.

For stable residential IPs, keep this cooldown long (the project default is six
hours). Repeatedly probing a fixed exit every few minutes after a block usually
extends the restriction rather than recovering it.

The WebUI `Agent pool` tab shows online, offline, and benched nodes, their most
recent public IP observed through Cloudflare, queue depth, platform/architecture,
per-source Guba/TGB outcomes, and recent jobs.
