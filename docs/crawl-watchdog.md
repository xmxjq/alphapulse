# Crawl interruption notifications

The optional Linux `alphapulse-watchdog` runs independently on an always-on
Synology NAS. It reads `/api/crawl-health` through the existing Cloudflare
Access service credential and posts notifications through ServerChan.
It does not acquire proxies, crawl sources, restart containers or reset holds.
No listener or new network exposure is required.

The health API returns fixed source names, timestamps, backlog counts, proxy
hold/budget flags and fixed diagnostic categories only. It never returns URLs,
headers, credentials, exception messages or raw provider responses. SQLite
connections are read-only with bounded query execution. The existing deployment's
Cloudflare Access policy must also protect this new `/api/` path.

## Rules

- Check once per minute, with a 25-second HTTP timeout and no redirects/proxy inheritance.
- Detect a persistent paid-proxy authentication hold or a reached durable
  extraction budget on the next check. Crawler protections still act immediately.
- Detect when a source has a continuously observed backlog and no new successful
  response for at least ten minutes. Successful progress resets the timer; an empty
  backlog or disabled source does not alarm. Blocked/invalid HTTP 200 responses do
  not count as successful responses for raw-enabled sources.
- Detect when neither crawl cycles nor successful responses have progressed for
  at least `max(15 minutes, 2 * poll interval + 5 minutes)`.
- A failed, stale, malformed or incomplete health observation is a monitoring
  failure, not a healthy/empty result. Detect after three continuous minutes.
  Previously known incidents cannot recover during unknown health.
- Hold each detected alert condition for fifteen continuous minutes before its first
  notification. If it clears sooner, silently discard it without an alert or a
  recovery notification. A new occurrence starts a new confirmation window.
  This confirmation window follows the detection thresholds above: for example,
  a source requires ten minutes of observed no-progress, then fifteen more minutes
  with the stall condition still active. Pending timers survive daemon restarts.
- Batch changes, deduplicate the same incident, and repeat active faults no more
  than once per hour. Send a condition-cleared notification after a valid recovery.
- Persist notification attempt markers before POST. A timeout or restart does not
  replay that message. Later hourly reminders are new observations, not delivery retries.
  API acceptance is not proof of receipt in the user's WeChat client.

## Deployment

Build from `agent/` with `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build
-trimpath -ldflags="-s -w" -o alphapulse-watchdog ./cmd/alphapulse-watchdog`.
Install under `/volume1/alphapulse-watchdog` and install
`scripts/alphapulse-watchdog-synology.sh` as
`/usr/local/etc/rc.d/alphapulse-watchdog.sh`, owned by root and mode 0755.
Use a DSM root scheduled task to run that manager's `start` action every minute
as a supervisor; `start` is idempotent and does not create a second daemon.
The rc.d wrapper also starts on boot. Do not modify unrelated cron entries.

The daemon defaults to the NAS's existing
`/volume1/alphapulse-agent/cloudflare-authorization.token` and
`/volume1/stock-alert/sendkey`, read internally without copying them.
Existing agent and stock-alert configuration and state remain unchanged.
Its own state and singleton lock live in `/volume1/alphapulse-watchdog`.
Keep that directory mode 0700 and its state mode 0600.

Before starting, run `--dry-run` for read-only health verification. Run
`--test-notification` once for a clearly labeled message and obtain the user's
receipt confirmation. Its attempted/accepted markers prevent an accidental repeat.

The NAS needs outbound connectivity to both endpoints. A NAS outage, whole-site
power outage, or a failure of the notification provider cannot be reported by
this single monitor. Per-source checks rely on persisted pending tasks; a failure
before task discovery may instead be detected by the aggregate progress check.
Backlog detection waits ten minutes after initial observation/restart to avoid
misclassifying newly discovered work. SQLite state and raw capture must be enabled;
unsupported configurations produce monitoring-failure alerts rather than false health.

Rollback: disable only the watchdog supervisor entry, call the wrapper's `stop`,
and restore the saved web source before restarting only the web container.
Keep all watchdog files, state and backups for inspection; do not delete them.
