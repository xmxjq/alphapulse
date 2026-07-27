# Cloudflare Access

AlphaPulse is published from `mac-mini-tv` through a named Cloudflare Tunnel.
The worker does not need a public IP and does not accept inbound Internet
connections.

## Public Names

- `alphapulse.sanae.edu.kg`: browser dashboard, protected by an email allowlist
- `alphapulse-api.sanae.edu.kg`: read-only API, protected by Cloudflare Access
  service tokens

Both hostnames route to `http://127.0.0.1:8000` on the worker. Docker publishes
port 8000 on loopback only, so the Cloudflare Tunnel and local SSH forwarding
are the only supported paths to the origin.

## Worker Files

These files are intentionally not committed:

- `~/.cloudflared/cert.pem`: Cloudflare tunnel-management certificate
- `~/.cloudflared/<tunnel-id>.json`: named-tunnel credentials
- `~/.cloudflared/alphapulse.yml`: worker-specific ingress configuration

The tunnel runs as the logged-in user through
`scripts/com.alphapulse.cloudflared.plist`. Check it with:

```bash
launchctl print gui/$(id -u)/com.alphapulse.cloudflared
tail -100 ~/Library/Logs/alphapulse-cloudflared.log
cloudflared tunnel info alphapulse-mac-mini-tv
```

## Human Access

The WebUI Access application uses an Allow policy containing individual email
addresses. Add and remove people in Cloudflare Zero Trust rather than sharing a
common password.

## LLM Access

Create one service token per person or integration. Do not share one token
across the group. A client sends:

```text
CF-Access-Client-Id: <client-id>
CF-Access-Client-Secret: <client-secret>
```

The primary compact endpoint is:

```text
GET /api/llm/guba/report/YYYY-MM-DD
```

Revoke a compromised or retired client in Cloudflare Zero Trust without
changing the tunnel or restarting AlphaPulse.

## Security Boundary

- Do not publish port 8000 on `0.0.0.0`.
- Do not expose MongoDB, rqlite, noVNC, Docker, or browser-debugging ports.
- Keep Cloudflare API tokens and Access service-token secrets outside Git.
- Configure Access before starting the tunnel.
- Keep the final catch-all ingress rule as `http_status:404`.
