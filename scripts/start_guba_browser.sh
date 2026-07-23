#!/bin/sh
set -eu

export DISPLAY="${DISPLAY:-:99}"

PROFILE_DIR="${GUBA_BROWSER_PROFILE_DIR:-/data/profile}"
PROXY_URL="${GUBA_BROWSER_PROXY:-http://host.docker.internal:10809}"
START_URL="${GUBA_BROWSER_START_URL:-https://guba.eastmoney.com/}"
SCREEN_SIZE="${GUBA_BROWSER_SCREEN_SIZE:-1440x1000x24}"

CHROMIUM_BIN="${CHROMIUM_BIN:-}"
if [ -z "$CHROMIUM_BIN" ]; then
    CHROMIUM_BIN="$(find /root/.cache/ms-playwright -path '*/chrome-linux/chrome' -type f | head -n 1)"
fi
if [ -z "$CHROMIUM_BIN" ] || [ ! -x "$CHROMIUM_BIN" ]; then
    echo "Chromium binary not found" >&2
    exit 1
fi

mkdir -p "$PROFILE_DIR"

Xvfb "$DISPLAY" -screen 0 "$SCREEN_SIZE" -ac +extension RANDR &
fluxbox >/tmp/fluxbox.log 2>&1 &
x11vnc \
    -display "$DISPLAY" \
    -forever \
    -shared \
    -localhost \
    -nopw \
    -noxdamage \
    -rfbport 5900 \
    >/tmp/x11vnc.log 2>&1 &
websockify \
    --web=/usr/share/novnc/ \
    6080 \
    localhost:5900 \
    >/tmp/novnc.log 2>&1 &
nginx -c /etc/nginx/nginx-cdp.conf

exec "$CHROMIUM_BIN" \
    --no-sandbox \
    --disable-dev-shm-usage \
    --disable-features=Translate \
    --lang=zh-CN \
    --user-data-dir="$PROFILE_DIR" \
    --proxy-server="$PROXY_URL" \
    --remote-debugging-address=0.0.0.0 \
    --remote-debugging-port=9222 \
    --remote-allow-origins='*' \
    --window-size=1440,1000 \
    --no-first-run \
    --no-default-browser-check \
    "$START_URL"
