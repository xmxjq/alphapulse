#!/bin/sh

# QNAP QTS service for the project's xmxjq-NAS agent.

PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin

BASE=/share/CACHEDEV1_DATA/alphapulse-agent
BINARY="$BASE/alphapulse-agent"
AGENT_TOKEN="$BASE/agent.token"
CF_TOKEN="$BASE/cloudflare-authorization.token"
PID_FILE="$BASE/agent.pid"
LOG_FILE="$BASE/agent.log"
MAX_LOG_BYTES=5242880

find_pid()
{
    [ -r "$PID_FILE" ] || return 1
    pid="$(cat "$PID_FILE" 2>/dev/null)"
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null)"
    [ "$exe" = "$BINARY" ] || return 1
    printf '%s\n' "$pid"
}

trim_log()
{
    [ -f "$LOG_FILE" ] || return 0
    bytes="$(wc -c <"$LOG_FILE" 2>/dev/null)"
    [ -n "$bytes" ] || return 0
    [ "$bytes" -gt "$MAX_LOG_BYTES" ] 2>/dev/null || return 0
    tail -n 3000 "$LOG_FILE" >"$LOG_FILE.trim" 2>/dev/null || return 0
    cat "$LOG_FILE.trim" >"$LOG_FILE"
    rm -f "$LOG_FILE.trim"
}

start_agent()
{
    if pid="$(find_pid)"; then
        echo "alphapulse-agent already running: $pid"
        return 0
    fi
    [ -x "$BINARY" ] || {
        echo "Missing executable: $BINARY" >&2
        return 1
    }
    [ -r "$AGENT_TOKEN" ] || {
        echo "Missing agent token: $AGENT_TOKEN" >&2
        return 1
    }
    [ -r "$CF_TOKEN" ] || {
        echo "Missing Cloudflare token: $CF_TOKEN" >&2
        return 1
    }
    trim_log
    umask 077
    setsid "$BINARY" \
      --server https://alphapulse-api.sanae.edu.kg \
      --id xmxjq-nas-telecom \
      --token-file "$AGENT_TOKEN" \
      --cloudflare-authorization-file "$CF_TOKEN" \
      --max-concurrency 1 \
      --request-interval-min 30s \
      --request-interval-max 60s \
      --active-timezone Asia/Shanghai \
      --active-window 08:30-12:00 \
      --active-window 14:00-18:00 \
      --active-window 20:00-23:00 \
      --active-window-jitter 20m \
      --poll-wait 20 \
      --heartbeat-interval 30 \
      >>"$LOG_FILE" 2>&1 </dev/null &
    echo "$!" >"$PID_FILE"
    sleep 2
    pid="$(find_pid)" || {
        echo "alphapulse-agent failed to start" >&2
        tail -n 20 "$LOG_FILE" >&2 2>/dev/null || true
        return 1
    }
    echo "alphapulse-agent started: $pid"
}

stop_agent()
{
    pid="$(find_pid)" || {
        rm -f "$PID_FILE"
        echo "alphapulse-agent is not running"
        return 0
    }
    kill -TERM "$pid"
    count=0
    while kill -0 "$pid" 2>/dev/null && [ "$count" -lt 20 ]; do
        sleep 1
        count=$((count + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid"
    fi
    rm -f "$PID_FILE"
    echo "alphapulse-agent stopped"
}

status_agent()
{
    if pid="$(find_pid)"; then
        echo "alphapulse-agent running: $pid"
        return 0
    fi
    echo "alphapulse-agent stopped"
    return 1
}

case "${1:-start}" in
    start)
        start_agent
        ;;
    stop)
        stop_agent
        ;;
    restart)
        stop_agent
        start_agent
        ;;
    status)
        status_agent
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}" >&2
        exit 2
        ;;
esac
