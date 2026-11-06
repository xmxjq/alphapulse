#!/bin/sh
set -eu
PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/bin
BASE=/volume1/alphapulse-watchdog
BIN="$BASE/alphapulse-watchdog"
PIDFILE="$BASE/watchdog.pid"

find_pid() {
    [ -r "$PIDFILE" ] || return 1
    IFS= read -r pid < "$PIDFILE"
    case "$pid" in ''|*[!0-9]*) return 1;; esac
    kill -0 "$pid" 2>/dev/null || return 1
    [ "$(readlink "/proc/$pid/exe")" = "$BIN" ] || return 1
    printf '%s\n' "$pid"
}

case "${1:-status}" in
    start)
        if find_pid >/dev/null; then exit 0; fi
        test -x "$BIN"
        umask 077
        # Retain bounded historical logs rather than deleting or truncating them.
        if [ -f "$BASE/watchdog.log" ]; then
            bytes=$(wc -c < "$BASE/watchdog.log")
            if [ "$bytes" -gt 1048576 ]; then
                mv -n "$BASE/watchdog.log" "$BASE/watchdog.log.before-$(date +%Y%m%dT%H%M%S)-$$"
            fi
        fi
        nohup "$BIN" >>"$BASE/watchdog.log" 2>&1 </dev/null &
        sleep 2
        find_pid >/dev/null
        ;;
    stop)
        if pid=$(find_pid); then
            kill -TERM "$pid"
            count=0
            while kill -0 "$pid" 2>/dev/null; do
                count=$((count+1))
                [ "$count" -le 40 ] || exit 1
                sleep 1
            done
        fi
        ;;
    status)
        find_pid
        ;;
    *) exit 2;;
esac
