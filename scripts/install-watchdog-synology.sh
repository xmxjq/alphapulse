#!/bin/sh
set -eu
umask 077
PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/bin
STAGE=/tmp/alphapulse-watchdog-stage-20261106
BASE=/volume1/alphapulse-watchdog
WRAPPER=/usr/local/etc/rc.d/alphapulse-watchdog.sh
EXPECTED=${1:?binary checksum required}
case "$EXPECTED" in ''|*[!0-9a-f]*) exit 2;; esac
[ "${#EXPECTED}" -eq 64 ]
[ "$(id -u)" -eq 0 ]
test -d "$STAGE"
test ! -L "$STAGE"
test ! -e "$BASE"
test ! -e "$WRAPPER"
test -r /volume1/stock-alert/sendkey
test -r /volume1/alphapulse-agent/cloudflare-authorization.token
test -f /etc/crontab
test ! -L /etc/crontab
BEFORE=$(sha256sum /volume1/stock-alert/config.json)
PID_BEFORE=$(/usr/local/etc/rc.d/stock-alert.sh status)

mkdir -m 700 "$BASE"
cp "$STAGE/alphapulse-watchdog" "$BASE/alphapulse-watchdog"
chmod 755 "$BASE/alphapulse-watchdog"
ACTUAL=$(sha256sum "$BASE/alphapulse-watchdog")
[ "${ACTUAL%% *}" = "$EXPECTED" ]
cp "$STAGE/manager.sh" "$WRAPPER"
chmod 755 "$WRAPPER"

# These commands print only fixed-status JSON, never provider response bodies.
"$BASE/alphapulse-watchdog" --dry-run
"$BASE/alphapulse-watchdog" --test-notification
"$WRAPPER" start

ENTRY='* * * * * root /usr/local/etc/rc.d/alphapulse-watchdog.sh start >/dev/null 2>&1 # alphapulse-watchdog'
COUNT=$(grep -Fc '# alphapulse-watchdog' /etc/crontab || test "$?" -eq 1)
[ "$COUNT" = 0 ]
STAMP=$(date +%Y%m%dT%H%M%S)
BACKUP="/etc/crontab.before-alphapulse-watchdog-$STAMP"
NEW="/etc/crontab.alphapulse-watchdog-$STAMP.new"
test ! -e "$BACKUP"
test ! -e "$NEW"
cp -p /etc/crontab "$BACKUP"
cp -p /etc/crontab "$NEW"
printf '\n%s\n' "$ENTRY" >> "$NEW"
SIZE=$(wc -c < /etc/crontab)
head -c "$SIZE" "$NEW" | cmp -s /etc/crontab -
[ "$(grep -Fxc "$ENTRY" "$NEW")" = 1 ]
mv "$NEW" /etc/crontab
cmp -s "$BACKUP" /etc/crontab && exit 3
[ "$(sha256sum /volume1/stock-alert/config.json)" = "$BEFORE" ]
[ "$(/usr/local/etc/rc.d/stock-alert.sh status)" = "$PID_BEFORE" ]
"$WRAPPER" status >/dev/null
printf '{"ok":true,"installed":true,"test_accepted":true,"stock_alert_unchanged":true,"supervisor_installed":true}\n'
