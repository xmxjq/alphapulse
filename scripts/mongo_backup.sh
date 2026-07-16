#!/usr/bin/env bash
# Dump the standalone alphapulse mongo (docker-compose.mongo.yml) to a gzip
# archive and prune old backups. Designed to run unattended from launchd/cron.
#
#   BACKUP_DIR      where archives go (default ~/backups/alphapulse-mongo)
#   RETENTION_DAYS  delete archives older than this (default 14)
#
# Restore with:
#   docker compose -f docker-compose.mongo.yml exec -T mongo sh -c \
#     'exec mongorestore --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
#        --authenticationDatabase admin --archive --gzip' < mongo-<stamp>.archive.gz
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_DIR/docker-compose.mongo.yml"
BACKUP_DIR="${BACKUP_DIR:-$HOME/backups/alphapulse-mongo}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

# launchd/cron start with a minimal PATH; make sure docker is findable
export PATH="/usr/local/bin:/opt/homebrew/bin:$HOME/.orbstack/bin:$PATH"

mkdir -p "$BACKUP_DIR"
stamp="$(date +%Y%m%d-%H%M%S)"
out="$BACKUP_DIR/mongo-$stamp.archive.gz"
tmp="$out.partial"

# Credentials come from the container's own environment — none stored here.
docker compose -f "$COMPOSE_FILE" exec -T mongo sh -c \
  'exec mongodump --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
     --authenticationDatabase admin --archive --gzip --quiet' \
  > "$tmp"

size="$(stat -f%z "$tmp" 2>/dev/null || stat -c%s "$tmp")"
if [ "$size" -lt 1024 ]; then
  echo "$(date '+%F %T') backup looks empty ($size bytes), keeping $tmp for inspection" >&2
  exit 1
fi
mv "$tmp" "$out"
echo "$(date '+%F %T') wrote $out ($(du -h "$out" | cut -f1))"

find "$BACKUP_DIR" -name 'mongo-*.archive.gz' -mtime +"$RETENTION_DAYS" -delete
find "$BACKUP_DIR" -name '*.partial' -mtime +1 -delete
