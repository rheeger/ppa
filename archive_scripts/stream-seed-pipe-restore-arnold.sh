#!/usr/bin/env bash
# Stream archive_seed from local PG17 Docker straight into pg_restore on Arnold (no remote .dump file).
# Use when /srv/hfa-secure (or root /) cannot hold a full custom-format dump alongside PGDATA + vault.
# Progress: step logs + optional pv (brew install pv). Set ARCHIVE_STREAM_BYTES for ETA.
set -euo pipefail

PG_ENV_FILE="${PG_ENV_FILE:-.env.pgvector}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.pgvector.yml}"
DUMP_SCHEMA="${DUMP_SCHEMA:-archive_seed}"
ARNOLD_HOST="${ARNOLD_HOST:-arnold@192.168.50.27}"
REMOTE_PG_CONTAINER="${REMOTE_PG_CONTAINER:-hfa-archive-postgres}"
ARCHIVE_STREAM_BYTES="${ARCHIVE_STREAM_BYTES:-}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { printf '[%s] archive-pipe-restore: %s\n' "$(ts)" "$*" >&2; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f "$PG_ENV_FILE" ]]; then
  log "error: missing $PG_ENV_FILE"
  exit 1
fi

PW="$(grep '^POSTGRES_PASSWORD=' "$PG_ENV_FILE" | cut -d= -f2-)"
U="$(grep '^POSTGRES_USER=' "$PG_ENV_FILE" | cut -d= -f2-)"
D="$(grep '^POSTGRES_DB=' "$PG_ENV_FILE" | cut -d= -f2-)"

COMPOSE=(docker compose --env-file "$PG_ENV_FILE" -f "$COMPOSE_FILE")

log "step 1/5 start — schema=${DUMP_SCHEMA} → ${ARNOLD_HOST} ${REMOTE_PG_CONTAINER} (stdin pg_restore)"
if ! "${COMPOSE[@]}" exec -T archive-postgres pg_isready -U "$U" -d "$D" >/dev/null 2>&1; then
  log "error: local archive-postgres not ready (make pg-up)"
  exit 1
fi

if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$ARNOLD_HOST" 'sudo -n true'; then
  log "error: ssh or sudo -n failed"
  exit 1
fi

if ! ssh -o BatchMode=yes "$ARNOLD_HOST" "sudo docker exec \"$REMOTE_PG_CONTAINER\" pg_isready -U archive -d archive" >/dev/null 2>&1; then
  log "error: remote ${REMOTE_PG_CONTAINER} not accepting connections"
  exit 1
fi
log "step 1/5 complete — local and remote Postgres ready"

log "step 1b/5 — ensure pgvector extension on remote (required before schema restore)"
if ! ssh -o BatchMode=yes "$ARNOLD_HOST" \
  "sudo docker exec \"$REMOTE_PG_CONTAINER\" psql -U archive -d archive -v ON_ERROR_STOP=1 -c \"CREATE EXTENSION IF NOT EXISTS vector\""; then
  log "error: CREATE EXTENSION vector failed on remote"
  exit 1
fi

PV=(cat)
if command -v pv >/dev/null 2>&1; then
  if [[ -n "$ARCHIVE_STREAM_BYTES" ]] && [[ "$ARCHIVE_STREAM_BYTES" =~ ^[0-9]+$ ]]; then
    PV=(pv -f -s "$ARCHIVE_STREAM_BYTES" -p -t -e -r -b -N "dump→restore")
  else
    PV=(pv -f -p -t -e -r -b -N "dump→restore")
  fi
  log "step 2/5 — pv meter active (set ARCHIVE_STREAM_BYTES for ETA)"
else
  log "step 2/5 — install pv for byte/rate (brew install pv)"
fi

log "step 3/5 start — long stage: pg_dump | … | ssh docker exec -i pg_restore (no remote dump file)"
# Remote: load gate .env for HFA_ARCHIVE_PG_PASSWORD; pass stream to pg_restore via docker -i.
# shellcheck disable=SC2029
"${COMPOSE[@]}" exec -T -e PGPASSWORD="$PW" archive-postgres pg_dump \
  -U "$U" -d "$D" \
  --format=custom --schema="$DUMP_SCHEMA" --no-owner --no-privileges \
  | "${PV[@]}" \
  | ssh "$ARNOLD_HOST" \
    'set -a; . /home/arnold/openclaw/.env; set +a; exec sudo docker exec -i -e PGPASSWORD="$HFA_ARCHIVE_PG_PASSWORD" '"$REMOTE_PG_CONTAINER"' pg_restore -U archive -d archive --clean --if-exists --no-owner --no-privileges'

log "step 4/5 complete — pg_restore finished"
log "step 5/5 — verify counts on Arnold (example):"
log "  ssh ${ARNOLD_HOST} \"sudo docker exec ${REMOTE_PG_CONTAINER} psql -U archive -d archive -Atc \\\"select count(*) from ${DUMP_SCHEMA}.cards;\\\"\""
