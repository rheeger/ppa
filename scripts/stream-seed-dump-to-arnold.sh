#!/usr/bin/env bash
# Stream PG 17 custom-format dump of archive_seed from local Docker to Arnold PGDATA.
# Operator visibility: step lines + pv (bytes, rate, ETA if size known).
# Requires: running archive-postgres (make pg-up), SSH to Arnold, sudo tee on remote.
set -euo pipefail

PG_ENV_FILE="${PG_ENV_FILE:-.env.pgvector}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.pgvector.yml}"
DUMP_SCHEMA="${DUMP_SCHEMA:-archive_seed}"
ARNOLD_HOST="${ARNOLD_HOST:-arnold@192.168.50.27}"
REMOTE_DUMP_PATH="${REMOTE_DUMP_PATH:-/srv/hfa-secure/postgres/archive_seed.dump}"
# Optional: exact or approximate dump size in bytes for pv ETA (e.g. 135GiB ≈ 144955146240)
ARCHIVE_STREAM_BYTES="${ARCHIVE_STREAM_BYTES:-}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { printf '[%s] archive-dump-stream: %s\n' "$(ts)" "$*" >&2; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f "$PG_ENV_FILE" ]]; then
  log "error: missing $PG_ENV_FILE (copy from .env.pgvector.example)"
  exit 1
fi

PW="$(grep '^POSTGRES_PASSWORD=' "$PG_ENV_FILE" | cut -d= -f2-)"
U="$(grep '^POSTGRES_USER=' "$PG_ENV_FILE" | cut -d= -f2-)"
D="$(grep '^POSTGRES_DB=' "$PG_ENV_FILE" | cut -d= -f2-)"

COMPOSE=(docker compose --env-file "$PG_ENV_FILE" -f "$COMPOSE_FILE")

log "step 1/4 start — schema=${DUMP_SCHEMA} remote=${ARNOLD_HOST}:${REMOTE_DUMP_PATH}"
if ! "${COMPOSE[@]}" exec -T archive-postgres pg_isready -U "$U" -d "$D" >/dev/null 2>&1; then
  log "error: archive-postgres is not ready (try: make pg-up)"
  exit 1
fi

if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$ARNOLD_HOST" 'sudo -n true'; then
  log "error: ssh to ${ARNOLD_HOST} failed or sudo -n not available"
  exit 1
fi
log "step 1/4 complete — docker service up, ssh ok"

log "step 2/4 start — pg_dump (PG 17 in container) → pipe"
PV=(cat)
if command -v pv >/dev/null 2>&1; then
  if [[ -n "$ARCHIVE_STREAM_BYTES" ]] && [[ "$ARCHIVE_STREAM_BYTES" =~ ^[0-9]+$ ]]; then
    PV=(pv -f -s "$ARCHIVE_STREAM_BYTES" -p -t -e -r -b -N "dump+upload")
    log "step 2/4 — using pv with size=${ARCHIVE_STREAM_BYTES} bytes (ETA enabled)"
  else
    PV=(pv -f -p -t -e -r -b -N "dump+upload")
    log "step 2/4 — using pv without total size (rate + bytes; set ARCHIVE_STREAM_BYTES for ETA)"
  fi
else
  log "step 2/4 — pv not installed: no byte meter (brew install pv)"
fi

log "step 3/4 start — streaming (this is the long stage; pg_dump emits no per-row progress)"
# shellcheck disable=SC2094
"${COMPOSE[@]}" exec -T -e PGPASSWORD="$PW" archive-postgres pg_dump \
  -U "$U" -d "$D" \
  --format=custom --schema="$DUMP_SCHEMA" --no-owner --no-privileges \
  | "${PV[@]}" \
  | ssh "$ARNOLD_HOST" "sudo tee \"$REMOTE_DUMP_PATH\" > /dev/null"

log "step 3/4 complete — stream finished"

REMOTE_LS="$(ssh "$ARNOLD_HOST" "sudo ls -ln \"$REMOTE_DUMP_PATH\" 2>/dev/null || true")"
log "step 4/4 complete — remote file: ${REMOTE_LS}"
log "next on Arnold: pg_restore inside hfa-archive-postgres (see hey-arnold-hfa runbook)"
