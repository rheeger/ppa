#!/usr/bin/env bash
# Local pg_dump of one schema: custom file (-Fc) and/or parallel directory (-Fd -j).
# Progress: step 1/4–4/4, pv when single-file; optional PG_DUMP_FAST=-Z0; PG_DUMP_JOBS>=2 → parallel + docker cp.
set -euo pipefail

PG_ENV_FILE="${PG_ENV_FILE:-.env.pgvector}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.pgvector.yml}"
DUMP_SCHEMA="${DUMP_SCHEMA:-archive_seed}"
ARCHIVE_DUMP_OUT="${ARCHIVE_DUMP_OUT:-archive_seed.dump}"
ARCHIVE_STREAM_BYTES="${ARCHIVE_STREAM_BYTES:-}"
# 1 = pg_dump -Z0 (much faster CPU, larger .dump on disk)
PG_DUMP_FAST="${PG_DUMP_FAST:-0}"
# >=2 = directory format + parallel workers (often much faster end-to-end than single-threaded -Fc)
PG_DUMP_JOBS="${PG_DUMP_JOBS:-0}"
# Must match docker-compose container_name for docker cp after parallel dump
PG_DUMP_CONTAINER_NAME="${PG_DUMP_CONTAINER_NAME:-archive-pgvector}"
# Full transcript (stdout+stderr): default logs/archive-dump.log. Disable: ARCHIVE_DUMP_LOG=0
ARCHIVE_DUMP_LOG="${ARCHIVE_DUMP_LOG:-1}"
ARCHIVE_DUMP_LOG_FILE="${ARCHIVE_DUMP_LOG_FILE:-}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { printf '[%s] archive-dump-local: %s\n' "$(ts)" "$*" >&2; }

file_size() {
  local f="$1"
  if stat -f%z "$f" >/dev/null 2>&1; then stat -f%z "$f"; else stat -c%s "$f"; fi
}

dir_size_bytes() {
  local d="$1"
  [[ -d "$d" ]] || { echo 0; return; }
  du -sk "$d" 2>/dev/null | awk '{print $1 * 1024}'
}

parallel_out_dir() {
  if [[ "$ARCHIVE_DUMP_OUT" == *.dump ]]; then
    printf '%s' "${ARCHIVE_DUMP_OUT%.dump}.dump.d"
  else
    printf '%s.d' "$ARCHIVE_DUMP_OUT"
  fi
}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "$ARCHIVE_DUMP_LOG" != "0" ]]; then
  ARCHIVE_DUMP_LOG_FILE="${ARCHIVE_DUMP_LOG_FILE:-$ROOT/logs/archive-dump.log}"
  mkdir -p "$(dirname "$ARCHIVE_DUMP_LOG_FILE")"
  : >>"$ARCHIVE_DUMP_LOG_FILE"
  printf '[%s] archive-dump-local: full transcript → %s\n' "$(ts)" "$ARCHIVE_DUMP_LOG_FILE" | tee -a "$ARCHIVE_DUMP_LOG_FILE"
  exec > >(tee -a "$ARCHIVE_DUMP_LOG_FILE") 2>&1
fi

if [[ ! "$DUMP_SCHEMA" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
  log "error: DUMP_SCHEMA must be a single SQL identifier (got ${DUMP_SCHEMA})"
  exit 1
fi

if [[ ! -f "$PG_ENV_FILE" ]]; then
  log "error: missing ${PG_ENV_FILE}"
  exit 1
fi

COMPOSE=(docker compose --env-file "$PG_ENV_FILE" -f "$COMPOSE_FILE")
PW="$(grep '^POSTGRES_PASSWORD=' "$PG_ENV_FILE" | cut -d= -f2-)"
U="$(grep '^POSTGRES_USER=' "$PG_ENV_FILE" | cut -d= -f2-)"
D="$(grep '^POSTGRES_DB=' "$PG_ENV_FILE" | cut -d= -f2-)"

log "step 1/4 start — schema=${DUMP_SCHEMA} out=${ARCHIVE_DUMP_OUT} fast=${PG_DUMP_FAST} jobs=${PG_DUMP_JOBS}"
if ! "${COMPOSE[@]}" exec -T archive-postgres pg_isready -U "$U" -d "$D" >/dev/null 2>&1; then
  log "error: archive-postgres not ready (make pg-up)"
  exit 1
fi
log "step 1/4 complete — postgres accepting connections"

log "step 2/4 start — estimate schema size on server (for pv eta)"
ESTIMATE=""
if bytes="$("${COMPOSE[@]}" exec -T -e PGPASSWORD="$PW" archive-postgres psql -U "$U" -d "$D" -Atq -c \
  "SELECT COALESCE(SUM(pg_total_relation_size(c.oid))::bigint,0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='${DUMP_SCHEMA}'")"; then
  ESTIMATE="$bytes"
  log "step 2/4 complete — estimated relation bytes bytes=${ESTIMATE}"
else
  log "step 2/4 complete — estimate skipped"
fi

PV_SIZE="${ARCHIVE_STREAM_BYTES:-$ESTIMATE}"
if [[ -n "$PV_SIZE" ]] && ! [[ "$PV_SIZE" =~ ^[0-9]+$ ]]; then
  PV_SIZE=""
fi

SECONDS=0
if [[ "${PG_DUMP_JOBS}" =~ ^[0-9]+$ ]] && [[ "${PG_DUMP_JOBS}" -ge 2 ]]; then
  PARALLEL_DIR="$(parallel_out_dir)"
  log "step 3/4 start — parallel directory dump (-Fd -j ${PG_DUMP_JOBS}) → ${PARALLEL_DIR} (then docker cp)"
  if [[ -f "$ARCHIVE_DUMP_OUT" ]]; then
    log "removing stale single-file ${ARCHIVE_DUMP_OUT} so restore picks ${PARALLEL_DIR}"
    rm -f "$ARCHIVE_DUMP_OUT"
  fi
  rm -rf "$PARALLEL_DIR"
  mkdir -p "$PARALLEL_DIR"
  MARKER="/tmp/hfa_pgdump_${RANDOM}_$$"
  "${COMPOSE[@]}" exec -T archive-postgres mkdir -p "$MARKER"
  extra=()
  if [[ "$PG_DUMP_FAST" == "1" ]]; then
    extra+=(-Z0)
    log "step 3/4 — also using -Z0 inside directory dump (minimal compression)"
  fi
  "${COMPOSE[@]}" exec -T -e PGPASSWORD="$PW" archive-postgres pg_dump \
    -U "$U" -d "$D" \
    -Fd -j "$PG_DUMP_JOBS" -f "$MARKER" \
    --schema="$DUMP_SCHEMA" --no-owner --no-privileges "${extra[@]}"
  log "step 3/4 — pg_dump finished; docker cp ${PG_DUMP_CONTAINER_NAME}:${MARKER}/ → host (I/O bound)"
  docker cp "${PG_DUMP_CONTAINER_NAME}:${MARKER}/." "$PARALLEL_DIR/"
  "${COMPOSE[@]}" exec -T archive-postgres rm -rf "$MARKER"
  final=$(dir_size_bytes "$PARALLEL_DIR")
  log "step 4/4 complete — wrote directory ${PARALLEL_DIR} total_bytes≈${final} elapsed_total=${SECONDS}s"
  log "complete — run: ARCHIVE_DUMP_ARTIFACT=${PARALLEL_DIR} make scp-restore-seed-arnold"
  exit 0
fi

log "step 3/4 start — pg_dump custom format -Fc (long stage)$([[ "$PG_DUMP_FAST" == "1" ]] && echo ' with -Z0 (faster, larger file)')"
rm -f "${ARCHIVE_DUMP_OUT}.tmp"

dump_cmd() {
  local -a z=()
  if [[ "$PG_DUMP_FAST" == "1" ]]; then
    z=(-Z0)
  fi
  "${COMPOSE[@]}" exec -T -e PGPASSWORD="$PW" archive-postgres pg_dump \
    -U "$U" -d "$D" \
    --format=custom --schema="$DUMP_SCHEMA" --no-owner --no-privileges "${z[@]}"
}

if command -v pv >/dev/null 2>&1; then
  pv_args=(-f -p -t -e -r -b -N "pg_dump")
  if [[ -n "$PV_SIZE" ]] && [[ "$PV_SIZE" -gt 0 ]]; then
    pv_args+=(-s "$PV_SIZE")
    log "step 3/4 — pv bytes + rate + eta (size=${PV_SIZE})"
  else
    log "step 3/4 — pv bytes + rate (no eta — set ARCHIVE_STREAM_BYTES if you know dump size)"
  fi
  dump_cmd | pv "${pv_args[@]}" > "${ARCHIVE_DUMP_OUT}.tmp"
else
  log "step 3/4 — no pv (brew install pv). heartbeat every 60s"
  dump_cmd > "${ARCHIVE_DUMP_OUT}.tmp" &
  dpid=$!
  while kill -0 "$dpid" 2>/dev/null; do
    sleep 60
    if [[ -f "${ARCHIVE_DUMP_OUT}.tmp" ]]; then
      sz=$(file_size "${ARCHIVE_DUMP_OUT}.tmp")
      log "step 3/4 pg_dump still running bytes_written=${sz} elapsed=${SECONDS}s"
    else
      log "step 3/4 pg_dump still running elapsed=${SECONDS}s"
    fi
  done
  wait "$dpid"
fi

mv "${ARCHIVE_DUMP_OUT}.tmp" "$ARCHIVE_DUMP_OUT"
final=$(file_size "$ARCHIVE_DUMP_OUT")
log "step 4/4 complete — wrote ${ARCHIVE_DUMP_OUT} size_bytes=${final} elapsed_total=${SECONDS}s"
log "complete — next: make scp-restore-seed-arnold (tip: PG_DUMP_FAST=1 PG_DUMP_JOBS=8 make dump-seed-schema-fast for faster re-dumps)"
