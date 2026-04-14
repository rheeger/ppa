#!/usr/bin/env bash
# Local pg_dump artifact (custom file or parallel directory) → rsync → pg_restore -j on Arnold.
# Set ARCHIVE_DUMP_ARTIFACT to a directory for parallel (-Fd) dumps, else defaults to ARCHIVE_DUMP_OUT file.
set -euo pipefail

ARCHIVE_DUMP_OUT="${ARCHIVE_DUMP_OUT:-archive_seed.dump}"
ARCHIVE_DUMP_ARTIFACT="${ARCHIVE_DUMP_ARTIFACT:-}"
ARNOLD_HOST="${ARNOLD_HOST:-arnold@192.168.50.27}"
REMOTE_PG_CONTAINER="${REMOTE_PG_CONTAINER:-hfa-archive-postgres}"
PG_RESTORE_JOBS="${PG_RESTORE_JOBS:-32}"
REMOTE_REMOVE_DUMP_AFTER="${REMOTE_REMOVE_DUMP_AFTER:-1}"
ARCHIVE_SCP_RESTORE_LOG="${ARCHIVE_SCP_RESTORE_LOG:-1}"
ARCHIVE_SCP_RESTORE_LOG_FILE="${ARCHIVE_SCP_RESTORE_LOG_FILE:-}"
ARCHIVE_SCP_RESTORE_STATUS_FILE="${ARCHIVE_SCP_RESTORE_STATUS_FILE:-}"
RSYNC_HEARTBEAT_SEC="${RSYNC_HEARTBEAT_SEC:-60}"
RESTORE_HEARTBEAT_SEC="${RESTORE_HEARTBEAT_SEC:-60}"
START_STEP="${START_STEP:-1}"
RESTORE_LOG_INTERVAL="${RESTORE_LOG_INTERVAL:-2000}"
REMOTE_RESTORE_VERBOSE_LOG="${REMOTE_RESTORE_VERBOSE_LOG:-/tmp/archive-pg-restore-$(date -u +%Y%m%dT%H%M%SZ).log}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { printf '[%s] archive-scp-restore: %s\n' "$(ts)" "$*" >&2; }

human_bytes() {
  awk -v b="${1:-0}" 'BEGIN {
    if (b < 1024) { printf "%d B", b; exit }
    split("KiB MiB GiB TiB", u, " ")
    n = b
    i = 0
    while (n >= 1024 && i < 4) { n /= 1024; i++ }
    if (i == 0) { printf "%d B", b; exit }
    printf "%.2f %s", n, u[i]
  }'
}

# Time-based heartbeat while a foreground command runs (subshell uses exported *_T0 epoch).
heartbeat_start() {
  local label="$1" interval="${2:-60}"
  heartbeat_stop
  export HEARTBEAT_LABEL="$label"
  export HEARTBEAT_INTERVAL="$interval"
  export HEARTBEAT_T0
  HEARTBEAT_T0=$(date +%s)
  (
    trap 'exit 0' TERM INT
    while :; do
      sleep "$HEARTBEAT_INTERVAL" &
      wait "$!" || exit 0
      now=$(date +%s)
      printf '[%s] archive-scp-restore: %s heartbeat interval=%ss elapsed=%ss (still running)\n' "$(ts)" "$HEARTBEAT_LABEL" "$HEARTBEAT_INTERVAL" "$((now - HEARTBEAT_T0))" >&2
    done
  ) &
  HEARTBEAT_PID=$!
}

heartbeat_stop() {
  if [[ -n "${HEARTBEAT_PID:-}" ]]; then
    if command -v pkill >/dev/null 2>&1; then
      pkill -TERM -P "$HEARTBEAT_PID" 2>/dev/null || true
    fi
    kill -TERM "$HEARTBEAT_PID" 2>/dev/null || true
    wait "$HEARTBEAT_PID" 2>/dev/null || true
    HEARTBEAT_PID=
  fi
}

# Staging under /srv/hfa-secure needs sudo + chown arnold so rsync works; use this for large dumps when /home is on a small root LV.
remote_staging_needs_sudo() {
  local p="$1"
  [[ "$p" == /srv/hfa-secure/* ]]
}

prepare_remote_staging_dir() {
  local dir="$1"
  if remote_staging_needs_sudo "$dir"; then
    log "preparing staging dir on encrypted volume (sudo): $dir"
    ssh -o BatchMode=yes "$ARNOLD_HOST" "sudo rm -rf \"$dir\" && sudo mkdir -p \"$dir\" && sudo chown arnold:arnold \"$dir\""
  else
    ssh -o BatchMode=yes "$ARNOLD_HOST" "rm -rf \"$dir\" && mkdir -p \"$dir\""
  fi
}

prepare_remote_staging_file_dest() {
  local f="$1"
  local d
  d="$(dirname "$f")"
  if remote_staging_needs_sudo "$f"; then
    log "preparing staging file dest on encrypted volume (sudo): $f"
    ssh -o BatchMode=yes "$ARNOLD_HOST" "sudo mkdir -p \"$d\" && sudo chown arnold:arnold \"$d\" && sudo rm -f \"$f\""
  else
    ssh -o BatchMode=yes "$ARNOLD_HOST" "mkdir -p \"$d\" && rm -f \"$f\""
  fi
}

file_size() {
  local f="$1"
  if stat -f%z "$f" >/dev/null 2>&1; then stat -f%z "$f"; else stat -c%s "$f"; fi
}

dir_size_bytes() {
  local d="$1"
  [[ -d "$d" ]] || { echo 0; return; }
  du -sk "$d" 2>/dev/null | awk '{print $1 * 1024}'
}

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "$ARCHIVE_SCP_RESTORE_LOG" != "0" ]]; then
  ARCHIVE_SCP_RESTORE_LOG_FILE="${ARCHIVE_SCP_RESTORE_LOG_FILE:-$ROOT/logs/archive-scp-restore.log}"
  ARCHIVE_SCP_RESTORE_STATUS_FILE="${ARCHIVE_SCP_RESTORE_STATUS_FILE:-$ROOT/logs/archive-scp-restore.status}"
  mkdir -p "$(dirname "$ARCHIVE_SCP_RESTORE_LOG_FILE")"
  : >>"$ARCHIVE_SCP_RESTORE_LOG_FILE"
  printf '[%s] archive-scp-restore: full transcript → %s\n' "$(ts)" "$ARCHIVE_SCP_RESTORE_LOG_FILE" | tee -a "$ARCHIVE_SCP_RESTORE_LOG_FILE"
  exec > >(tee -a "$ARCHIVE_SCP_RESTORE_LOG_FILE") 2>&1
fi

CURRENT_STEP=0
CURRENT_STEP_LABEL="init"
RUN_COMPLETED=0

status_write() {
  local state="$1" message="$2"
  local status_file="${ARCHIVE_SCP_RESTORE_STATUS_FILE:-}"
  local tmp
  [[ -n "$status_file" ]] || return 0
  mkdir -p "$(dirname "$status_file")"
  tmp="${status_file}.tmp"
  {
    printf 'updated_at=%s\n' "$(ts)"
    printf 'state=%s\n' "$state"
    printf 'step=%s\n' "$CURRENT_STEP"
    printf 'step_label=%s\n' "$CURRENT_STEP_LABEL"
    printf 'message=%s\n' "$message"
  } >"$tmp"
  mv "$tmp" "$status_file"
}

cleanup() {
  local st=$?
  heartbeat_stop
  if [[ "$RUN_COMPLETED" != "1" ]]; then
    status_write "failed" "exit_code=${st} step=${CURRENT_STEP} label=${CURRENT_STEP_LABEL}"
    if [[ "$st" -ne 0 ]]; then
      log "failed — exit_code=${st} step=${CURRENT_STEP}/5 label=${CURRENT_STEP_LABEL}"
    fi
  fi
}

trap cleanup EXIT INT TERM

step_should_run() {
  local n="$1"
  (( START_STEP <= n ))
}

step_skip() {
  local n="$1" label="$2"
  CURRENT_STEP="$n"
  CURRENT_STEP_LABEL="$label"
  status_write "running" "step ${n}/5 skipped via START_STEP=${START_STEP}"
  log "step ${n}/5 skipped — ${label} (START_STEP=${START_STEP})"
}

ARTIFACT="${ARCHIVE_DUMP_ARTIFACT:-}"
if [[ -z "$ARTIFACT" ]]; then
  parallel_dir="${ARCHIVE_DUMP_OUT%.dump}.dump.d"
  if [[ -f "$ARCHIVE_DUMP_OUT" ]]; then
    ARTIFACT="$ARCHIVE_DUMP_OUT"
  elif [[ -d "$parallel_dir" ]]; then
    ARTIFACT="$parallel_dir"
    log "using parallel dump directory ${ARTIFACT} (no ${ARCHIVE_DUMP_OUT} file — set ARCHIVE_DUMP_ARTIFACT= to override)"
  else
    ARTIFACT="$ARCHIVE_DUMP_OUT"
  fi
fi

MODE=file
if [[ -d "$ARTIFACT" ]]; then
  MODE=directory
elif [[ ! -f "$ARTIFACT" ]]; then
  log "error: missing artifact (file or dir): ${ARTIFACT} — run make dump-seed-schema"
  exit 1
fi

REMOTE_STAGING="${REMOTE_STAGING:-/srv/hfa-secure/archive_seed.dump.incoming}"
REMOTE_STAGING_DIR="${REMOTE_STAGING_DIR:-/srv/hfa-secure/archive_dump_restore_staging}"
REMOTE_PGDATA_DUMP="${REMOTE_PGDATA_DUMP:-/srv/hfa-secure/postgres/$(basename "$ARTIFACT")}"

RUN_T0=$SECONDS
if [[ "$MODE" == "directory" ]]; then
  _artifact_bytes="$(dir_size_bytes "$ARTIFACT")"
else
  _artifact_bytes="$(file_size "$ARTIFACT")"
fi
log "run start — host=${ARNOLD_HOST} container=${REMOTE_PG_CONTAINER} mode=${MODE} artifact=$(pwd)/${ARTIFACT} size_bytes≈${_artifact_bytes} ($(human_bytes "$_artifact_bytes")) pg_restore_jobs=${PG_RESTORE_JOBS} staging_dir=${REMOTE_STAGING_DIR:-$REMOTE_STAGING}"
status_write "running" "run started"

if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$ARNOLD_HOST" 'sudo -n true'; then
  log "error: ssh or sudo -n failed for ${ARNOLD_HOST}"
  exit 1
fi

if ! ssh -o BatchMode=yes "$ARNOLD_HOST" "sudo docker exec \"$REMOTE_PG_CONTAINER\" pg_isready -U archive -d archive" >/dev/null 2>&1; then
  log "error: remote ${REMOTE_PG_CONTAINER} not accepting connections"
  exit 1
fi

log "preflight — remote disk (encrypted mount)"
ssh -o BatchMode=yes "$ARNOLD_HOST" "df -h /srv/hfa-secure 2>/dev/null || df -h /" || true

_step1_t0=$SECONDS
if step_should_run 1; then
  CURRENT_STEP=1
  CURRENT_STEP_LABEL="extension"
  status_write "running" "step 1/5 start"
  log "step 1/5 start — CREATE EXTENSION vector (if missing)"
  ssh -o BatchMode=yes "$ARNOLD_HOST" \
    "sudo docker exec \"$REMOTE_PG_CONTAINER\" psql -U archive -d archive -v ON_ERROR_STOP=1 -c \"CREATE EXTENSION IF NOT EXISTS vector\""
  status_write "running" "step 1/5 complete"
  log "step 1/5 complete — extension ready elapsed=$((SECONDS - _step1_t0))s"
else
  step_skip 1 "extension"
fi

RSYNC_FLAGS=(-av --inplace)
if rsync --help 2>&1 | grep -q 'info=progress2'; then
  RSYNC_FLAGS+=(--info=progress2)
else
  RSYNC_FLAGS+=(--progress)
fi
# When /srv/hfa-secure is root-only (0700), receiver must run rsync as root.
RSYNC_PATH_SECURE=(--rsync-path='sudo rsync')

if [[ "$MODE" == "file" ]]; then
  if step_should_run 2; then
    _step2_t0=$SECONDS
    CURRENT_STEP=2
    CURRENT_STEP_LABEL="rsync-file"
    local_bytes="$(file_size "$ARTIFACT")"
    status_write "running" "step 2/5 start"
    log "step 2/5 start — rsync file → ${ARNOLD_HOST}:${REMOTE_STAGING} size_bytes=${local_bytes} ($(human_bytes "$local_bytes")) (rsync --info=progress2 + heartbeat ${RSYNC_HEARTBEAT_SEC}s)"
    prepare_remote_staging_file_dest "$REMOTE_STAGING"
    heartbeat_start "step 2/5 rsync file" "$RSYNC_HEARTBEAT_SEC"
    _rsync_rc=0
    if remote_staging_needs_sudo "$REMOTE_STAGING"; then
      rsync "${RSYNC_FLAGS[@]}" "${RSYNC_PATH_SECURE[@]}" -e "ssh -o BatchMode=yes" "$ARTIFACT" "${ARNOLD_HOST}:${REMOTE_STAGING}" || _rsync_rc=$?
    else
      rsync "${RSYNC_FLAGS[@]}" -e "ssh -o BatchMode=yes" "$ARTIFACT" "${ARNOLD_HOST}:${REMOTE_STAGING}" || _rsync_rc=$?
    fi
    heartbeat_stop
    [[ "$_rsync_rc" -eq 0 ]] || exit "$_rsync_rc"
    _rs_elapsed=$((SECONDS - _step2_t0))
    if [[ "$local_bytes" -gt 0 && "$_rs_elapsed" -gt 0 ]]; then
      _rate=$((local_bytes / _rs_elapsed))
      status_write "running" "step 2/5 complete"
      log "step 2/5 complete — rsync finished elapsed=${_rs_elapsed}s avg_rate≈${_rate} B/s ($(human_bytes "$_rate")/s)"
    else
      status_write "running" "step 2/5 complete"
      log "step 2/5 complete — rsync finished elapsed=${_rs_elapsed}s"
    fi
  else
    step_skip 2 "rsync-file"
  fi

  if remote_staging_needs_sudo "$REMOTE_STAGING"; then _stg_rm=(sudo rm -f); else _stg_rm=(rm -f); fi
  if step_should_run 3; then
    _step3_t0=$SECONDS
    CURRENT_STEP=3
    CURRENT_STEP_LABEL="install-file"
    status_write "running" "step 3/5 start"
    log "step 3/5 start — install dump file into PGDATA (postgres uid)"
    ssh -o BatchMode=yes "$ARNOLD_HOST" bash -s <<REMOTE
set -euo pipefail
pg_uid=\$(sudo docker run --rm pgvector/pgvector:pg17 id -u postgres)
pg_gid=\$(sudo docker run --rm pgvector/pgvector:pg17 id -g postgres)
sudo install -D -m 600 -o "\$pg_uid" -g "\$pg_gid" "$REMOTE_STAGING" "$REMOTE_PGDATA_DUMP"
${_stg_rm[0]} ${_stg_rm[1]} "$REMOTE_STAGING"
echo "installed $REMOTE_PGDATA_DUMP"
REMOTE
    status_write "running" "step 3/5 complete"
    log "step 3/5 complete — install file elapsed=$((SECONDS - _step3_t0))s"
  else
    step_skip 3 "install-file"
  fi
  inside_name="/var/lib/postgresql/data/$(basename "$ARTIFACT")"
else
  if step_should_run 2; then
    _step2_t0=$SECONDS
    CURRENT_STEP=2
    CURRENT_STEP_LABEL="rsync-directory"
    local_bytes="$(dir_size_bytes "$ARTIFACT")"
    status_write "running" "step 2/5 start"
    log "step 2/5 start — rsync directory → ${ARNOLD_HOST}:${REMOTE_STAGING_DIR}/ total_bytes≈${local_bytes} ($(human_bytes "$local_bytes")) (rsync --info=progress2 + heartbeat ${RSYNC_HEARTBEAT_SEC}s)"
    prepare_remote_staging_dir "$REMOTE_STAGING_DIR"
    heartbeat_start "step 2/5 rsync directory" "$RSYNC_HEARTBEAT_SEC"
    _rsync_rc=0
    if remote_staging_needs_sudo "$REMOTE_STAGING_DIR"; then
      rsync "${RSYNC_FLAGS[@]}" "${RSYNC_PATH_SECURE[@]}" -e "ssh -o BatchMode=yes" "${ARTIFACT}/" "${ARNOLD_HOST}:${REMOTE_STAGING_DIR}/" || _rsync_rc=$?
    else
      rsync "${RSYNC_FLAGS[@]}" -e "ssh -o BatchMode=yes" "${ARTIFACT}/" "${ARNOLD_HOST}:${REMOTE_STAGING_DIR}/" || _rsync_rc=$?
    fi
    heartbeat_stop
    [[ "$_rsync_rc" -eq 0 ]] || exit "$_rsync_rc"
    _rs_elapsed=$((SECONDS - _step2_t0))
    if [[ "$local_bytes" -gt 0 && "$_rs_elapsed" -gt 0 ]]; then
      _rate=$((local_bytes / _rs_elapsed))
      status_write "running" "step 2/5 complete"
      log "step 2/5 complete — rsync finished elapsed=${_rs_elapsed}s avg_rate≈${_rate} B/s ($(human_bytes "$_rate")/s)"
    else
      status_write "running" "step 2/5 complete"
      log "step 2/5 complete — rsync finished elapsed=${_rs_elapsed}s"
    fi
  else
    step_skip 2 "rsync-directory"
  fi

  # Same filesystem as PGDATA: mv avoids ~2× disk peak (rsync staging + full copy).
  INSTALL_DUMP_METHOD=cp
  if remote_staging_needs_sudo "$REMOTE_STAGING_DIR" && [[ "$REMOTE_PGDATA_DUMP" == /srv/hfa-secure/* ]]; then
    INSTALL_DUMP_METHOD=mv
  fi
  if remote_staging_needs_sudo "$REMOTE_STAGING_DIR"; then _stgdir_rm=(sudo rm -rf); else _stgdir_rm=(rm -rf); fi
  if step_should_run 3; then
    _step3_t0=$SECONDS
    CURRENT_STEP=3
    CURRENT_STEP_LABEL="install-directory"
    status_write "running" "step 3/5 start"
    log "step 3/5 start — install dump directory into PGDATA method=${INSTALL_DUMP_METHOD} dest=${REMOTE_PGDATA_DUMP}"
    ssh -o BatchMode=yes "$ARNOLD_HOST" bash -s <<REMOTE
set -euo pipefail
pg_uid=\$(sudo docker run --rm pgvector/pgvector:pg17 id -u postgres)
pg_gid=\$(sudo docker run --rm pgvector/pgvector:pg17 id -g postgres)
sudo rm -rf "$REMOTE_PGDATA_DUMP"
if [[ "$INSTALL_DUMP_METHOD" == mv ]]; then
  sudo mv "$REMOTE_STAGING_DIR" "$REMOTE_PGDATA_DUMP"
else
  sudo mkdir -p "$REMOTE_PGDATA_DUMP"
  sudo cp -a "$REMOTE_STAGING_DIR/." "$REMOTE_PGDATA_DUMP/"
  ${_stgdir_rm[0]} ${_stgdir_rm[1]} "$REMOTE_STAGING_DIR"
fi
sudo chown -R "\$pg_uid:\$pg_gid" "$REMOTE_PGDATA_DUMP"
sudo chmod -R u+rwX,g-rwx,o-rwx "$REMOTE_PGDATA_DUMP" || true
echo "installed dir $REMOTE_PGDATA_DUMP"
REMOTE
    status_write "running" "step 3/5 complete"
    log "step 3/5 complete — install directory elapsed=$((SECONDS - _step3_t0))s"
  else
    step_skip 3 "install-directory"
  fi
  inside_name="/var/lib/postgresql/data/$(basename "$ARTIFACT")"
fi

_step4_t0=$SECONDS
if step_should_run 4; then
  CURRENT_STEP=4
  CURRENT_STEP_LABEL="pg-restore"
  status_write "running" "step 4/5 start"
  log "step 4/5 start — pg_restore -j${PG_RESTORE_JOBS} inside=${inside_name} verbose_log=${REMOTE_RESTORE_VERBOSE_LOG} (verbose every ${RESTORE_LOG_INTERVAL} lines + heartbeat ${RESTORE_HEARTBEAT_SEC}s)"
  ssh -o BatchMode=yes "$ARNOLD_HOST" bash -s <<REMOTE
set -euo pipefail
set -a
. /home/arnold/openclaw/.env
set +a
CONTAINER="${REMOTE_PG_CONTAINER}"
INSIDE="${inside_name}"
JOBS="${PG_RESTORE_JOBS}"
HB=${RESTORE_HEARTBEAT_SEC}
LOG_INT=${RESTORE_LOG_INTERVAL}
VERBOSE_LOG="${REMOTE_RESTORE_VERBOSE_LOG}"
printf '[%s] archive-scp-restore: pg_restore subprocess starting jobs=%s path=%s verbose_log=%s\n' "\$(date -u +%Y-%m-%dT%H:%M:%SZ)" "\$JOBS" "\$INSIDE" "\$VERBOSE_LOG" >&2
(while sleep "\$HB"; do
  db_size=\$(sudo docker exec "\$CONTAINER" psql -U archive -d archive -Atc "select pg_database_size('archive'), pg_size_pretty(pg_database_size('archive'))" 2>/dev/null | tr '\n' ' ' || true)
  active_sessions=\$(sudo docker exec "\$CONTAINER" psql -U archive -d archive -Atc "select count(*) from pg_stat_activity where datname='archive' and pid <> pg_backend_pid()" 2>/dev/null | tr '\n' ' ' || true)
  printf '[%s] archive-scp-restore: pg_restore heartbeat interval=%ss db_size=%s active_sessions=%s (restore still running)\n' "\$(date -u +%Y-%m-%dT%H:%M:%SZ)" "\$HB" "\${db_size:-unknown}" "\${active_sessions:-unknown}"
done) &
_hb=\$!
trap 'kill \$_hb 2>/dev/null || true' EXIT
set -o pipefail
sudo docker exec -e PGPASSWORD="\$HFA_ARCHIVE_PG_PASSWORD" "\$CONTAINER" \
  pg_restore -U archive -d archive --clean --if-exists --no-owner --no-privileges --jobs="\$JOBS" -v "\$INSIDE" 2>&1 \
| tee "\$VERBOSE_LOG" \
| awk -v interval="\$LOG_INT" '
  BEGIN { IGNORECASE = 1 }
  NR % interval == 0 {
    printf("[%s] archive-scp-restore: pg_restore verbose lines=%d\n", strftime("%Y-%m-%dT%H:%M:%SZ"), NR)
    fflush()
  }
  /(ERROR|FATAL|WARNING)/ {
    printf("[%s] archive-scp-restore: pg_restore notable: %s\n", strftime("%Y-%m-%dT%H:%M:%SZ"), \$0)
    fflush()
  }'
st=\${PIPESTATUS[0]}
kill \$_hb 2>/dev/null || true
wait \$_hb 2>/dev/null || true
exit \$st
REMOTE
  status_write "running" "step 4/5 complete"
  log "step 4/5 complete — pg_restore finished elapsed=$((SECONDS - _step4_t0))s verbose_log=${REMOTE_RESTORE_VERBOSE_LOG}"
else
  step_skip 4 "pg-restore"
fi

_step5_t0=$SECONDS
if [[ "$REMOTE_REMOVE_DUMP_AFTER" == "1" ]]; then
  if step_should_run 5; then
    CURRENT_STEP=5
    CURRENT_STEP_LABEL="cleanup-remote-artifact"
    status_write "running" "step 5/5 start"
    log "step 5/5 start — remove remote dump artifact to free space"
    if [[ "$MODE" == "file" ]]; then
      ssh -o BatchMode=yes "$ARNOLD_HOST" "sudo rm -f \"$REMOTE_PGDATA_DUMP\""
    else
      ssh -o BatchMode=yes "$ARNOLD_HOST" "sudo rm -rf \"$REMOTE_PGDATA_DUMP\""
    fi
    status_write "running" "step 5/5 complete"
    log "step 5/5 complete — remote artifact removed elapsed=$((SECONDS - _step5_t0))s"
  else
    step_skip 5 "cleanup-remote-artifact"
  fi
else
  log "step 5/5 skipped — keeping remote artifact (REMOTE_REMOVE_DUMP_AFTER=0): $REMOTE_PGDATA_DUMP"
fi

RUN_COMPLETED=1
status_write "complete" "restore flow complete"
log "complete — total_elapsed=$((SECONDS - RUN_T0))s artifact_bytes≈${_artifact_bytes} ($(human_bytes "$_artifact_bytes")) jobs=${PG_RESTORE_JOBS} example: ssh $ARNOLD_HOST \"sudo docker exec $REMOTE_PG_CONTAINER psql -U archive -d archive -Atc 'select count(*) from archive_seed.cards'\""
