#!/usr/bin/env bash
# Poll archive-scp-restore.log until success/failure/timeout, then run HFA post-restore steps.
# Logs to logs/scp-restore-watch.log (line-buffered via tee).
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HFA_ROOT="$(cd "$ROOT/../hey-arnold-hfa" && pwd)"
LOG="${ARCHIVE_SCP_RESTORE_LOG_FILE:-$ROOT/logs/archive-scp-restore.log}"
STATUS_FILE="${ARCHIVE_SCP_RESTORE_STATUS_FILE:-$ROOT/logs/archive-scp-restore.status}"
WATCH_LOG="${SCP_RESTORE_WATCH_LOG:-$ROOT/logs/scp-restore-watch.log}"
INTERVAL="${POLL_INTERVAL_SEC:-120}"
MAX_WAIT="${POLL_MAX_WAIT_SEC:-172800}" # 48h
LOCK_DIR="${WATCH_LOG}.lock"

mkdir -p "$(dirname "$WATCH_LOG")"
: >>"$WATCH_LOG"
exec > >(tee -a "$WATCH_LOG") 2>&1

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
wl() { printf '[%s] scp-restore-watch: %s\n' "$(ts)" "$*"; }

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  wl "error: watcher already running lock=${LOCK_DIR}"
  exit 1
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

read_status_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { print substr($0, index($0, "=") + 1); exit }' "$STATUS_FILE" 2>/dev/null || true
}

wl "start poll_interval=${INTERVAL}s max_wait=${MAX_WAIT}s log=${LOG} status=${STATUS_FILE} hfa=${HFA_ROOT}"

if [[ ! -f "$LOG" ]]; then
  wl "error: log file missing — run scp-restore first"
  exit 1
fi

start=$(date +%s)
while true; do
  now=$(date +%s)
  elapsed=$((now - start))
  if ((elapsed > MAX_WAIT)); then
    wl "timeout elapsed_wait=${elapsed}s — increase POLL_MAX_WAIT_SEC or check job"
    exit 2
  fi

  if [[ -f "$STATUS_FILE" ]]; then
    state="$(read_status_value state)"
    step="$(read_status_value step)"
    step_label="$(read_status_value step_label)"
    message="$(read_status_value message)"
    updated_at="$(read_status_value updated_at)"
    if [[ "$state" == "complete" ]]; then
      wl "detected completion via status file updated_at=${updated_at} step=${step} label=${step_label} message=${message}"
      break
    fi
    if [[ "$state" == "failed" ]]; then
      wl "detected failure via status file updated_at=${updated_at} step=${step} label=${step_label} message=${message}"
      exit 1
    fi
  else
    state=""
    step=""
    step_label=""
    message=""
    updated_at=""
  fi

  if grep -q 'complete — total_elapsed' "$LOG" 2>/dev/null; then
    wl "detected completion marker (total_elapsed)"
    break
  fi
  if grep -qE 'make: \*\*\* \[scp-restore-seed-arnold\]|rsync error: errors selecting|rsync\(.*\): error:' "$LOG" 2>/dev/null; then
    wl "detected failure pattern in log — see ${LOG}"
    exit 1
  fi

  # rsync --info=progress2 uses CR; tail chunk only (log can grow to GiB-scale).
  last=$(tail -c 262144 "$LOG" 2>/dev/null | tr '\r' '\n' | grep 'archive-scp-restore:' | tail -1 || true)
  wl "poll elapsed_wait=${elapsed}s state=${state:-unknown} step=${step:-?} label=${step_label:-unknown} updated_at=${updated_at:-unknown} message=${message:-unknown} last=${last:0:200}"
  sleep "$INTERVAL"
done

if ! grep -q 'step 4/5 complete — pg_restore finished' "$LOG" 2>/dev/null; then
  wl "warning: missing step 4/5 complete — inspect ${LOG} for pg_restore errors"
fi

if [[ ! -d "$HFA_ROOT" ]]; then
  wl "error: hey-arnold-hfa not found at ${HFA_ROOT}"
  exit 1
fi

wl "running policy-check + hfa-archive-post-seed-restore"
set -euo pipefail
cd "$HFA_ROOT"
make policy-check
make hfa-archive-post-seed-restore

wl "done — todos: phase 4–6 restore path + phase 7–9 verify/index/mcp (automated). Phase 10 (ngrok/public URL + OpenClaw evidence) is manual."
