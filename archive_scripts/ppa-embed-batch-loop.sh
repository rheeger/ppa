#!/usr/bin/env bash
# Continuous submit/poll/ingest loop for the OpenAI Batch API embedding path.
#
# Stops when there are no pending chunks left OR the user Ctrl-Cs.
# Each cycle:
#   1. `embed-batch-submit` — fills the OpenAI queue up to
#      PPA_BATCH_MAX_OUTSTANDING (default 6) batches, then exits cleanly.
#   2. `embed-batch-poll`   — refreshes status; auto-reclaims batches that
#      OpenAI rejected with ``token_limit_exceeded`` so the chunks stay pending.
#   3. `embed-batch-ingest` — downloads any completed outputs and writes the
#      vectors into ``{schema}.embeddings``.
#   4. Sleep ``LOOP_INTERVAL_SEC`` (default 300s = 5min) then repeat.
#
# Expects:
#   - OPENAI_API_KEY in env (or ~/.ppa/openai_key.txt will be sourced)
#   - PPA_INDEX_DSN, PPA_PATH, PPA_INDEX_SCHEMA set (or sensible defaults)
#   - Run from the ppa repo root with .venv/bin/python installed
set -euo pipefail

# Resolve repo root (script lives in archive_scripts/).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON="${PYTHON:-.venv/bin/python}"
LOOP_INTERVAL_SEC="${LOOP_INTERVAL_SEC:-300}"
PPA_BATCH_MAX_OUTSTANDING="${PPA_BATCH_MAX_OUTSTANDING:-6}"
PPA_BATCH_REQUESTS_PER_BATCH="${PPA_BATCH_REQUESTS_PER_BATCH:-50000}"
PPA_BATCH_INGEST_WORKERS="${PPA_BATCH_INGEST_WORKERS:-4}"
PPA_EMBEDDING_MODEL="${PPA_EMBEDDING_MODEL:-text-embedding-3-small}"
PPA_EMBEDDING_VERSION="${PPA_EMBEDDING_VERSION:-1}"
PPA_STATEMENT_TIMEOUT_MS="${PPA_STATEMENT_TIMEOUT_MS:-3600000}"

# Fallback: source the local OpenAI key file if env var not set.
if [[ -z "${OPENAI_API_KEY:-}" && -r "${HOME}/.ppa/openai_key.txt" ]]; then
  export OPENAI_API_KEY="$(cat "${HOME}/.ppa/openai_key.txt")"
fi

if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "OPENAI_API_KEY not set (and ~/.ppa/openai_key.txt missing). Aborting." >&2
  exit 1
fi

LOG_DIR="_artifacts/_embedding-runs"
mkdir -p "$LOG_DIR"
LOOP_LOG="$LOG_DIR/batch-loop-$(date +%Y%m%d-%H%M%S).log"
echo "[loop] writing consolidated log to $LOOP_LOG"

export PPA_BATCH_MAX_OUTSTANDING PPA_STATEMENT_TIMEOUT_MS

iteration=0
while true; do
  iteration=$((iteration + 1))
  ts="$(date +'%Y-%m-%d %H:%M:%S')"
  echo "=== [$ts] iteration $iteration ===" | tee -a "$LOOP_LOG"

  # 1. Submit — fills outstanding queue; exits cleanly at max_outstanding.
  $PYTHON -m archive_cli embed-batch-submit \
    --embedding-model "$PPA_EMBEDDING_MODEL" \
    --embedding-version "$PPA_EMBEDDING_VERSION" \
    --requests-per-batch "$PPA_BATCH_REQUESTS_PER_BATCH" 2>&1 | tee -a "$LOOP_LOG" || true

  # 2. Poll — auto-reclaims token_limit_exceeded failures.
  $PYTHON -m archive_cli embed-batch-poll 2>&1 | tee -a "$LOOP_LOG" || true

  # 3. Ingest — writes completed outputs into embeddings table.
  $PYTHON -m archive_cli embed-batch-ingest --workers "$PPA_BATCH_INGEST_WORKERS" 2>&1 | tee -a "$LOOP_LOG" || true

  # 4. Status — one-line summary.
  $PYTHON -m archive_cli embed-batch-status 2>&1 | tee -a "$LOOP_LOG" || true

  # 5. Exit if no pending remain AND no in-flight batches.
  remaining=$(
    $PYTHON -m archive_cli embed-batch-status 2>/dev/null \
      | $PYTHON -c "import sys, json; d=json.load(sys.stdin); \
                    pending=d.get('pending_chunks_in_corpus',0); \
                    inflight=sum(b['count'] for b in d.get('batches_by_status',[]) \
                                  if b['status'] in ('validating','in_progress','finalizing')); \
                    print(pending + inflight)"
  )
  if [[ "$remaining" == "0" ]]; then
    echo "[loop] no pending chunks and no in-flight batches — done." | tee -a "$LOOP_LOG"
    exit 0
  fi

  echo "[loop] sleeping ${LOOP_INTERVAL_SEC}s (remaining work signal=$remaining)" | tee -a "$LOOP_LOG"
  sleep "$LOOP_INTERVAL_SEC"
done
