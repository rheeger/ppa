#!/usr/bin/env bash
# Run slice-seed at 1%, 5%, 10% against the benchmark seed vault with increasing
# --progress-every and --cluster-cap (larger % → higher cap so transitive closure
# can grow with the slice). After each slice output is built, runs the same verify
# path as `make test-slice-verify`: bootstrap-postgres (once), rebuild-indexes,
# health-check --manifest archive_tests/slice_manifest.json (override with PPA_SLICE_MANIFEST).
# Logs wall time per step; compare ppa.slice lines for cache_hit=true vs first cold build.
#
# Optional: rm -f "$SEED/_meta/vault-scan-cache.sqlite3" before running to force
# a cold tier-1 rebuild (~40+ min on ~1.85M notes).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SEED="${PPA_BENCHMARK_SOURCE_VAULT:-$HOME/Archive/seed/hf-archives-seed-20260307-235127}"
LOGDIR="${SLICE_BENCH_LOGDIR:-/tmp/ppa-slice-cache-bench-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$LOGDIR"
export PYTHONUNBUFFERED=1

# Matches Makefile LOCAL_PPA_INDEX_DSN_CMD (requires docker compose Postgres up).
resolve_index_dsn() {
  local pg_env="${PG_ENV_FILE:-$ROOT/.env.pgvector}"
  local compose="${COMPOSE_FILE:-$ROOT/docker-compose.pgvector.yml}"
  local user pass db port
  user="$(grep '^POSTGRES_USER=' "$pg_env" | cut -d= -f2)"
  pass="$(grep '^POSTGRES_PASSWORD=' "$pg_env" | cut -d= -f2)"
  db="$(grep '^POSTGRES_DB=' "$pg_env" | cut -d= -f2)"
  port="$(docker compose --env-file "$pg_env" -f "$compose" port archive-postgres 5432 | awk -F: '{print $NF}')"
  printf 'postgresql://%s:%s@127.0.0.1:%s/%s' "$user" "$pass" "$port" "$db"
}

SLICE_MANIFEST="${PPA_SLICE_MANIFEST:-$ROOT/archive_tests/slice_manifest.json}"
# Dedicated schema so this bench run does not clobber archive_test_slice from `make test-slice-verify`.
export PPA_INDEX_SCHEMA="${PPA_INDEX_SCHEMA:-archive_slice_cache_bench}"
export PPA_PRIMARY_USER_UID="${PPA_PRIMARY_USER_UID:-hfa-person-9c9dbd68e803}"
export PPA_EMBEDDING_PROVIDER="${PPA_EMBEDDING_PROVIDER:-hash}"
export PPA_EMBEDDING_MODEL="${PPA_EMBEDDING_MODEL:-archive-hash-dev}"
export PPA_EMBEDDING_VERSION="${PPA_EMBEDDING_VERSION:-1}"
if [[ -z "${PPA_INDEX_DSN:-}" ]]; then
  export PPA_INDEX_DSN="$(resolve_index_dsn)"
fi

_BENCH_BOOTSTRAP_DONE=0

run_step() {
  local title="$1"
  shift
  echo ""
  echo "======== ${title} ========"
  echo "started $(date -Iseconds)"
  # shellcheck disable=SC2086
  /usr/bin/time -p "$@" 2>&1
  echo "finished $(date -Iseconds)"
}

# Same sequence as Makefile test-slice-verify; PPA_PATH = slice output directory.
run_verify_slice() {
  local slice_path="$1"
  local tag="$2"
  local vreport="$LOGDIR/verify-$tag"
  mkdir -p "$vreport"
  export PPA_PATH="$slice_path"
  echo ""
  echo "======== VERIFY ${tag} (PPA_PATH=$slice_path) ========"
  echo "started $(date -Iseconds)"
  if [[ "$_BENCH_BOOTSTRAP_DONE" -eq 0 ]]; then
    /usr/bin/time -p .venv/bin/python -m archive_cli --log-file "$LOGDIR/bootstrap-${tag}.log" bootstrap-postgres
    _BENCH_BOOTSTRAP_DONE=1
  fi
  /usr/bin/time -p .venv/bin/python -m archive_cli --log-file "$LOGDIR/rebuild-${tag}.log" rebuild-indexes
  if ! /usr/bin/time -p .venv/bin/python -m archive_cli --log-file "$LOGDIR/health-${tag}.log" health-check \
    --manifest "$SLICE_MANIFEST" --report-format both --report-dir "$vreport"
  then
    echo "WARNING: health-check failed for ${tag} — see $LOGDIR/health-${tag}.log and $vreport (continuing bench)" >&2
  fi
  echo "verify finished $(date -Iseconds)"
}

{
  echo "LOGDIR=$LOGDIR"
  echo "SEED=$SEED"
  echo "cache file (if any): $SEED/_meta/vault-scan-cache.sqlite3"
  ls -la "$SEED/_meta/vault-scan-cache.sqlite3" 2>/dev/null || echo "(no cache file yet)"
  echo ""

  # 1% — tighter hub cap (default-ish); 5% / 10% escalate cluster_cap with slice size
  run_step "1% cluster_cap=400 progress_every=5000" \
    .venv/bin/python -m archive_cli --log-file "$LOGDIR/1pct.log" slice-seed \
      --config archive_archive_tests/slice_config.json --source-vault "$SEED" --output "$LOGDIR/out-1pct" \
      --target-percent 1 --cluster-cap 400 --progress-every 5000 --dangling-rounds 3
  run_verify_slice "$LOGDIR/out-1pct" "1pct"

  # 1% repeat — same %, should be cache hit on scan if cache exists
  run_step "1% repeat cluster_cap=400 (expect cache_hit on scan)" \
    .venv/bin/python -m archive_cli --log-file "$LOGDIR/1pct-repeat.log" slice-seed \
      --config archive_archive_tests/slice_config.json --source-vault "$SEED" --output "$LOGDIR/out-1pct-repeat" \
      --target-percent 1 --cluster-cap 400 --progress-every 5000 --dangling-rounds 3
  run_verify_slice "$LOGDIR/out-1pct-repeat" "1pct-repeat"

  # 5%
  run_step "5% cluster_cap=1000 progress_every=15000" \
    .venv/bin/python -m archive_cli --log-file "$LOGDIR/5pct.log" slice-seed \
      --config archive_archive_tests/slice_config.json --source-vault "$SEED" --output "$LOGDIR/out-5pct" \
      --target-percent 5 --cluster-cap 1000 --progress-every 15000 --dangling-rounds 3
  run_verify_slice "$LOGDIR/out-5pct" "5pct"

  # 10%
  run_step "10% cluster_cap=2500 progress_every=30000" \
    .venv/bin/python -m archive_cli --log-file "$LOGDIR/10pct.log" slice-seed \
      --config archive_archive_tests/slice_config.json --source-vault "$SEED" --output "$LOGDIR/out-10pct" \
      --target-percent 10 --cluster-cap 2500 --progress-every 30000 --dangling-rounds 3
  run_verify_slice "$LOGDIR/out-10pct" "10pct"

  echo ""
  echo "DONE $(date -Iseconds)"
  echo "Grep logs for cache: rg 'cache_hit|vault-cache (hit|miss)|slice-seed scan' $LOGDIR/*.log"
  echo "Verify reports: $LOGDIR/verify-*/"
} 2>&1 | tee "$LOGDIR/runner.log"
