#!/usr/bin/env bash
# Remove disposable PPA slice outputs, extract staging, and bench dirs under /tmp.
# Does not touch PPA_BENCHMARK_SOURCE_VAULT or Archive/tests unless you pass --archive-benchmark-sample.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_PPA="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL_SLICES_ROOT="${REPO_PPA}/.slices"

DRY_RUN=0
REMOVE_LOCAL_SLICES=0
REMOVE_ARCHIVE_BENCHMARK=0

usage() {
  cat <<'EOF'
Usage: clean-ppa-machine-artifacts.sh [options]

  --dry-run                  Print what would be removed; do not delete.
  --remove-local-slices      Also rm -rf .slices/{1pct,5pct,10pct} under this repo (large).
  --archive-benchmark-sample Also remove ~/Archive/tests/hf-archives-benchmark-sample (make rebuild-benchmark-sample to recreate).

Default removes only known /tmp PPA artifact paths (Makefile + docs); never the seed vault.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --remove-local-slices) REMOVE_LOCAL_SLICES=1 ;;
    --archive-benchmark-sample) REMOVE_ARCHIVE_BENCHMARK=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
  shift
done

rm_path() {
  local p="$1"
  if [[ ! -e "$p" && ! -L "$p" ]]; then
    return 0
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "would remove: $p"
    return 0
  fi
  echo "removing: $p"
  rm -rf "$p"
}

# Fixed paths referenced by Makefile / SLICE_TESTING.md / scripts.
fixed_paths=(
  /tmp/ppa-test-slice
  /tmp/ppa-test-slice-smoke
  /tmp/ppa-test-slice-seed
  /tmp/ppa-slice-smoke-extract-staging
  /tmp/ppa-slice-full-extract-staging
  /tmp/ppa-extract-demo-staging
  /tmp/ppa-extract-demo-vault
  /tmp/ppa-cli-empty
  /tmp/ppa-manifest-slice
  /tmp/ppa-slice-test
  /tmp/5pct-slice
  /tmp/bench-results
  /tmp/extract-bench
  /tmp/ppa-restore
)

log_paths=(
  /tmp/ppa-extract-demo.log
  /tmp/ppa-slice-smoke-build.log
  /tmp/ppa-slice-smoke-extract.log
)

for p in "${fixed_paths[@]}"; do
  rm_path "$p"
done

for p in "${log_paths[@]}"; do
  rm_path "$p"
done

shopt -s nullglob
for p in /tmp/ppa-1pct-cache-probe-* /tmp/ppa-slice-cache-bench-*; do
  rm_path "$p"
done
shopt -u nullglob

if [[ "$REMOVE_LOCAL_SLICES" -eq 1 ]]; then
  for pct in 1pct 5pct 10pct; do
    rm_path "${LOCAL_SLICES_ROOT}/${pct}"
  done
fi

if [[ "$REMOVE_ARCHIVE_BENCHMARK" -eq 1 ]]; then
  rm_path "/Users/rheeger/Archive/tests/hf-archives-benchmark-sample"
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "(dry-run; no files removed)"
else
  echo "done."
fi
