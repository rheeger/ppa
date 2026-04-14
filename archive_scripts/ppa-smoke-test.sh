#!/usr/bin/env bash
# PPA CLI Smoke Test
#
# Exercises the core CLI surface against whatever Postgres is configured.
# Requires PPA_INDEX_DSN to be set (or discoverable via env/config).
# Works in CI, locally against Docker, and on Arnold over SSH.
#
# Exit code: 0 if all checks pass, non-zero on first failure (set -e).
set -euo pipefail

ppa_run() {
  if command -v ppa >/dev/null 2>&1; then
    ppa "$@"
  else
    python3 -m archive_cli "$@"
  fi
}

echo "=== PPA Smoke Test ==="
echo ""

echo "--- health ---"
ppa_run health

echo ""
echo "--- stats ---"
ppa_run stats

echo ""
echo "--- search ---"
ppa_run search "test" --limit 1

echo ""
echo "--- index-status ---"
ppa_run index-status

echo ""
echo "--- embedding-status ---"
ppa_run embedding-status

echo ""
echo "--- hybrid-search ---"
ppa_run hybrid-search "test query" --limit 1

echo ""
echo "=== All checks passed ==="
