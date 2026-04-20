#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
if [ ! -x "$PYTHON" ]; then PYTHON="python3"; fi

echo "=== Phase 4 Pre-Rebuild Verification ==="
echo ""

GIT_COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")"
echo "Rollback anchor commit: $GIT_COMMIT"
echo "$GIT_COMMIT" > /tmp/ppa-rebuild-anchor.txt
echo ""

echo "--- [1/5] Running unit tests ---"
"$PYTHON" -m pytest archive_tests/ -x --tb=short -q -m "not integration and not slow"
echo "PASS: Unit tests"
echo ""

echo "--- [2/5] Running slice verification ---"
make test-slice-verify-smoke
echo "PASS: Slice verification"
echo ""

echo "--- [3/5] Running health check ---"
"$PYTHON" -m archive_cli health-check
echo "PASS: Health check"
echo ""

echo "--- [4/5] Vault validation ---"
"$PYTHON" -m archive_cli validate
echo "PASS: Vault validation"
echo ""

echo "--- [5/5] Benchmark extrapolation ---"
if [ -f /tmp/bench-results/benchmark-5pct.json ]; then
    python3 -c "
import json
with open('/tmp/bench-results/benchmark-5pct.json') as f:
    d = json.load(f)
print('5% slice rebuild time:', d.get('rebuild_seconds', '?'), 'seconds')
print('Projected full rebuild (Rust):', round(d.get('rebuild_seconds', 0) * 20 / 60, 1), 'minutes')
print('Recommended workers:', d.get('optimal_workers', '?'))
"
else
    echo "No benchmark results found. Run 'make benchmark-5pct' first."
fi
echo ""

echo "=== All pre-rebuild checks passed ==="
echo "Recommended: ppa rebuild-indexes --force-full-rebuild --workers N"
