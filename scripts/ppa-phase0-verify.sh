#!/usr/bin/env bash
set -euo pipefail

echo "=== Phase 0 Verification ==="
echo ""

echo "1. Unit tests (no Postgres)..."
make test-unit
echo "   PASS"
echo ""

echo "2. Fixture validation..."
python -m pytest tests/test_fixtures.py -v --tb=short
echo "   PASS"
echo ""

echo "3. Rebuild manifest unit tests..."
python -m pytest tests/test_rebuild_manifest.py -v --tb=short
echo "   PASS"
echo ""

echo "4. Health-check unit tests..."
python -m pytest tests/test_health_check.py -v --tb=short
echo "   PASS"
echo ""

echo "=== Phase 0 Verification Complete ==="
