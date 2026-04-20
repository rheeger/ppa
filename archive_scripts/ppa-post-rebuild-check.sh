#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
if [ ! -x "$PYTHON" ]; then PYTHON="python3"; fi

echo "=== Phase 4 Post-Rebuild Verification ==="
echo ""

echo "--- [1/8] Index Status ---"
"$PYTHON" -m archive_cli index-status
echo ""

echo "--- [2/8] Validation ---"
"$PYTHON" -m archive_cli validate
echo "PASS: Zero validation errors"
echo ""

echo "--- [3/8] Temporal Neighbors ---"
"$PYTHON" -m archive_cli temporal-neighbors --timestamp "2025-12-27T20:14:00-08:00" --limit 5
echo ""

echo "--- [4/8] Derived Card Queries ---"
for TYPE in meal_order ride place purchase flight; do
    echo "  $TYPE:"
    "$PYTHON" -m archive_cli query --type "$TYPE" --limit 3
    echo ""
done

echo "--- [5/8] Quality Report ---"
"$PYTHON" -m archive_cli quality-report
echo ""

echo "--- [6/8] Ingestion Log ---"
"$PYTHON" -c "
import os
import psycopg
dsn = os.environ.get('PPA_INDEX_DSN', '')
schema = os.environ.get('PPA_INDEX_SCHEMA', 'ppa')
if not dsn:
    print('PPA_INDEX_DSN not set -- skipping')
else:
    with psycopg.connect(dsn) as conn:
        card_count = conn.execute(f'SELECT COUNT(*) AS c FROM {schema}.cards').fetchone()[0]
        log_count = conn.execute(f'SELECT COUNT(*) AS c FROM {schema}.ingestion_log').fetchone()[0]
        print(f'Cards: {card_count}, Ingestion log: {log_count}')
        if log_count == card_count:
            print('PASS: ingestion_log count matches cards count')
        else:
            print(f'WARNING: count mismatch ({log_count} != {card_count})')
"
echo ""

echo "--- [7/8] Edge Type Counts ---"
"$PYTHON" -c "
import os
import psycopg
dsn = os.environ.get('PPA_INDEX_DSN', '')
schema = os.environ.get('PPA_INDEX_SCHEMA', 'ppa')
if not dsn:
    print('PPA_INDEX_DSN not set -- skipping')
else:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            f'SELECT edge_type, COUNT(*) FROM {schema}.edges GROUP BY edge_type ORDER BY count DESC'
        ).fetchall()
        for r in rows:
            print(f'  {r[0]}: {r[1]}')
"
echo ""

echo "--- [8/8] Health Check ---"
"$PYTHON" -m archive_cli health-check
echo ""

echo "=== Post-rebuild verification complete ==="
