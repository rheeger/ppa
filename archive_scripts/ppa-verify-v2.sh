#!/usr/bin/env bash
set -euo pipefail

ARNOLD_HOST="${ARNOLD_HOST:-arnold@192.168.50.27}"
SSH="ssh ${ARNOLD_HOST}"
FAIL=0

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

echo "=== PPA v2 Post-Deployment Verification ==="
echo ""

echo "--- 1. Structured Health Check ---"
$SSH 'set -a; . /home/arnold/openclaw/.env; set +a; cd /home/arnold/openclaw/ppa && sudo -u archive PPA_PATH="$PPA_PATH" PPA_INDEX_DSN="$PPA_INDEX_DSN" PPA_INDEX_SCHEMA="$PPA_INDEX_SCHEMA" /home/arnold/openclaw/venv/bin/python -m archive_cli health-check --v2 --report-format both' \
  && pass "health-check" || fail "health-check"

echo ""
echo "--- 2. Index Status ---"
$SSH 'set -a; . /home/arnold/openclaw/.env; set +a; cd /home/arnold/openclaw/ppa && sudo -u archive PPA_PATH="$PPA_PATH" PPA_INDEX_DSN="$PPA_INDEX_DSN" PPA_INDEX_SCHEMA="$PPA_INDEX_SCHEMA" /home/arnold/openclaw/venv/bin/python -m archive_cli index-status' \
  && pass "index-status" || fail "index-status"

echo ""
echo "--- 3. Latency Targets ---"
$SSH 'set -a; . /home/arnold/openclaw/.env; set +a; cd /home/arnold/openclaw/ppa && sudo -u archive PPA_INDEX_DSN="$PPA_INDEX_DSN" PPA_INDEX_SCHEMA="$PPA_INDEX_SCHEMA" /home/arnold/openclaw/venv/bin/python -m archive_cli latency-check --format table' \
  && pass "latency" || fail "latency"

echo ""
echo "--- 4. MCP Server Smoke Test ---"
MCP_SMOKE=$($SSH 'set -a; . /home/arnold/openclaw/.env; set +a; cd /home/arnold/openclaw/ppa && sudo -u archive PPA_PATH="$PPA_PATH" PPA_INDEX_DSN="$PPA_INDEX_DSN" PPA_INDEX_SCHEMA="$PPA_INDEX_SCHEMA" /home/arnold/openclaw/venv/bin/python <<'"'"'PY'"'"'
from archive_cli.server import mcp
tool_names = [t.name for t in mcp._tool_manager.list_tools()]
assert tool_names, "No MCP tools registered"
print(f"MCP tools registered: {len(tool_names)}")
from archive_cli.commands._resolve import resolve_index
idx = resolve_index()
fts = idx.search("test", limit=3)
print(f"FTS smoke: {len(fts or [])} results")
tn = idx.temporal_neighbors("2025-06-15T12:00:00Z", limit=3)
print(f"Temporal smoke: {len((tn or {}).get('cards', []))} results")
print("SMOKE_OK")
PY' 2>&1) || MCP_SMOKE="SMOKE_FAIL"
echo "$MCP_SMOKE"
echo "$MCP_SMOKE" | grep -q "SMOKE_OK" && pass "mcp-smoke" || fail "mcp-smoke"

echo ""
echo "--- 5. Maintenance Timer ---"
$SSH 'sudo systemctl status ppa-maintain.timer --no-pager -l 2>/dev/null || echo "NOT ENABLED"'

echo ""
echo "--- 6. MCP Service ---"
$SSH 'sudo systemctl status ppa-mcp.service --no-pager -l'

echo ""
echo "--- 7. Postgres GUCs ---"
$SSH 'sudo docker exec ppa-postgres psql -U archive -d archive -c "SHOW shared_buffers; SHOW work_mem; SHOW maintenance_work_mem; SHOW effective_cache_size;"'

echo ""
echo "--- 8. Disk Usage ---"
$SSH 'df -h /srv/hfa-secure / ; sudo du -sh /srv/hfa-secure/vault /srv/hfa-secure/postgres 2>/dev/null'

echo ""
echo "--- 9. archive_crate Rust binary ---"
$SSH 'cd /home/arnold/openclaw/ppa && sudo -u archive /home/arnold/openclaw/venv/bin/python -c "import archive_crate; print(archive_crate.__file__)" 2>&1' | head -3

echo ""
echo "=== Verification Complete: ${FAIL} failures ==="
[ "$FAIL" -gt 0 ] && exit 1 || exit 0
