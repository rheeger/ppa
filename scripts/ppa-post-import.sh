#!/bin/bash
set -euo pipefail

VAULT="${PPA_PATH:-/srv/hfa-secure/vault}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
TIMESTAMP="$(date -Iseconds)"
WORK_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

DEDUP_FILE="$WORK_DIR/dedup.txt"
VALIDATE_FILE="$WORK_DIR/validate.txt"
STATS_FILE="$WORK_DIR/stats.txt"
RELINK_FILE="$WORK_DIR/relink.txt"

echo "=== PPA Post-Import — $TIMESTAMP ==="

echo "--- otter-transcripts-relink ---"
if "$PYTHON" -m archive_sync --vault "$VAULT" otter-transcripts-relink >"$RELINK_FILE" 2>&1; then
  RELINK_STATUS=0
else
  RELINK_STATUS=$?
fi
cat "$RELINK_FILE"

echo "--- dedup-sweep ---"
if "$PYTHON" -m archive_doctor --vault "$VAULT" dedup-sweep >"$DEDUP_FILE" 2>&1; then
  DEDUP_STATUS=0
else
  DEDUP_STATUS=$?
fi
cat "$DEDUP_FILE"

echo "--- validate ---"
if "$PYTHON" -m archive_doctor --vault "$VAULT" validate >"$VALIDATE_FILE" 2>&1; then
  VALIDATE_STATUS=0
else
  VALIDATE_STATUS=$?
fi
cat "$VALIDATE_FILE"

echo "--- stats ---"
if "$PYTHON" -m archive_doctor --vault "$VAULT" stats >"$STATS_FILE" 2>&1; then
  STATS_STATUS=0
else
  STATS_STATUS=$?
fi
cat "$STATS_FILE"

"$PYTHON" - "$VAULT" "$TIMESTAMP" "$RELINK_FILE" "$DEDUP_FILE" "$VALIDATE_FILE" "$STATS_FILE" "$RELINK_STATUS" "$DEDUP_STATUS" "$VALIDATE_STATUS" "$STATS_STATUS" <<'PY'
import json
import sys
from pathlib import Path

vault = Path(sys.argv[1])
timestamp = sys.argv[2]
relink_file = Path(sys.argv[3])
dedup_file = Path(sys.argv[4])
validate_file = Path(sys.argv[5])
stats_file = Path(sys.argv[6])
relink_status = int(sys.argv[7])
dedup_status = int(sys.argv[8])
validate_status = int(sys.argv[9])
stats_status = int(sys.argv[10])

try:
    from hfa.config import load_config

    max_entries = load_config(vault).max_enrichment_log_entries
except Exception:
    max_entries = 100

log_path = vault / "_meta" / "enrichment-log.json"
try:
    log = json.loads(log_path.read_text(encoding="utf-8"))
    if not isinstance(log, list):
        log = []
except Exception:
    log = []

log.append(
    {
        "timestamp": timestamp,
        "relink_status": relink_status,
        "dedup_status": dedup_status,
        "validate_status": validate_status,
        "stats_status": stats_status,
        "relink": relink_file.read_text(encoding="utf-8"),
        "dedup": dedup_file.read_text(encoding="utf-8"),
        "validate": validate_file.read_text(encoding="utf-8"),
        "stats": stats_file.read_text(encoding="utf-8"),
    }
)
log_path.write_text(json.dumps(log[-max_entries:], indent=2), encoding="utf-8")
PY

echo "=== Post-import complete ==="

if [ "$RELINK_STATUS" -ne 0 ] || [ "$DEDUP_STATUS" -ne 0 ] || [ "$VALIDATE_STATUS" -ne 0 ] || [ "$STATS_STATUS" -ne 0 ]; then
  exit 1
fi
