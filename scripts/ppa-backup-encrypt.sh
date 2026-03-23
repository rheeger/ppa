#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

VAULT="${PPA_PATH:-/srv/ppa-secure/vault}"
BACKUP_BASE="${PPA_BACKUP_BASE:-/mnt/user/backups/ppa-encrypted}"
PASSPHRASE="${PPA_BACKUP_PASSPHRASE:-}"
PASSPHRASE_OP_REF="${PPA_BACKUP_PASSPHRASE_OP_REF:-}"
PASSPHRASE_FILE="${PPA_BACKUP_PASSPHRASE_FILE:-}"
RETENTION_DAYS="${PPA_BACKUP_RETENTION_DAYS:-30}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_PARENT="$(dirname "$BACKUP_BASE")"
ARTIFACT_DIR="$BACKUP_BASE/artifacts/$TIMESTAMP"
LATEST_DIR="$(ppa_latest_backup_dir "$BACKUP_BASE")"
ARCHIVE_FILE="$ARTIFACT_DIR/ppa-backup.tar.enc"
MANIFEST_FILE="$ARTIFACT_DIR/ppa-backup.manifest.json.enc"
CHECKSUM_FILE="$ARTIFACT_DIR/ppa-backup.tar.enc.sha256"

command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 1; }

if [ ! -d "$VAULT" ]; then
  echo "PPA vault not found at $VAULT" >&2
  exit 1
fi

if [ ! -d "$BACKUP_PARENT" ]; then
  echo "Backup parent directory not mounted: $BACKUP_PARENT" >&2
  exit 1
fi

secret_value="$(ppa_resolve_secret_value "$PASSPHRASE" "$PASSPHRASE_OP_REF" "$PASSPHRASE_FILE" "archive backup passphrase")"
secret_file="$(ppa_make_secret_file "$secret_value")"
manifest_plain="$(mktemp)"
trap 'rm -f "$secret_file" "$manifest_plain"' EXIT

mkdir -p "$ARTIFACT_DIR" "$LATEST_DIR"

python3 - "$VAULT" "$manifest_plain" "$TIMESTAMP" <<'PY'
import json
import os
import sys
from pathlib import Path

vault = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
timestamp = sys.argv[3]
card_count = 0
meta_count = 0
byte_count = 0
paths = []
for path in sorted(vault.rglob("*")):
    if not path.is_file():
        continue
    rel = path.relative_to(vault).as_posix()
    if path.suffix == ".md":
        card_count += 1
    elif path.suffix == ".json":
        meta_count += 1
    byte_count += path.stat().st_size
    paths.append(rel)

manifest = {
    "timestamp": timestamp,
    "vault_path": str(vault),
    "card_count": card_count,
    "meta_count": meta_count,
    "byte_count": byte_count,
    "paths": paths,
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
PY

tar -C "$VAULT" -cf - . \
  | openssl enc -aes-256-cbc -pbkdf2 -salt -pass "file:$secret_file" -out "$ARCHIVE_FILE"

openssl enc -aes-256-cbc -pbkdf2 -salt -pass "file:$secret_file" \
  -in "$manifest_plain" -out "$MANIFEST_FILE"

sha256sum "$ARCHIVE_FILE" > "$CHECKSUM_FILE"

rm -rf "$LATEST_DIR"
mkdir -p "$LATEST_DIR"
cp "$ARCHIVE_FILE" "$(ppa_latest_backup_archive "$BACKUP_BASE")"
cp "$MANIFEST_FILE" "$(ppa_latest_backup_manifest "$BACKUP_BASE")"
cp "$CHECKSUM_FILE" "$(ppa_latest_backup_checksum "$BACKUP_BASE")"

find "$BACKUP_BASE/artifacts" -mindepth 1 -maxdepth 1 -type d -mtime +"$RETENTION_DAYS" -exec rm -rf {} +

card_count="$(python3 - "$manifest_plain" <<'PY'
import json
import sys
from pathlib import Path
print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["card_count"])
PY
)"
meta_count="$(python3 - "$manifest_plain" <<'PY'
import json
import sys
from pathlib import Path
print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["meta_count"])
PY
)"

echo "ppa-backup: timestamp=$TIMESTAMP cards=$card_count meta=$meta_count archive=$ARCHIVE_FILE"
