#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

VAULT="${PPA_PATH:-/srv/hfa-secure/vault}"
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

ppa_require_openssl
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v gsha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "sha256sum or shasum is required for backup integrity; refusing plaintext fallback" >&2
  exit 1
fi
CANONICAL_ONLY="${PPA_BACKUP_CANONICAL_ONLY:-}"

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
file_list="$(mktemp)"
trap 'rm -f "$secret_file" "$manifest_plain" "$file_list"' EXIT

mkdir -p "$ARTIFACT_DIR" "$LATEST_DIR"

python3 - "$VAULT" "$manifest_plain" "$TIMESTAMP" "$CANONICAL_ONLY" "$file_list" <<'PY'
import json
import os
import sys
from pathlib import Path

vault = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
timestamp = sys.argv[3]
canonical_only = sys.argv[4] in {"1", "true", "yes"}
file_list_path = Path(sys.argv[5])
secret_basenames = {
    "credentials.json",
    "client_secret.json",
    "gcp-oauth.keys.json",
    "gemini_key.txt",
    ".env",
}
secret_markers = ("passphrase", "refresh-token", "refresh_token", "service-account", "service_account")
secret_suffixes = (".pem", ".key", ".p12", ".pfx")
reconstructible_names = {
    "dedup-candidates.json",
    "llm-cache.json",
    "processors.json",
    "source-updaters.json",
    "vault-scan-cache.sqlite3",
    "query-embed-cache.sqlite",
}

def skip(rel: str) -> bool:
    name = Path(rel).name.lower()
    if name in secret_basenames or name.startswith(".env."):
        return True
    if name.endswith(secret_suffixes):
        return True
    if any(marker in name for marker in secret_markers):
        return True
    if name.endswith(("-wal", "-shm")):
        return True
    if canonical_only:
        if name in reconstructible_names:
            return True
        if rel == "_meta/rust-search-index" or rel.startswith("_meta/rust-search-index/"):
            return True
        if rel == "_templates" or rel.startswith("_templates/"):
            return True
    return False

card_count = 0
meta_count = 0
byte_count = 0
paths = []
for path in sorted(vault.rglob("*")):
    if path.is_symlink() or not path.is_file():
        continue
    rel = path.relative_to(vault).as_posix()
    if skip(rel):
        continue
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
    "encryption": "openssl-enc-aes-256-cbc-pbkdf2",
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
file_list_path.write_text("\n".join(paths) + ("\n" if paths else ""), encoding="utf-8")
PY

# Stream selected files into openssl. Never write a plaintext archive.
tar -C "$VAULT" -T "$file_list" -cf - \
  | openssl enc -aes-256-cbc -pbkdf2 -salt -pass "file:$secret_file" -out "$ARCHIVE_FILE"

openssl enc -aes-256-cbc -pbkdf2 -salt -pass "file:$secret_file" \
  -in "$manifest_plain" -out "$MANIFEST_FILE"

ppa_sha256sum "$ARCHIVE_FILE" > "$CHECKSUM_FILE"

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
