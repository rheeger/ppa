#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:$PYTHONPATH}"

BACKUP_BASE="${PPA_BACKUP_BASE:-/mnt/user/backups/ppa-encrypted}"
ARCHIVE_FILE="${PPA_BACKUP_FILE:-$(ppa_latest_backup_archive "$BACKUP_BASE")}"
CHECKSUM_FILE="${PPA_BACKUP_CHECKSUM_FILE:-$(ppa_latest_backup_checksum "$BACKUP_BASE")}"
RESTORE_DIR="${PPA_RESTORE_DIR:-/tmp/ppa-restore}"
PASSPHRASE="${PPA_BACKUP_PASSPHRASE:-}"
PASSPHRASE_OP_REF="${PPA_BACKUP_PASSPHRASE_OP_REF:-}"
PASSPHRASE_FILE="${PPA_BACKUP_PASSPHRASE_FILE:-}"
ACTIVE_ROOT="${PPA_PATH:-}"

ppa_require_openssl
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v gsha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "sha256sum or shasum is required for backup integrity; refusing plaintext fallback" >&2
  exit 1
fi

if [ ! -f "$ARCHIVE_FILE" ]; then
  echo "Encrypted archive not found: $ARCHIVE_FILE" >&2
  exit 1
fi
if [ ! -f "$CHECKSUM_FILE" ]; then
  echo "Checksum file not found: $CHECKSUM_FILE" >&2
  exit 1
fi

if [ -n "$ACTIVE_ROOT" ] && ppa_paths_overlap "$RESTORE_DIR" "$ACTIVE_ROOT"; then
  echo "ppa-backup-restore: refusing to restore onto active root $ACTIVE_ROOT" >&2
  exit 1
fi

ppa_sha256sum -c "$CHECKSUM_FILE"

secret_value="$(ppa_resolve_secret_value "$PASSPHRASE" "$PASSPHRASE_OP_REF" "$PASSPHRASE_FILE" "archive backup passphrase")"
secret_file="$(ppa_make_secret_file "$secret_value")"
trap 'rm -f "$secret_file"' EXIT

if [ -e "$RESTORE_DIR" ] && [ -n "$(ls -A "$RESTORE_DIR" 2>/dev/null || true)" ]; then
  echo "ppa-backup-restore: destination is not empty: $RESTORE_DIR" >&2
  exit 1
fi
mkdir -p "$RESTORE_DIR"

# Contained extract through P05 path rules. No raw tar -xf fallback.
python3 - "$ARCHIVE_FILE" "$RESTORE_DIR" "$secret_file" <<'PY'
import sys
from archive_engine.recovery import decrypt_and_contained_extract

decrypt_and_contained_extract(sys.argv[1], sys.argv[2], passphrase_file=sys.argv[3])
PY

echo "ppa-backup-restore: restored_to=$RESTORE_DIR archive=$ARCHIVE_FILE"
