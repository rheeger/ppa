#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

BACKUP_BASE="${PPA_BACKUP_BASE:-/mnt/user/backups/ppa-encrypted}"
ARCHIVE_FILE="${PPA_BACKUP_FILE:-$(ppa_latest_backup_archive "$BACKUP_BASE")}"
CHECKSUM_FILE="${PPA_BACKUP_CHECKSUM_FILE:-$(ppa_latest_backup_checksum "$BACKUP_BASE")}"
RESTORE_DIR="${PPA_RESTORE_DIR:-/tmp/ppa-restore}"
PASSPHRASE="${PPA_BACKUP_PASSPHRASE:-}"
PASSPHRASE_OP_REF="${PPA_BACKUP_PASSPHRASE_OP_REF:-}"
PASSPHRASE_FILE="${PPA_BACKUP_PASSPHRASE_FILE:-}"

command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 1; }

if [ ! -f "$ARCHIVE_FILE" ]; then
  echo "Encrypted archive not found: $ARCHIVE_FILE" >&2
  exit 1
fi
if [ ! -f "$CHECKSUM_FILE" ]; then
  echo "Checksum file not found: $CHECKSUM_FILE" >&2
  exit 1
fi

sha256sum -c "$CHECKSUM_FILE"

secret_value="$(ppa_resolve_secret_value "$PASSPHRASE" "$PASSPHRASE_OP_REF" "$PASSPHRASE_FILE" "archive backup passphrase")"
secret_file="$(ppa_make_secret_file "$secret_value")"
trap 'rm -f "$secret_file"' EXIT

rm -rf "$RESTORE_DIR"
mkdir -p "$RESTORE_DIR"
openssl enc -d -aes-256-cbc -pbkdf2 -pass "file:$secret_file" -in "$ARCHIVE_FILE" \
  | tar -C "$RESTORE_DIR" -xf -

echo "ppa-backup-restore: restored_to=$RESTORE_DIR archive=$ARCHIVE_FILE"
