#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$SCRIPT_DIR/ppa-backup-encrypt.sh"

if [ "${PPA_BACKUP_UPLOAD_GDRIVE_ON_BACKUP:-0}" = "1" ] && [ -n "${PPA_BACKUP_GDRIVE_DEST:-}" ]; then
  bash "$SCRIPT_DIR/ppa-backup-upload-gdrive.sh"
fi

if [ "${PPA_BACKUP_UPLOAD_ICLOUD_ON_BACKUP:-0}" = "1" ] && [ -n "${PPA_BACKUP_ICLOUD_DEST:-}" ]; then
  bash "$SCRIPT_DIR/ppa-backup-upload-icloud.sh"
fi
