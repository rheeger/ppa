#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DESTINATION="${PPA_BACKUP_GDRIVE_DEST:-}"

if [ -z "$DESTINATION" ]; then
  echo "PPA_BACKUP_GDRIVE_DEST is not configured." >&2
  exit 1
fi

exec bash "$SCRIPT_DIR/ppa-backup-upload.sh" "$DESTINATION" "gdrive"
