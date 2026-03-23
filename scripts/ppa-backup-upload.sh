#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

DESTINATION="${1:-}"
LABEL="${2:-remote}"
BACKUP_BASE="${PPA_BACKUP_BASE:-/mnt/user/backups/ppa-encrypted}"

if [ -z "$DESTINATION" ]; then
  echo "Missing backup destination for ${LABEL}" >&2
  exit 1
fi

ARCHIVE_FILE="$(ppa_latest_backup_archive "$BACKUP_BASE")"
MANIFEST_FILE="$(ppa_latest_backup_manifest "$BACKUP_BASE")"
CHECKSUM_FILE="$(ppa_latest_backup_checksum "$BACKUP_BASE")"

for required in "$ARCHIVE_FILE" "$MANIFEST_FILE" "$CHECKSUM_FILE"; do
  if [ ! -f "$required" ]; then
    echo "Missing encrypted backup artifact: $required" >&2
    exit 1
  fi
done

case "$ARCHIVE_FILE" in
  *.enc) ;;
  *)
    echo "Refusing to upload non-encrypted artifact: $ARCHIVE_FILE" >&2
    exit 1
    ;;
esac

upload_one() {
  local source="$1"
  local destination_root="$2"
  local destination_name="$3"

  if [[ "$destination_root" == *:* ]]; then
    command -v rclone >/dev/null 2>&1 || {
      echo "rclone is required for remote destination $destination_root" >&2
      exit 1
    }
    rclone copyto "$source" "${destination_root%/}/$destination_name"
    return
  fi

  mkdir -p "$destination_root"
  cp "$source" "$destination_root/$destination_name"
}

upload_one "$ARCHIVE_FILE" "$DESTINATION" "$(basename "$ARCHIVE_FILE")"
upload_one "$MANIFEST_FILE" "$DESTINATION" "$(basename "$MANIFEST_FILE")"
upload_one "$CHECKSUM_FILE" "$DESTINATION" "$(basename "$CHECKSUM_FILE")"

echo "ppa-backup-upload: target=$LABEL destination=$DESTINATION artifact=$(basename "$ARCHIVE_FILE")"
