#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

ppa_require_root

SECURE_DIR="${PPA_SECURE_DIR:-/mnt/user/archive-secure}"
IMAGE="${PPA_IMAGE:-$SECURE_DIR/hfa-vault.img}"
MAPPER="${PPA_MAPPER:-hfa-archive-vault}"
MOUNT_ROOT="${PPA_MOUNT_ROOT:-/srv/hfa-secure}"

if mountpoint -q "$MOUNT_ROOT"; then
  umount "$MOUNT_ROOT"
fi

if [ -e "/dev/mapper/$MAPPER" ]; then
  cryptsetup close "$MAPPER"
fi

while read -r loop_dev _; do
  [ -n "$loop_dev" ] || continue
  losetup -d "$loop_dev" || true
done < <(losetup -j "$IMAGE")

echo "Archive locked for image $IMAGE"
