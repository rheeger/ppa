#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

ppa_require_root

MAPPER="${PPA_MAPPER:-hfa-archive-vault}"
MOUNT_ROOT="${PPA_MOUNT_ROOT:-/srv/hfa-secure}"
UNIX_USER="${PPA_UNIX_USER:-archive}"
UNIX_GROUP="${PPA_UNIX_GROUP:-archive}"

if [ ! -e "/dev/mapper/$MAPPER" ]; then
  echo "Archive mapper is not unlocked: $MAPPER" >&2
  exit 1
fi

mkdir -p "$MOUNT_ROOT"
if mountpoint -q "$MOUNT_ROOT"; then
  echo "Archive mount already active at $MOUNT_ROOT"
  exit 0
fi

mount -o nodev,nosuid "/dev/mapper/$MAPPER" "$MOUNT_ROOT"
chown root "$MOUNT_ROOT"
chgrp "$UNIX_GROUP" "$MOUNT_ROOT"
chmod 750 "$MOUNT_ROOT"
install -d -m 700 -o "$UNIX_USER" -g "$UNIX_GROUP" "$MOUNT_ROOT/vault" "$MOUNT_ROOT/postgres"

echo "Archive mounted at $MOUNT_ROOT"
