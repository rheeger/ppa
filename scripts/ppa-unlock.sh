#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

ppa_require_root

IMAGE="${PPA_IMAGE:-/mnt/user/ppa-secure/ppa-vault.img}"
MAPPER="${PPA_MAPPER:-ppa-vault}"
UNLOCK_KEY="${PPA_UNLOCK_KEY:-}"
UNLOCK_KEY_OP_REF="${PPA_UNLOCK_KEY_OP_REF:-}"
UNLOCK_KEY_FILE="${PPA_UNLOCK_KEY_FILE:-}"

command -v cryptsetup >/dev/null 2>&1 || { echo "cryptsetup is required" >&2; exit 1; }
command -v losetup >/dev/null 2>&1 || { echo "losetup is required" >&2; exit 1; }

if [ ! -f "$IMAGE" ]; then
  echo "Encrypted archive image not found: $IMAGE" >&2
  exit 1
fi

if [ -e "/dev/mapper/$MAPPER" ]; then
  echo "Archive mapper already unlocked: $MAPPER"
  exit 0
fi

secret_value="$(ppa_resolve_secret_value "$UNLOCK_KEY" "$UNLOCK_KEY_OP_REF" "$UNLOCK_KEY_FILE" "archive unlock key")"
secret_file="$(ppa_make_secret_file "$secret_value")"
trap 'rm -f "$secret_file"' EXIT

loop_dev="$(losetup --find --show "$IMAGE")"
cryptsetup open --key-file "$secret_file" "$loop_dev" "$MAPPER"

echo "Archive mapper unlocked: /dev/mapper/$MAPPER"
