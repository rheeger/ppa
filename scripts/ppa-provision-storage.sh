#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/ppa-lib.sh"

ppa_require_root

SECURE_DIR="${PPA_SECURE_DIR:-/mnt/user/ppa-secure}"
IMAGE="${PPA_IMAGE:-$SECURE_DIR/ppa-vault.img}"
MAPPER="${PPA_MAPPER:-ppa-vault}"
MOUNT_ROOT="${PPA_MOUNT_ROOT:-/srv/ppa-secure}"
UNIX_USER="${PPA_UNIX_USER:-archive}"
UNIX_GROUP="${PPA_UNIX_GROUP:-archive}"
IMAGE_SIZE="${PPA_IMAGE_SIZE:-256G}"
UNLOCK_KEY="${PPA_UNLOCK_KEY:-}"
UNLOCK_KEY_OP_REF="${PPA_UNLOCK_KEY_OP_REF:-}"
UNLOCK_KEY_FILE="${PPA_UNLOCK_KEY_FILE:-}"

command -v cryptsetup >/dev/null 2>&1 || { echo "cryptsetup is required" >&2; exit 1; }
command -v losetup >/dev/null 2>&1 || { echo "losetup is required" >&2; exit 1; }
command -v mkfs.ext4 >/dev/null 2>&1 || { echo "mkfs.ext4 is required" >&2; exit 1; }

mkdir -p "$SECURE_DIR"
if [ -e "$IMAGE" ]; then
  echo "Encrypted archive image already exists at $IMAGE" >&2
  exit 1
fi

secret_value="$(ppa_resolve_secret_value "$UNLOCK_KEY" "$UNLOCK_KEY_OP_REF" "$UNLOCK_KEY_FILE" "archive unlock key")"
secret_file="$(ppa_make_secret_file "$secret_value")"
loop_dev=""
cleanup() {
  if mountpoint -q "$MOUNT_ROOT" 2>/dev/null; then
    umount "$MOUNT_ROOT" || true
  fi
  if [ -e "/dev/mapper/$MAPPER" ]; then
    cryptsetup close "$MAPPER" || true
  fi
  if [ -n "$loop_dev" ]; then
    losetup -d "$loop_dev" || true
  fi
  rm -f "$secret_file"
}
trap cleanup EXIT

truncate -s "$IMAGE_SIZE" "$IMAGE"
chmod 600 "$IMAGE"
loop_dev="$(losetup --find --show "$IMAGE")"
cryptsetup luksFormat --batch-mode --key-file "$secret_file" "$loop_dev"
cryptsetup open --key-file "$secret_file" "$loop_dev" "$MAPPER"
mkfs.ext4 -L ppa-archive "/dev/mapper/$MAPPER"

mkdir -p "$MOUNT_ROOT"
mount "/dev/mapper/$MAPPER" "$MOUNT_ROOT"
install -d -m 700 -o "$UNIX_USER" -g "$UNIX_GROUP" "$MOUNT_ROOT/vault" "$MOUNT_ROOT/postgres"

echo "Provisioned encrypted archive image at $IMAGE"
