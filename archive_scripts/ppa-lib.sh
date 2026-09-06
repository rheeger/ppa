#!/bin/bash
set -euo pipefail

ppa_require_root() {
  if [ "${EUID:-$(id -u)}" -ne 0 ]; then
    echo "This command must run as root." >&2
    exit 1
  fi
}

ppa_op_read() {
  local reference="$1"
  local token="${OP_SERVICE_ACCOUNT_TOKEN:-}"
  local token_file="${OP_TOKENS_SA_TOKEN_PATH:-/home/arnold/.openclaw/credentials/op-tokens-service-account-token}"
  local value=""
  if [ -z "$reference" ]; then
    echo "Missing op:// reference" >&2
    return 1
  fi
  if [ -z "$token" ] && [ -f "$token_file" ]; then
    token="$(tr -d '\n' < "$token_file")"
  fi
  if [ -z "$token" ]; then
    echo "OP_SERVICE_ACCOUNT_TOKEN is not available for op:// read" >&2
    return 1
  fi
  value="$(OP_SERVICE_ACCOUNT_TOKEN="$token" op read "$reference")" || return 1
  if [ -z "$value" ]; then
    echo "Resolved empty secret from $reference" >&2
    return 1
  fi
  printf '%s' "$value"
}

ppa_resolve_secret_value() {
  local direct_value="${1:-}"
  local op_ref="${2:-}"
  local file_path="${3:-}"
  local label="${4:-secret}"
  local resolved=""

  if [ -n "$direct_value" ]; then
    resolved="$direct_value"
  elif [ -n "$op_ref" ]; then
    resolved="$(ppa_op_read "$op_ref")" || return 1
  elif [ -n "$file_path" ] && [ -f "$file_path" ]; then
    resolved="$(tr -d '\n' < "$file_path")"
  else
    echo "Could not resolve ${label}." >&2
    return 1
  fi

  if [ -z "$resolved" ]; then
    echo "Resolved empty ${label}." >&2
    return 1
  fi

  printf '%s' "$resolved"
}

ppa_make_secret_file() {
  local secret_value="$1"
  local secret_file
  secret_file="$(mktemp)"
  chmod 600 "$secret_file"
  printf '%s' "$secret_value" > "$secret_file"
  printf '%s' "$secret_file"
}

ppa_require_openssl() {
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required for encrypted backup; refusing plaintext fallback" >&2
    exit 1
  fi
}

ppa_sha256sum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  elif command -v gsha256sum >/dev/null 2>&1; then
    gsha256sum "$@"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$@"
  else
    echo "sha256sum or shasum is required for backup integrity; refusing plaintext fallback" >&2
    exit 1
  fi
}

ppa_paths_overlap() {
  local left="${1:?}"
  local right="${2:?}"
  python3 - "$left" "$right" <<'PY'
import sys
from pathlib import Path
left = Path(sys.argv[1]).expanduser()
right = Path(sys.argv[2]).expanduser()
try:
    a = left.resolve()
    b = right.resolve()
except OSError:
    sys.exit(1)
sys.exit(0 if a == b or a in b.parents or b in a.parents else 1)
PY
}

ppa_latest_backup_dir() {
  local backup_base="${1:?backup base required}"
  printf '%s' "${backup_base%/}/latest"
}

ppa_latest_backup_archive() {
  local backup_base="${1:?backup base required}"
  printf '%s/ppa-backup.tar.enc' "$(ppa_latest_backup_dir "$backup_base")"
}

ppa_latest_backup_manifest() {
  local backup_base="${1:?backup base required}"
  printf '%s/ppa-backup.manifest.json.enc' "$(ppa_latest_backup_dir "$backup_base")"
}

ppa_latest_backup_checksum() {
  local backup_base="${1:?backup base required}"
  printf '%s/ppa-backup.tar.enc.sha256' "$(ppa_latest_backup_dir "$backup_base")"
}
