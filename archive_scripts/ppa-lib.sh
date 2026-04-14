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
