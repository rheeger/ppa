#!/bin/zsh
# Install (or uninstall) the loopback HTTP MCP launchd job on Ginger.
# Does not print the token. Creates ~/.ppa/mcp-http-token if missing.
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
PPA_ROOT="${SCRIPT_DIR:h}"
LABEL="com.rheeger.ppa.mcp-http"
PLIST_SRC="${SCRIPT_DIR}/${LABEL}.plist"
PLIST_DEST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
TOKEN_FILE="${PPA_MCP_AUTH_TOKEN_FILE:-$HOME/.ppa/mcp-http-token}"

usage() {
  echo "usage: $0 [--install|--uninstall|--status]" >&2
  exit 2
}

cmd="${1:---install}"

uninstall() {
  launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
  rm -f "$PLIST_DEST"
  echo "uninstalled ${LABEL}" >&2
}

status() {
  launchctl print "gui/$(id -u)/${LABEL}" 2>/dev/null | sed -n '1,20p' || echo "${LABEL} not loaded" >&2
}

install() {
  mkdir -p "${HOME}/Library/LaunchAgents" "${HOME}/.ppa" "${PPA_ROOT}/logs"
  if [[ ! -s "$TOKEN_FILE" ]]; then
    python3 -c 'import secrets; print(secrets.token_hex(32))' >"$TOKEN_FILE"
    chmod 600 "$TOKEN_FILE"
    echo "wrote new token file ${TOKEN_FILE} (0600). Store the same value in op://Arnold-Passkey-Gate/PPA_MCP_TOKEN/credential" >&2
  else
    chmod 600 "$TOKEN_FILE"
    echo "reusing token file ${TOKEN_FILE}" >&2
  fi
  sed -e "s|__PPA_REPO__|${PPA_ROOT}|g" "$PLIST_SRC" >"$PLIST_DEST"
  launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST_DEST"
  launchctl enable "gui/$(id -u)/${LABEL}"
  launchctl kickstart -k "gui/$(id -u)/${LABEL}"
  echo "installed ${LABEL} -> ${PLIST_DEST}" >&2
}

case "$cmd" in
  --install) install ;;
  --uninstall) uninstall ;;
  --status) status ;;
  *) usage ;;
esac
