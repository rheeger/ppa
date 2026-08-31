#!/bin/zsh
# Loopback streamable-HTTP MCP for Arnold (via Tailscale Serve).
# Token: PPA_MCP_AUTH_TOKEN or ~/.ppa/mcp-http-token
# Publish: this script also runs `tailscale serve --bg` when PPA_MCP_TAILSCALE_SERVE=1 (default).
set -euo pipefail
# launchd PATH is minimal — Tailscale and Homebrew must be explicit.
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:${PATH:-}"

SCRIPT_DIR="${0:A:h}"
PPA_ROOT="${SCRIPT_DIR:h}"

_SEED="${PPA_SEED_VAULT:-$HOME/Archive/seed/hf-archives-seed-20260307-235127}"
export PPA_PATH="${PPA_PATH:-$_SEED}"
export PPA_INDEX_SCHEMA="${PPA_INDEX_SCHEMA:-ppa}"
export PPA_MCP_TOOL_PROFILE="${PPA_MCP_TOOL_PROFILE:-read-only}"
# Hybrid search over ~1.4M cards exceeds the 30s query default.
export PPA_STATEMENT_TIMEOUT_MS="${PPA_STATEMENT_TIMEOUT_MS:-180000}"
export PPA_MCP_HTTP_PORT="${PPA_MCP_HTTP_PORT:-8765}"
export PPA_MCP_HTTP=1
if [[ -z "${PPA_MCP_HTTP_HOST:-}" ]]; then
  # Prefer the Tailscale address so Arnold can reach us without Tailscale Serve.
  if command -v tailscale >/dev/null 2>&1; then
    export PPA_MCP_HTTP_HOST="$(command tailscale ip -4 2>/dev/null | head -1)"
  fi
  export PPA_MCP_HTTP_HOST="${PPA_MCP_HTTP_HOST:-127.0.0.1}"
fi

export PPA_EMBEDDING_PROVIDER="${PPA_EMBEDDING_PROVIDER:-openai}"
export PPA_EMBEDDING_MODEL="${PPA_EMBEDDING_MODEL:-text-embedding-3-small}"
export PPA_EMBEDDING_VERSION="${PPA_EMBEDDING_VERSION:-1}"
export PPA_USE_ARNOLD_OPENAI_KEY="${PPA_USE_ARNOLD_OPENAI_KEY:-1}"
export PPA_INSTANCE_NAME="${PPA_INSTANCE_NAME:-Heeger-Friedman Family Archives}"
export PPA_FORBID_REBUILD="${PPA_FORBID_REBUILD:-1}"

if [[ -z "${OPENAI_API_KEY:-}" && -r "${HOME}/.ppa/openai_key.txt" ]]; then
  export OPENAI_API_KEY="$(<"${HOME}/.ppa/openai_key.txt")"
fi

if [[ -z "${PPA_INDEX_DSN:-}" ]]; then
  local_port="${PPA_INDEX_PORT:-}"
  if [[ -z "$local_port" && -r "${HOME}/.ppa/local-postgres-port" ]]; then
    local_port="$(<"${HOME}/.ppa/local-postgres-port")"
    local_port="${local_port//$'\n'/}"
  fi
  export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:${local_port:-50731}/archive"
fi

cd "$PPA_ROOT"
export PYTHONPATH="${PPA_ROOT}${PYTHONPATH:+:$PYTHONPATH}"

PYTHON="${PPA_PYTHON:-$PPA_ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  echo "run-http-mcp: missing python at $PYTHON" >&2
  exit 1
fi

# Optional: Tailscale Serve (HTTPS on :443). Off by default — this tailnet
# has not enabled Serve. Binding the Tailscale IP is the LAN/tailnet path.
if [[ "${PPA_MCP_TAILSCALE_SERVE:-0}" == "1" ]] && command -v tailscale >/dev/null 2>&1; then
  tailscale serve --bg "http://127.0.0.1:${PPA_MCP_HTTP_PORT}" >/dev/null 2>&1 || \
    echo "run-http-mcp: tailscale serve failed (MCP still listening on ${PPA_MCP_HTTP_HOST})" >&2
fi

echo "ppa-http-mcp host=${PPA_MCP_HTTP_HOST} port=${PPA_MCP_HTTP_PORT} schema=${PPA_INDEX_SCHEMA} profile=${PPA_MCP_TOOL_PROFILE}" >&2
exec "$PYTHON" -m archive_cli --log-file "${PPA_HTTP_LOG:-$PPA_ROOT/logs/ppa-http-mcp.log}" serve --http --bind "$PPA_MCP_HTTP_HOST" --port "$PPA_MCP_HTTP_PORT"
