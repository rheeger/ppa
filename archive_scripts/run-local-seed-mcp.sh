#!/bin/zsh
# Cursor MCP: local seed vault + ppa-schema Postgres. Matches ppa/Makefile PPA_PATH.
# Override: PPA_PATH=... PPA_INDEX_DSN=... PPA_INDEX_SCHEMA=... PPA_INDEX_PORT=...
#           PPA_PYTHON=... PPA_PYTHON_FALLBACK=...
# Lives under archive_scripts/; repo root is one level up.
#
# History: pre-2026-04-19 this script defaulted to the legacy ``archive_seed``
# Postgres schema. Phase 4 rebuild + Phase 5 embeddings (6,770,930 vectors)
# landed in the ``ppa`` schema, and ``archive_seed`` was a stale pre-rename
# snapshot that has since been dropped. Default schema is now ``ppa``.
#
# Handshake contract (Cursor times out createClient at ~60s):
#   - Do not call ``docker compose port`` (or any other Docker wait) before exec.
#   - Docker is optional. Use $PPA_INDEX_DSN, then last-known port, then 50731.
#   - Status goes to stderr only. stdout is the MCP JSON-RPC stream.
#   - No lock file — Cursor may spawn several createClient processes at once.
#
# Python resolution (first executable that can ``import mcp, archive_cli`` wins):
#   1. $PPA_PYTHON
#   2. $PPA_ROOT/.venv/bin/python  (skipped if the Homebrew target is dangling)
#   3. $PPA_PYTHON_FALLBACK, defaulting to the known-good track-a venv on this machine
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
PPA_ROOT="${SCRIPT_DIR:h}"

_SEED="${PPA_SEED_VAULT:-$HOME/Archive/seed/hf-archives-seed-20260307-235127}"
export PPA_PATH="${PPA_PATH:-$_SEED}"
export PPA_INDEX_SCHEMA="${PPA_INDEX_SCHEMA:-ppa}"

export PPA_EMBEDDING_PROVIDER="openai"
export PPA_EMBEDDING_MODEL="text-embedding-3-small"
export PPA_EMBEDDING_VERSION="1"
export PPA_USE_ARNOLD_OPENAI_KEY="1"

export PPA_INSTANCE_NAME="Heeger-Friedman Family Archives"

# Hybrid search / embeddings: Cursor may pass OPENAI_API_KEY; otherwise use the
# local key file (same convention as archive_scripts/ppa-embed-batch-loop.sh).
if [[ -z "${OPENAI_API_KEY:-}" && -r "${HOME}/.ppa/openai_key.txt" ]]; then
  export OPENAI_API_KEY="$(<"${HOME}/.ppa/openai_key.txt")"
fi

# Cursor may start this script with cwd != repo root. The repo venv has an
# editable ppa install; the documented fallback does not — PYTHONPATH + cd
# make ``import archive_cli`` resolve to this tree either way.
cd "$PPA_ROOT"
export PYTHONPATH="${PPA_ROOT}${PYTHONPATH:+:$PYTHONPATH}"

_DEFAULT_FALLBACK="$HOME/Code/rheeger/ppa-wt-track-a/.venv/bin/python"
_DEFAULT_PORT="50731"
_PORT_FILE="${PPA_LOCAL_PORT_FILE:-$HOME/.ppa/local-postgres-port}"

# A dangling Homebrew symlink is not executable. An interpreter that cannot
# import the real ``mcp`` package would start the FastMCP stub and crash.
_python_can_serve() {
  local py="$1"
  [[ -n "$py" && -e "$py" && -x "$py" ]] || return 1
  # Cap the import probe so a wedged interpreter cannot eat Cursor's 60s budget.
  "$py" -c "import mcp, archive_cli" >/dev/null 2>&1 &
  local pid=$!
  local i
  for i in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      return $?
    fi
    sleep 0.1
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 1
}

_resolve_python() {
  local candidate
  if [[ -n "${PPA_PYTHON:-}" ]]; then
    candidate="$PPA_PYTHON"
    if _python_can_serve "$candidate"; then
      print -r -- "$candidate"
      return 0
    fi
    echo "PPA_PYTHON is set but cannot run MCP (missing file or ``import mcp, archive_cli`` failed): $candidate" >&2
    exit 1
  fi
  for candidate in "$PPA_ROOT/.venv/bin/python" "${PPA_PYTHON_FALLBACK:-$_DEFAULT_FALLBACK}"; do
    if _python_can_serve "$candidate"; then
      print -r -- "$candidate"
      return 0
    fi
  done
  echo "No working Python for archive-local MCP." >&2
  echo "Need an interpreter that can ``import mcp`` and ``import archive_cli``." >&2
  echo "Set PPA_PYTHON, restore $PPA_ROOT/.venv (Homebrew python@3.13), or install mcp into the fallback." >&2
  echo "Documented fallback: ${PPA_PYTHON_FALLBACK:-$_DEFAULT_FALLBACK}" >&2
  exit 1
}

_write_last_port() {
  local p="$1"
  [[ "$p" == <-> ]] || return 0
  mkdir -p "${_PORT_FILE:h}" 2>/dev/null || return 0
  print -r -- "$p" >| "$_PORT_FILE" 2>/dev/null || true
}

_read_last_port() {
  local p
  [[ -r "$_PORT_FILE" ]] || return 1
  p="$(<"$_PORT_FILE")"
  p="${p//$'\n'/}"
  [[ "$p" == <-> ]] || return 1
  print -r -- "$p"
}

_port_from_dsn() {
  local dsn="$1"
  if [[ "$dsn" =~ :([0-9]+)/ ]]; then
    print -r -- "$match[1]"
    return 0
  fi
  return 1
}

# Resolve the interpreter before any optional I/O so a missing venv fails in <2s.
PYTHON="$(_resolve_python)"

# Docker is optional. Never block the MCP handshake on the daemon or compose.
if [[ -z "${PPA_INDEX_DSN:-}" ]]; then
  local_port="${PPA_INDEX_PORT:-}"
  if [[ -z "$local_port" ]]; then
    local_port="$(_read_last_port || true)"
  fi
  if [[ -z "$local_port" ]]; then
    local_port="$_DEFAULT_PORT"
  fi
  export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:${local_port}/archive"
  _write_last_port "$local_port"
else
  discovered="$(_port_from_dsn "$PPA_INDEX_DSN" || true)"
  if [[ -n "$discovered" ]]; then
    _write_last_port "$discovered"
  fi
fi

echo "archive-local MCP python=$PYTHON schema=$PPA_INDEX_SCHEMA (docker not required for handshake)" >&2

exec "$PYTHON" -m archive_cli
