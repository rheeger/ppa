#!/bin/zsh
# Cursor MCP: local Docker Postgres (ppa schema) + seed vault tree. Matches ppa/Makefile PPA_PATH.
# Override: PPA_PATH=... PPA_INDEX_SCHEMA=... PPA_PYTHON=... PPA_PYTHON_FALLBACK=...
# Lives under archive_scripts/; repo root is one level up.
#
# History: pre-2026-04-19 this script defaulted to the legacy ``archive_seed``
# Postgres schema. Phase 4 rebuild + Phase 5 embeddings (6,770,930 vectors)
# landed in the ``ppa`` schema, and ``archive_seed`` was a stale pre-rename
# snapshot that has since been dropped. Default schema is now ``ppa``.
#
# Python resolution (first executable wins):
#   1. $PPA_PYTHON
#   2. $PPA_ROOT/.venv/bin/python  (skipped if the Homebrew target is dangling)
#   3. $PPA_PYTHON_FALLBACK, defaulting to the known-good track-a venv on this machine
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
PPA_ROOT="${SCRIPT_DIR:h}"
ENV_FILE="$PPA_ROOT/.env.pgvector"

if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE="$PPA_ROOT/.env.pgvector.example"
fi

POSTGRES_PORT="$(
  cd "$PPA_ROOT" &&
    docker compose --env-file "$ENV_FILE" -f docker-compose.pgvector.yml port archive-postgres 5432 | awk -F: '{print $NF}'
)"

if [[ -z "$POSTGRES_PORT" ]]; then
  echo "archive-postgres is not running" >&2
  exit 1
fi

_SEED="${PPA_SEED_VAULT:-$HOME/Archive/seed/hf-archives-seed-20260307-235127}"
export PPA_PATH="${PPA_PATH:-$_SEED}"
export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:${POSTGRES_PORT}/archive"
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

# A dangling Homebrew symlink is not executable. An interpreter that cannot
# import the real ``mcp`` package would start the FastMCP stub and crash.
_python_can_serve() {
  local py="$1"
  [[ -x "$py" ]] || return 1
  "$py" -c "import mcp, archive_cli" >/dev/null 2>&1
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

PYTHON="$(_resolve_python)"
exec "$PYTHON" -m archive_cli
