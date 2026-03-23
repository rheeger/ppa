#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
LOCAL_PORT="${PPA_TUNNEL_PORT:-5433}"

if ! lsof -i :"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Arnold SSH tunnel not running on port $LOCAL_PORT." >&2
  echo "Start it first:  $SCRIPT_DIR/scripts/ppa-tunnel.sh" >&2
  exit 1
fi

export PPA_PATH="/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127"
export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:${LOCAL_PORT}/archive"
export PPA_INDEX_SCHEMA="archive_seed"

export PPA_EMBEDDING_PROVIDER="openai"
export PPA_EMBEDDING_MODEL="text-embedding-3-small"
export PPA_EMBEDDING_VERSION="1"
export PPA_USE_ARNOLD_OPENAI_KEY="1"

export PPA_INSTANCE_NAME="Heeger-Friedman Family Archives"
export PPA_FORBID_REBUILD="1"

exec "$SCRIPT_DIR/.venv/bin/python" -m archive_mcp
