#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
ENV_FILE="$SCRIPT_DIR/.env.pgvector"

if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE="$SCRIPT_DIR/.env.pgvector.example"
fi

POSTGRES_PORT="$(
  cd "$SCRIPT_DIR" &&
    docker compose --env-file "$ENV_FILE" -f docker-compose.pgvector.yml port archive-postgres 5432 | awk -F: '{print $NF}'
)"

if [[ -z "$POSTGRES_PORT" ]]; then
  echo "archive-postgres is not running" >&2
  exit 1
fi

export PPA_PATH="/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127"
export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:${POSTGRES_PORT}/archive"
export PPA_INDEX_SCHEMA="archive_seed"

export PPA_EMBEDDING_PROVIDER="openai"
export PPA_EMBEDDING_MODEL="text-embedding-3-small"
export PPA_EMBEDDING_VERSION="1"
export PPA_USE_ARNOLD_OPENAI_KEY="1"

export PPA_INSTANCE_NAME="Heeger-Friedman Family Archives"

exec "$SCRIPT_DIR/.venv/bin/python" -m archive_mcp
