#!/bin/zsh
# Cursor MCP: local Docker Postgres (ppa schema) + seed vault tree. Matches ppa/Makefile PPA_PATH.
# Override: PPA_PATH=... PPA_INDEX_SCHEMA=...
# Lives under archive_scripts/; repo root is one level up.
#
# History: pre-2026-04-19 this script defaulted to the legacy ``archive_seed``
# Postgres schema. Phase 4 rebuild + Phase 5 embeddings (6,770,930 vectors)
# landed in the ``ppa`` schema, and ``archive_seed`` was a stale pre-rename
# snapshot that has since been dropped. Default schema is now ``ppa``.
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

exec "$PPA_ROOT/.venv/bin/python" -m archive_cli
