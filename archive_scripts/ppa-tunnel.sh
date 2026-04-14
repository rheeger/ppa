#!/bin/zsh
set -euo pipefail

ARNOLD_HOST="${ARNOLD_HOST:-arnold@192.168.50.27}"
LOCAL_PORT="${PPA_TUNNEL_PORT:-5433}"
REMOTE_PORT="5432"

if lsof -i :"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "ppa-tunnel: port $LOCAL_PORT already in use (tunnel may already be running)"
  exit 0
fi

echo "ppa-tunnel: forwarding 127.0.0.1:${LOCAL_PORT} -> Arnold Postgres (${ARNOLD_HOST}:${REMOTE_PORT})"
exec ssh -N -L "${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" "$ARNOLD_HOST"
