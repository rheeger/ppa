# Shared tunnel port config for launchd, MCP, and manual scripts.
if [[ -f "${HOME}/.config/ppa/tunnel.env" ]]; then
  # shellcheck disable=SC1091
  source "${HOME}/.config/ppa/tunnel.env"
fi
export PPA_TUNNEL_PORT="${PPA_TUNNEL_PORT:-58471}"
