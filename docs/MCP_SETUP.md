# PPA MCP setup

## Quick start

1. `pip install -e .` from the `ppa` repo (puts `ppa` on your PATH).
2. Set the minimum env vars: `PPA_INDEX_DSN`, `PPA_PATH`, `PPA_INDEX_SCHEMA` (see [PPA_RUNTIME_CONTRACT.md](PPA_RUNTIME_CONTRACT.md) §2).
3. Run `ppa mcp-config` and paste the JSON into your MCP client (`~/.cursor/mcp.json`, Claude Desktop config, etc.). Secrets such as `OPENAI_API_KEY` are never printed — add those in the client’s `env` block separately.

## Local vs remote (Arnold)

- **Local:** Postgres on this machine (Docker or native); `PPA_INDEX_DSN` points at `127.0.0.1` on the Postgres port.
- **Remote:** Postgres on another host reachable via SSH. Either run `scripts/ppa-tunnel.sh` manually or use `ppa serve --tunnel user@host` so the tunnel is a child of the MCP process (dies when the client stops the server). Point `PPA_INDEX_DSN` at `127.0.0.1:PPA_TUNNEL_PORT` (default `5433`).

See the template [ppa.mcp-example.json](../ppa.mcp-example.json) for both patterns.

## Optional env for generated config

| Variable | Effect |
| -------- | ------ |
| `PPA_MCP_CONFIG_SERVER_NAME` | Name of the server block (default `ppa`) |
| `PPA_MCP_TUNNEL_HOST` | If set, `ppa mcp-config` adds `"args": ["serve", "--tunnel", "<value>"]` |
