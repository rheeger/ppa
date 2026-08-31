# PPA MCP setup

## Quick start

1. `pip install -e .` from the `ppa` repo (puts `ppa` on your PATH).
2. Set the minimum env vars: `PPA_INDEX_DSN`, `PPA_PATH`, `PPA_INDEX_SCHEMA` (see [PPA_RUNTIME_CONTRACT.md](PPA_RUNTIME_CONTRACT.md) §2).
3. Run `ppa mcp-config` and paste the JSON into your MCP client (`~/.cursor/mcp.json`, Claude Desktop config, etc.). Secrets such as `OPENAI_API_KEY` are never printed — add those in the client’s `env` block separately.

## Local vs remote (Arnold)

- **Local stdio:** Postgres on this machine (Docker or native); `PPA_INDEX_DSN` points at `127.0.0.1` on the Postgres port. Cursor uses `archive_scripts/run-local-seed-mcp.sh`.
- **Remote Postgres (legacy):** SSH tunnel to another host's Postgres. `ppa serve --tunnel user@host` or `scripts/ppa-tunnel.sh`.
- **Arnold client (canonical):** Arnold does **not** host a vault copy. Ginger serves streamable HTTP on loopback; Tailscale Serve publishes it on the personal tailnet. Arnold calls it through the passkey gate with `PPA_MCP_TOKEN`.

```
Arnold --tailnet--> https://ginger-m4-max.tail0c38c5.ts.net/mcp
                 Authorization: Bearer <PPA_MCP_TOKEN>
                 token lives in op://Arnold-Passkey-Gate/PPA_MCP_TOKEN/credential
```

When this laptop sleeps, Arnold gets connection errors. That is the accepted tradeoff.

### Enable the HTTP listener on Ginger

```bash
archive_scripts/install-mcp-http-launchd.sh --install
# stores token at ~/.ppa/mcp-http-token (0600)
# persist the same value in 1Password: op://Arnold-Passkey-Gate/PPA_MCP_TOKEN/credential
```

`run-http-mcp.sh` binds `127.0.0.1:8765`, uses `PPA_MCP_TOOL_PROFILE=read-only`, and runs `tailscale serve --bg`. Health (no auth): `GET /health`. MCP path: `/mcp`.

See the template [ppa.mcp-example.json](examples/ppa.mcp-example.json) for stdio patterns.

## Optional env for generated config

| Variable                     | Effect                                                                   |
| ---------------------------- | ------------------------------------------------------------------------ |
| `PPA_MCP_CONFIG_SERVER_NAME` | Name of the server block (default `ppa`)                                 |
| `PPA_MCP_TUNNEL_HOST`        | If set, `ppa mcp-config` adds `"args": ["serve", "--tunnel", "<value>"]` |
| `PPA_MCP_HTTP_URL`           | If set, `ppa mcp-config` emits a URL + bearer-header client block        |
| `PPA_MCP_HTTP=1`             | `ppa mcp-config` adds `"args": ["serve", "--http"]`                      |
