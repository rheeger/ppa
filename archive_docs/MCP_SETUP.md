# PPA MCP setup

## Quick start

1. Install with a native `archive_crate` (hashed wheels + Python 3.12, or a developer checkout that builds the crate). `pip install -e .` alone is not the supported retrieval path.
2. Bind an instance (`ppa setup` or an existing `ppa.json` root). Set `PPA_INDEX_DSN`, `PPA_PATH` / instance dir, `PPA_INDEX_SCHEMA` (see [PPA_RUNTIME_CONTRACT.md](PPA_RUNTIME_CONTRACT.md) §2).
3. Run `ppa mcp-config` and paste the JSON into your MCP client. Secrets such as `OPENAI_API_KEY` are never printed — add those in the client’s `env` block separately.

`ppa analytics` / `archive_analytics` are shipped (subscriptions, trip costs, changes-since, typed query, neighbor context). Coverage is the eligible stored set. `archive_knowledge` is an empty search fallback, not a 46-facet cache. Living status: [STATUS.md](STATUS.md).

## Local vs remote

- **Local stdio (current product):** Postgres on this machine (Docker or native); `PPA_INDEX_DSN` points at `127.0.0.1`. The MCP process binds the current instance vault and serving index.
- **Remote Postgres (optional):** SSH tunnel to another host's Postgres. `ppa serve --tunnel user@host`.
- **HTTP MCP:** An instance choice on the machine that owns the vault. It is not a required topology.

### Historical Arnold / Ginger topology

The following was one creator-machine deployment. It is **not** the canonical product architecture and is **not** a second-instance prerequisite.

```
Arnold --tailnet--> http://ginger-m4-max.tail0c38c5.ts.net:8765/mcp
                 Authorization: Bearer <PPA_MCP_TOKEN>
```

`archive_scripts/install-mcp-http-launchd.sh` and `run-http-mcp.sh` remain host helpers for that machine. See the template [ppa.mcp-example.json](examples/ppa.mcp-example.json) for stdio patterns.

## Optional env for generated config

| Variable                     | Effect                                                                   |
| ---------------------------- | ------------------------------------------------------------------------ |
| `PPA_MCP_CONFIG_SERVER_NAME` | Name of the server block (default `ppa`)                                 |
| `PPA_MCP_TUNNEL_HOST`        | If set, `ppa mcp-config` adds `"args": ["serve", "--tunnel", "<value>"]` |
| `PPA_MCP_HTTP_URL`           | If set, `ppa mcp-config` emits a URL + bearer-header client block        |
| `PPA_MCP_HTTP=1`             | `ppa mcp-config` adds `"args": ["serve", "--http"]`                      |
