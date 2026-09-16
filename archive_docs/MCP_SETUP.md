# MCP connection reference

The Model Context Protocol (MCP) lets compatible agents query the same PPA archive. Changing clients keeps the stored records and prepared catalog in place. This reference describes connection and access behavior in development deployments. The [specification](SPECIFICATION.md#queries) defines available operations.

## Connection methods

| Method | Behavior |
| --- | --- |
| HTTP MCP | The archive host runs retrieval and returns results to every connected client. This is the living-seed path. |
| Local stdio | A client launches PPA in its own process. Use this for fixtures, slices, or a machine that is not the HTTP owner. |

The living seed index is tens of GB. Cursor, Claude Code, Codex, and Cline must share the HTTP process documented in [http-mcp-singleton.md](runbooks/http-mcp-singleton.md). A per-window stdio copy mmaps that index again and can reboot the machine.

Both methods use the same archive runtime. HTTP authentication and network transport are deployment settings. A Postgres SSH tunnel only forwards warehouse access. It does not provide a remote vault or search index.

## Access

Tool profiles select available operations. `read-only` includes retrieval and raw card reads. `remote-read` exposes a smaller set without raw reads. `full` includes maintenance and is the default when the profile is unset. Invalid profiles deny operations.

Record access is separate from tool access. Source and domain restrictions apply before search, counts, and relationship expansion. A cloud client receives the content returned by its authorized calls. See [privacy](PRIVACY_CONTRACT.md) and [data boundaries](DATA_BOUNDARIES.md).

## Configuration

`ppa mcp-config` emits a client configuration from the current environment. Set `PPA_MCP_HTTP_URL` to print an HTTP block (`type`, `url`, bearer header placeholder). Without that variable it prints a stdio `command` block. Clients may require a different configuration format or an absolute executable path. The command omits secret-named variables, but a password embedded in a warehouse connection string can still appear.

The [runtime contract](PPA_RUNTIME_CONTRACT.md) defines configuration fields. The [agent reference](AGENT_USAGE.md) covers retrieval and source checks.
