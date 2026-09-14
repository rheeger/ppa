# MCP connection reference

The Model Context Protocol (MCP) lets compatible agents query the same PPA archive. Changing clients keeps the stored records and prepared catalog in place. This reference describes connection and access behavior in development deployments. The [specification](SPECIFICATION.md#queries) defines available operations.

## Connection methods

| Method | Behavior |
| --- | --- |
| Local stdio | The client launches PPA against the configured local archive |
| HTTP MCP | The archive host runs retrieval and returns results to a remote client |

Both use the same archive runtime. HTTP authentication and network transport are deployment settings. A Postgres SSH tunnel only forwards warehouse access; it does not provide a remote vault or search index.

## Access

Tool profiles select available operations. `read-only` includes retrieval and raw card reads; `remote-read` exposes a smaller set without raw reads. `full` includes maintenance and is the default when the profile is unset. Invalid profiles deny operations.

Record access is separate from tool access. Source and domain restrictions apply before search, counts, and relationship expansion. A cloud client receives the content returned by its authorized calls. See [privacy](PRIVACY_CONTRACT.md) and [data boundaries](DATA_BOUNDARIES.md).

## Configuration

`ppa mcp-config` emits a client configuration from the current environment. Clients may require a different configuration format or an absolute executable path. The command omits secret-named variables, but a password embedded in a warehouse connection string can still appear.

The [runtime contract](PPA_RUNTIME_CONTRACT.md) defines configuration fields. The [agent reference](AGENT_USAGE.md) covers retrieval and source checks.
