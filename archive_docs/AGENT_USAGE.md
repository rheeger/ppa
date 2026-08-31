# HFA Agent Usage

The live agent contract is the MCP toolset:

- Server `instructions` — `archive_cli.mcp_instructions.build_server_instructions()`
- Per-tool recipes — `archive_cli.mcp_instructions.TOOL_DESCRIPTIONS`

Clients receive both on initialize / tools/list. Update that module. Do not fork
do's, don'ts, type filters, or routing into skill docs.

## What agents get automatically

1. High-level system: PPA is a retrieval engine; cards are truth; search is navigation.
2. Do / don't for successful queries (underscore types, people_filter is a name, ground with reads).
3. Routing: hybrid vs query vs search vs person vs timeline.
4. Per-tool parameter recipes when the agent inspects a tool.

## CLI parity (no MCP)

`ppa search`, `ppa query`, `ppa hybrid-search`, `ppa read`, `ppa graph`, `ppa health`,
`ppa status` — same semantics as the MCP tools. `ppa health` is the shell equivalent
of `archive_stats` / `archive_status_json`.

## Ops tools (not retrieval)

`archive_rebuild_indexes`, `archive_embed_pending`, seed-link tools, and similar are
operational. Do not use them as a reasoning shortcut. Chunk rows power vector/hybrid
search; they are not canonical evidence.
