# HFA Agent Usage

The live agent contract is two layers:

- Server `instructions` — `archive_cli.mcp_instructions.build_server_instructions()`
  (safety, type rules, job router, don'ts)
- Job recipes — `.cursor/skills/archive-query/` (`SKILL.md` plus one file per job)
- Per-tool recipes — `archive_cli.mcp_instructions.TOOL_DESCRIPTIONS`

Clients receive the MCP layer on initialize / tools/list. Cursor agents must
open the matching job file before retrieving. Do not fork job recipes into
AGENTS.md. Edit the skill file, then keep the MCP router in sync.

## Jobs

1. Identify a person — `identify-person.md`
2. Census their channels — `census-channels.md`
3. Read a stack — `read-a-stack.md`
4. Answer a fact — `answer-a-fact.md`
5. Reconstruct a story — `reconstruct-a-story.md`

## What agents get automatically

1. High-level system: PPA is a retrieval engine; cards are truth; search is navigation.
2. Job router with stop tests (candidate person cards, channel census, stolen aliases).
3. Don'ts (underscore types, people_filter is a name, ground with reads).
4. Per-tool parameter recipes when the agent inspects a tool.

## CLI parity (no MCP)

`ppa search`, `ppa query`, `ppa hybrid-search`, `ppa read`, `ppa graph`, `ppa health`,
`ppa status` — same retrieval family as the MCP tools. They are **not** one command:
`ppa health` is structural/behavioral checks; `ppa status` / `archive_status_json`
are current-instance production status; `archive_stats` is corpus counts.

## Ops tools (not retrieval)

`archive_rebuild_indexes`, `archive_embed_pending`, seed-link tools, and similar are
operational. Do not use them as a reasoning shortcut. Chunk rows power vector/hybrid
search; they are not canonical evidence.
