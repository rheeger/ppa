# PPA agent usage

What you can ask, and what the archive will not invent for you, is in [README.md](../README.md). This page is the retrieval contract: jobs, tool rules, and CLI parity.

The live agent contract is two layers:

- Server instructions from `archive_cli.mcp_instructions.build_server_instructions()` (safety, type rules, job router, don'ts)
- Job recipes in `.cursor/skills/archive-query/` (`SKILL.md` plus one file per job)
- Per-tool recipes in `archive_cli.mcp_instructions.TOOL_DESCRIPTIONS`

Clients receive the MCP layer on initialize and tools/list. Cursor agents must open the matching job file before retrieving. Do not fork job recipes into other instruction files. Edit the skill file, then keep the MCP router in sync.

## Jobs

1. Identify a person: `identify-person.md`
2. Census their channels: `census-channels.md`
3. Read a stack: `read-a-stack.md`
4. Answer a fact: `answer-a-fact.md`
5. Reconstruct a story: `reconstruct-a-story.md`

A "who is X" or profile write-up is job 1, then job 2, then job 5. A single dated question is job 3 or 4.

## What agents get automatically

1. PPA is a lookup engine. Cards are truth. Search is navigation. Read before you cite.
2. A job router with stop tests (candidate person cards, channel census, stolen aliases).
3. Don'ts: types use underscores (`email_message`, not `email-message`); `people_filter` is a name or slug, never an email; ground claims with `archive_read`. `archive_person` accepts name, slug, email, or phone.
4. Per-tool parameter recipes when the agent inspects a tool.

`archive_knowledge` falls back to ordinary search. It is not a living profile.

## CLI parity (no MCP)

`ppa search`, `ppa query`, `ppa hybrid-search`, `ppa analytics`, `ppa read`, `ppa evidence`, `ppa graph`, `ppa person`, `ppa health`, and `ppa status` are the same lookup family as the MCP tools. They are not one command:

- `ppa health` is structural and behavioral checks.
- `ppa status` / `archive_status_json` are current-instance production status.
- `archive_stats` is corpus counts.

## Ops tools (not retrieval)

`archive_rebuild_indexes`, `archive_embed_pending`, seed-link tools, and similar are operational. Do not use them as a reasoning shortcut. Chunk rows power vector and hybrid search. They are not canonical evidence.

Long CLI jobs (`maintain`, `rebuild-indexes`, `embed-pending`, `slice-seed`, `enrich-emails`, `extract-emails`, Gmail catch-up) must start detached: `setsid`, stdin from `/dev/null`, PPID 1, `--log-file` before the subcommand. Never a Cursor-managed terminal. See `.cursor/skills/long-running-jobs/SKILL.md`.
