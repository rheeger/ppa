# Living archive HTTP MCP

The living seed index is tens of GB. One process may mmap it. Every Cursor window, agent worker, Claude Code session, Codex session, and Cline session must call that process over HTTP. They must not launch `run-local-seed-mcp.sh`.

## Owner

LaunchAgent `com.rheeger.ppa.mcp-http` starts `ppa-http-mcp/archive_scripts/run-http-mcp.sh`. That script binds the Tailscale IPv4 on port 8765 and writes `~/.ppa/mcp-http-owner.json`.

Clients use:

```
http://ginger-m4-max.tail935a40.ts.net:8765/mcp
```

`/health` is public. `/mcp` needs `Authorization: Bearer` from `~/.ppa/mcp-http-token`. Do not commit that token.

`archive-arnold` stays on stdio. It talks to the Arnold machine, not this index.

## What stdio does now

`run-local-seed-mcp.sh` still exists for handshake-only or emergency use. With `PPA_SERVING_INDEX_FOLLOW_HTTP=1` (the default in that script) it refuses to open the serving index while HTTP `/health` answers. Prefault is off unless `PPA_SERVING_PREFAULT=1`.

## Restart after a code deploy

Copy the intended tree into `/Users/rheeger/Code/rheeger/ppa-http-mcp` (that checkout is the LaunchAgent working directory). Rebuild `archive_crate` in the shared `.venv`. Then:

```bash
launchctl kickstart -k "gui/$(id -u)/com.rheeger.ppa.mcp-http"
```

Confirm PPID 1, `/health` 200, and a single `archive_cli serve --http` process. `/health` answers only after `prepare_mcp_serving` finishes. That is one streamed open (~41s / ~25GB RSS on generation 1789310628025). Prefault stays off unless `PPA_SERVING_PREFAULT=1`.

Do not start a second `serve --http`. Do not cold-open `archive_cli` to ask a question.

The crate streams `cards.jsonl` / `chunks.jsonl` / `edges.jsonl` into typed structs. It drops `search_text` after parse. Release builds must not strip debuginfo. rustc strip leaves `LC_SYMTAB.stroff` 4-byte aligned, and macOS 27 dyld refuses the extension (`mis-aligned LINKEDIT string pool`).

## Client configs

`ppa mcp-config` emits an HTTP block when `PPA_MCP_HTTP_URL` is set. Cursor, Claude Code, Codex, and Cline should all name the server `archive-local` and point at the URL above. Codex reads the bearer header from `~/.ppa/mcp-http-headers.py` so the token stays out of `config.toml`.
