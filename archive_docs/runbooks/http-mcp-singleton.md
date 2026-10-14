# Living archive HTTP MCP

The living seed index is tens of GB. One process may mmap it. Every Cursor window, agent worker, Claude Code session, Codex session, and Cline session must call that process over HTTP. They must not launch `run-local-seed-mcp.sh`.

## Owner

LaunchAgent `com.rheeger.ppa.mcp-http` starts `archive_scripts/run-http-mcp.sh` from the main checkout (`/Users/rheeger/Code/rheeger/ppa`). That script binds `0.0.0.0:8765` and writes `~/.ppa/mcp-http-owner.json` with dial host `127.0.0.1`.

Local clients use:

```
http://127.0.0.1:8765/mcp
```

Arnold on the tailnet still uses `http://<tailscale-ip>:8765/mcp`. Do not point Cursor at the MagicDNS name. This Mac cannot hairpin to its own Tailscale IP.

`/health` is public. `/mcp` needs `Authorization: Bearer` from `~/.ppa/mcp-http-token`. Do not commit that token.

`archive-arnold` stays on stdio. It talks to the Arnold machine, not this index.

## What stdio does now

`run-local-seed-mcp.sh` still exists for handshake-only or emergency use. With `PPA_SERVING_INDEX_FOLLOW_HTTP=1` (the default in that script) it refuses to open the serving index while HTTP `/health` answers. Prefault is off unless `PPA_SERVING_PREFAULT=1`.

## Restart after a code deploy

The LaunchAgent working directory is the main checkout. Rebuild `archive_crate` in that checkout's `.venv` when the extension changed. Then:

```bash
launchctl kickstart -k "gui/$(id -u)/com.rheeger.ppa.mcp-http"
```

Confirm PPID 1, `/health` 200, and a single `archive_cli serve --http` process. `/health` answers only after `prepare_mcp_serving` finishes. That is one streamed open (~41s / ~25GB RSS on generation 1789310628025). Prefault stays off unless `PPA_SERVING_PREFAULT=1`.

Do not start a second `serve --http`. Do not cold-open `archive_cli` to ask a question.

The crate streams `cards.jsonl` / `chunks.jsonl` / `edges.jsonl` into typed structs. It drops `search_text` after parse. Release builds must not strip debuginfo. rustc strip leaves `LC_SYMTAB.stroff` 4-byte aligned, and macOS 27 dyld refuses the extension (`mis-aligned LINKEDIT string pool`).

## Client configs

`ppa mcp-config` emits an HTTP block when `PPA_MCP_HTTP_URL` is set. Local Cursor should name the server `archive-local` and point at `http://127.0.0.1:8765/mcp`. Codex reads the bearer header from `~/.ppa/mcp-http-headers.py` so the token stays out of `config.toml`.
