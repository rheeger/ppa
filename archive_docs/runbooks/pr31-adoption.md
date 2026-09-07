# PR31 live adoption (R0)

Instance: local seed living corpus. Arnold is not in scope.

| Item | Value |
| --- | --- |
| Vault | `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` |
| Schema | `ppa` |
| DSN | `postgresql://archive:archive@127.0.0.1:50731/archive` |
| Worktree | `/Users/rheeger/Code/rheeger/ppa` on `codex/ppa-pr31-correctness-closeout` |
| Warm reads | existing `user-archive-local` MCP only |
| Jobs | new `archive_cli` / `publish-serving-index.py` processes with `--log-file` before the subcommand |

Code deploy, derived-index publish, and canonical identity writes are separate. This runbook authorizes only the commands below.

## Current instance (2026-09-07)

- Warehouse is mid nightly rematerialize (`ppa-maintain-nightly-20260907.log`, step 5/6). Do not start a second rebuild, maintain, or publish until that process exits.
- ACTIVE serving generation `1788743317023` is **hash-dev**, `embedding_count=0`, ~9.5 GiB. It is not the quality rollback target.
- Retained openai generation `1788716974760` is **text-embedding-3-small** v1, `embedding_count=4355992`, `nlist=2087`, ~34 GiB. That is the rollback pointer.
- `_meta/scan-rejections.json` is present and empty (`count=0`). Default publish may proceed.
- `_meta/thread-projection-receipts.json` already has pending `p31.g.1` receipts from this worktree. Nightly maintain should drain them; do not invent a second drain.
- Disk: ~143 GiB free on the data volume. Budget one new ~34 GiB generation plus ~34 GiB staging. Keep both existing generations until the new ACTIVE validates.
- `PPA_SERVING_INDEX_MAX_RSS_MB` for seed publish is **32768** (cutover contract). Default 8192 is too small.

## What this approval does and does not do

**Do**

1. Land the executable (commit / PR / worktree crate rebuild after nightly exits).
2. Dry-run `identity-repair merge`, then `--apply` only `auto_merge_eligible` pairs (stub↔named or stub↔stub). Named+named stay journaled proposals.
3. Apply derived thread recount: `identity-repair rollup-threads --apply`.
4. Full serving-index publish from the warehouse with openai embeddings (the 4.35M×1536 train). Incremental publish onto the hash ACTIVE is not a substitute.
5. Prove with the warm MCP after one stdio reload. Record statuses, not personal identifiers.

**Do not**

- Kill the in-flight nightly, the HTTP MCP (`serve --http` on `100.93.60.13:8765`), or start a competing publisher.
- Run `rebuild-indexes` or another full warehouse rematerialize. Nightly already owns that.
- `identity-repair canonicalize --apply` or `resolve-people --apply`. Those rewrite stored join keys / historical decisions and stay review items.
- Auto-undo historical named merges.
- Cold `ppa person` / `query` / `search` against this vault.
- Copy phones, emails, or legal names into git.

## Pause / resume

| Boundary | Rule |
| --- | --- |
| Before any R1 job | `pgrep -lf 'ppa-maintain-nightly|archive_cli.*maintain'` must be empty for this vault. |
| Before publish | no live `PUBLISHER.lease`; a zero-byte leftover `PUBLISHER.lock` is not a holder if no publisher PID exists. |
| If nightly is still in step 5/6 | wait. Tail `logs/ppa-maintain-nightly-20260907.log`. |
| If nightly incremental-publishes onto hash ACTIVE | ignore that generation for quality; still run the full openai publish below. |
| Resource stop | free disk under 80 GiB, or train RSS above 32768 MiB without headroom → stop and keep `1788716974760`. |

## Shared env for jobs

Set from the worktree. Do not export API keys in the shell history file if you can avoid it; nightly already loads `~/.ppa/openai_key.txt`.

```bash
cd /Users/rheeger/Code/rheeger/ppa
export PPA_PATH="/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127"
export PPA_INDEX_SCHEMA=ppa
export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:50731/archive"
export PPA_ARCHIVE_INSTANCE_ROLE=local-seed
export PPA_ENGINE=rust
export PPA_EMBEDDING_PROVIDER=openai
export PPA_EMBEDDING_MODEL=text-embedding-3-small
export PPA_EMBEDDING_VERSION=1
export PPA_SERVING_INDEX_MAX_RSS_MB=32768
export PPA_SERVING_TRAIN_MEMORY_MB=32768
export PPA_PHONE_REGION=US
unset PPA_SERVING_SKIP_EMBEDDINGS
unset PPA_SERVING_REPAIR_GENERATION
```

## R1 commands (exact order)

### 0. Wait for nightly

```bash
while pgrep -f 'archive_scripts/ppa-maintain-nightly.py|archive_cli .* maintain' >/dev/null; do
  tail -n 3 logs/ppa-maintain-nightly-20260907.log
  sleep 60
done
```

Expected: process gone; log shows maintain finished or failed at incremental publish. Either way, warehouse rematerialize must have completed step 5/6. If rematerialize failed, stop — do not publish.

### 1. Rebuild the worktree crate (code deploy)

```bash
rustup run 1.85 .venv/bin/python -m maturin develop --release --manifest-path archive_crate/Cargo.toml
```

Do this only after nightly has released the extension. Do not rebuild while maintain is running.

### 2. Identity dry-run (no vault writes)

```bash
.venv/bin/python -m archive_cli --log-file logs/pr31-seed-identity-merge-dry.log \
  identity-repair merge --output logs/pr31-seed-identity-merge-dry.json
```

Record `redirected` count and `queued` count only. Inspect reasons: redirected must be stub-eligible; queued are review items.

### 3. Identity apply (canonical decisions, bounded)

```bash
.venv/bin/python -m archive_cli --log-file logs/pr31-seed-identity-merge-apply.log \
  identity-repair merge --apply --output logs/pr31-seed-identity-merge-apply.json
```

This is the only approved live merge writer. It calls `merge_identities()`. Ambiguous pairs go to `_meta/identity-proposals.json`, not `dedup-candidates.json`.

### 4. Derived thread recount

```bash
.venv/bin/python -m archive_cli --log-file logs/pr31-seed-rollup-apply.log \
  identity-repair rollup-threads --apply --output logs/pr31-seed-rollup-apply.json
```

### 5. Conversation proposals (journal only)

```bash
.venv/bin/python -m archive_cli --log-file logs/pr31-seed-same-conversation-dry.log \
  identity-repair same-conversation --output logs/pr31-seed-same-conversation-dry.json
.venv/bin/python -m archive_cli --log-file logs/pr31-seed-same-conversation-apply.log \
  identity-repair same-conversation --apply --output logs/pr31-seed-same-conversation-apply.json
```

Preview must not write. Apply journals `proposed_link` state; it does not merge people.

### 6. Full serving-index publish (4.35M×1536)

There is no `ppa` publish subcommand. `dirty_uids=None` is a full rebuild (not incremental onto the hash ACTIVE). Honor the log-file contract in-process:

```bash
mkdir -p logs
PYTHONPATH=/Users/rheeger/Code/rheeger/ppa \
.venv/bin/python - <<'PY'
from pathlib import Path
import json
from archive_cli.log import attach_file_log, configure_logging
from archive_cli.serving_index import publish_serving_index
from archive_cli.store import get_archive_store

configure_logging()
attach_file_log(Path("logs/pr31-seed-serving-publish.log"))
store = get_archive_store()
result = publish_serving_index(store)
Path("logs/pr31-seed-serving-publish.json").write_text(
    json.dumps(result, indent=2, default=str) + "\n",
    encoding="utf-8",
)
raise SystemExit(0 if result.get("ok") else 1)
PY
```

Expected receipt:

- `ok: true`
- new generation id ≠ `1788743317023`
- embedding spec `openai` / `text-embedding-3-small` / dim 1536
- `embedding_count` on the order of 4.35M (warehouse embeddings after nightly, not 0)
- native open + canaries passed (fail-closed; no hash fallback)

If publish fails, ACTIVE must remain unchanged. Do not delete `1788716974760`.

### 7. Reload stdio MCP once

After crate rebuild + publish, the Cursor `user-archive-local` stdio process must be restarted so it maps the new generation and the new `resolve_person_card`. Do **not** kill the HTTP MCP. Do **not** start a second cold `archive_cli` query process.

### 8. Warm MCP canaries

Use `archive_status_json`, `archive_person`, `archive_query`, `archive_search` / `archive_hybrid_search`. Record only:

- serving generation, `embedding_count`, provider namespace, `serving_index_ready`
- person lookup `status` (`unique` / `ambiguous` / `unresolved`) and candidate counts
- people_filter responses that return `ambiguous` / `unresolved` instead of a silent winner

Needles stay out of git. A nonsense name (`Zzzyx Notaperson`) is the unresolved control.

## Rollback

1. Stop any in-flight publisher (`kill` only the publish PID, not MCP, not postgres).
2. Write `1788716974760` to `<vault>/_meta/rust-search-index/ACTIVE` and fsync. Keep COMPLETE on that generation.
3. Reload stdio MCP.
4. Identity applies are `merge_identities()` receipts. Do not fabricate undo. Owner reviews `logs/pr31-seed-identity-merge-apply.json` if a merge was wrong.
5. Leave `_meta/identity-proposals.json` in place; it is not an applied merge.

## Recoverability limits

- Hash ACTIVE `1788743317023` is not a search-quality rollback.
- Nightly already rematerialized the warehouse; rolling back code does not rewind Postgres. Vault cards remain canonical.
- A failed publish must leave staging deleted (fail-closed export). If staging remains, delete only the failed dest generation, never the openai parent.
- This runbook does not restore pre-PR31 named-person merges.
