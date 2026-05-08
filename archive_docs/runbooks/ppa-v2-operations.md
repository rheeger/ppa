# PPA v2 Operations Runbook

## Post-v2 Steady State

After v2 deployment, regular operations are incremental. Full rebuilds are not
part of the normal operational cadence.

## Common Scenarios

| Scenario                      | What to do                                                                                                                        |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Code change, no schema bump   | `cd /path/to/hey-arnold && make ppa-sync ppa-install`, then `ssh arnold 'sudo systemctl restart ppa-mcp'`.                        |
| Code change, schema bump      | `make ppa-sync ppa-install`, then on Arnold run `ppa migrate`, `ppa rebuild-indexes --force-full-rebuild`, and restart `ppa-mcp`. |
| New data synced to local seed | Run `make ppa-vault-rsync`. The next `ppa maintain` incrementally indexes new vault files.                                        |
| Disaster recovery             | Re-rsync seed with `make ppa-deploy-v2-rollback`, or restore from the latest encrypted backup.                                    |

## Source of Truth

The local seed vault is canonical:

```text
/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127/
```

Arnold's vault at `/srv/hfa-secure/vault` is a deployment copy produced by
rsync. The Postgres index is always derivable from vault + code.

## Embedding Cache

Phase 9 restores embeddings from a direct TSV export built from the local seed
Postgres `ppa.embeddings` table. Do not run fresh embedding generation during
Phase 9.

```bash
export PPA_EMBEDDING_RECOVERY_CACHE_DIR=/Users/rheeger/Archive/seed/embedding-cache-seed-20260427
```

Verified cache:

```text
embeddings.tsv         111G
embeddings.tsv.rows    6,770,930
embeddings.tsv.sha256  92281ea1ba67ab9d1df176da11e0cf22a18122f773e050b9a72ea48aa726368f
```

Restore command:

```bash
ppa import-embedding-cache --input-dir "$PPA_EMBEDDING_RECOVERY_CACHE_DIR"
```

Do not use `embed-pending`, `embed-batch-submit`, or OpenAI embedding APIs as
part of Phase 9. If the cache is missing or has the wrong row count/checksum,
stop and restore the cache first.

## Encrypted Vault

The production vault lives on a LUKS-encrypted volume at `/srv/hfa-secure`.

- Mount chain: `mnt-user.mount` -> `ppa-unlock.service` -> `ppa-mount.service`
- Manual unlock/mount: `make ppa-unlock && make ppa-mount`
- Manual lock: `make ppa-lock`
- Rebuilds use `archive_crate`; Phase 9 blocks if the Rust engine is unavailable.

## Backups

- Check backup timer: `make ppa-backup-status`
- Backups should cover both `/srv/hfa-secure/vault` and `/srv/hfa-secure/postgres`
- Preflight checks backup timer state and volume headroom

## Monitoring

- Health: `make ppa-health-v2`
- Latency: `make ppa-latency-check`
- Maintenance: `make ppa-maintain-status`
- Full verification: `bash archive_scripts/ppa-verify-v2.sh`

## Latency Targets

| Query Type                   | Target |
| ---------------------------- | ------ |
| `archive_search`             | < 2s   |
| `archive_temporal_neighbors` | < 2s   |
| `archive_hybrid_search`      | < 5s   |
| `archive_query`              | < 2s   |

`archive_knowledge` is omitted because Phase 7 was skipped; it falls back to
lexical search and inherits the `archive_search` budget.
