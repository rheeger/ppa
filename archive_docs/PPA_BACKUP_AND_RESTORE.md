# Back up and restore a PPA archive

A personal archive lets you keep history after its original account or service is gone. A backup needs to preserve the records you may no longer be able to import, along with identity corrections and other saved decisions. A search index cannot recreate those records or decisions.

## Decide what to retain

| Data | Recovery treatment |
| --- | --- |
| Canonical Markdown and attachments | Preserve the bytes; an index cannot recreate missing source content |
| Identity maps, corrections, and other decision state | Retain required state so a restore preserves the same interpretation |
| Warehouse, chunks, embeddings, and serving generations | Rebuild when necessary; retain compatible caches when their recovery path supports reuse |
| Credentials and decryption secrets | Store separately through the deployment's secret-management process |

Use the [recovery state inventory](RECOVERY_STATE_INVENTORY.md) for exact paths and classifications. The [manifest contract](RECOVERY_CONTRACT.md) requires checks for missing, corrupt, unknown, or incompatible state. A backup with required state missing must not be reported as a complete restore.

A vault bundle does not automatically export warehouse-only suppression decisions or human linker reviews. The state inventory identifies these separately. Preserve them through a verified export or database recovery path before describing a restore as complete.

## Recovery workflow

1. Bind the archive you intend to protect and inventory its canonical and required decision state.
2. Create and verify a backup using the configured recovery tooling. Keep the decryption secret recoverable separately from the backup.
3. Restore into a new root. Validate the manifest, file hashes, and required versions before activation.
4. Bind a separate warehouse schema and rebuild the derived indexes for the restored instance.
5. Read known cards and verify representative searches and corrected identities before relying on the restored archive.

The engine implementation is in `archive_engine/recovery.py`; command adapters are in `archive_cli/commands/recovery.py`, with registration in `archive_cli/command_registry.py`. Inspect `ppa backup --help` and `ppa restore --help` for installed command options. Missing encryption or warehouse dependencies must remain visible as unavailable capabilities.

A checksum confirms the stored bytes. Retrieval checks establish whether the restored archive still supports the expected answers. The strongest fixture check restores a corrected record and then verifies that exact reads and retrieval still support the same facts.

## Storage protection

PPA's backup workflow uses external encryption tooling. That does not encrypt the live vault, warehouse, caches, or temporary files. The operator owns storage protection and backup destinations. See [data boundaries](DATA_BOUNDARIES.md) for what each location may contain.

Run long backup, restore, and rebuild jobs using the [detached-job instructions](../.cursor/skills/long-running-jobs/SKILL.md). A restored archive needs its own instance binding before any operation that writes to a warehouse.

## Deployment-specific instructions

The [historical Arnold backup runbook](runbooks/historical-arnold-backup.md) records that host's paths, schedules, sizes, and commands. The [local archive recovery runbook](runbooks/local-archive-recovery.md) describes the maintainer's later host. Neither establishes that another instance has a working backup.
