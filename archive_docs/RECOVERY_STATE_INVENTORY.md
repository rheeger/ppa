# Recovery state inventory

A vault backup needs to retain more than searchable text. Attachments, identity decisions, and corrections can change what a later answer means. This inventory tells contributors which state must survive and which state the engine can reconstruct.

The inventory describes files and tables written by the repository. It classifies recovery needs; a deployment must still verify its own backups. Classifications follow `archive_engine/recovery_manifest.py` and the [recovery contract](RECOVERY_CONTRACT.md).

## Vault files

| Owner | Location | Class | Presence | Code owner | Recovery strategy |
| --- | --- | --- | --- | --- | --- |
| Canonical cards | `<family>/**/*.md` | canonical | required if present | `archive_vault/vault.py`, families in `archive_vault/card_contracts.py` | Hash and restore bytes. Families: People, Finance, Medical, Vaccinations, Email*, IMessage*, Beeper*, Calendar, Photos, Documents, MeetingTranscripts, Git*, Transactions/*, Entities/*, Knowledge, Agent. |
| Attachments | `Attachments/**`, `*Attachments/**` | canonical | required if present | `archive_vault/vault.py` (`EXCLUDED_DIRS` skips these during card walks) | Hash and restore binaries. Not derived from markdown. |
| Identity map | `_meta/identity-map.json` | decision-critical | required | `archive_vault/identity.py` | Required vault file. Alias → canonical redirects. |
| Sync / cursor state | `_meta/sync-state.json` | decision-critical | required | `archive_vault/sync_state.py` | Required. Replay without cursors is unsafe. |
| Own emails | `_meta/own-emails.json` | decision-critical | required | `archive_sync/adapters/gmail_correspondents.py` | Required. Self/account aliases. |
| Nicknames | `_meta/nicknames.json` | decision-critical | required | `archive_vault/identity_resolver.py` | Required. Resolution aliases. |
| Vault config | `_meta/ppa-config.json` | decision-critical | required | `archive_vault/config.py` | Required. Merge/dedup/finance thresholds. |
| LLM config | `_meta/llm-config.json` | secret-reference | required | `archive_vault/llm_provider.py` | Provider/model names only. Reject credential keys. |
| Dedup candidates | `_meta/dedup-candidates.json` | reconstructible | optional | `archive_doctor/handler.py` | Regenerate via doctor/validate. Init creates `[]`. |
| Enrichment log | `_meta/enrichment-log.json` | disposable | optional | `archive_scripts/ppa-post-import.sh` | Debug log. Init creates `[]`. |
| LLM cache | `_meta/llm-cache.json` | reconstructible | optional | `archive_vault/llm_provider.py` | Recompute. Optional acceleration. |
| Validation report | `_meta/validation-report.json` | disposable | optional | `archive_doctor/handler.py` | Regenerate via validate. |
| Processor snapshot | `_meta/processors.json` | reconstructible | optional | `archive_sync/processors/runner.py` | Rebuild status. Clear expired leases. |
| Source-updater snapshot | `_meta/source-updaters.json` | reconstructible | optional | `archive_cli/source_updaters/cli.py`, `archive_cli/status/aggregate.py` | Status fallback. Cursors are in `sync-state.json`. |
| Vault scan cache | `_meta/vault-scan-cache.sqlite3` | reconstructible | optional | `archive_cli/vault_cache.py` | Rebuild. Do not copy a live WAL as a checkpoint. |
| Query embed cache | `_meta/query-embed-cache.sqlite` | reconstructible | optional | `archive_cli/index_config.py` | Rebuild. |
| Serving index | `_meta/rust-search-index/` | reconstructible | optional | `archive_cli/index_config.py` | Rebuild after restoring required records and decisions. |
| Canonical decisions | `_meta/canonical-decisions.json` | decision-critical | required **when present** | `archive_vault/decisions.py` | Human field overrides, clear-override, source conflicts. Not reconstructible. |
| Change journal | `_meta/change-journal.sqlite3` | decision-critical | required **when present** | `archive_vault/change_journal.py` | Committed change journal. Recovery checkpoint binds from this file. |
| Benchmark sample | `_meta/benchmark-sample.json` | disposable | optional | `archive_cli/benchmark.py` | Disposable. |
| Hygiene rollback kit | `_artifacts/hygiene-rollback-kit/<run>/` | decision-critical | required **when present** | `archive_cli/corpus_hygiene/apply.py` | Preimages of suppressed cards. Missing directory is fine. |
| Other `_artifacts/**` | staging | disposable | optional | various | Temp staging. |
| Templates | `_templates/` | reconstructible | optional | `archive_scripts/ppa-init-vault.sh` | Recreate from init. |
| Obsidian | `.obsidian/` | disposable | optional | `archive_vault/vault.py` | Editor metadata. |
| SQLite WAL/SHM | `_meta/*-{wal,shm}` | disposable | optional | `archive_cli/vault_cache.py` | Never a consistent checkpoint. |

`ppa-init-vault.sh` also seeds the required `_meta` JSON files listed above.
Unknown files under `_meta/` that are not in this table are **unknown required
state** and reject the manifest.

## Postgres (derived warehouse)

Logical owners. Not hashed from a live database. Inspected from
`archive_cli/schema_ddl.py` and numbered migrations.

| Owner | Table(s) | Class | Strategy |
| --- | --- | --- | --- |
| Cards and typed projections | `cards`, `card_sources`, `card_people`, `card_orgs`, `card_classifications`, `external_ids`, `duplicate_uid_rows`, typed projection tables | reconstructible | Rebuild from markdown. |
| Graph and search data | `edges`, `chunks`, `embeddings`, `note_manifest` | reconstructible | Rebuild. Optional dump is acceleration only. |
| Rebuild worker | `rebuild_checkpoint` | reconstructible | Instance-local. Do not revive a foreign in-progress run. |
| Ingest / enrichment queues | `ingestion_log`, `enrichment_queue`, `retrieval_gaps` | reconstructible | Replay from vault + policy. |
| Schema bookkeeping | `schema_migrations`, `meta` | reconstructible | Recreate on bootstrap. |
| **Corpus suppression** | `card_corpus_state` (migration 006) | **decision-critical** | **Not reconstructible.** Requires a separate export; a file-only backup does not capture this table. |
| **Corpus decision history** | `email_corpus_decisions` (006) | **decision-critical** | Preserve with `card_corpus_state`. |
| Linker jobs | `link_jobs`, `link_candidates`, `link_evidence`, `promotion_queue`, `link_review_metrics`, `link_dead_ends` | reconstructible | Requeue; clear leases. Auto scores are recomputable. |
| **Linker human review** | `link_decisions`, `review_actions` | **decision-critical** | Human overrides are not reconstructible. Preserve separately if not already recorded in the vault. |
| Source updater cursors | `source_updater_state` (007) | **decision-critical** | Vault copy is `sync-state.json`. Export PG `cursor_payload` if it is not that copy. |
| Source updater history | `source_updater_runs` | reconstructible | History only. |
| Processor execution | `processor_state`, `processor_runs`, `processor_input_state` (008) | reconstructible | Rebuild via maintain. Clear leases. Use `OutputReceipt` revisions to identify affected outputs. |
| Embedding batch ops | `embed_batches`, `embed_batch_requests` | reconstructible | Re-submit if needed. |

A warehouse dump is not a canonical checkpoint. An inconsistent dump must not be
labeled one.

## Secrets (excluded from ordinary manifests)

| Owner | Location | Code owner | Strategy |
| --- | --- | --- | --- |
| Google OAuth refresh tokens | env (`GOOGLE_*`), 1Password refs in `archive_auth/accounts.py`, `~/.gmail-mcp/credentials.json`, token cache dirs in `archive_auth/token_manager.py` | `archive_auth/token_manager.py` | Exclude values. Keep account/provider names. |
| LLM keys | `~/.ppa/gemini_key.txt`, provider env vars | `archive_vault/llm_provider.py` | Exclude. |
| Backup unlock | `PPA_BACKUP_PASSPHRASE`, `PPA_BACKUP_PASSPHRASE_OP_REF`, passphrase files | `archive_scripts/ppa-backup-encrypt.sh` | Exclude. Recover separately. |
| Vault-dropped credential files | `credentials.json`, `client_secret.json`, `.env`, `*passphrase*`, `*refresh_token*` | filename rules in `recovery_manifest.py` | Record path only; no hash or bytes. |

A separate credential recovery export is out of default scope.

## Backup bundles and remaining decision state

The current engine uses `archive_scripts/ppa-backup-encrypt.sh` for encrypted bundles and writes the versioned recovery manifest alongside the artifacts. The older path-count manifest lacks the classifications and per-file hashes defined by the [recovery contract](RECOVERY_CONTRACT.md).

A file-only backup does not automatically export warehouse-only decisions. Before treating a backup as complete, account for:

1. Suppression and quarantine decisions in `card_corpus_state` and `email_corpus_decisions`.
2. Human review decisions in `review_actions` and `link_decisions` that are not already preserved in the vault.
3. Postgres source cursors that have no equivalent in `_meta/sync-state.json`.

Field overrides and recorded identity decisions live in `_meta/canonical-decisions.json`. Archive identity and the journal checkpoint bind from `_meta/change-journal.sqlite3`. Preserve both when present; neither is a substitute for exporting warehouse-only decisions.
