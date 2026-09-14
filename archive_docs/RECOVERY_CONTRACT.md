# Recovery contract

Restoring a personal archive should recover the records and the decisions that shaped them, including identity corrections. The recovery manifest inventories required state and verifies its integrity before a restored archive can become active.

Canonical Markdown remains authoritative for record contents. The manifest describes what must survive and what can be rebuilt. A manifest records the state of the archive it inventories; it does not establish that a separate deployment has a verified backup. See [backup and restore](PPA_BACKUP_AND_RESTORE.md) for the operating workflow.

## Format

| Field | Contract |
| --- | --- |
| `format_name` | `ppa.recovery_manifest` |
| `format_version` | `1` |
| `required_versions.recovery_manifest` | must equal `1` |
| `missing_state_policy` | `reject_required` |
| `archive_id` | Binding via `archive_engine.changes.recovery_checkpoint_binding` when `_meta/change-journal.sqlite3` exists; otherwise `{status: unavailable, reason: awaiting_p02_journal_integration, value: null}` |
| `checkpoint` | same binding as `archive_id` |

Readers reject an unknown `format_name`, an unsupported `format_version`, a
missing `required_versions` map, or a missing-state policy other than
`reject_required`.

Archive identity and the journal checkpoint are **not** inferred from display
names, directory names, or file counts. The manifest binds them from the change journal when it is present and records them as unavailable when it is absent.

## Artifact records

Each included file has:

- `rel_path`: vault-relative POSIX path
- `classification`: `canonical`, `decision_critical`, `reconstructible`, `secret_reference`, or `disposable`
- `presence`: `required` or `optional`
- `owner_id`: catalog identity from `RECOVERY_STATE_INVENTORY.md`
- `size`: byte length
- `sha256`: hex digest of the file bytes

Credential material never receives `size` or `sha256`. Exclusions are recorded as:

- `classification`: `secret_material`
- `presence`: `excluded`
- `reason`: `credential_filename`
- `size` / `sha256`: `null`

Logical (Postgres-only) owners appear under `logical_owners` with
`materialized: false`. They are inventoried, not hashed from a warehouse dump.

## Validation

`generate_manifest` and `validate_manifest` fail closed:

| Condition | Error |
| --- | --- |
| Required catalog file missing | `MissingRequiredStateError` |
| Required file hash/size mismatch or invalid required JSON | `CorruptRequiredStateError` |
| Unclassified file that is not disposable/secret | `UnknownRequiredStateError` |
| Symlink or non-regular path | `UnknownRequiredStateError` |
| Credential filename or secret JSON keys in an included file | `SecretMaterialError` |
| Exclusion that carries a hash or size | `SecretMaterialError` |
| Unsupported format/version/policy | `IncompatibleManifestError` |

A restore must not activate while any of these errors stand. `archive_engine.recovery` also checks the encrypted bundle's checksum and decryption result.

## Secrets

Ordinary manifests preserve **names and provider references** only:

- `_meta/llm-config.json` may record provider and model
- OAuth account names and 1Password *references* may be documented
- Refresh tokens, API keys, client secrets, backup passphrases, and `~/.ppa/gemini_key.txt` are excluded

If an included JSON object has a non-empty `api_key`, `refresh_token`,
`client_secret`, `password`, `passphrase`, `private_key`, or adjacent key, generation
rejects. Pagination cursors such as `page_token` in `sync-state.json` are not
credentials.

## Classes

The [state inventory](RECOVERY_STATE_INVENTORY.md) lists paths, owners, and recovery strategies.

1. **Canonical**: cards and attachments. Missing files cannot be invented.
2. **Decision-critical**: identity maps, cursors, config, hygiene rollback preimages, and Postgres-only suppression/review decisions.
3. **Reconstructible**: warehouse, chunks, embeddings, serving generations, scan caches.
4. **Secret reference / secret material**: references stay; values do not.
5. **Disposable**: logs, WAL/SHM, editor metadata, staging.

Some suppression and human linker-review decisions live only in Postgres. The manifest inventories those tables as `decision_critical`; it does not export their rows. A vault bundle alone must not be described as a complete copy of that state.

## Recovery integration

Archive identity and checkpoint bind through `recovery_checkpoint_binding` when the change journal exists. Field corrections and identity decisions recorded in `_meta/canonical-decisions.json` use that journal's write protocol.

The file manifest is implemented in `archive_engine/recovery_manifest.py`. Encrypted bundles and contained extraction are implemented in `archive_engine/recovery.py`. The CLI registers `backup`, `verify-backup`, `restore`, and `activate-restore` through `archive_cli/command_registry.py`.

A restore writes a new root and validates required files before activation. Activation rebuilds the derived warehouse and search index. The [backup and restore reference](PPA_BACKUP_AND_RESTORE.md) describes how to verify that the restored records remain usable.
