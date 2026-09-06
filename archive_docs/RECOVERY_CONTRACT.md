# Recovery Contract

This document is the P07-A recoverability contract. It describes the versioned
archive manifest and the fail-closed rules for enumerating a synthetic archive.
It does not claim that a live seed backup exists.

Canonical markdown remains the source of meaning. The manifest is an inventory
and integrity record so a later restore can tell whether the same story is still
queryable.

## Format

| Field | Contract |
| --- | --- |
| `format_name` | `ppa.recovery_manifest` |
| `format_version` | `1` |
| `required_versions.recovery_manifest` | must equal `1` |
| `missing_state_policy` | `reject_required` |
| `archive_id` | P02 binding via `archive_engine.changes.recovery_checkpoint_binding` when `_meta/change-journal.sqlite3` exists; otherwise `{status: unavailable, reason: awaiting_p02_journal_integration, value: null}` |
| `checkpoint` | same binding as `archive_id` |

Readers reject an unknown `format_name`, an unsupported `format_version`, a
missing `required_versions` map, or a missing-state policy other than
`reject_required`.

Archive identity and the journal checkpoint are **not** inferred from display
names, directory names, or file counts. P07-B binds them from the P02 journal
when that file is present and leaves the P07-A placeholder when it is not.

## Artifact records

Each included file has:

- `rel_path` — vault-relative POSIX path
- `classification` — `canonical`, `decision_critical`, `reconstructible`, `secret_reference`, or `disposable`
- `presence` — `required` or `optional`
- `owner_id` — catalog identity from `RECOVERY_STATE_INVENTORY.md`
- `size` — byte length
- `sha256` — hex digest of the file bytes

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

A later restore must not activate while any of these errors stand. Wrong
credentials and tamper checks for encrypted envelopes are P07-D.

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

See `RECOVERY_STATE_INVENTORY.md` for the inspected owner matrix.

1. **Canonical** — cards and attachments. Missing files cannot be invented.
2. **Decision-critical** — identity maps, cursors, config, hygiene rollback preimages, and Postgres-only suppression/review decisions.
3. **Reconstructible** — warehouse, chunks, embeddings, serving generations, scan caches.
4. **Secret reference / secret material** — references stay; values do not.
5. **Disposable** — logs, WAL/SHM, editor metadata, staging.

Suppression and linker review state currently lives only in Postgres. That is
**not** reconstructible. P07-B exports it into versioned canonical decision
records.

## Seam handed to P07-B / P02

- `archive_id` and `checkpoint` bind through `recovery_checkpoint_binding` when
  the P02 journal file exists.
- P07-B writes correction decisions to `_meta/canonical-decisions.json` through
  that journal; it does not invent a second write protocol.
- P07-C export `card_corpus_state`, `email_corpus_decisions`, human
  `link_decisions` / `review_actions`, and any PG cursor that is not a copy of
  `_meta/sync-state.json`.
- P07-D adds the encrypted envelope, contained restore, and rebuilt-evidence proof.

Module: `archive_engine/recovery_manifest.py`. No CLI registration in this slice.
