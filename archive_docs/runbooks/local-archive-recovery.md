# Local archive recovery (current host)

This is the current-host recovery proposal for the living Mac archive (v2.5).
It does **not** assert that encrypted backups already run on a schedule, that a
production restore drill has been completed, or that fixture timings apply to
the seed vault. Retention, backup destination, and a live drill remain R0/R1
operator decisions.

The Arnold-oriented [PPA_BACKUP_AND_RESTORE.md](../PPA_BACKUP_AND_RESTORE.md)
describes the historical Unraid/Arnold layout. Do not treat those paths as
this machine's live schedule.

## What a recoverable bundle is

Canonical markdown, attachments, identity maps, the change journal, and
canonical decisions are the story. Postgres, embeddings, serving generations,
and scan caches are reconstructible. Restore must work after those derived
files are discarded.

Ordinary bundles exclude credential material (API keys, OAuth refresh tokens,
backup passphrases). Unlock the envelope from 1Password or an equivalent
store that is not inside the archive.

## Encryption capability

Supported tool and format (legacy-compatible):

| Field | Value |
| --- | --- |
| Tool | `openssl` |
| Format | `openssl enc -aes-256-cbc -pbkdf2` (`openssl-enc-aes-256-cbc-pbkdf2`) |
| Integrity | SHA-256 of the ciphertext (`sha256sum` or `shasum -a 256`) |

CBC is **not** authenticated encryption. Integrity is the checksum plus the
P07 recovery manifest hashes after a contained extract. Do not declare old
backups AEAD.

If `openssl` is missing, backup and restore **fail closed**. There is no
plaintext fallback.

Check before a drill:

```bash
command -v openssl && openssl version
```

## Create (proposal)

Engine/command module (P09 registers the CLI later):

```bash
# After P09 wires the parser, the intended command is:
#   ppa recovery backup --vault "$PPA_PATH" --dest "$PPA_BACKUP_BASE"
# Until then, call the existing script with an explicit passphrase source:

export PPA_PATH=/path/to/vault
export PPA_BACKUP_BASE=/path/to/encrypted-backups
export PPA_BACKUP_PASSPHRASE_FILE=/path/to/passphrase  # not the vault
export PPA_BACKUP_CANONICAL_ONLY=1   # omit reconstructible caches
bash archive_scripts/ppa-backup-encrypt.sh
```

`archive_engine.recovery.create_encrypted_bundle` is the verified path used by
tests. It invokes that script and writes `ppa-recovery-manifest.json` next to
the ciphertext.

## Restore into a new root

Never point `PPA_RESTORE_DIR` at the active vault. Restore creates a new
directory, extracts only contained regular files (P05 path rules), and
rejects `..`, symlinks, and device members.

```bash
export PPA_BACKUP_BASE=/path/to/encrypted-backups
export PPA_RESTORE_DIR=/path/to/new-restored-vault
export PPA_BACKUP_PASSPHRASE_FILE=/path/to/passphrase
# PPA_PATH remains the active vault; restore must not overlap it.
bash archive_scripts/ppa-backup-restore.sh
```

Then rebuild derived state against a **new** Postgres schema / serving path.
Do not attach the restored tree to the live DSN and do not copy a live
SQLite WAL in as a checkpoint.

```bash
export PPA_PATH=/path/to/new-restored-vault
export PPA_INDEX_DSN=...          # isolated
export PPA_SERVING_INDEX_PATH=... # empty directory
ppa bootstrap-postgres --force    # after P09, or existing admin entry
ppa rebuild-indexes --force-full-rebuild --no-cache
```

Query through MCP/CLI (`archive_read` / `archive_search`) and confirm the
human correction and identity redirects still cite the same story.

## Fixture measurement versus production

P07-D records RPO/RTO on a synthetic fixture (`scale_profile: fixture`).
Those numbers are elapsed restore+rebuild of the test archive and the
journal checkpoint present in that fixture. They are **not** a production
recovery claim for the seed vault.

A live drill, when the operator chooses one, should record: last durable
journal checkpoint, ciphertext SHA-256, restored file counts, rebuild
duration, and the query that proved the same story. Do not infer host-wide
RPO from the fixture.

## Command proposal for P09

`archive_cli/commands/recovery.py` already exposes `backup_archive`,
`verify_backup`, `restore_archive`, and `activate_restored_archive`.
Suggested parser names: `ppa recovery backup|verify|restore|activate`.
Do not register them in this slice.
