# HFA Security Model

## Scope

This document covers the production security posture for the HFA vault, `ppa`, the Hey Arnold VM, the passkey gate, and remote archive access from Robbie's Mac.

It assumes:

- the canonical vault lives on encrypted parity-backed storage
- `ppa` stays local-only on the VM
- public remote access terminates at the passkey gate
- no plaintext archive content is uploaded to cloud backup destinations

## Trust Boundaries

### Trusted

- the encrypted storage boundary on the server and VM
- the passkey gate as the public TLS termination and policy enforcement point
- the dedicated archive runtime identity
- the dedicated 1Password gate service account for archive-adjacent secrets

### Not Trusted By Default

- OpenClaw agent code execution on the VM
- general shell access inside the `arnold` runtime context
- localhost assumptions for archive data access
- cloud backup destinations beyond receipt of encrypted artifacts

## Threat Model

### Primary adversary

A compromised or misaligned OpenClaw agent with code execution on the Hey Arnold VM.

### Secondary adversary

Theft of a remote archive client credential from Robbie's Mac or from a 1Password item.

### Tertiary adversary

Accidental or malicious plaintext replication into parity-backed backup paths, cloud-synced folders, or host-visible staging directories.

## Security Objectives

- Keep raw archive contents encrypted at rest on parity-backed storage.
- Force public archive access through the gate.
- Keep `ppa` and Postgres off the public network.
- Separate archive read, sensitive read, admin, and public remote-read capabilities.
- Ensure OpenClaw cannot directly read raw vault files from the mounted archive path.
- Restrict cloud backups to encrypted artifacts only.

## Non-Goals

- No direct public `ppa` exposure.
- No plaintext mirrors on Unraid, Google Drive, iCloud, or any other remote destination.
- No claim of true end-to-end opaque MCP payload encryption through the gate.

## Access Classes

### `mcp.archive.read`

Low-risk structured retrieval.

- `archive_search`
- `archive_query`
- `archive_timeline`
- `archive_stats`
- `archive_vector_search`
- `archive_hybrid_search`

Tier target: `0`

### `mcp.archive.sensitive`

Retrieval paths that can expose dense personal content or direct note bodies.

- `archive_read`
- `archive_person`
- `archive_graph`

Tier target: `1`

### `mcp.archive.admin`

Maintenance and index lifecycle operations.

- `archive_validate`
- `archive_duplicates`
- `archive_bootstrap_postgres`
- `archive_rebuild_indexes`
- `archive_index_status`
- `archive_embedding_status`
- `archive_embedding_backlog`
- `archive_embed_pending`

Tier target: `2`

### `mcp.archive.remote.read`

Dedicated public-client scope for Robbie's Mac.

Initial target surface:

- `archive_search`
- `archive_query`
- `archive_timeline`
- `archive_stats`

This intentionally excludes raw note reads and admin/index operations.

## Transport Model

### Feasible secure model

- `Mac -> HTTPS/TLS -> passkey-gate`
- `passkey-gate -> local-only transport -> ppa`
- `ppa -> local vault mount + localhost Postgres`

### Why TLS terminates at the gate

The gate must:

- authenticate the remote archive client
- enforce tool policy
- issue scoped tickets
- audit requests

That means the gate is trusted to inspect archive request metadata and content for authorized calls. True end-to-end encrypted MCP payloads that remain opaque to the gate are not compatible with this first design.

### Local transport preference

Prefer a Unix domain socket or equivalent host-local transport between the gate-side MCP execution path and `ppa`. Loopback TCP is acceptable if socket transport is impractical.

## Identity And Runtime Isolation

### `arnold`

- runs OpenClaw
- runs the passkey gate
- may request archive access through approved paths
- should not own or directly read the mounted vault tree

### `archive`

- runs `ppa`
- owns the mounted vault and archive index paths
- is the primary runtime identity for canonical archive reads

### `postgres`

- optional dedicated identity if Postgres is split from `archive`
- should read only encrypted-mounted Postgres data paths

## Storage Model

### Preferred

An encrypted container file stored on parity-backed server storage and opened on the VM at boot/unlock time.

Example:

- storage root: `/mnt/user/archive-secure/`
- encrypted artifact: `/mnt/user/archive-secure/hfa-vault.img`
- mount root: `/srv/hfa-secure/`
- canonical vault: `/srv/hfa-secure/vault`
- Postgres data: `/srv/hfa-secure/postgres`

### Invariants

- plaintext archive data exists only inside the mounted unlocked runtime path
- unlock secrets are resolved remotely or from dedicated secret paths, never committed in repo
- services fail closed when the encrypted mount is unavailable

## Backup Model

### Canonical durability

- parity-backed encrypted archive volume on Orthanc/Unraid

### Secondary backups

- encrypted backup artifacts only
- local parity-backed backup destination
- optional encrypted upload targets for Google Drive and iCloud

### Hard rule

No plaintext archive content may be uploaded to cloud or copied to remote backup destinations.

## Archive Containment Inside `hey-arnold-hfa`

`hey-arnold-hfa` is the dedicated archive tree. Containment should come from:

- archive-prefixed Make targets
- archive-prefixed scripts
- archive-prefixed systemd units
- archive-scoped docs and runbooks
- a stable `ppa` runtime contract

This avoids inventing a second archive tree while still keeping the service operable and extractable later.

## Future Extraction Seam

If `ppa` later becomes its own repo:

- preserve the `python -m archive_mcp serve` entrypoint
- preserve the archive env contract
- move archive-specific deploy assets with minimal renaming
- package shared HFA code rather than relying forever on ad hoc relative path imports
