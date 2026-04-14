# HFA Archive Resource Manifest

## Purpose

This runbook is the source of truth for the resources that must exist outside the codebase before the production archive stack is considered complete.

It is intentionally provisioning-oriented. The archive design assumes we may need to create these resources rather than merely point code at whatever already exists.

## 1Password Resources

### Required

- `op://Arnold-Passkey-Gate/HFA_ARCHIVE_UNLOCK_KEY/credential`
  - unlock secret for the encrypted archive volume (LUKS passphrase for `hfa-vault.img`)
- `op://Arnold-Passkey-Gate/ARCHIVE_REMOTE_CLIENT_TOKEN/credential`
  - public remote archive client token for Robbie's Mac

#### Creating `HFA_ARCHIVE_UNLOCK_KEY` (new vault / first provision)

You are not recovering an old key — the passphrase **formats** the LUKS container on first **`hfa-archive-provision-storage`**. If you lost the old passphrase and the old `.img` is gone, generate a **new** secret and store it in 1Password before running provision.

1. **Generate** a strong passphrase (example): `openssl rand -base64 32`
2. In 1Password vault **`Arnold-Passkey-Gate`**, create an item whose **title is exactly** **`HFA_ARCHIVE_UNLOCK_KEY`** (matches the default `op://…` ref on Arnold).
3. Use type **API Credential** (same pattern as other gate-vault secrets) and put the passphrase in the field **`credential`** (the CLI and `op read` use `…/credential`).
4. Ensure the **Arnold VM service account** token at **`op-tokens-service-account-token`** (gate SA) can read **`Arnold-Passkey-Gate`** — same as your other gate items.

**CLI (after `op signin`):**

```bash
op item create --vault "Arnold-Passkey-Gate" --category "API Credential" \
  --title "HFA_ARCHIVE_UNLOCK_KEY" \
  credential="$(openssl rand -base64 32)"
```

Copy the generated value from the item in the app if you need a human-readable backup; **`op read`** is what Arnold uses.

5. On Arnold, **`HFA_ARCHIVE_UNLOCK_KEY_OP_REF`** in **`/home/arnold/openclaw/.env`** should stay **`op://Arnold-Passkey-Gate/HFA_ARCHIVE_UNLOCK_KEY/credential`** (default from **`make hfa-archive-secure-env`**).

### Recommended

- `op://Arnold-Passkey-Gate/HFA_ARCHIVE_BACKUP_PASSPHRASE/credential`
  - passphrase for encrypted archive backup artifacts
- `op://Arnold-Passkey-Gate/HFA_ARCHIVE_PG_PASSWORD/credential`
  - Postgres password if you want to move away from a static inline default

### Existing gate service-account requirement

- token broker service-account token file:
  - `/home/arnold/.openclaw/credentials/op-tokens-service-account-token`
- file mode: `600`
- access scope: `Arnold-Passkey-Gate` vault only

## Capacity and topology (where the DB lives)

- **Canonical vault** and **archive Postgres PGDATA** both live on the **encrypted mount** **`/srv/hfa-secure`** (see VM runtime paths below). That mount is **not** the Ubuntu **`/`** disk.
- **Target size (recorded):** provision or grow the primary encrypted image (**`hfa-vault.img`**) to **at least ~256 GiB** so **vault + full derived index + headroom** fit on one ext4 filesystem. The Makefile default for **new** images is **`HFA_ARCHIVE_IMAGE_SIZE=256G`**.
- **VM root (`/`)** is separate: it must stay **well below 100%** for Docker and logs; growing it does **not** replace growing **`hfa-vault.img`**. See **[hfa-archive-expand-storage.md](./hfa-archive-expand-storage.md)** for resize procedures.

## Orthanc / Unraid Resources

### VM gotcha: `/mnt/user` must be real Unraid storage

On Arnold, **`/mnt/user` must be a mount** (VirtIOFS, NFS, or equivalent) to the Unraid **array**, not an ordinary directory on the VM’s **`/`** disk.

If **`mountpoint /mnt/user`** reports **“not a mountpoint”**, then **`hfa-vault.img` lives on the Ubuntu root LV** (same 46 GiB disk as `/`). A **100 GiB sparse** image still **consumes tens of GiB of real blocks** as it fills — plus **Docker, snap, `/home`**, and the OS — so **`/` hits 100%** even while **`/srv/hfa-secure`** (the opened LUKS volume) looks fine.

**Check on Arnold:** `mountpoint /mnt/user && df -h /mnt/user`

**Setup and migration:** [hfa-unraid-virtiofs-setup.md](./hfa-unraid-virtiofs-setup.md) (VirtIOFS tag **`unraid-user`**, `mnt-user.mount`, `make hfa-unraid-migrate-archive-secure`).

### Required storage roots

- primary parity-backed encrypted archive storage:
  - `/mnt/user/archive-secure/` (must be **on the array**, via a mounted `/mnt/user` — see above)
- encrypted backup destination:
  - `/mnt/user/backups/hfa-encrypted/`

### Expected artifacts

- primary encrypted archive image (sparse; default **256 GiB** for new provision via `HFA_ARCHIVE_IMAGE_SIZE`):
  - `/mnt/user/archive-secure/hfa-vault.img`
- optional split Postgres image:
  - `/mnt/user/archive-secure/hfa-pg.img`

## VM Runtime Paths

- mount root:
  - `/srv/hfa-secure/`
- canonical vault:
  - `/srv/hfa-secure/vault`
- archive Postgres data:
  - `/srv/hfa-secure/postgres`
- archive runtime workspace:
  - `/home/arnold/openclaw/ppa`

## Cloud Backup Destinations

### Google Drive

Configure one of:

- `HFA_BACKUP_GDRIVE_DEST` as an `rclone` remote path
- or another operator-approved destination abstraction that still receives encrypted artifacts only

### iCloud

Configure one of:

- `HFA_BACKUP_ICLOUD_DEST` as a trusted local synced path
- or another operator-approved destination abstraction that still receives encrypted artifacts only

## Provisioning Checklist

- Create required 1Password items.
- Verify the gate service account can resolve the required archive items.
- Create the required Orthanc/Unraid paths with correct ownership and access.
- Confirm parity coverage for the underlying storage path.
- Confirm cloud backup destinations are configured to receive encrypted artifacts only.

## Safety Rules

- Do not place raw vault contents in any of these resource locations except the mounted encrypted runtime path.
- Do not store the public remote archive client token in repo or plaintext config committed to git.
- If a resource is missing or provenance is unclear, stop and document the gap instead of improvising around it.
