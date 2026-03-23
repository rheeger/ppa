# HFA Archive Rollout And Verification

## Recorded launch decisions

- **Encrypted archive capacity:** primary **`hfa-vault.img`** (LUKS + ext4 for **`/srv/hfa-secure`**) is sized for **vault + Postgres PGDATA** going forward — target **≥ 256 GiB** for production (`HFA_ARCHIVE_IMAGE_SIZE` default **256G** for new provision; existing images: [hfa-archive-expand-storage.md](./hfa-archive-expand-storage.md)). This is **separate** from the Ubuntu **VM root** disk (**`/`**), which only needs enough space for OS + Docker + logs.
- **Backup vs. first cutover (v1):** backup readiness is **not** a hard gate for declaring the initial archive+Postgres+gate rollout complete. Encrypted layout and backup automation should still be tracked and closed in a **follow-up** milestone (timer, destinations, restore drill). Update this bullet if policy changes.
- **Public archive API smoke tests (ticket + `/call`):** use the **same HTTPS origin as the production ngrok gate URL** (no separate “staging” gate hostname for this checklist). Auth is the dedicated **`ARCHIVE_REMOTE_CLIENT_TOKEN`** bearer unless you have added another edge layer; if so, document it next to this bullet.
- **Operator SSH for this rollout:** **LAN only** — `arnold@192.168.50.27` with default `make` / `rsync` targets; no required `ARNOLD_SSH_OPTS` / ProxyJump for the documented window. If that changes (e.g. remote laptop), set overrides and note them in rollout notes.

## Stage 1. Resource Preparation

- create the required 1Password items
- create the required Orthanc/Unraid storage paths
- verify gate service-account access
- verify parity-backed storage placement

## Stage 2. Gate And Policy

- add `mcp.archive.*` policy entries
- compile policy
- deploy gate and policy
- verify exact `server.tool` allowlist enforcement for archive calls

## Stage 3. Archive Runtime Containment

- prepare the `archive` Unix identity
- provision the encrypted archive artifact
- unlock and mount the encrypted archive volume
- configure the local `archive` MCP server for MCPorter
- enable `hfa-archive-postgres.service`
- enable `hfa-archive-mcp.service`

### Encrypted mount, gate secrets, and who can write what

- **Passkey-gate** enforces HTTP/API identity and policy (tokens, tickets, internal auth). It does **not** mount the encrypted archive volume or grant filesystem access to `/srv/hfa-secure`. Treat volume unlock and mount as **VM + `hfa-archive-unlock` / systemd + 1Password service-account material** in `/home/arnold/openclaw/.env`, same as the rest of HFA archive ops.
- **`archive`** owns the sensitive paths under the mount (e.g. `/srv/hfa-secure/vault`, `/srv/hfa-secure/postgres`) with tight modes. **`arnold`** is not expected to own the vault tree.
- **Easiest first-time vault copy:** `rsync` from the operator machine **as `arnold`** into `/srv/hfa-secure/vault/`, then **one** ownership fix on Arnold: `sudo chown -R archive:archive /srv/hfa-secure/vault` (adjust group if your unit uses a non-default `HFA_ARCHIVE_UNIX_GROUP`). ACLs or `rsync` as `archive` are optional later optimizations.
- If later you want **`arnold` to write into the vault** without that `chown` pass, that is a **separate decision** (ACLs, a dedicated ingest script + `sudo`, or a group bit). It is optional and not required for a one-time cutover.

### Disk preflight (do this before any large dump or restore)

Arnold typically has **two** independent space problems:

| Filesystem                                                          | What lives there                                                        | Typical failure                                                                           |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **`/`** (VM root disk, often small, e.g. ~46 GiB)                   | Docker engine, **`/var/lib/docker`** image/layer storage, logs, journal | **`/` hits 100%** → `docker`, `apt`, or logging breaks even if `/srv/hfa-secure` is fine. |
| **`/srv/hfa-secure`** (encrypted LUKS LV, fixed size, e.g. ~98 GiB) | Vault, Postgres **PGDATA**, optional **`archive_seed.dump`**            | Not enough room for **vault + live cluster + full dump file** at the same time.           |

**Before** streaming a multi‑tens–to–100+ GiB dump, run:

```bash
ssh arnold@192.168.50.27 'df -h / /srv/hfa-secure; echo ---; sudo du -sh /srv/hfa-secure/vault /srv/hfa-secure/postgres 2>/dev/null; echo ---; sudo docker system df 2>/dev/null || true'
```

**Rules of thumb:**

- **`/`**: keep a few **gigabytes free** (e.g. ≥5 GiB `Avail`). If root is full, recover with **`journalctl --vacuum-time=3d`**, **`apt clean`**, and careful **`docker system prune`** (see Docker docs — `prune -af` removes unused images). Longer-term: **grow the VM disk** or move Docker data root to a larger volume.
- **`/srv/hfa-secure`**: if **approx_dump_size + vault + existing PGDATA + margin** exceeds **Avail** on that mount, **do not** write a full `.dump` next to PGDATA. Use **`make pipe-restore-seed-arnold`** in `archive-mcp` (streams **`pg_dump` → `pg_restore`** into `hfa-archive-postgres` with **no** giant file on the VM). You still need the **encrypted LV large enough for the restored live cluster + vault** after the dump file is gone — if not, **grow the sparse image / LUKS volume** before cutover.

- **After a full-disk episode:** if **`rm`** or writes on `/srv/hfa-secure` return **Input/output error**, treat the mount as suspect — check **`dmesg`**, ensure **`/`** is not stuck at **100%** (Docker/journal), then **stop** services using the mount, **unmount**, and **fsck** the underlying filesystem per your Unraid/Linux procedure before retrying.

- **Growing capacity:** see **[hfa-archive-expand-storage.md](./hfa-archive-expand-storage.md)** (LUKS file grow + optional VM root grow).

### Fast path (large dumps, e.g. 100+ GiB): local file → rsync → parallel `pg_restore`

If **`/srv/hfa-secure`** has room for **dump file + live PGDATA** (see **Disk preflight**), prefer **`archive-mcp`**:

```bash
cd /path/to/archive-mcp
make dump-seed-schema
# optional: PG_RESTORE_JOBS=8
make scp-restore-seed-arnold
```

This uses **`rsync`** (resumable) and **`pg_restore --jobs`** from a file on PGDATA — typically **much faster** than piping **`pg_dump` through SSH**.

### Shortest path: copy `archive_seed` dump onto the mount (one stream, no spare copy)

`hfa-archive-runtime-prepare` creates `/srv/hfa-secure/postgres` as **`700` and owned by `archive`**, so `scp arnold@…:/srv/hfa-secure/postgres/…` usually **fails**. Avoid landing the dump under `/home/arnold` first unless you must: stream once straight into the archive-owned directory.

**Prerequisites:** encrypted mount is up; Postgres data parent exists; `arnold` can run `sudo -u archive …` (NOPASSWD or interactive sudo) for the redirect below.

**Local dump (PG 17):** use `pg_dump` from the running Docker service, not an older host client (e.g. Homebrew `postgresql@14`).

```bash
cd /path/to/archive-mcp
make dump-seed-schema
# writes archive_seed.dump; override: make dump-schema DUMP_SCHEMA=other ARCHIVE_DUMP_OUT=other.dump
```

**Stream to Arnold** (one write on the encrypted volume; adjust host). For **large** dumps (~100+ GiB), prefer **`make dump-seed-schema-stream-arnold`** in `archive-mcp` — it logs `step 1/4`…`4/4` and pipes through **`pv`** (install with `brew install pv`) for bytes, rate, and optional ETA when `ARCHIVE_STREAM_BYTES` is set. The remote side uses **`sudo tee`** because PGDATA is owned by the container `postgres` uid, not the `archive` user.

**If the encrypted volume cannot fit dump + vault + PGDATA** (see **Disk preflight** above), use **pipe restore** instead — **no** `archive_seed.dump` on Arnold:

```bash
cd /path/to/archive-mcp
export PATH="/opt/homebrew/bin:$PATH"   # if pv is from Homebrew
export ARCHIVE_STREAM_BYTES=$((135 * 1024 * 1024 * 1024))   # optional ETA
make pipe-restore-seed-arnold
```

Then skip the `pg_restore` file path in the next subsection (restore already ran over the pipe).

After `make dump-seed-schema`, you can still pipe a local file once (then remove the local `.dump` if you do not want two copies on disk):

```bash
cd /path/to/archive-mcp
make dump-seed-schema
cat archive_seed.dump | ssh arnold@192.168.50.27 'sudo tee /srv/hfa-secure/postgres/archive_seed.dump > /dev/null'
```

**Fallback** if `sudo -u archive` is not available in one shot: `scp` the dump to `/home/arnold/archive_seed.dump`, then a **single** `sudo install -o archive -g archive -m 600 … /srv/hfa-secure/postgres/archive_seed.dump` (or `sudo mv` into place).

**Restore on Arnold (PG 17):** the systemd unit runs `pgvector/pgvector:pg17`. After DB bootstrap prerequisites, restore **from inside the container** so `pg_restore` matches the server major. The host mount `HFA_ARCHIVE_PGDATA` (`/srv/hfa-secure/postgres`) appears in the container as `/var/lib/postgresql/data`, so a dump placed next to the cluster on the host is visible inside:

```bash
# On Arnold — set PGPASSWORD from .env or use .pgpass inside exec as needed
sudo docker exec hfa-archive-postgres pg_restore \
  -U archive -d archive --clean --if-exists --no-owner --no-privileges \
  /var/lib/postgresql/data/archive_seed.dump
```

If `pg_restore` prompts for a password, pass it from the same `.env` (after `set -a; . /home/arnold/openclaw/.env`) via `docker exec -e PGPASSWORD="$HFA_ARCHIVE_PG_PASSWORD"` instead of embedding a literal password in docs or shell history.

### Arnold manual checks: do not hardcode `ARCHIVE_INDEX_DSN`

`HFA_ARCHIVE_PG_PASSWORD` on the VM may differ from the Makefile default. Copy-pasting `postgresql://archive:archive@127.0.0.1:5432/archive` in one-off checks can **pass while systemd is wrong** (or fail when the real password is correct).

**Rule:** for any SSH sanity check (`psql`, `python -c 'import archive_mcp…'`, etc.), **`set -a; . /home/arnold/openclaw/.env; set +a`** on the remote shell first, then use **`$ARCHIVE_INDEX_DSN`**, **`$ARCHIVE_INDEX_SCHEMA`**, **`$HFA_VAULT_PATH`**, **`$HFA_LIB_PATH`** as exported there.

Example — `archive_mcp.server` import as user `archive` (adjust host):

```bash
ssh arnold@192.168.50.27 'set -a; . /home/arnold/openclaw/.env; set +a; sudo -u archive env \
  HFA_VAULT_PATH="$HFA_VAULT_PATH" \
  HFA_LIB_PATH="$HFA_LIB_PATH" \
  ARCHIVE_INDEX_DSN="$ARCHIVE_INDEX_DSN" \
  ARCHIVE_INDEX_SCHEMA="$ARCHIVE_INDEX_SCHEMA" \
  /home/arnold/openclaw/venv/bin/python -c "import archive_mcp.server; print(\"server_import_ok\")"'
```

## Stage 4. Backup Migration

- run `hfa-backup-encrypt`
- verify encrypted artifact creation
- verify no plaintext copy is placed in the backup destination
- test `hfa-backup-restore` into a scratch path
- test encrypted upload to Google Drive and iCloud

## Stage 5. Public Mac Access

- provision the remote archive client token
- enable the public archive route on the gate
- request a scoped archive ticket (`POST …/api/archive/remote/ticket`) against the **prod ngrok gate base URL** (see recorded decisions above)
- perform a remote read-only call from the Mac (`POST …/api/archive/remote/call`)
- confirm admin and sensitive operations remain blocked

## Verification Checklist

### Transport

- public archive access uses HTTPS/TLS to the gate
- `archive-mcp` has no public listener
- gate-to-archive transport remains local-only

### Storage

- encrypted archive image exists on parity-backed storage
- encrypted mount is unreadable while locked
- canonical vault path resolves inside the encrypted mount

### Permissions

- `arnold` does not directly own the vault tree
- archive runtime identity owns archive data paths
- service-account token files remain `600`

### Backup

- only encrypted artifacts land in backup destinations
- Google Drive upload uses encrypted artifact only
- iCloud upload uses encrypted artifact only
- scratch restore succeeds and checksum matches

### Public Remote Client

- bearer token is separate from the gate internal token
- issued ticket is scoped to `mcp.archive.remote.read`
- blocked tools fail closed

## Negative Tests

- path traversal note reads return not found or denied
- unapproved archive admin calls do not execute
- plaintext vault paths are rejected by backup upload scripts
- cloud upload scripts reject non-encrypted artifacts
