# PPA Backup and Restore Runbook

> **Scope**: The HFA instance on Arnold (192.168.50.27).
> **Updated**: 2026-03-23

## What Gets Backed Up

The HFA PPA has two independent data stores that must both be recoverable:

| Store                          | Location on Arnold                         | Size                                 | Backup method                       |
| ------------------------------ | ------------------------------------------ | ------------------------------------ | ----------------------------------- |
| **Vault** (canonical markdown) | `/srv/hfa-secure/vault`                    | ~11G, ~1.85M files                   | tar + AES-256-CBC encryption        |
| **Index** (derived Postgres)   | `/srv/hfa-secure/postgres` (Docker volume) | ~82G (208G on disk with indexes/WAL) | `pg_dump -Fc -Z4` (~12G compressed) |

The vault is the canonical source of truth. The index is derived and can be rebuilt from the vault (though rebuilding takes hours and may OOM on Arnold -- prefer dump-restore).

## Backup Locations

| Artifact                | Path                                                               | Retention      |
| ----------------------- | ------------------------------------------------------------------ | -------------- |
| Vault encrypted archive | `/mnt/user/backups/hfa-encrypted/artifacts/<timestamp>/`           | 30 days        |
| Vault latest symlink    | `/mnt/user/backups/hfa-encrypted/latest/`                          | Always current |
| Postgres dump           | `/mnt/user/backups/hfa-encrypted/pg/archive_seed.<timestamp>.dump` | 7 days         |
| Postgres latest symlink | `/mnt/user/backups/hfa-encrypted/pg/archive_seed.latest.dump`      | Always current |

## Encryption

The vault backup is encrypted with AES-256-CBC using a passphrase stored at:

```
/home/arnold/.openclaw/credentials/hfa-archive-backup-passphrase
```

**CRITICAL**: This passphrase must also be stored in 1Password (or equivalent) for disaster recovery. If the passphrase is lost, the encrypted vault backup is unrecoverable.

The Postgres dump is NOT encrypted separately -- it lives on the Unraid share alongside the encrypted vault artifacts. For off-site backups, both should be included in the encrypted upload flow.

## Running Backups

### Vault backup (manual)

```bash
ssh arnold@192.168.50.27
set -a; . /home/arnold/openclaw/.env; set +a
cd /home/arnold/.openclaw/worktrees/hfa
sudo -E bash scripts/hfa-backup.sh
```

Or via Make (from the `hey-arnold-hfa` repo on Mac):

```bash
make hfa-backup-encrypt
```

The systemd service runs as `User=root` so it can read the `archive`-owned vault.

### Postgres backup (manual)

```bash
ssh arnold@192.168.50.27
bash /home/arnold/.openclaw/worktrees/hfa/scripts/ppa-pg-backup.sh
```

Expected runtime: 5-15 minutes. Output: ~12G compressed dump.

### Automatic backups

Daily backups are scheduled via the Unraid User Scripts plugin on Orthanc (192.168.50.11), not via Arnold's systemd timer. The Orthanc script SSHs to Arnold and runs both vault and Postgres backups in sequence.

- **Script**: `/boot/config/plugins/user.scripts/scripts/hfa-ppa-backup/script` on Orthanc
- **Schedule**: daily at 03:00 UTC (`0 3 * * *`)
- **Scheduling authority**: Orthanc cron (Arnold's `hfa-backup.timer` is disabled)

To check or manage the schedule, use the Unraid UI at `http://192.168.50.11/Settings/Userscripts` or SSH to Orthanc:

```bash
ssh root@192.168.50.11
crontab -l | grep ppa
cat /boot/config/plugins/user.scripts/schedule.json | grep -A5 ppa
```

## Restore Procedures

### Restore the vault

If the vault is lost or corrupted, restore from the encrypted backup:

```bash
ssh arnold@192.168.50.27

# Verify the encrypted artifact
set -a; . /home/arnold/openclaw/.env; set +a
cd /home/arnold/.openclaw/worktrees/hfa

# Restore to a temp directory first (never overwrite production directly)
sudo -E HFA_ARCHIVE_RESTORE_DIR=/tmp/hfa-vault-restore bash scripts/hfa-backup-restore.sh

# Verify the restore
ls /tmp/hfa-vault-restore/ | head -20
find /tmp/hfa-vault-restore/ -name '*.md' | wc -l  # should be ~1.85M

# If verified, swap into production (stop MCP first)
sudo systemctl stop hfa-archive-mcp.service
sudo rsync -a --delete /tmp/hfa-vault-restore/ /srv/hfa-secure/vault/
sudo chown -R archive:archive /srv/hfa-secure/vault/
sudo chmod -R 700 /srv/hfa-secure/vault/
sudo systemctl start hfa-archive-mcp.service

# Clean up temp restore
sudo rm -rf /tmp/hfa-vault-restore
```

### Restore the Postgres index

If the Postgres data is lost or corrupted:

```bash
ssh arnold@192.168.50.27

# Stop the existing container
sudo systemctl stop hfa-archive-postgres.service

# Clear the existing data (if corrupted)
# WARNING: This destroys the current database
sudo rm -rf /srv/hfa-secure/postgres/*

# Restart Postgres (creates fresh data directory)
sudo systemctl start hfa-archive-postgres.service

# Wait for Postgres to be ready
sleep 5
sudo docker exec hfa-archive-postgres pg_isready -U archive

# Restore from dump
sudo docker run --rm \
  -v /mnt/user/backups/hfa-encrypted/pg:/backups \
  --network container:hfa-archive-postgres \
  pgvector/pgvector:pg17 \
  pg_restore -U archive -d archive --clean --if-exists \
  /backups/archive_seed.latest.dump

# Verify
sudo docker exec hfa-archive-postgres psql -U archive -d archive \
  -c "SELECT count(*) AS cards FROM archive_seed.cards;"
# Expected: ~1,837,313

# Restart MCP
sudo systemctl restart hfa-archive-mcp.service
```

**Alternative**: If no Postgres dump exists but the vault is intact, you can rebuild the index from the vault. However, this takes hours and may OOM on Arnold. Prefer rebuilding locally and doing a fresh dump-restore:

```bash
# On Mac (local):
# 1. Start local Docker Postgres
# 2. Run rebuild-indexes locally
# 3. pg_dump to directory format
# 4. rsync + pg_restore to Arnold
```

### Full disaster recovery (both vault and index lost)

1. Restore the vault first (from encrypted backup)
2. Restore the Postgres index (from pg_dump, or rebuild from restored vault)
3. Verify with the health check: `bash scripts/ppa-health.sh`
4. Re-enable MCP: `sudo systemctl start hfa-archive-mcp.service`

## Retention Policy

| Artifact                 | Retention | Rationale                                                                                                         |
| ------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------- |
| Vault encrypted archives | 30 days   | One archive is ~11G encrypted. 30 days = ~330G on Unraid (9.9T free).                                             |
| Postgres dumps           | 7 days    | One dump is ~12G. 7 days = ~84G. Longer retention is unnecessary because the index can be rebuilt from the vault. |

## Verification Checklist

After any backup or restore:

- [ ] `archive_seed.cards` count matches expected (~1,837,313)
- [ ] Index count matches: 179 indexes
- [ ] MCP service responds
- [ ] Health check passes: `bash scripts/ppa-health.sh`
- [ ] Vault file count matches: ~1,847,087 files

## Known Issues

1. ~~Vault backup timer runs as arnold~~ **Fixed**: service runs as `User=root`.
2. ~~No Postgres backup timer~~ **Fixed**: Orthanc user script runs both vault and Postgres backups daily at 03:00 UTC.
3. ~~Backup passphrase not in 1Password~~ **Fixed**: stored in `Arnold-Passkey-Gate` vault as `HFA_ARCHIVE_BACKUP_PASSPHRASE`.
4. **Arnold root partition was at 100%** due to a leftover dump artifact. Cleaned up (now 50% used). Monitor root partition usage via the health check (`root_disk` metric).
