# Expand Arnold Archive Storage (256 GiB target)

## What you are sizing (two different disks)

| Layer                       | What it is                                                                               | Holds                                                                         | Production archive DB?                          |
| --------------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ----------------------------------------------- |
| **Encrypted archive mount** | LUKS + ext4 on `hfa-vault.img` (sparse file under `/mnt/user/archive-secure/` on Unraid) | **`/srv/hfa-secure/vault`**, **`/srv/hfa-secure/postgres`** (Postgres PGDATA) | **Yes** — vault + index live here               |
| **VM root disk**            | Ubuntu LVM on the VM vdisk (e.g. `/dev/mapper/ubuntu--vg-ubuntu--lv`)                    | OS, **`/var/lib/docker`**, logs, journals                                     | **No** — only Docker engine + images + metadata |

Going forward, **the canonical vault and the Postgres cluster for `ppa` both live on the encrypted mount**, not on `/`.

- **256 GiB (or larger)** is the right class of target for **`hfa-vault.img`** / the LUKS filesystem so you can fit **vault + full `archive_seed` cluster + WAL/headroom** without fighting the ~100 GiB ceiling you hit earlier.
- The **VM vdisk** must be large enough that **`/`** never sits at **100%** (Docker layers, logs). That is a **separate** resize (often **64–128 GiB** is plenty for OS+Docker if PGDATA stays on `/srv/hfa-secure`). You can still grow the VM disk to **256 GiB** if you want one simple number for the Unraid VM, but it is **not** what holds the encrypted archive data.

## New installs

`make hfa-archive-provision-storage` (from `hey-arnold`) passes **`HFA_ARCHIVE_IMAGE_SIZE`** (Makefile default **256G** unless overridden). That only applies when **`hfa-vault.img` does not exist yet**.

## Grow an existing `hfa-vault.img` (LUKS + ext4)

**Downtime required.** Take a **backup** or accept rollback risk before resizing.

1. **Space on Unraid** — the share backing `/mnt/user/archive-secure` must have **free array capacity** for the larger sparse file.

2. **On Arnold (as root)** — stop consumers, unmount, close LUKS, grow file, reopen, resize crypto + filesystem, remount:

```bash
# Stop services that use the mount (adjust to your units)
sudo systemctl stop hfa-ppa.service hfa-archive-postgres.service || true
sudo umount /srv/hfa-secure

# Note active loop device for the image (adjust path if .env differs)
IMG=/mnt/user/archive-secure/hfa-vault.img
MAP=hfa-archive-vault
LOOP=$(sudo losetup -j "$IMG" | head -1 | cut -d: -f1)
sudo cryptsetup close "$MAP" || true
[ -n "$LOOP" ] && sudo losetup -d "$LOOP"

# Grow sparse file (example: absolute size 256G)
sudo truncate -s 256G "$IMG"

# Reattach and unlock (use your normal unlock path / key material)
sudo losetup --find --show "$IMG"
# then: sudo cryptsetup open /dev/loopX "$MAP" --key-file ...   # match your ops

sudo cryptsetup resize "$MAP"
sudo resize2fs "/dev/mapper/$MAP"

sudo mount "/dev/mapper/$MAP" /srv/hfa-secure
df -h /srv/hfa-secure
```

3. **Reconcile ownership** if needed (`vault` / `postgres` dirs under mount), then start **`hfa-archive-postgres`**, then **`hfa-ppa`**.

Exact **`losetup` / `cryptsetup open`** steps should match how **`hfa-archive-unlock.sh`** attaches the image on your host (loop device vs persistent mapping). When in doubt, use the same sequence as a clean boot after **`hfa-archive-unlock`** / **`hfa-archive-mount`**, inserting **truncate → cryptsetup resize → resize2fs** after the backing file is larger and before final **`mount`**.

## Grow the VM root disk (Ubuntu `/`)

If **`df -h /`** shows **`/`** full or tight:

1. Increase the **VM virtual disk** size in **Unraid VM manager** (or your hypervisor).
2. Inside the guest: grow the partition (if needed), **`pvresize`**, **`lvextend`**, **`resize2fs`** (or `xfs_growfs`) on the root LV.

Use distribution-specific guides; do not shrink ext4 root casually.

## Verification

```bash
df -h / /srv/hfa-secure
sudo du -sh /srv/hfa-secure/vault /srv/hfa-secure/postgres
```
