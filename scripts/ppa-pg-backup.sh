#!/bin/bash
set -euo pipefail

PG_CONTAINER="${PPA_PG_CONTAINER:-ppa-postgres}"
PG_USER="${PPA_PG_USER:-archive}"
PG_DB="${PPA_PG_DB:-archive}"
BACKUP_DIR="${PPA_PG_BACKUP_DIR:-/mnt/user/backups/ppa-encrypted/pg}"
RETENTION_DAYS="${PPA_PG_BACKUP_RETENTION_DAYS:-7}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DUMP_FILE="archive_seed.${TIMESTAMP}.dump"

sudo mkdir -p "$BACKUP_DIR"

echo "ppa-pg-backup: starting pg_dump -> ${BACKUP_DIR}/${DUMP_FILE}"
echo "ppa-pg-backup: streaming from container stdout (no temp file inside container)"

sudo docker exec "$PG_CONTAINER" \
  pg_dump -U "$PG_USER" -d "$PG_DB" -Fc -Z4 \
  | sudo tee "${BACKUP_DIR}/${DUMP_FILE}" > /dev/null

DUMP_SIZE=$(sudo du -sh "${BACKUP_DIR}/${DUMP_FILE}" | awk '{print $1}')
echo "ppa-pg-backup: timestamp=$TIMESTAMP file=${BACKUP_DIR}/${DUMP_FILE} size=${DUMP_SIZE}"

if [ "$RETENTION_DAYS" -gt 0 ]; then
  PRUNED=$(sudo find "$BACKUP_DIR" -name 'archive_seed.*.dump' -not -name 'archive_seed.latest.dump' -mtime +"$RETENTION_DAYS" -print -delete 2>/dev/null | wc -l)
  if [ "$PRUNED" -gt 0 ]; then
    echo "ppa-pg-backup: pruned $PRUNED dumps older than ${RETENTION_DAYS} days"
  fi
fi

LATEST_LINK="${BACKUP_DIR}/archive_seed.latest.dump"
sudo ln -sf "${BACKUP_DIR}/${DUMP_FILE}" "$LATEST_LINK"
echo "ppa-pg-backup: latest symlink -> ${DUMP_FILE}"
