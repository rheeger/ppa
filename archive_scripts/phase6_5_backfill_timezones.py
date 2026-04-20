"""Phase 6.5 Step 16.3 — backfill offset/naive timestamps to UTC-Z (idempotent)."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from archive_sync.adapters.datetime_canon import (AUDITED_TIMESTAMP_FIELDS,
                                                  classify_timestamp,
                                                  to_utc_z_iso)
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, write_card

log = logging.getLogger("phase6_5.backfill_timezones")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--vault", type=Path, required=True)
    p.add_argument("--audit-json", type=Path, required=True, help="report-*.json from Step 16.1")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    audit = json.loads(Path(args.audit_json).read_text(encoding="utf-8"))
    types_to_visit = {
        row["card_type"]
        for row in audit
        if row.get("category") in ("offset", "naive") and int(row.get("count") or 0) > 0
    }
    vault = Path(args.vault).resolve()

    from archive_cli.vault_cache import VaultScanCache

    cache = VaultScanCache.build_or_load(vault, tier=1)
    rewritten = 0
    examined = 0

    for rel_path, fm in cache.all_frontmatters():
        ct = str(fm.get("type") or "")
        if ct not in types_to_visit or ct not in AUDITED_TIMESTAMP_FIELDS:
            continue
        examined += 1
        fm_mut = dict(fm)
        changed_fields: list[str] = []
        for field in AUDITED_TIMESTAMP_FIELDS[ct]:
            v = fm_mut.get(field)
            if v is None:
                continue
            sval = v if isinstance(v, str) else str(v)
            category, _ = classify_timestamp(sval)
            if category in ("offset", "naive"):
                new_v = to_utc_z_iso(sval)
                if new_v and new_v != sval:
                    fm_mut[field] = new_v
                    changed_fields.append(field)
        if not changed_fields:
            continue
        rewritten += 1
        if args.dry_run:
            continue
        note_fm, body, existing_prov = read_note(vault, rel_path)
        for f in changed_fields:
            note_fm[f] = fm_mut[f]
        card = validate_card_strict(note_fm)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        incoming = {
            f: ProvenanceEntry(
                source="phase6_5_step16_backfill",
                date=today,
                method="deterministic",
                model="",
                input_hash="",
            )
            for f in changed_fields
        }
        prov = merge_provenance(existing_prov, incoming)
        write_card(vault, rel_path, card, body, prov)

    log.info("examined_cards=%d rewritten=%s dry_run=%s", examined, rewritten, args.dry_run)


if __name__ == "__main__":
    main()
