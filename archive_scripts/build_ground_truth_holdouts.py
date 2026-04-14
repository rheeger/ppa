#!/usr/bin/env python3
"""Build ground-truth JSON from a vault slice by running extractors (bootstrap).

Usage (from repo root with venv):
  PPA_PATH=.slices/10pct .venv/bin/python scripts/build_ground_truth_holdouts.py \\
    --vault .slices/10pct --provider doordash --positives 22 --negatives 8

Writes archive_sync/extractors/specs/<provider>-ground-truth.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from archive_sync.extractors.preprocessing import clean_email_body
from archive_sync.extractors.registry import build_default_registry
from archive_vault.vault import iter_email_message_notes

GT_FIELDS: dict[str, tuple[str, ...]] = {
    "meal_order": ("restaurant", "service", "total", "subtotal"),
    "accommodation": ("property_name", "booking_source", "confirmation_code", "total_cost"),
    "ride": ("service", "pickup_location", "dropoff_location", "fare"),
    "flight": ("airline", "confirmation_code", "origin_airport", "destination_airport", "fare_amount"),
    "grocery_order": ("service", "store", "total"),
    "shipment": ("carrier", "tracking_number"),
    "car_rental": ("company", "confirmation_code", "pickup_at", "total_cost"),
}

PROVIDER_DOMAINS: dict[str, str] = {
    "doordash": "doordash.com",
    "airbnb": "airbnb.com",
    "uber_eats": "uber.com",
    "uber_rides": "uber.com",
    "united": "united.com",
    "lyft": "lyft",
    "instacart": "instacart",
    "shipping": "ups.com",
    "rental_cars": "nationalcar.com",
}


def _extract(ext, note):
    fm = note.frontmatter
    uid = str(fm.get("uid") or "")
    body = clean_email_body(note.body)
    return uid, ext.extract(fm, body, uid, str(note.rel_path), raw_body=note.body)


def _gt_fields(card: dict, card_type: str) -> dict:
    keys = GT_FIELDS.get(card_type, ())
    out = {}
    for k in keys:
        v = card.get(k)
        if v in (None, "", [], 0, 0.0):
            continue
        out[k] = v
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vault", type=Path, required=True)
    ap.add_argument(
        "--provider",
        choices=sorted(PROVIDER_DOMAINS.keys()),
        required=True,
    )
    ap.add_argument("--positives", type=int, default=22)
    ap.add_argument("--negatives", type=int, default=8)
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output path (default: archive_sync/extractors/specs/<provider>-ground-truth.json)",
    )
    args = ap.parse_args()

    vault = args.vault.resolve()
    ext_id = args.provider
    reg = build_default_registry()
    ext = next(e for e in reg.all_extractors() if e.extractor_id == ext_id)

    domain = PROVIDER_DOMAINS[args.provider]
    rows: list[tuple[object, str]] = []
    for note in iter_email_message_notes(vault):
        fm = note.frontmatter
        if fm.get("type") != "email_message":
            continue
        from_e = str(fm.get("from_email") or "").lower()
        subj = str(fm.get("subject") or "")
        if domain not in from_e:
            continue
        if not ext.matches(from_e, subj):
            continue
        rows.append((note, subj[:120]))

    pos: list[tuple[str, dict]] = []
    neg: list[tuple[str, str]] = []
    for note, subj in rows:
        uid, results = _extract(ext, note)
        if results:
            card = results[0].card.model_dump(mode="python")
            ctype = str(card.get("type") or ext.output_card_type)
            fields = _gt_fields(card, ctype)
            pos.append((uid, {"type": ctype, "fields": fields}))
        else:
            neg.append((uid, subj))

    holdout: list[dict] = []
    for uid, ec in pos[: args.positives]:
        holdout.append({"uid": uid, "expected_cards": [ec]})
    for uid, _subj in neg[: args.negatives]:
        holdout.append({"uid": uid, "expected_cards": []})

    out_path = args.out
    if out_path is None:
        root = Path(__file__).resolve().parents[1]
        out_path = root / "archive_sync/extractors/specs" / f"{args.provider}-ground-truth.json"

    payload = {
        "provider": args.provider.capitalize(),
        "extractor_id": ext_id,
        "vault_note": f"Holdouts bootstrapped from extractor output on {vault.name}; re-verify manually.",
        "holdout_emails": holdout,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} ({len(holdout)} holdouts: {min(len(pos), args.positives)} pos, {min(len(neg), args.negatives)} neg)")


if __name__ == "__main__":
    main()
