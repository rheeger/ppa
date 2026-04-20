"""Backfill triage classifications from sidecar SQLite into vault frontmatter + Postgres.

Reads from `_artifacts/_classify_index_*.db` (built by the existing classify pipeline,
keyed by gmail_thread_id) and:

1. Writes `triage_classification`, `triage_confidence`, `triage_card_types`,
   `triage_classified_at`, `triage_classify_model` onto every email_thread .md card
   in the target vault (durable, replaces sidecar).
2. Materializes the `{schema}.card_classifications` projection table in the target
   Postgres schema with one row per email_thread card_uid.
3. For email_message cards, materializes a derived row using the parent thread's
   classification (joined via gmail_thread_id) so the semantic-linker JOIN is fast.

Usage:
    .venv/bin/python archive_scripts/phase6_backfill_classifications.py \\
        --classify-db _artifacts/_classify_index_1pct_v11.db \\
        --vault .slices/1pct \\
        --schema ppa_1pct \\
        [--dry-run]

Notes:
- Only writes frontmatter for cards whose gmail_thread_id is in the classify db.
- Idempotent: re-running overwrites with the latest values.
- Skips cards already at the latest classification + model (no-op).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from archive_vault.vault import read_note_file, write_card


def _load_classifications(db_path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    con = sqlite3.connect(str(db_path))
    cur = con.execute(
        """
        SELECT thread_id, category, confidence, card_types, classify_model,
               classify_prompt_version, classified_at
        FROM thread_classifications
        """
    )
    for tid, cat, conf, ct_json, model, prompt_ver, classified_at in cur.fetchall():
        try:
            card_types = json.loads(ct_json) if ct_json else []
        except json.JSONDecodeError:
            card_types = []
        rows[str(tid)] = {
            "classification": str(cat or "").strip(),
            "confidence": float(conf or 0.0),
            "card_types": card_types,
            "model": str(model or ""),
            "prompt_version": str(prompt_ver or ""),
            "classified_at": str(classified_at or ""),
        }
    con.close()
    return rows


def _walk_email_thread_cards(vault: Path):
    for rel in (vault / "EmailThreads").rglob("*.md"):
        yield rel


def _walk_email_message_cards(vault: Path):
    for rel in (vault / "Email").rglob("*.md"):
        yield rel


def _backfill_frontmatter(
    vault: Path, classifications: dict[str, dict[str, Any]], dry_run: bool
) -> tuple[int, int, int]:
    """Walk EmailThreads/, write triage_* fields onto each card whose gmail_thread_id
    has a classification. Returns (matched, written, skipped_already_current)."""
    matched = 0
    written = 0
    skipped = 0
    for rel_path in _walk_email_thread_cards(vault):
        try:
            note = read_note_file(rel_path, vault_root=vault)
        except Exception as exc:
            print(f"[backfill] skip unreadable {rel_path.relative_to(vault)}: {exc}", file=sys.stderr)
            continue
        fm = dict(note.frontmatter)
        tid = str(fm.get("gmail_thread_id") or "").strip()
        if not tid or tid not in classifications:
            continue
        matched += 1
        c = classifications[tid]
        existing_class = str(fm.get("triage_classification") or "")
        existing_model = str(fm.get("triage_classify_model") or "")
        if existing_class == c["classification"] and existing_model == c["model"]:
            skipped += 1
            continue
        fm["triage_classification"] = c["classification"]
        fm["triage_confidence"] = c["confidence"]
        fm["triage_card_types"] = c["card_types"]
        fm["triage_classified_at"] = c["classified_at"]
        fm["triage_classify_model"] = c["model"]
        if not dry_run:
            write_card(rel_path, fm, note.body, vault_root=vault)
        written += 1
    return matched, written, skipped


def _backfill_postgres(
    schema: str,
    dsn: str,
    classifications: dict[str, dict[str, Any]],
    dry_run: bool,
) -> tuple[int, int]:
    """Materialize {schema}.card_classifications from email_thread / email_message cards."""
    inserted_threads = 0
    inserted_messages = 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        # Ensure the table exists (mirrors schema_ddl.py).
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.card_classifications (
                card_uid TEXT PRIMARY KEY,
                classification TEXT NOT NULL,
                confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                card_types JSONB NOT NULL DEFAULT '[]'::jsonb,
                classified_at TIMESTAMPTZ,
                classify_model TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_card_classifications_class ON {schema}.card_classifications(classification)"
        )
        conn.commit()

        # Threads: one row per email_thread card with a known gmail_thread_id.
        # gmail_thread_id lives in the external_ids table, not on the email_threads projection.
        thread_rows = conn.execute(
            f"""
            SELECT c.uid AS card_uid, ei.external_id AS gmail_thread_id
            FROM {schema}.cards c
            JOIN {schema}.external_ids ei ON ei.card_uid = c.uid
            WHERE c.type = 'email_thread'
              AND ei.provider = 'gmail' AND ei.field_name = 'gmail_thread_id'
            """
        ).fetchall()
        for r in thread_rows:
            tid = str(r["gmail_thread_id"])
            c = classifications.get(tid)
            if c is None:
                continue
            if not dry_run:
                conn.execute(
                    f"""
                    INSERT INTO {schema}.card_classifications
                        (card_uid, classification, confidence, card_types, classified_at, classify_model)
                    VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (card_uid) DO UPDATE SET
                        classification = EXCLUDED.classification,
                        confidence = EXCLUDED.confidence,
                        card_types = EXCLUDED.card_types,
                        classified_at = EXCLUDED.classified_at,
                        classify_model = EXCLUDED.classify_model
                    """,
                    (
                        r["card_uid"],
                        c["classification"],
                        c["confidence"],
                        json.dumps(c["card_types"]),
                        c["classified_at"] or None,
                        c["model"],
                    ),
                )
            inserted_threads += 1

        # Messages: inherit classification from parent thread via gmail_thread_id (external_ids).
        msg_rows = conn.execute(
            f"""
            SELECT c.uid AS card_uid, ei.external_id AS gmail_thread_id
            FROM {schema}.cards c
            JOIN {schema}.external_ids ei ON ei.card_uid = c.uid
            WHERE c.type = 'email_message'
              AND ei.provider = 'gmail' AND ei.field_name = 'gmail_thread_id'
            """
        ).fetchall()
        for r in msg_rows:
            tid = str(r["gmail_thread_id"])
            c = classifications.get(tid)
            if c is None:
                continue
            if not dry_run:
                conn.execute(
                    f"""
                    INSERT INTO {schema}.card_classifications
                        (card_uid, classification, confidence, card_types, classified_at, classify_model)
                    VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (card_uid) DO UPDATE SET
                        classification = EXCLUDED.classification,
                        confidence = EXCLUDED.confidence,
                        card_types = EXCLUDED.card_types,
                        classified_at = EXCLUDED.classified_at,
                        classify_model = EXCLUDED.classify_model
                    """,
                    (
                        r["card_uid"],
                        c["classification"],
                        c["confidence"],
                        json.dumps(c["card_types"]),
                        c["classified_at"] or None,
                        c["model"],
                    ),
                )
            inserted_messages += 1

        conn.commit()
    return inserted_threads, inserted_messages


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--classify-db", required=True, type=Path)
    p.add_argument("--vault", required=True, type=Path)
    p.add_argument("--schema", required=True)
    p.add_argument("--dsn", default=os.environ.get("PPA_INDEX_DSN", ""))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-frontmatter", action="store_true",
                   help="Only write to Postgres; leave .md files untouched")
    args = p.parse_args()
    if not args.dsn:
        raise SystemExit("PPA_INDEX_DSN env or --dsn required")
    if not args.classify_db.exists():
        raise SystemExit(f"classify db not found: {args.classify_db}")
    if not args.vault.exists():
        raise SystemExit(f"vault not found: {args.vault}")

    print(f"[backfill] reading classifications from {args.classify_db}")
    classifications = _load_classifications(args.classify_db)
    cat_counts = Counter(c["classification"] for c in classifications.values())
    print(f"[backfill] {len(classifications)} thread classifications:")
    for cat, n in cat_counts.most_common():
        print(f"  {cat}: {n}")

    if not args.skip_frontmatter:
        print(f"[backfill] writing frontmatter into {args.vault}/EmailThreads/")
        matched, written, skipped = _backfill_frontmatter(args.vault, classifications, args.dry_run)
        print(f"[backfill] frontmatter: matched={matched} written={written} skipped_unchanged={skipped}")

    print(f"[backfill] materializing into {args.schema}.card_classifications")
    n_threads, n_messages = _backfill_postgres(args.schema, args.dsn, classifications, args.dry_run)
    print(f"[backfill] postgres: thread rows={n_threads} message rows={n_messages}")
    print(f"[backfill] done {'(DRY RUN)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
