#!/usr/bin/env python3
"""Archive doctor operations backed by the shared HFA library."""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from hfa.config import load_config
from hfa.identity import (load_identity_map, save_identity_map,
                          upsert_identity_map)
from hfa.identity_resolver import (is_same_person, load_nicknames,
                                   merge_into_existing, normalize_person_name)
from hfa.provenance import validate_provenance
from hfa.schema import PersonCard, validate_card_strict
from hfa.sync_state import load_sync_state, save_sync_state
from hfa.vault import (extract_wikilinks, iter_note_paths, iter_notes,
                       iter_parsed_notes, read_note)

WIKILINK_FIELDS = (
    "people",
    "orgs",
    "thread",
    "message",
    "messages",
    "attachments",
    "calendar_events",
    "source_messages",
    "source_threads",
    "meeting_transcripts",
)
HASH_FIELDS_BY_TYPE = {
    "email_thread": ("thread_body_sha",),
    "email_message": ("message_body_sha",),
    "email_attachment": ("attachment_metadata_sha",),
    "imessage_thread": ("thread_body_sha",),
    "calendar_event": ("event_body_sha",),
    "meeting_transcript": ("transcript_body_sha",),
    "media_asset": ("metadata_sha",),
}


def get_vault_path() -> str:
    return os.environ.get("PPA_PATH", os.path.join(os.path.expanduser("~"), "Archive", "vault"))


def _load_card_records(vault: Path) -> list[dict]:
    records: list[dict] = []
    for note in iter_parsed_notes(vault):
        try:
            card = validate_card_strict(note.frontmatter)
        except Exception:
            continue
        records.append(
            {
                "rel_path": note.rel_path,
                "card": card,
                "frontmatter": note.frontmatter,
                "body": note.body,
                "provenance": note.provenance,
            }
        )
    return records


def _load_people_records(vault: Path) -> list[dict]:
    records: list[dict] = []
    people_dir = vault / "People"
    if not people_dir.exists():
        return records
    for path in sorted(people_dir.glob("*.md")):
        try:
            frontmatter, body, provenance = read_note(vault, str(path.relative_to(vault)))
            card = validate_card_strict(frontmatter)
        except Exception:
            continue
        if not isinstance(card, PersonCard):
            continue
        records.append(
            {
                "rel_path": path.relative_to(vault),
                "card": card,
                "frontmatter": frontmatter,
                "body": body,
                "provenance": provenance,
            }
        )
    return records


def _person_identifiers(card: PersonCard) -> dict[str, object]:
    return {
        "summary": card.summary,
        "name": card.summary,
        "emails": card.emails,
        "phones": card.phones,
        "company": card.company,
        "title": card.title,
        "github": card.github,
        "linkedin": card.linkedin,
        "twitter": card.twitter,
        "instagram": card.instagram,
        "telegram": card.telegram,
        "discord": card.discord,
    }


def _field_count(frontmatter: dict) -> int:
    return sum(1 for value in frontmatter.values() if value not in ("", [], None, 0))


def _load_dedup_candidates(vault: Path) -> list[dict]:
    path = vault / "_meta" / "dedup-candidates.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            payload = []
    except (OSError, json.JSONDecodeError):
        payload = []
    return payload


def _write_dedup_candidates(vault: Path, payload: list[dict]) -> None:
    path = vault / "_meta" / "dedup-candidates.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _cleanup_deleted_wikilinks(vault: Path, removed_wikilinks: set[str]) -> None:
    if not removed_wikilinks:
        return
    cleaned = {key: value for key, value in load_identity_map(vault).items() if value not in removed_wikilinks}
    save_identity_map(vault, cleaned)


def _iter_frontmatter_wikilinks(frontmatter: dict) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for field_name in WIKILINK_FIELDS:
        value = frontmatter.get(field_name)
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if not isinstance(candidate, str):
                continue
            for slug in extract_wikilinks(candidate):
                if slug:
                    links.append((field_name, slug))
    return links


def _slug_set(vault: Path) -> set[str]:
    try:
        from archive_mcp.vault_cache import VaultScanCache

        return VaultScanCache.build_or_load(vault, tier=1, progress_every=0).all_stems()
    except Exception:
        return {rel_path.stem for rel_path in iter_note_paths(vault)}


def cmd_dedup_sweep(args):
    vault = Path(args.vault)
    config = load_config(vault)
    nicknames = load_nicknames(vault)
    people_records = _load_people_records(vault)
    dedup_candidates = _load_dedup_candidates(vault)

    index: dict[str, list[Path]] = defaultdict(list)
    records_by_path = {record["rel_path"]: record for record in people_records}
    for record in people_records:
        card: PersonCard = record["card"]
        for email in card.emails:
            index[f"email:{email.lower()}"].append(record["rel_path"])
        for phone in card.phones:
            index[f"phone:{phone}"].append(record["rel_path"])
        normalized_name = normalize_person_name(card.summary)
        if normalized_name:
            index[f"name:{normalized_name}"].append(record["rel_path"])

    seen_pairs: set[frozenset[Path]] = set()
    auto_merged = 0
    flagged = 0
    removed_wikilinks: set[str] = set()

    for bucket_paths in index.values():
        if len(bucket_paths) < 2:
            continue
        unique_paths = sorted(set(bucket_paths))
        for idx, left in enumerate(unique_paths):
            for right in unique_paths[idx + 1 :]:
                pair = frozenset((left, right))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                if left not in records_by_path or right not in records_by_path:
                    continue

                left_record = records_by_path[left]
                right_record = records_by_path[right]
                match, confidence, reasons = is_same_person(
                    left_record["card"].model_dump(mode="python"),
                    right_record["card"].model_dump(mode="python"),
                    nicknames,
                    config=config,
                )
                if not match and confidence < config.conflict_threshold:
                    continue

                if confidence >= config.merge_threshold and config.dedup_sweep_auto_merge:
                    primary, secondary = sorted(
                        [left_record, right_record],
                        key=lambda record: (-_field_count(record["frontmatter"]), str(record["rel_path"])),
                    )
                    primary_wikilink = f"[[{primary['rel_path'].stem}]]"
                    secondary_wikilink = f"[[{secondary['rel_path'].stem}]]"
                    merge_into_existing(
                        vault,
                        primary_wikilink,
                        secondary["card"].model_dump(mode="python"),
                        secondary["provenance"],
                    )
                    try:
                        (vault / secondary["rel_path"]).unlink()
                    except OSError:
                        pass
                    removed_wikilinks.add(secondary_wikilink)
                    updated_frontmatter, _, _ = read_note(vault, str(primary["rel_path"]))
                    updated_card = validate_card_strict(updated_frontmatter)
                    if isinstance(updated_card, PersonCard):
                        upsert_identity_map(vault, primary_wikilink, _person_identifiers(updated_card))
                    records_by_path.pop(secondary["rel_path"], None)
                    auto_merged += 1
                    continue

                dedup_candidates.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "incoming": left_record["frontmatter"],
                        "existing": f"[[{right_record['rel_path'].stem}]]",
                        "confidence": confidence,
                        "reasons": reasons,
                    }
                )
                flagged += 1

    _cleanup_deleted_wikilinks(vault, removed_wikilinks)
    _write_dedup_candidates(vault, dedup_candidates)
    print(f"dedup-sweep: auto-merged={auto_merged} flagged={flagged}")


def cmd_validate(args):
    vault = Path(args.vault)
    slugs = _slug_set(vault)
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_cards": 0,
        "valid": 0,
        "errors": [],
    }

    for note in iter_parsed_notes(vault):
        report["total_cards"] += 1
        try:
            card = validate_card_strict(note.frontmatter)
            errors = validate_provenance(card.model_dump(mode="python"), note.provenance)
            for field_name, slug in _iter_frontmatter_wikilinks(note.frontmatter):
                if slug not in slugs:
                    errors.append(f"Field '{field_name}' contains orphaned wikilink '[[{slug}]]'")
            if errors:
                raise ValueError("; ".join(errors))
            report["valid"] += 1
        except Exception as exc:
            report["errors"].append({"path": str(note.rel_path), "error": str(exc)})

    report_path = vault / "_meta" / "validation-report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"validate: valid={report['valid']} total={report['total_cards']} errors={len(report['errors'])}")


def cmd_stats(args):
    vault = Path(args.vault)
    slugs = _slug_set(vault)
    records = _load_card_records(vault)
    by_type: dict[str, int] = defaultdict(int)
    by_source: dict[str, int] = defaultdict(int)
    hash_coverage: dict[str, dict[str, int]] = defaultdict(lambda: {"with_hash": 0, "total": 0})
    provenance_valid = 0
    schema_valid = 0
    orphaned_links = 0

    for record in records:
        card = record["card"]
        schema_valid += 1
        by_type[card.type] += 1
        for source in card.source:
            by_source[source] += 1
        if not validate_provenance(card.model_dump(mode="python"), record["provenance"]):
            provenance_valid += 1
        for _, slug in _iter_frontmatter_wikilinks(record["frontmatter"]):
            if slug not in slugs:
                orphaned_links += 1
        hash_fields = HASH_FIELDS_BY_TYPE.get(card.type, ())
        for field_name in hash_fields:
            metric_key = f"{card.type}.{field_name}"
            hash_coverage[metric_key]["total"] += 1
            if str(record["frontmatter"].get(field_name, "")).strip():
                hash_coverage[metric_key]["with_hash"] += 1

    dedup_path = vault / "_meta" / "dedup-candidates.json"
    try:
        dedup_candidates = json.loads(dedup_path.read_text(encoding="utf-8"))
        pending = len(dedup_candidates) if isinstance(dedup_candidates, list) else 0
    except (OSError, json.JSONDecodeError):
        pending = 0

    sync_state = load_sync_state(vault)
    quick_update_by_source: dict[str, dict[str, int]] = {}
    quick_update_totals: dict[str, int] = defaultdict(int)
    for source_key, payload in sync_state.items():
        if not isinstance(payload, dict):
            continue
        skip_details = payload.get("skip_details", {})
        normalized: dict[str, int] = {}
        if isinstance(skip_details, dict):
            for key, value in skip_details.items():
                if str(key).startswith("skipped_"):
                    normalized[str(key)] = int(value or 0)
        for key, value in payload.items():
            if str(key).startswith("skipped_"):
                normalized[str(key)] = int(value or 0)
        if not normalized:
            continue
        quick_update_by_source[source_key] = dict(sorted(normalized.items()))
        for key, value in normalized.items():
            quick_update_totals[key] += int(value or 0)

    print(f"# Archive Doctor Stats — {datetime.now().isoformat()}")
    print(f"Total notes: {len(records)}")
    print("By type:")
    for card_type, count in sorted(by_type.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {card_type}: {count}")
    print("By source:")
    for source, count in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {source}: {count}")
    print("Quality:")
    print(f"  Cards with provenance: {provenance_valid}/{len(records)}")
    print(f"  Schema-valid cards: {schema_valid}/{len(records)}")
    print(f"  Dedup candidates pending: {pending}")
    print(f"  Orphaned wikilinks: {orphaned_links}")
    if hash_coverage:
        print("Hash coverage:")
        for metric_key, counts in sorted(hash_coverage.items()):
            print(f"  {metric_key}: {counts['with_hash']}/{counts['total']}")
    if quick_update_totals:
        print("Quick update totals:")
        for key, value in sorted(quick_update_totals.items()):
            print(f"  {key}: {value}")
    if quick_update_by_source:
        print("Quick update by source:")
        for source_key, values in sorted(quick_update_by_source.items()):
            rendered = ", ".join(f"{key}={value}" for key, value in sorted(values.items()))
            print(f"  {source_key}: {rendered}")


def cmd_purge_source(args):
    vault = Path(args.vault)
    source = args.source.strip()
    removed_wikilinks: set[str] = set()
    removed_files = 0
    for rel_path, _ in iter_notes(vault):
        frontmatter, _, _ = read_note(vault, str(rel_path))
        sources = frontmatter.get("source", [])
        if source not in sources:
            continue
        if rel_path.parts and rel_path.parts[0] == "People":
            removed_wikilinks.add(f"[[{rel_path.stem}]]")
        try:
            (vault / rel_path).unlink()
            removed_files += 1
        except OSError:
            pass

    _cleanup_deleted_wikilinks(vault, removed_wikilinks)
    state = {key: value for key, value in load_sync_state(vault).items() if not key.startswith(source)}
    save_sync_state(vault, state)
    print(f"purge-source: source={source} removed_files={removed_files} removed_wikilinks={len(removed_wikilinks)}")


def main():
    parser = argparse.ArgumentParser(description="Archive doctor")
    parser.add_argument("--vault", default=get_vault_path())
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("dedup-sweep").set_defaults(func=cmd_dedup_sweep)
    sub.add_parser("validate").set_defaults(func=cmd_validate)
    sub.add_parser("stats").set_defaults(func=cmd_stats)
    purge = sub.add_parser("purge-source")
    purge.add_argument("--source", required=True, help="Source value to remove from the vault")
    purge.set_defaults(func=cmd_purge_source)
    args = parser.parse_args()
    os.environ["PPA_PATH"] = args.vault
    args.func(args)


if __name__ == "__main__":
    main()
