"""Shared extraction quality flags (critical + heuristic + round-trip + duplicate).

Used by ``generate_extraction_quality_report`` and ``build_enrichment_benchmark``.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from archive_sync.extractors.field_metrics import CRITICAL_FIELDS
from archive_sync.extractors.field_validation import validate_field, validate_provenance_round_trip
from archive_sync.extractors.preprocessing import clean_email_body
from archive_vault.vault import _tier2_cache_path, iter_note_paths, read_note_file, read_note_frontmatter_file


def strict_field_flags(ct: str, fm: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    specs = CRITICAL_FIELDS.get(ct, [])
    for field_name, pred in specs:
        try:
            if not pred(fm):
                flags.append(f"critical_fail:{field_name}")
        except Exception:
            flags.append(f"critical_err:{field_name}")
    return flags


def heuristic_flags(ct: str, fm: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if ct == "meal_order":
        r = str(fm.get("restaurant") or "")
        rl = r.lower()
        if "http://" in rl or "https://" in rl or "<http" in rl:
            flags.append("heuristic:url_in_restaurant")
        if "bank statement" in rl or ("learn more" in rl and len(r) > 60):
            flags.append("heuristic:footer_noise_restaurant")
        items = fm.get("items")
        if not isinstance(items, list) or len(items) == 0:
            flags.append("heuristic:empty_items")
    if ct == "ride":
        pl = str(fm.get("pickup_location") or "").strip()
        dl = str(fm.get("dropoff_location") or "").strip()
        if pl.lower() in ("location:", "pickup", "") or len(pl) < 4:
            flags.append("heuristic:weak_pickup")
        if dl.lower() in ("location:", "dropoff", "") or len(dl) < 4:
            flags.append("heuristic:weak_dropoff")
    if ct == "flight":
        for k in ("origin_airport", "destination_airport"):
            v = str(fm.get(k) or "").strip()
            if v and validate_field("flight", k, v) is None:
                flags.append(f"heuristic:non_iata_{k}")
    if ct == "accommodation":
        if not str(fm.get("check_in") or "").strip():
            flags.append("heuristic:missing_check_in")
        if not str(fm.get("check_out") or "").strip():
            flags.append("heuristic:missing_check_out")
    if ct == "grocery_order":
        items = fm.get("items")
        if not isinstance(items, list) or len(items) == 0:
            flags.append("heuristic:empty_items")
    return flags


def all_flags(ct: str, fm: dict[str, Any]) -> list[str]:
    return strict_field_flags(ct, fm) + heuristic_flags(ct, fm)


def wikilink_uid(source_email: str) -> str:
    m = re.search(r"\[\[([^\]]+)\]\]", (source_email or "").strip())
    return m.group(1).strip() if m else ""


def source_uids_needed(by_type: dict[str, list[tuple[Path, dict[str, Any]]]]) -> set[str]:
    need: set[str] = set()
    for rows in by_type.values():
        for _path, fm in rows:
            uid = wikilink_uid(str(fm.get("source_email") or ""))
            if uid:
                need.add(uid)
    return need


def email_uid_index(vault: Path, only_uids: set[str]) -> dict[str, Path]:
    """Map UIDs to paths for email notes.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, reads frontmatter from SQLite instead of
    walking the filesystem.
    """

    idx: dict[str, Path] = {}
    if not only_uids:
        return idx
    vault = Path(vault)

    from archive_cli.ppa_engine import ppa_engine

    if ppa_engine() == "rust":
        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.frontmatter_dicts_from_cache(
                    str(cache_path), types=["email_message"], prefix="Email/",
                )
                for row in rows:
                    fm = row["frontmatter"]
                    uid = fm.get("uid")
                    if not isinstance(uid, str) or not uid.strip():
                        continue
                    uid = uid.strip()
                    if uid in only_uids:
                        idx[uid] = vault / row["rel_path"]
                return idx
            except Exception:
                pass

    remaining = set(only_uids)
    for rel_path in iter_note_paths(vault):
        if not rel_path.parts or rel_path.parts[0] != "Email":
            continue
        path = vault / rel_path
        try:
            fmrec = read_note_frontmatter_file(path, vault_root=vault)
        except OSError:
            continue
        uid = fmrec.frontmatter.get("uid")
        if not isinstance(uid, str) or not uid.strip():
            continue
        uid = uid.strip()
        if uid not in remaining:
            continue
        idx[uid] = path
        remaining.discard(uid)
        if not remaining:
            break
    return idx


def round_trip_flags(
    ct: str, fm: dict[str, Any], vault: Path, uid_to_path: dict[str, Path]
) -> list[str]:
    uid = wikilink_uid(str(fm.get("source_email") or ""))
    if not uid:
        return []
    path = uid_to_path.get(uid)
    if not path:
        return []
    try:
        rec = read_note_file(path, vault_root=vault)
        body = rec.body
    except OSError:
        return []
    clean = clean_email_body(body)
    warnings = validate_provenance_round_trip(dict(fm), clean, ct)
    return [
        f"heuristic:round_trip_fail:{w.split('.', 1)[1].split(':', 1)[0]}"
        for w in warnings
        if "." in w
    ]


def dedup_key(ct: str, fm: dict[str, Any]) -> tuple[Any, ...]:
    if ct == "meal_order":
        try:
            tot = float(fm.get("total") or 0)
        except (TypeError, ValueError):
            tot = 0.0
        return (
            ct,
            str(fm.get("restaurant") or "").strip().lower(),
            tot,
            str(fm.get("created") or "")[:10],
        )
    if ct == "ride":
        try:
            fare = float(fm.get("fare") or 0)
        except (TypeError, ValueError):
            fare = 0.0
        return (ct, str(fm.get("pickup_at") or ""), fare)
    if ct == "flight":
        return (ct, str(fm.get("confirmation_code") or "").strip().upper())
    if ct == "shipment":
        return (ct, str(fm.get("tracking_number") or "").strip())
    if ct == "accommodation":
        return (
            ct,
            str(fm.get("confirmation_code") or "").strip(),
            str(fm.get("check_in") or "")[:10],
        )
    if ct == "car_rental":
        return (ct, str(fm.get("confirmation_code") or "").strip())
    if ct == "grocery_order":
        try:
            tot = float(fm.get("total") or 0)
        except (TypeError, ValueError):
            tot = 0.0
        return (
            ct,
            str(fm.get("store") or "").strip().lower(),
            tot,
            str(fm.get("created") or "")[:10],
        )
    return (ct, str(fm.get("uid") or ""))


def duplicate_uids(by_type: dict[str, list[tuple[Path, dict[str, Any]]]]) -> set[str]:
    suspicious: set[str] = set()
    for ct, rows in by_type.items():
        counts: Counter[tuple[Any, ...]] = Counter()
        for _path, fm in rows:
            counts[dedup_key(ct, fm)] += 1
        for _path, fm in rows:
            uid = str(fm.get("uid") or "")
            if uid and counts[dedup_key(ct, fm)] > 1:
                suspicious.add(uid)
    return suspicious


def card_quality_flags(
    ct: str,
    fm: dict[str, Any],
    *,
    vault: Path,
    uid_to_path: dict[str, Path],
    dup_uids: set[str],
) -> list[str]:
    """Same flag union as the markdown quality report (strict + heuristic + round-trip + duplicate)."""

    fl = all_flags(ct, fm)
    fl.extend(round_trip_flags(ct, fm, vault, uid_to_path))
    uid = str(fm.get("uid") or "")
    if uid and uid in dup_uids:
        fl.append("heuristic:duplicate_suspect")
    return fl


def is_ground_truth_eligible(flags: list[str]) -> bool:
    """Benchmark positives: no critical or heuristic flags (same bar as plan Step 8)."""

    for f in flags:
        if f.startswith("critical_fail:") or f.startswith("critical_err:") or f.startswith("heuristic:"):
            return False
    return True


def load_staging_cards(root: Path) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
    by_type: dict[str, list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    for path in sorted(root.rglob("*.md")):
        if path.name.startswith("_"):
            continue
        try:
            rec = read_note_file(path)
        except OSError:
            continue
        fm = rec.frontmatter
        ct = str(fm.get("type") or "").strip() or "unknown"
        if ct == "email_message":
            continue
        by_type[ct].append((path, fm))
    return by_type
