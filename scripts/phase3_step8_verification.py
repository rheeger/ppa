#!/usr/bin/env python3
"""Phase 3 Step 8c–8e: factual trace samples, dedup suspects, template-era histogram.

Writes:
  _artifacts/_staging/factual-trace-notes.md
  _artifacts/_staging/dedup-check-notes.md
  _artifacts/_staging/template-era-scan.md

Run from repo root: ``.venv/bin/python scripts/phase3_step8_verification.py``"""
from __future__ import annotations

import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(_ROOT / "scripts"))

import generate_extraction_quality_report as gq
from archive_sync.extractors.field_validation import \
    validate_provenance_round_trip
from archive_sync.extractors.preprocessing import clean_email_body
from hfa.vault import read_note_file

STAGING = Path("_artifacts/_staging")
VAULT = Path("/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127")
SAMPLES_PER_TYPE = 5
SEED = 42


def _year_from_path(p: Path) -> str:
    for part in p.parts:
        if re.match(r"^\d{4}-\d{2}$", part):
            return part[:4]
    return "unknown"


def _fmt_body(fm: dict[str, Any], ct: str) -> str:
    """Short human-readable field dump for trace."""
    skip = {"uid", "type", "created", "updated", "source_email", "extraction_confidence"}
    lines = []
    for k in sorted(fm.keys()):
        if k in skip:
            continue
        v = fm[k]
        s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
        if len(s) > 200:
            s = s[:197] + "..."
        lines.append(f"  - `{k}`: {s}")
    return "\n".join(lines)


def trace_factual(
    by_type: dict[str, list[tuple[Path, dict[str, Any]]]],
    uid_to_path: dict[str, Path],
) -> str:
    random.seed(SEED)
    lines: list[str] = [
        "# Factual trace — Phase 3 full-seed staging",
        "",
        "Automated spot-check: for each sampled card, `validate_provenance_round_trip` against "
        "`clean_email_body()` of the linked `source_email`. Empty warning list ⇒ values "
        "the round-trip validator accepts as present in the source.",
        "",
    ]
    for ct in sorted(by_type.keys()):
        rows = by_type[ct]
        if not rows:
            continue
        pick = rows[:]
        random.shuffle(pick)
        pick = pick[:SAMPLES_PER_TYPE]
        lines.append(f"## `{ct}`")
        lines.append("")
        for path, fm in pick:
            uid = str(fm.get("uid") or path.stem)
            src = gq._wikilink_uid(str(fm.get("source_email") or ""))
            if not src:
                lines.append(f"### {uid}")
                lines.append("- **No source_email wikilink — skip**")
                lines.append("")
                continue
            ep = uid_to_path.get(src)
            if not ep:
                lines.append(f"### {uid}")
                lines.append(f"- **Source email `{src}` not found in vault index**")
                lines.append("")
                continue
            try:
                rec = read_note_file(ep, vault_root=VAULT)
                body = clean_email_body(rec.body or "")
            except OSError as exc:
                lines.append(f"### {uid}")
                lines.append(f"- **Read error:** {exc}")
                lines.append("")
                continue
            warns = validate_provenance_round_trip(dict(fm), body, ct)
            lines.append(f"### {uid}")
            lines.append(f"- **Source:** `{src}` ({ep.relative_to(VAULT)})")
            lines.append(f"- **Staging file:** `{path}`")
            lines.append("")
            lines.append(_fmt_body(dict(fm), ct))
            lines.append("")
            if warns:
                lines.append("- **Round-trip warnings:**")
                for w in warns[:12]:
                    lines.append(f"  - {w}")
                if len(warns) > 12:
                    lines.append(f"  - … ({len(warns) - 12} more)")
            else:
                lines.append("- **Round-trip:** no warnings (sample fields appear in cleaned source).")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def dedup_notes(by_type: dict[str, list[tuple[Path, dict[str, Any]]]]) -> str:
    lines = [
        "# Deduplication check — Phase 3 full-seed staging",
        "",
        "Grouping uses the same keys as `_dedup_key` in `generate_extraction_quality_report.py`. "
        "Multiple cards per key are **duplicate suspects** (may be legitimate if two emails "
        "describe the same order).",
        "",
    ]
    total = 0
    suspects = 0
    for ct in sorted(by_type.keys()):
        rows = by_type[ct]
        buckets: dict[tuple[Any, ...], list[str]] = defaultdict(list)
        for path, fm in rows:
            k = gq._dedup_key(ct, fm)
            buckets[k].append(str(fm.get("uid") or path.stem))
        dup_groups = {k: uids for k, uids in buckets.items() if len(uids) > 1}
        n = sum(len(uids) for uids in dup_groups.values())
        total += len(rows)
        suspects += n
        lines.append(f"## `{ct}`")
        lines.append("")
        lines.append(f"- Cards: {len(rows)}")
        lines.append(f"- Duplicate-key groups: {len(dup_groups)} (covering {n} card UIDs)")
        lines.append("")
        for i, (key, uids) in enumerate(sorted(dup_groups.items(), key=lambda x: -len(x[1]))[:25]):
            lines.append(f"### Group {i + 1}: `{key}` ({len(uids)} cards)")
            for u in sorted(uids)[:20]:
                lines.append(f"- `{u}`")
            if len(uids) > 20:
                lines.append(f"- … ({len(uids) - 20} more)")
            lines.append("")
        if not dup_groups:
            lines.append("*(no duplicate keys for this type)*")
            lines.append("")
    lines.insert(4, f"**Overall duplicate-suspect card count:** {suspects} / {total} " f"({100.0 * suspects / max(1, total):.1f}%)")
    lines.insert(5, "")
    return "\n".join(lines)


def era_scan(by_type: dict[str, list[tuple[Path, dict[str, Any]]]]) -> str:
    lines = [
        "# Template-era scan — cards by calendar year (from staging path `YYYY-MM` segment)",
        "",
        "If a year has very few cards while neighbors are high, consider an uncovered template era.",
        "",
    ]
    for ct in sorted(by_type.keys()):
        rows = by_type[ct]
        years = Counter()
        for path, _fm in rows:
            years[_year_from_path(path)] += 1
        lines.append(f"## `{ct}`")
        lines.append("")
        lines.append("| Year | Count |")
        lines.append("|------|------:|")
        for y, c in sorted(years.items()):
            lines.append(f"| {y} | {c} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    root = STAGING.resolve()
    by_type = gq.load_staging_cards(root)
    need = gq._source_uids_needed(by_type)
    uid_to_path = gq._email_uid_index(VAULT, need)

    (root / "factual-trace-notes.md").write_text(trace_factual(by_type, uid_to_path), encoding="utf-8")
    (root / "dedup-check-notes.md").write_text(dedup_notes(by_type), encoding="utf-8")
    (root / "template-era-scan.md").write_text(era_scan(by_type), encoding="utf-8")
    missing = need - set(uid_to_path.keys())
    print(f"Wrote notes under {root}")
    print(f"Source UIDs resolved: {len(uid_to_path)} / {len(need)} needed")
    if missing:
        print(f"WARNING: {len(missing)} source UIDs not found in vault (sample: {list(missing)[:5]})")


if __name__ == "__main__":
    main()
