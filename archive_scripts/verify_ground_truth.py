#!/usr/bin/env python3
"""Verify extractor output against ground truth holdout annotations.

Usage:
  .venv/bin/python scripts/verify_ground_truth.py \\
    --ground-truth archive_sync/extractors/specs/doordash-ground-truth.json \\
    --vault-path /path/to/vault
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from archive_sync.extractors.preprocessing import clean_email_body
from archive_sync.extractors.registry import build_default_registry
from archive_vault.vault import iter_email_message_notes, read_note_by_uid


def _build_uid_index(vault: str) -> dict[str, Any]:
    """One pass over Email/ notes — O(1) lookup per holdout instead of full vault scan each time."""
    idx: dict[str, Any] = {}
    for note in iter_email_message_notes(vault):
        uid = str(note.frontmatter.get("uid") or "")
        if uid:
            idx[uid] = note
    return idx


def _norm_text(s: Any) -> str:
    if s is None:
        return ""
    t = str(s).strip().lower()
    return re.sub(r"\s+", " ", t)


def _money_close(a: Any, b: Any, tol: float = 0.01) -> bool:
    try:
        x = float(a)
        y = float(b)
    except (TypeError, ValueError):
        return _norm_text(a) == _norm_text(b)
    return abs(x - y) <= tol


def _field_match(expected: Any, actual: Any) -> bool:
    if isinstance(expected, (int, float)) or isinstance(actual, (int, float)):
        return _money_close(expected, actual)
    if isinstance(expected, list) and isinstance(actual, list):
        if not expected:
            return not actual
        for exp in expected:
            if isinstance(exp, dict) and "name" in exp:
                en = _norm_text(exp.get("name"))
                if not any(
                    isinstance(g, dict) and _norm_text(g.get("name")) == en for g in actual
                ):
                    return False
            elif exp not in actual:
                return False
        return True
    return _norm_text(expected) == _norm_text(actual)


def _extract_for_email(
    extractor_id: str,
    uid: str,
    vault: str,
    *,
    uid_index: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    reg = build_default_registry()
    ext = None
    for e in reg.all_extractors():
        if e.extractor_id == extractor_id:
            ext = e
            break
    if ext is None:
        raise SystemExit(f"Unknown extractor_id {extractor_id!r}")
    if uid_index is not None and uid in uid_index:
        note = uid_index[uid]
        fm = note.frontmatter
        raw = note.body
        body = clean_email_body(raw)
        results = ext.extract(fm, body, uid, str(note.rel_path), raw_body=raw)
        return [er.card.model_dump(mode="python") for er in results]
    got = read_note_by_uid(vault, uid)
    if not got:
        return []
    _rp, fm, body, _prov = got
    raw = body
    body = clean_email_body(body)
    results = ext.extract(fm, body, uid, str(got[0]), raw_body=raw)
    return [er.card.model_dump(mode="python") for er in results]


def run_verification(ground_truth_path: Path, vault_path: str) -> str:
    data = json.loads(ground_truth_path.read_text(encoding="utf-8"))
    extractor_id = str(data.get("extractor_id") or "")
    holdout = data.get("holdout_emails") or []
    if not extractor_id:
        raise SystemExit("ground truth missing extractor_id")

    uid_index = _build_uid_index(vault_path)

    lines: list[str] = []
    lines.append(f"# Ground Truth Verification: {extractor_id}")
    lines.append("")

    emission_ok = 0
    emission_total = len(holdout)

    prec_hit: dict[str, int] = {}
    prec_pop: dict[str, int] = {}
    rec_hit: dict[str, int] = {}
    rec_exp: dict[str, int] = {}

    for entry in holdout:
        uid = str(entry.get("uid") or "")
        expected_cards = entry.get("expected_cards") or []
        actual = _extract_for_email(extractor_id, uid, vault_path, uid_index=uid_index)
        exp_empty = not expected_cards
        act_empty = not actual
        if exp_empty == act_empty:
            emission_ok += 1
        if exp_empty:
            continue

        exp = expected_cards[0]
        exp_fields: dict[str, Any] = dict(exp.get("fields") or {})
        exp_type = str(exp.get("type") or "")
        act = actual[0] if actual else None
        if not act or str(act.get("type")) != exp_type:
            for fk, ev in exp_fields.items():
                if ev not in (None, "", [], 0, 0.0):
                    rec_exp[fk] = rec_exp.get(fk, 0) + 1
            continue

        for fk, ev in exp_fields.items():
            if ev in (None, "", [], 0, 0.0):
                continue
            rec_exp[fk] = rec_exp.get(fk, 0) + 1
            av = act.get(fk)
            populated = av not in (None, "", [], 0, 0.0)
            if populated:
                prec_pop[fk] = prec_pop.get(fk, 0) + 1
            if _field_match(ev, av):
                rec_hit[fk] = rec_hit.get(fk, 0) + 1
                if populated:
                    prec_hit[fk] = prec_hit.get(fk, 0) + 1

    lines.append(f"**Holdout emails:** {emission_total}")
    lines.append(f"**Card emission accuracy:** {emission_ok}/{emission_total}")
    lines.append("")
    lines.append("## Per-field Precision and Recall")
    lines.append("")
    lines.append("| Field | Precision | Recall |")
    lines.append("|-------|-----------|--------|")
    all_keys = sorted(set(prec_pop) | set(rec_exp))
    for fk in all_keys:
        ph, pp = prec_hit.get(fk, 0), prec_pop.get(fk, 0)
        rh, re = rec_hit.get(fk, 0), rec_exp.get(fk, 0)
        pstr = f"{ph}/{pp}" if pp else "—"
        rstr = f"{rh}/{re}" if re else "—"
        lines.append(f"| {fk} | {pstr} | {rstr} |")

    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ground-truth", required=True, type=Path)
    ap.add_argument("--vault-path", required=True)
    args = ap.parse_args()
    text = run_verification(args.ground_truth, str(Path(args.vault_path)))
    print(text)


if __name__ == "__main__":
    main()
