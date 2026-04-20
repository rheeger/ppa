"""Sample N candidates and build a compact, scannable review markdown.

Per-pair block layout (~6-10 lines):
- type pair + scores
- one-line summary of source (key fields per card type)
- one-line summary of target
- structural-link cue if obvious (e.g., target.thread points at source.uid)
- Verdict line

Total length scales as ~10 lines × N pairs (vs the verbose original which dumped
full frontmatter of every card).

Usage:
    PPA_INDEX_DSN=... .venv/bin/python archive_scripts/phase6_sample_for_review.py \\
        --cache _artifacts/_phase6-iterations/cache-1pct-filtered-v2.json \\
        --schema ppa_1pct --vault .slices/1pct \\
        --turn 15-final-recommended \\
        --n-cross 60 --n-same 40
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import random
import re
from pathlib import Path
from typing import Any

import psycopg
import yaml
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
ITER_DIR = REPO_ROOT / "_artifacts" / "_phase6-iterations"

_PROVENANCE_BLOCK_RE = re.compile(r"<!--\s*provenance.*?-->", re.DOTALL)


def _apply_production_gate(cache_path: Path) -> list[dict[str, Any]]:
    """Mirror the production policy v5 gate from evaluate_seed_link_candidate."""
    cache = json.loads(cache_path.read_text())
    surfaced: list[dict[str, Any]] = []
    AUTO_PROMOTE_FLOOR = 0.50
    AUTO_REVIEW_FLOOR = 0.40
    for v in cache.values():
        if v.get("llm_verdict") != "YES":
            continue
        is_cross = v["source_type"] != v["target_type"]
        min_llm, min_emb = (0.70, 0.55) if is_cross else (0.90, 0.85)
        if v["llm_score"] < min_llm or v["embedding_similarity"] < min_emb:
            continue
        score = v["llm_score"] * v["embedding_similarity"]
        if score < AUTO_REVIEW_FLOOR:
            continue
        decision = "auto_promote" if score >= AUTO_PROMOTE_FLOOR else "review"
        surfaced.append({
            **v,
            "final_confidence": round(score, 6),
            "decision": decision,
        })
    return surfaced


def _hydrate(dsn: str, schema: str, uids: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        for chunk in [uids[i:i+500] for i in range(0, len(uids), 500)]:
            rows = conn.execute(
                f"SELECT uid, rel_path, type, summary FROM {schema}.cards WHERE uid = ANY(%s)",
                (chunk,),
            ).fetchall()
            for r in rows:
                out[str(r["uid"])] = {
                    "uid": str(r["uid"]),
                    "rel_path": str(r["rel_path"]),
                    "type": str(r["type"]),
                    "summary": str(r.get("summary") or ""),
                }
    return out


def _load_card(vault: Path, rel_path: str) -> tuple[dict[str, Any], str]:
    """Return (frontmatter_dict, body_text) for a card. Body has provenance stripped."""
    abs_path = (vault / rel_path).resolve()
    if not abs_path.exists():
        return {}, ""
    raw = abs_path.read_text(encoding="utf-8", errors="replace")
    parts = raw.split("---", 2)
    fm: dict[str, Any] = {}
    body = ""
    if len(parts) >= 3:
        try:
            fm = yaml.safe_load(parts[1]) or {}
        except Exception:
            fm = {}
        body = _PROVENANCE_BLOCK_RE.sub("", parts[2]).strip()
    return fm, body


def _truncate(s: str, n: int = 100) -> str:
    s = str(s).replace("\n", " ").replace("|", "\\|").strip()
    if len(s) > n:
        s = s[: n - 1] + "…"
    return s


def _summary_line(card_type: str, fm: dict[str, Any], body: str, summary: str) -> str:
    """One-line decision-relevant summary per card type."""
    bits: list[str] = []
    s = (summary or fm.get("summary") or "").strip()
    if s:
        bits.append(_truncate(s, 80))

    if card_type in {"email_thread", "email_message"}:
        subj = fm.get("subject")
        if subj and subj != s:
            bits.append(f"subject={_truncate(subj, 80)}")
        if card_type == "email_message":
            sender = fm.get("from_email") or fm.get("from_name")
            sent_at = fm.get("sent_at", "")
            if sender:
                bits.append(f"from={sender}")
            if sent_at:
                bits.append(f"at={str(sent_at)[:10]}")
        else:
            mc = fm.get("message_count")
            participants = fm.get("participants") or []
            if mc:
                bits.append(f"msgs={mc}")
            if participants:
                bits.append(f"with={_truncate(','.join(participants[:3]), 60)}")
    elif card_type in {"beeper_thread", "beeper_message", "imessage_thread", "imessage_message"}:
        if card_type.endswith("_message"):
            sender = fm.get("sender_name") or fm.get("from_email")
            sent_at = fm.get("sent_at", "")
            if sender:
                bits.append(f"from={sender}")
            if sent_at:
                bits.append(f"at={str(sent_at)[:10]}")
        else:
            counter = fm.get("counterpart_names") or fm.get("participant_names") or []
            mc = fm.get("message_count")
            if counter:
                bits.append(f"with={_truncate(','.join(map(str, counter[:3])), 60)}")
            if mc:
                bits.append(f"msgs={mc}")
    elif card_type == "calendar_event":
        t = fm.get("title")
        if t and t != s:
            bits.append(f"title={_truncate(t, 80)}")
        start = fm.get("start_at", "")
        if start:
            bits.append(f"at={str(start)[:16]}")
    elif card_type == "meeting_transcript":
        title = fm.get("title") or fm.get("meeting_title")
        if title and title != s:
            bits.append(f"title={_truncate(title, 80)}")
        date = fm.get("recorded_at") or fm.get("transcript_date") or fm.get("start_at")
        if date:
            bits.append(f"at={str(date)[:10]}")
    elif card_type == "document":
        title = fm.get("title") or fm.get("filename")
        if title and title != s:
            bits.append(f"title={_truncate(title, 80)}")
        if body:
            bits.append(f"body=`{_truncate(body, 80)}`")
    elif card_type == "person":
        first = fm.get("first_name", "")
        last = fm.get("last_name", "")
        emails = fm.get("emails") or []
        if first or last:
            bits.append(f"name={(first + ' ' + last).strip()}")
        if emails:
            bits.append(f"emails={_truncate(','.join(emails[:2]), 60)}")
    elif card_type in {"finance", "purchase", "meal_order", "grocery_order", "ride", "flight",
                        "accommodation", "car_rental", "subscription", "event_ticket",
                        "shipment", "payroll"}:
        for fld in ("merchant", "vendor", "service_provider", "title", "amount",
                    "venue", "destination", "origin", "transaction_date", "start_at"):
            v = fm.get(fld)
            if v:
                bits.append(f"{fld}={_truncate(v, 60)}")
                if len(bits) > 4:
                    break
    elif card_type == "place":
        for fld in ("name", "address", "city"):
            v = fm.get(fld)
            if v:
                bits.append(f"{fld}={_truncate(v, 60)}")
    elif card_type == "organization":
        for fld in ("name", "domain", "industry"):
            v = fm.get(fld)
            if v:
                bits.append(f"{fld}={_truncate(v, 60)}")
    elif card_type == "medical_record":
        for fld in ("provider_name", "diagnosis", "treatment_summary", "service_date"):
            v = fm.get(fld)
            if v:
                bits.append(f"{fld}={_truncate(v, 60)}")
    elif card_type == "vaccination":
        for fld in ("vaccine_name", "administered_at", "lot_number"):
            v = fm.get(fld)
            if v:
                bits.append(f"{fld}={_truncate(v, 60)}")
    elif card_type in {"git_thread", "git_message", "git_commit", "git_repository"}:
        for fld in ("title", "subject", "repo", "author_name", "commit_sha"):
            v = fm.get(fld)
            if v:
                bits.append(f"{fld}={_truncate(v, 60)}")

    if not bits and body:
        bits.append(f"body=`{_truncate(body, 100)}`")
    return " · ".join(bits) if bits else "_(no decision-relevant fields)_"


def _structural_link(src_uid: str, src_fm: dict, tgt_uid: str, tgt_fm: dict) -> str:
    """Return a short tag if there's an obvious frontmatter link; '' otherwise."""
    def _refs(fm: dict, fields: tuple[str, ...]) -> set[str]:
        refs: set[str] = set()
        for f in fields:
            for item in fm.get(f) or []:
                s = str(item)
                m = re.search(r"\[\[([^\]|]+)", s)
                if m:
                    refs.add(m.group(1).strip())
                else:
                    refs.add(s.strip())
        return refs

    container_fields = ("messages", "attachments", "calendar_events", "people",
                         "source_messages", "source_threads", "meeting_transcripts")
    src_refs = _refs(src_fm, container_fields)
    tgt_refs = _refs(tgt_fm, container_fields)
    if tgt_uid in src_refs:
        return "🔗 source lists target in frontmatter"
    if src_uid in tgt_refs:
        return "🔗 target lists source in frontmatter"

    def _scalar_ref(fm: dict, field: str) -> str:
        v = fm.get(field)
        if not v:
            return ""
        m = re.search(r"\[\[([^\]|]+)", str(v))
        return m.group(1).strip() if m else str(v).strip()

    for f in ("thread", "parent_message", "calendar_event", "transcript", "repository"):
        if _scalar_ref(tgt_fm, f) == src_uid:
            return f"🔗 target.{f} -> source.uid"
        if _scalar_ref(src_fm, f) == tgt_uid:
            return f"🔗 source.{f} -> target.uid"

    src_gtid = src_fm.get("gmail_thread_id")
    tgt_gtid = tgt_fm.get("gmail_thread_id")
    if src_gtid and tgt_gtid and src_gtid == tgt_gtid:
        return f"🔗 same gmail_thread_id={src_gtid[:24]}"

    return ""


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--cache", required=True, type=Path)
    p.add_argument("--schema", required=True)
    p.add_argument("--vault", required=True, type=Path)
    p.add_argument("--n-cross", type=int, default=60)
    p.add_argument("--n-same", type=int, default=40)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dsn", default=os.environ.get("PPA_INDEX_DSN", ""))
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    if not args.dsn:
        raise SystemExit("--dsn or PPA_INDEX_DSN required")

    surf = _apply_production_gate(args.cache)
    cross = [d for d in surf if d["source_type"] != d["target_type"]]
    same = [d for d in surf if d["source_type"] == d["target_type"]]
    print(f"surfaced={len(surf)} cross={len(cross)} same={len(same)}")

    rng = random.Random(args.seed)
    pick_cross = rng.sample(cross, min(args.n_cross, len(cross)))
    pick_same = rng.sample(same, min(args.n_same, len(same)))
    sample = pick_cross + pick_same
    rng.shuffle(sample)
    print(f"sampled cross={len(pick_cross)} same={len(pick_same)} total={len(sample)}")

    uids = list({d["source_uid"] for d in sample} | {d["target_uid"] for d in sample})
    meta = _hydrate(args.dsn, args.schema, uids)

    date = _dt.date.today().strftime("%Y%m%d")
    out_path = args.out or (ITER_DIR / f"review-1pct-{date}.md")

    md: list[str] = [
        f"# Phase 6 surfaced-candidate review — {date}",
        "",
        f"- cache: `{args.cache.name}` (filtered, type-allowlisted)",
        f"- vault: `{args.vault}`",
        f"- schema: `{args.schema}`",
        "- gate: production policy v5 — dual-tier formula. Same-type pairs require"
        " `verdict=YES, llm>=0.90, emb>=0.85`. Cross-type pairs require"
        " `verdict=YES, llm>=0.70, emb>=0.55`. Floor: 0.50 auto-promote / 0.40 review.",
        f"- surfaced total: **{len(surf)}** (auto={sum(1 for d in surf if d['decision']=='auto_promote')}, review={len(surf) - sum(1 for d in surf if d['decision']=='auto_promote')})",
        f"- sampled: **{len(sample)}** ({len(pick_cross)} cross-type + {len(pick_same)} same-type)",
        "",
        "## How to read this",
        "",
        "Each pair has 3 lines: scores, source one-liner, target one-liner. A 🔗 tag",
        "means the frontmatter already structurally links the two — almost always TP.",
        "",
        "Mark each pair: replace `__` after `Verdict N:` with `TP`, `FP`, or `Unclear`.",
        "",
        "Aim: precision ≥ 0.90 to validate the calibrated gate.",
        "",
        "---",
        "",
    ]

    for i, d in enumerate(sample, 1):
        src_fm, src_body = _load_card(args.vault, d["source_rel_path"])
        tgt_fm, tgt_body = _load_card(args.vault, d["target_rel_path"])
        src_meta = meta.get(d["source_uid"], {})
        tgt_meta = meta.get(d["target_uid"], {})

        src_line = _summary_line(d["source_type"], src_fm, src_body, src_meta.get("summary", ""))
        tgt_line = _summary_line(d["target_type"], tgt_fm, tgt_body, tgt_meta.get("summary", ""))
        link_tag = _structural_link(d["source_uid"], src_fm, d["target_uid"], tgt_fm)

        # Add classifications if present
        src_class = str(src_fm.get("triage_classification") or "")
        tgt_class = str(tgt_fm.get("triage_classification") or "")
        class_tag = ""
        if src_class or tgt_class:
            class_tag = f"  · src.class={src_class or '-'}  tgt.class={tgt_class or '-'}"

        md.append(f"### {i}. {d['source_type']} → {d['target_type']}  "
                  f"emb={d['embedding_similarity']:.2f} llm={d['llm_score']:.2f} "
                  f"final={d['final_confidence']:.2f}  decision=`{d['decision']}`")
        md.append(f"- **S** `{d['source_rel_path']}`  ·  {src_line}")
        md.append(f"- **T** `{d['target_rel_path']}`  ·  {tgt_line}")
        if link_tag:
            md.append(f"- {link_tag}{class_tag}")
        elif class_tag:
            md.append(f"- {class_tag.lstrip()}")
        md.append(f"- **Verdict {i}:** __  (TP / FP / Unclear)")
        md.append("")

    md.extend([
        "---",
        "",
        "## Tally (fill in after review)",
        "",
        "- Cross-type TP / FP / Unclear: __ / __ / __",
        "- Same-type TP / FP / Unclear: __ / __ / __",
        "- Overall precision (TP / (TP+FP)): __",
        "",
        "**Calibration response:**",
        "- ≥0.95 → loosen gate (e.g. `auto_promote_floor=0.80`)",
        "- 0.90–0.95 → keep current gate",
        "- <0.90 → tighten (e.g. `min_llm=0.95`, `auto_promote_floor=0.90`)",
    ])
    out_path.write_text("\n".join(md) + "\n")
    print(f"wrote {out_path}  ({sum(1 for _ in open(out_path))} lines)")


if __name__ == "__main__":
    main()
