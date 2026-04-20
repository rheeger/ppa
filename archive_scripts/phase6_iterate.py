"""Re-sweep semantic linker against the same 102 sample UIDs and version artifacts.

Usage:
    OPENAI_API_KEY=... PPA_INDEX_DSN=... PPA_INDEX_SCHEMA=ppa PPA_PATH=... \
    .venv/bin/python archive_scripts/phase6_iterate.py <turn_label>

Writes:
    _artifacts/_phase6-iterations/turn-{label}-decisions.json
    _artifacts/_phase6-iterations/turn-{label}-summary.md
    _artifacts/_phase6-iterations/turn-{label}-diff.md   (if prior turn exists)

Reuses /tmp/phase6-sample-uids.json from the original sweep.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.seed_links import (
    MODULE_SEMANTIC,
    SeedCardSketch,
    SeedLinkCatalog,
    _generate_semantic_candidates,
    _persist_candidate,
    evaluate_seed_link_candidate,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
ITER_DIR = REPO_ROOT / "_artifacts" / "_phase6-iterations"


def _sketch_from_row(r: dict[str, Any]) -> SeedCardSketch:
    return SeedCardSketch(
        uid=r["uid"], rel_path=r["rel_path"],
        slug=r.get("slug") or r["rel_path"].split("/")[-1].removesuffix(".md"),
        card_type=r["type"], summary=r.get("summary") or "",
        frontmatter={}, body="", content_hash=r.get("content_hash") or "",
        activity_at="", wikilinks=[],
    )


def _build_min_catalog(conn, schema: str, source_uids: list[str]) -> SeedLinkCatalog:
    rows = conn.execute(
        f"SELECT uid, rel_path, slug, type, summary, content_hash FROM {schema}.cards WHERE uid = ANY(%s)",
        (source_uids,),
    ).fetchall()
    by_uid = {r["uid"]: _sketch_from_row(r) for r in rows}
    by_type: dict[str, list[SeedCardSketch]] = {}
    for sk in by_uid.values():
        by_type.setdefault(sk.card_type, []).append(sk)
    return SeedLinkCatalog(
        cards_by_uid=by_uid, cards_by_exact_slug={}, cards_by_slug={},
        cards_by_type=by_type,
        person_by_email={}, person_by_phone={}, person_by_handle={}, person_by_alias={},
        email_threads_by_thread_id={}, email_messages_by_thread_id={},
        email_messages_by_message_id={}, email_attachments_by_message_id={},
        email_attachments_by_thread_id={}, imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={}, calendar_events_by_event_id={},
        calendar_events_by_ical_uid={}, media_by_day={}, events_by_day={}, path_buckets={},
    )


def _hydrate(conn, schema: str, catalog: SeedLinkCatalog, target_uids: list[str]) -> None:
    missing = [u for u in target_uids if u not in catalog.cards_by_uid]
    if not missing:
        return
    rows = conn.execute(
        f"SELECT uid, rel_path, slug, type, summary, content_hash FROM {schema}.cards WHERE uid = ANY(%s)",
        (missing,),
    ).fetchall()
    for r in rows:
        sk = _sketch_from_row(r)
        catalog.cards_by_uid[sk.uid] = sk


def _band(c: float) -> str:
    if c >= 0.95:
        return "band_4_0.95+"
    if c >= 0.85:
        return "band_3_0.85_0.95"
    if c >= 0.70:
        return "band_2_0.70_0.85"
    if c >= 0.50:
        return "band_1_0.50_0.70"
    if c >= 0.30:
        return "band_a_0.30_0.50"
    return "band_0_below_0.30"


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: phase6_iterate.py <turn_label>  (e.g., 1, 2a, 2b, 3-final)")
    label = sys.argv[1]
    threshold = float(os.environ.get("PHASE6_KNN_THRESHOLD", "0.7"))
    k = int(os.environ.get("PHASE6_KNN_K", "10"))
    schema = os.environ.get("PPA_INDEX_SCHEMA", "ppa")
    dsn = os.environ["PPA_INDEX_DSN"]
    vault = Path(os.environ["PPA_PATH"])
    uids = json.loads(Path("/tmp/phase6-sample-uids.json").read_text())
    print(f"[turn-{label}] schema={schema} sources={len(uids)} k={k} threshold={threshold}")

    ITER_DIR.mkdir(parents=True, exist_ok=True)
    idx = PostgresArchiveIndex(vault=vault, dsn=dsn)
    idx.schema = schema

    decisions: list[dict[str, Any]] = []
    t0 = time.time()
    llm_calls = 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        # Wipe prior semantic decisions to avoid the "candidate already exists" filter
        # blocking us between iterations.
        deleted = conn.execute(
            f"DELETE FROM {schema}.link_candidates WHERE module_name = %s",
            (MODULE_SEMANTIC,),
        ).rowcount
        conn.commit()
        print(f"[turn-{label}] cleared {deleted} prior semantic candidates")

        catalog = _build_min_catalog(conn, schema, uids)
        for i, uid in enumerate(uids):
            src = catalog.cards_by_uid.get(uid)
            if src is None:
                continue
            cands = _generate_semantic_candidates(conn, schema, catalog, uid, k=k, threshold=threshold)
            _hydrate(conn, schema, catalog, [c.target_card_uid for c in cands])
            for cand in cands:
                if cand.target_card_uid not in catalog.cards_by_uid:
                    continue
                decision = evaluate_seed_link_candidate(idx.vault, catalog, cand)
                _persist_candidate(conn, idx, job_id=None, candidate=cand, decision=decision, commit=True)
                if decision.llm_model:
                    llm_calls += 1
                tgt = catalog.cards_by_uid[cand.target_card_uid]
                decisions.append({
                    "source_uid": cand.source_card_uid,
                    "source_rel_path": cand.source_rel_path, "source_type": src.card_type,
                    "target_uid": cand.target_card_uid,
                    "target_rel_path": cand.target_rel_path, "target_type": tgt.card_type,
                    "embedding_similarity": cand.features["embedding_similarity"],
                    "llm_score": decision.llm_score, "llm_model": decision.llm_model,
                    "embedding_score": decision.embedding_score,
                    "final_confidence": decision.final_confidence,
                    "decision": decision.decision,
                })
            if (i + 1) % 25 == 0:
                print(f"[turn-{label}] {i+1}/{len(uids)} | {len(decisions)} decisions | "
                      f"{llm_calls} LLM calls | {int(time.time()-t0)}s")

    decisions_path = ITER_DIR / f"turn-{label}-decisions.json"
    decisions_path.write_text(json.dumps(decisions, indent=2))

    bands: dict[str, list[dict[str, Any]]] = {}
    for d in decisions:
        bands.setdefault(_band(d["final_confidence"]), []).append(d)
    band_order = [
        "band_4_0.95+", "band_3_0.85_0.95", "band_2_0.70_0.85",
        "band_1_0.50_0.70", "band_a_0.30_0.50", "band_0_below_0.30",
    ]
    surfaceable = sum(len(bands.get(b, [])) for b in band_order if b.startswith("band_3") or b.startswith("band_4"))
    auto_promotable = len(bands.get("band_4_0.95+", []))
    elapsed = int(time.time() - t0)

    summary = [
        f"# Iteration {label} — semantic linker calibration",
        "",
        "## Run parameters",
        "",
        f"- schema: `{schema}` (production seed)",
        f"- source_uids: {len(uids)}  (3 per type, 34 types)",
        f"- k: {k}",
        f"- threshold: {threshold}",
        f"- candidates judged: **{len(decisions)}**",
        f"- LLM calls: {llm_calls} (gpt-4o-mini ~ ${llm_calls * 0.0001:.3f})",
        f"- wall time: {elapsed}s",
        "",
        "## Per-band counts",
        "",
        "| band | count |",
        "|---|---|",
    ]
    for b in band_order:
        summary.append(f"| {b} | {len(bands.get(b, []))} |")
    summary.append("")
    summary.append(f"- candidates above review_floor (0.85): **{surfaceable}**")
    summary.append(f"- candidates above auto_promote_floor (0.95): **{auto_promotable}**")
    summary.extend(["", "## All decisions sorted by final_confidence desc", ""])
    summary.append("| band | source -> target | src_type | tgt_type | emb | llm | final | decision | TP/FP/Unclear |")
    summary.append("|---|---|---|---|---|---|---|---|---|")
    for d in sorted(decisions, key=lambda x: -x["final_confidence"]):
        b = _band(d["final_confidence"])
        arrow = f"`{d['source_rel_path']}` -> `{d['target_rel_path']}`"
        summary.append(
            f"| {b} | {arrow} | {d['source_type']} | {d['target_type']} | "
            f"{d['embedding_similarity']:.3f} | {d['llm_score']:.2f} | "
            f"{d['final_confidence']:.3f} | {d['decision']} |   |"
        )
    (ITER_DIR / f"turn-{label}-summary.md").write_text("\n".join(summary) + "\n")
    print(f"[turn-{label}] wrote: {decisions_path.name}, turn-{label}-summary.md")
    print(f"[turn-{label}] surfaceable={surfaceable}  auto_promotable={auto_promotable}  "
          f"total={len(decisions)}  llm_calls={llm_calls}  elapsed={elapsed}s")


if __name__ == "__main__":
    main()
