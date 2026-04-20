"""Phase 6 calibration sweep against the production seed (schema=ppa).

Bypasses the full-vault VaultScanCache build (1.87M cards) by constructing a minimal
in-memory catalog directly from the `ppa.cards` table — sufficient for the semantic
linker since kNN already returns target metadata (uid, rel_path, type) from the DB.

Inputs:
- /tmp/phase6-sample-uids.json  (list of source card UIDs to sweep)
- env: PPA_INDEX_DSN, PPA_INDEX_SCHEMA=ppa, OPENAI_API_KEY, PPA_PATH

Outputs:
- _artifacts/_phase6-baseline/seed-baseline-{date}.json     (pre-Tier-3 graph state per source)
- _artifacts/_semantic-linker-calibration/seed-samples-{date}.json     (every decision)
- _artifacts/_semantic-linker-calibration/seed-summary-{date}.md       (per-band review tables)
- _artifacts/_phase6-tier3/seed-impact-{date}.md                       (recall delta + cost summary)
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.seed_links import (
    SEED_LINK_POLICY_VERSION,
    SeedCardSketch,
    SeedLinkCatalog,
    _generate_semantic_candidates,
    _persist_candidate,
    evaluate_seed_link_candidate,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = REPO_ROOT / "_artifacts"
DATE = _dt.date.today().strftime("%Y%m%d")


def _sketch_from_row(row: dict[str, Any]) -> SeedCardSketch:
    return SeedCardSketch(
        uid=row["uid"], rel_path=row["rel_path"],
        slug=row.get("slug") or row["rel_path"].split("/")[-1].removesuffix(".md"),
        card_type=row["type"], summary=row.get("summary") or "",
        frontmatter={}, body="", content_hash=row.get("content_hash") or "",
        activity_at="", wikilinks=[],
    )


def _build_min_catalog(conn, schema: str, source_uids: list[str]) -> SeedLinkCatalog:
    """Catalog with the source UIDs prepopulated. Targets are added on demand by sweep()."""
    rows = conn.execute(
        f"SELECT uid, rel_path, slug, type, summary, content_hash FROM {schema}.cards WHERE uid = ANY(%s)",
        (source_uids,),
    ).fetchall()
    cards_by_uid = {r["uid"]: _sketch_from_row(r) for r in rows}
    cards_by_type: dict[str, list[SeedCardSketch]] = {}
    for sk in cards_by_uid.values():
        cards_by_type.setdefault(sk.card_type, []).append(sk)
    return SeedLinkCatalog(
        cards_by_uid=cards_by_uid, cards_by_exact_slug={}, cards_by_slug={},
        cards_by_type=cards_by_type,
        person_by_email={}, person_by_phone={}, person_by_handle={}, person_by_alias={},
        email_threads_by_thread_id={}, email_messages_by_thread_id={},
        email_messages_by_message_id={}, email_attachments_by_message_id={},
        email_attachments_by_thread_id={}, imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={}, calendar_events_by_event_id={},
        calendar_events_by_ical_uid={}, media_by_day={}, events_by_day={}, path_buckets={},
    )


def _hydrate_targets(conn, schema: str, catalog: SeedLinkCatalog, target_uids: list[str]) -> None:
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


def _bandify(c: float) -> str:
    if c >= 0.95:
        return "band_4_0.95+"
    if c >= 0.85:
        return "band_3_0.85_0.95"
    if c >= 0.70:
        return "band_2_0.70_0.85"
    if c >= 0.50:
        return "band_1_0.50_0.70"
    return "band_0_below_0.50"


def main() -> None:
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY required (the LLM judge runs against OpenAI)")
    schema = os.environ.get("PPA_INDEX_SCHEMA", "ppa")
    dsn = os.environ["PPA_INDEX_DSN"]
    vault = Path(os.environ["PPA_PATH"])
    uids = json.loads(Path("/tmp/phase6-sample-uids.json").read_text())
    print(f"[seed-run] schema={schema} source_uids={len(uids)} vault={vault}")

    idx = PostgresArchiveIndex(vault=vault, dsn=dsn)
    idx.schema = schema

    baseline_dir = ARTIFACTS / "_phase6-baseline"
    calibration_dir = ARTIFACTS / "_semantic-linker-calibration"
    impact_dir = ARTIFACTS / "_phase6-tier3"
    for d in (baseline_dir, calibration_dir, impact_dir):
        d.mkdir(parents=True, exist_ok=True)

    decisions: list[dict[str, Any]] = []
    baseline_rows: list[dict[str, Any]] = []
    t0 = time.time()
    llm_calls = 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        catalog = _build_min_catalog(conn, schema, uids)
        print(f"[seed-run] catalog has {len(catalog.cards_by_uid)} source sketches")

        for i, uid in enumerate(uids):
            src = catalog.cards_by_uid.get(uid)
            if src is None:
                continue
            graph = idx.graph(src.rel_path, hops=1)
            base_neighbors = (graph or {}).get(src.rel_path, [])
            baseline_rows.append({
                "source_uid": uid, "source_rel_path": src.rel_path, "type": src.card_type,
                "neighbor_count": len(base_neighbors),
                "neighbor_edge_types": sorted({n["edge_type"] for n in base_neighbors}),
            })

            cands = _generate_semantic_candidates(conn, schema, catalog, uid, k=10, threshold=0.7)
            target_uids_seen = [c.target_card_uid for c in cands]
            _hydrate_targets(conn, schema, catalog, target_uids_seen)

            for cand in cands:
                if cand.target_card_uid not in catalog.cards_by_uid:
                    continue
                decision = evaluate_seed_link_candidate(idx.vault, catalog, cand)
                _persist_candidate(conn, idx, job_id=None, candidate=cand, decision=decision, commit=True)
                if decision.llm_model:
                    llm_calls += 1
                target_sketch = catalog.cards_by_uid[cand.target_card_uid]
                decisions.append({
                    "source_uid": cand.source_card_uid,
                    "source_rel_path": cand.source_rel_path,
                    "source_type": src.card_type,
                    "target_uid": cand.target_card_uid,
                    "target_rel_path": cand.target_rel_path,
                    "target_type": target_sketch.card_type,
                    "embedding_similarity": cand.features["embedding_similarity"],
                    "llm_score": decision.llm_score,
                    "llm_model": decision.llm_model,
                    "embedding_score": decision.embedding_score,
                    "final_confidence": decision.final_confidence,
                    "decision": decision.decision,
                    "decision_reason": decision.decision_reason,
                })
            if (i + 1) % 10 == 0:
                elapsed = time.time() - t0
                print(f"[seed-run] {i+1}/{len(uids)} sources processed | "
                      f"{len(decisions)} decisions | {llm_calls} LLM calls | {elapsed:.0f}s")

    # ---- artifacts ----
    Path(baseline_dir / f"seed-baseline-{DATE}.json").write_text(json.dumps({
        "tier": "tier2-baseline-seed",
        "snapshot_date": _dt.datetime.now(_dt.UTC).isoformat(),
        "schema": schema, "source_uid_count": len(uids),
        "anchors": baseline_rows,
    }, indent=2))
    print(f"[seed-run] wrote baseline -> {baseline_dir / f'seed-baseline-{DATE}.json'}")

    Path(calibration_dir / f"seed-samples-{DATE}.json").write_text(json.dumps(decisions, indent=2))
    print(f"[seed-run] wrote raw decisions -> {calibration_dir / f'seed-samples-{DATE}.json'}")

    bands: dict[str, list[dict[str, Any]]] = {}
    for d in decisions:
        bands.setdefault(_bandify(d["final_confidence"]), []).append(d)
    band_order = ["band_4_0.95+", "band_3_0.85_0.95", "band_2_0.70_0.85", "band_1_0.50_0.70", "band_0_below_0.50"]
    summary_lines = [
        f"# Semantic Linker Calibration -- seed sweep {DATE}",
        "",
        "## Run parameters",
        "",
        f"- Schema: `{schema}` (production seed, {1872706} cards, {6770930} chunks, "
        f"`text-embedding-3-small` v1)",
        f"- Source cards swept: {len(uids)} (3 per card type, 34 types)",
        f"- Total semantic candidates judged: **{len(decisions)}**",
        f"- LLM calls (real OpenAI gpt-4o-mini): **{llm_calls}**",
        f"- Wall time: {int(time.time() - t0)}s",
        f"- Policy version: {SEED_LINK_POLICY_VERSION}",
        "- k=10, threshold=0.7",
        "",
        "## Per-band counts",
        "",
        "| Band | Count |",
        "|---|---|",
    ]
    for b in band_order:
        summary_lines.append(f"| {b} | {len(bands.get(b, []))} |")
    summary_lines.extend(["", "## Sample classifications (review TP/FP/Unclear)", ""])
    summary_lines.append("| band | source -> target | src_type | tgt_type | embedding | llm | final | decision | TP/FP/Unclear |")
    summary_lines.append("|---|---|---|---|---|---|---|---|---|")
    for b in band_order:
        for d in sorted(bands.get(b, []), key=lambda x: -x["final_confidence"])[:25]:
            arrow = f"`{d['source_rel_path']}` -> `{d['target_rel_path']}`"
            summary_lines.append(
                f"| {b} | {arrow} | {d['source_type']} | {d['target_type']} | "
                f"{d['embedding_similarity']:.3f} | {d['llm_score']:.2f} | "
                f"{d['final_confidence']:.3f} | {d['decision']} |   |"
            )
    summary_lines.extend([
        "",
        "## Review procedure",
        "",
        "1. For each row above, open both source and target cards "
        "(`ppa read --uid <uid>` or grep the `.md` paths in the production vault).",
        "2. Mark TP / FP / Unclear in the rightmost column.",
        "3. Compute precision per band: `precision = TP / (TP + FP)`.",
        "4. Apply the floor table from `phase_6` plan Step 12d:",
        "   - Band 4 (0.95+) precision >= 0.95 -> `auto_promote_floor = 0.95`",
        "   - Band 4 precision 0.80-0.95 -> keep `auto_promote_floor = 0.99`",
        "   - All bands < 0.80 precision -> bump floors to `1.0` (effectively disable)",
        "5. Edit `LinkSurfacePolicy(LINK_TYPE_SEMANTICALLY_RELATED)` in "
        "`archive_cli/seed_links.py`; bump `SEED_LINK_POLICY_VERSION` (2 -> 3).",
    ])
    Path(calibration_dir / f"seed-summary-{DATE}.md").write_text("\n".join(summary_lines) + "\n")
    print(f"[seed-run] wrote calibration summary -> {calibration_dir / f'seed-summary-{DATE}.md'}")

    surfaceable = [d for d in decisions if d["final_confidence"] >= 0.85]
    auto_promotable = [d for d in decisions if d["final_confidence"] >= 0.99]
    impact_lines = [
        f"# Phase 6 Tier 3 Retrieval Impact -- seed sweep {DATE}",
        "",
        f"**Scope:** {len(uids)} source cards (3 per type, 34 types) from production seed `{schema}`.",
        "",
        "## New semantic edges discovered",
        "",
        f"- Total `semantically_related` candidates judged: **{len(decisions)}**",
        f"- Above review_floor (0.85): **{len(surfaceable)}** (would surface for human review today)",
        f"- Above auto_promote_floor (0.99): **{len(auto_promotable)}** (would auto-promote today; "
        f"floor is intentionally conservative pre-calibration)",
        "",
        "## Per-anchor neighbor delta",
        "",
        "| Anchor | Type | Baseline neighbors | New surfaceable semantic |",
        "|---|---|---|---|",
    ]
    new_by_src: dict[str, int] = {}
    for d in surfaceable:
        new_by_src[d["source_uid"]] = new_by_src.get(d["source_uid"], 0) + 1
    for row in baseline_rows:
        impact_lines.append(
            f"| `{row['source_rel_path']}` | {row['type']} | {row['neighbor_count']} | "
            f"{new_by_src.get(row['source_uid'], 0)} |"
        )
    impact_lines.extend([
        "",
        "## Cost",
        "",
        f"- LLM judge calls: {llm_calls} x gpt-4o-mini ~ ${llm_calls * 0.0001:.2f}",
        f"- Wall time: {int(time.time() - t0)}s",
        "",
        "## Next steps",
        "",
        "1. Review samples in `_artifacts/_semantic-linker-calibration/seed-summary-{DATE}.md`.",
        "2. Set calibrated floors per the procedure in that report.",
        "3. To run against the full vault, drop the `--limit` style filter from this script "
        "and either bound by source card count or run overnight.",
    ])
    Path(impact_dir / f"seed-impact-{DATE}.md").write_text("\n".join(impact_lines) + "\n")
    print(f"[seed-run] wrote impact report -> {impact_dir / f'seed-impact-{DATE}.md'}")


if __name__ == "__main__":
    main()
