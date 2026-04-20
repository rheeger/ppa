"""Phase 6 end-to-end demo against a tiny synthetic vault.

This script exercises the full Phase 6 pipeline (Tier 1 + Tier 2 + Tier 3) against an
isolated pgvector schema seeded with ~30 synthetic cards across multiple types. It uses
real OpenAI for the LLM judge (so calibration numbers are meaningful) and the hash
embedding provider so we don't burn embedding tokens for the demo.

Usage:

    OPENAI_API_KEY=$(echo "$OPENAI_CREDENTIALS" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['api-token'])") \
    PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:50051/archive" \
    .venv/bin/python archive_scripts/phase6_demo.py

Outputs:

- _artifacts/_phase6-baseline/demo-baseline-{date}.json  (Tier 2 baseline shape)
- _artifacts/_semantic-linker-calibration/demo-samples-{date}.json  (every semantic candidate decided)
- _artifacts/_semantic-linker-calibration/demo-summary-{date}.md  (per-band counts + sample bodies for review)
- _artifacts/_phase6-tier3/demo-impact-{date}.md  (post-linker neighborhood comparison)
"""

from __future__ import annotations

import datetime as _dt
import json
import os
from pathlib import Path
from typing import Any

from archive_cli import seed_links as sl
from archive_cli.embedding_provider import HashEmbeddingProvider
from archive_cli.index_config import get_default_embedding_model, get_default_embedding_version
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.seed_links import (
    SeedCardSketch,
    SeedLinkCatalog,
    _generate_semantic_candidates,
    _persist_candidate,
    evaluate_seed_link_candidate,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = REPO_ROOT / "_artifacts"
SCHEMA = "ppa_phase6_demo"
DATE = _dt.date.today().strftime("%Y%m%d")

# ---------------------------------------------------------------------------
# Synthetic dataset: 12 cards across 5 types with handcrafted semantic clusters
# ---------------------------------------------------------------------------
DATASET: list[dict[str, Any]] = [
    # Cluster A: travel itinerary discussion + booking + transcript
    {"uid": "u-doc-italy", "rel": "Documents/italy-trip-notes.md", "type": "document",
     "summary": "Italy trip planning notes",
     "text": "italy rome florence venice hotel booking flight itinerary september"},
    {"uid": "u-flight-jfk-fco", "rel": "Travel/flight-jfk-fco.md", "type": "flight",
     "summary": "JFK to FCO flight booking",
     "text": "italy rome florence venice hotel booking flight itinerary september"},
    {"uid": "u-acc-rome", "rel": "Travel/accommodation-rome-hotel.md", "type": "accommodation",
     "summary": "Rome hotel reservation",
     "text": "italy rome florence venice hotel booking flight itinerary september"},
    {"uid": "u-meeting-italy", "rel": "Meetings/2026-08-italy-trip-call.md", "type": "meeting_transcript",
     "summary": "Family call: Italy trip planning",
     "text": "italy rome florence venice hotel booking flight itinerary september"},

    # Cluster B: back pain conversation + medical record + amazon order
    {"uid": "u-msg-backpain", "rel": "IMessage/2026-03-back-pain-thread.md", "type": "imessage_message",
     "summary": "Conversation about back pain",
     "text": "back pain heating pad recommendation orthopedic guidance lumbar support"},
    {"uid": "u-med-back", "rel": "Medical/2026-03-back-strain.md", "type": "medical_record",
     "summary": "Back strain diagnosis",
     "text": "back pain heating pad recommendation orthopedic guidance lumbar support"},
    {"uid": "u-purchase-pad", "rel": "Purchases/2026-03-amazon-heating-pad.md", "type": "purchase",
     "summary": "Amazon order: heating pad",
     "text": "back pain heating pad recommendation orthopedic guidance lumbar support"},

    # Cluster C: Q3 strategy meeting + finance consultant payment
    {"uid": "u-transcript-q3", "rel": "Meetings/2026-09-q3-strategy.md", "type": "meeting_transcript",
     "summary": "Q3 strategy offsite transcript",
     "text": "quarterly strategy planning offsite agenda consultant deliverable"},
    {"uid": "u-finance-consult", "rel": "Finance/2026-09-consultant-payment.md", "type": "finance",
     "summary": "Consultant payment Q3",
     "text": "quarterly strategy planning offsite agenda consultant deliverable"},

    # Outliers: should NOT be matched to any cluster
    {"uid": "u-doc-recipes", "rel": "Documents/family-recipes.md", "type": "document",
     "summary": "Family recipe collection",
     "text": "grandma pasta recipe tomato basil oregano traditional kitchen"},
    {"uid": "u-msg-weather", "rel": "IMessage/2026-04-weather-chat.md", "type": "imessage_message",
     "summary": "Casual weather chat",
     "text": "rainy weekend forecast umbrella weather chilly april spring"},
    {"uid": "u-payroll", "rel": "Finance/2026-09-payroll-direct-deposit.md", "type": "payroll",
     "summary": "Payroll direct deposit",
     "text": "monthly payroll direct deposit gross net withholding tax"},
]


def _vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def _bootstrap_demo_schema(idx: PostgresArchiveIndex) -> None:
    print(f"[demo] dropping + recreating schema {idx.schema}")
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {idx.schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {idx.schema}")
        idx._create_schema(conn)
        runner = MigrationRunner(conn, idx.schema)
        runner.ensure_table()
        result = runner.run()
        print(f"[demo] migrations applied: {result.applied}")


def _seed_cards_with_embeddings(idx: PostgresArchiveIndex, provider: HashEmbeddingProvider) -> None:
    model = get_default_embedding_model()
    version = get_default_embedding_version()
    with idx._connect() as conn:
        for card in DATASET:
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.cards (uid, rel_path, slug, type, summary, content_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (card["uid"], card["rel"], card["rel"].split("/")[-1].removesuffix(".md"),
                 card["type"], card["summary"], f"sha256:{card['uid']}"),
            )
            chunk_key = f"{card['uid']}:body:0"
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.chunks
                  (chunk_key, card_uid, rel_path, chunk_type, chunk_index, source_fields,
                   content, content_hash, token_count)
                VALUES (%s, %s, %s, 'body', 0, '[]'::jsonb, %s, %s, %s)
                """,
                (chunk_key, card["uid"], card["rel"], card["text"],
                 f"sha256:{card['uid']}:body", len(card["text"].split())),
            )
            vec = provider.embed_texts([card["text"]])[0]
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.embeddings (chunk_key, embedding_model, embedding_version, embedding)
                VALUES (%s, %s, %s, %s::vector)
                """,
                (chunk_key, model, version, _vec_literal(vec)),
            )
        conn.commit()
    print(f"[demo] inserted {len(DATASET)} cards with hash embeddings")


def _build_catalog() -> SeedLinkCatalog:
    cards_by_uid: dict[str, SeedCardSketch] = {}
    cards_by_type: dict[str, list[SeedCardSketch]] = {}
    for card in DATASET:
        sketch = SeedCardSketch(
            uid=card["uid"], rel_path=card["rel"],
            slug=card["rel"].split("/")[-1].removesuffix(".md"),
            card_type=card["type"], summary=card["summary"],
            frontmatter={}, body="", content_hash=f"sha256:{card['uid']}",
            activity_at="2026-01-01", wikilinks=[],
        )
        cards_by_uid[card["uid"]] = sketch
        cards_by_type.setdefault(card["type"], []).append(sketch)
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


def _capture_baseline(idx: PostgresArchiveIndex, out: Path) -> dict[str, Any]:
    """Tier 2 baseline: how the existing graph looks BEFORE Tier 3 runs."""
    rows: list[dict[str, Any]] = []
    for card in DATASET:
        graph = idx.graph(card["rel"], hops=1)
        rows.append({
            "anchor": card["rel"],
            "type": card["type"],
            "neighbor_count": len(graph[card["rel"]]) if graph else 0,
            "neighbors": (graph[card["rel"]] if graph else []),
        })
    payload = {
        "tier": "tier2-baseline-demo",
        "snapshot_date": _dt.datetime.utcnow().isoformat() + "Z",
        "policy_version": sl.SEED_LINK_POLICY_VERSION,
        "graph_boost_formula": "0.22 * trust_weight",
        "card_count": len(DATASET),
        "anchors": rows,
    }
    out.write_text(json.dumps(payload, indent=2))
    print(f"[demo] wrote baseline -> {out}")
    return payload


def _run_semantic_linker(
    idx: PostgresArchiveIndex,
    catalog: SeedLinkCatalog,
) -> list[dict[str, Any]]:
    print("[demo] running semantic linker (real OpenAI judge for every candidate)")
    decisions: list[dict[str, Any]] = []
    with idx._connect() as conn:
        for card in DATASET:
            cands = _generate_semantic_candidates(
                conn, idx.schema, catalog, card["uid"], k=8, threshold=0.7,
            )
            for cand in cands:
                decision = evaluate_seed_link_candidate(idx.vault, catalog, cand)
                _persist_candidate(conn, idx, job_id=None, candidate=cand, decision=decision, commit=True)
                decisions.append({
                    "source_uid": cand.source_card_uid,
                    "source_rel_path": cand.source_rel_path,
                    "target_uid": cand.target_card_uid,
                    "target_rel_path": cand.target_rel_path,
                    "embedding_similarity": cand.features["embedding_similarity"],
                    "llm_score": decision.llm_score,
                    "llm_model": decision.llm_model,
                    "embedding_score": decision.embedding_score,
                    "final_confidence": decision.final_confidence,
                    "decision": decision.decision,
                    "decision_reason": decision.decision_reason,
                })
    print(f"[demo] judged {len(decisions)} candidates")
    return decisions


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


def _write_calibration_report(decisions: list[dict[str, Any]], out: Path) -> None:
    bands: dict[str, list[dict[str, Any]]] = {}
    for d in decisions:
        bands.setdefault(_bandify(d["final_confidence"]), []).append(d)
    lines = [
        f"# Semantic Linker Demo Calibration -- {DATE}",
        "",
        "## Run parameters",
        "",
        f"- Synthetic dataset: {len(DATASET)} cards across "
        f"{len({c['type'] for c in DATASET})} types",
        "- Embedding provider: hash (deterministic, demo-only)",
        "- LLM judge: OpenAI (real, via existing `llm_judge_candidate` chain)",
        "- k = 8, threshold = 0.7",
        "",
        "## Per-band counts",
        "",
        "| Band | Count |",
        "|---|---|",
    ]
    for band in [
        "band_4_0.95+", "band_3_0.85_0.95", "band_2_0.70_0.85",
        "band_1_0.50_0.70", "band_0_below_0.50",
    ]:
        lines.append(f"| {band} | {len(bands.get(band, []))} |")
    lines.extend(["", "## Sample classifications (review TP/FP/Unclear)", ""])
    lines.append(
        "| band | source -> target | embedding | llm | final | decision | TP/FP/Unclear |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    for band in [
        "band_4_0.95+", "band_3_0.85_0.95", "band_2_0.70_0.85",
        "band_1_0.50_0.70", "band_0_below_0.50",
    ]:
        for d in bands.get(band, [])[:25]:
            arrow = f"`{d['source_rel_path']}` -> `{d['target_rel_path']}`"
            lines.append(
                f"| {band} | {arrow} | {d['embedding_similarity']:.3f} | "
                f"{d['llm_score']:.2f} | {d['final_confidence']:.3f} | {d['decision']} |   |"
            )
    lines.extend([
        "",
        "## How to use this report",
        "",
        "1. For each row above, look up the source + target cards (paths shown).",
        "2. Mark TP (true positive — genuinely related), FP (false positive — coincidental "
        "similarity), or Unclear in the rightmost column.",
        "3. Compute precision per band: `precision = TP / (TP + FP)` (excluding Unclear).",
        "4. Apply the floor decision rule from `phase_6` plan Step 12d:",
        "   - Band 4 precision >= 0.95 → set `auto_promote_floor = 0.95`",
        "   - Band 4 precision 0.80-0.95 → keep `auto_promote_floor = 0.99`",
        "   - All bands < 0.80 → bump floors to 1.0 (effectively disable)",
        "5. Edit `LinkSurfacePolicy(LINK_TYPE_SEMANTICALLY_RELATED)` in "
        "`archive_cli/seed_links.py` and bump `SEED_LINK_POLICY_VERSION` (2 → 3).",
        "",
        "## Why so few rows?",
        "",
        "This is a 12-card demo dataset. A real calibration sweep against the slice will "
        "produce thousands of candidates per band; the runbook at "
        "`_artifacts/_semantic-linker-calibration/README.md` documents that flow.",
    ])
    out.write_text("\n".join(lines) + "\n")
    print(f"[demo] wrote calibration report -> {out}")


def _write_impact_report(
    baseline: dict[str, Any],
    decisions: list[dict[str, Any]],
    idx: PostgresArchiveIndex,
    out: Path,
) -> None:
    # For each anchor, count edges before vs after (semantic decisions persisted as
    # link_candidates -> we count the ones that would surface above review_floor).
    surfaced = [d for d in decisions if d["final_confidence"] >= 0.85]
    auto_promote = [d for d in decisions if d["final_confidence"] >= 0.99]
    by_source_after: dict[str, int] = {}
    for d in surfaced:
        by_source_after[d["source_rel_path"]] = by_source_after.get(d["source_rel_path"], 0) + 1
    lines = [
        f"# Phase 6 Tier 3 Retrieval Impact -- demo {DATE}",
        "",
        "**Scope:** synthetic 12-card vault with 3 designed semantic clusters + 3 outliers. "
        "Real-world impact will be measured against the slice baseline using the runbook at "
        "`_artifacts/_phase6-tier3/README.md`.",
        "",
        "## New edges introduced by semantic linker",
        "",
        f"- LINK_TYPE_SEMANTICALLY_RELATED candidates judged: **{len(decisions)}**",
        f"- Above review_floor (0.85): **{len(surfaced)}**",
        f"- Above auto_promote_floor (0.99): **{len(auto_promote)}**",
        "",
        "## Per-anchor neighbor delta (designed clusters)",
        "",
        "| Anchor | Type | Baseline neighbors | New semantic surfaceable |",
        "|---|---|---|---|",
    ]
    for row in baseline["anchors"]:
        lines.append(
            f"| `{row['anchor']}` | {row['type']} | {row['neighbor_count']} | "
            f"{by_source_after.get(row['anchor'], 0)} |"
        )
    lines.extend([
        "",
        "## Designed cluster expectations",
        "",
        "- **Cluster A (Italy travel):** `u-doc-italy`, `u-flight-jfk-fco`, "
        "`u-acc-rome`, `u-meeting-italy` — should mutually link.",
        "- **Cluster B (back pain):** `u-msg-backpain`, `u-med-back`, "
        "`u-purchase-pad` — should mutually link.",
        "- **Cluster C (Q3 strategy):** `u-transcript-q3`, `u-finance-consult` — should link.",
        "- **Outliers** (`u-doc-recipes`, `u-msg-weather`, `u-payroll`) should NOT link "
        "into clusters A/B/C.",
        "",
        "If the linker is working as intended, the 'New semantic surfaceable' column "
        "should be ≥ 1 for all in-cluster anchors and 0 for outliers.",
    ])
    out.write_text("\n".join(lines) + "\n")
    print(f"[demo] wrote impact report -> {out}")


def main() -> None:
    dsn = os.environ.get("PPA_INDEX_DSN", "postgresql://archive:archive@127.0.0.1:50051/archive")
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY must be set; see script docstring.")

    idx = PostgresArchiveIndex(vault=REPO_ROOT, dsn=dsn)
    idx.schema = SCHEMA

    baseline_dir = ARTIFACTS / "_phase6-baseline"
    calibration_dir = ARTIFACTS / "_semantic-linker-calibration"
    impact_dir = ARTIFACTS / "_phase6-tier3"
    for d in (baseline_dir, calibration_dir, impact_dir):
        d.mkdir(parents=True, exist_ok=True)

    _bootstrap_demo_schema(idx)
    _seed_cards_with_embeddings(idx, HashEmbeddingProvider())
    catalog = _build_catalog()

    baseline = _capture_baseline(idx, baseline_dir / f"demo-baseline-{DATE}.json")
    decisions = _run_semantic_linker(idx, catalog)

    samples_path = calibration_dir / f"demo-samples-{DATE}.json"
    samples_path.write_text(json.dumps(decisions, indent=2))
    print(f"[demo] wrote raw decisions -> {samples_path}")

    _write_calibration_report(decisions, calibration_dir / f"demo-summary-{DATE}.md")
    _write_impact_report(baseline, decisions, idx, impact_dir / f"demo-impact-{DATE}.md")

    print("\n[demo] complete.")
    print(f"  baseline:    {baseline_dir / f'demo-baseline-{DATE}.json'}")
    print(f"  samples:     {samples_path}")
    print(f"  calibration: {calibration_dir / f'demo-summary-{DATE}.md'}")
    print(f"  impact:      {impact_dir / f'demo-impact-{DATE}.md'}")


if __name__ == "__main__":
    main()
