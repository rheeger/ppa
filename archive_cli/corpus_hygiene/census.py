"""Dry-run census orchestration for email corpus hygiene."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from archive_cli.validation_gates.constants import GATE_RUN_STATUS_PASSED, GATE_SYNTHETIC_FIXTURES
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.report import GateRunReport, write_gate_report
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .classification_reuse import (
    ClassificationReuseLoader,
    EmailThreadRecord,
    load_card_classifications_from_rows,
    open_classify_index,
    thread_from_frontmatter,
)
from .constants import SECTION_B_CENSUS_ARTIFACT_GATE, SECTION_B_COMPLETION_STATE
from .decisions import DecisionBatch, EmailCorpusDecisionRecord
from .report import render_census_summary, write_decision_records_jsonl
from .review_buckets import bucket_samples


@dataclass
class CensusContext:
    vault_path: str = ""
    index_schema: str = ""
    archive_instance: str = ""
    engine_mode: str = "rust"
    gate: str = GATE_SYNTHETIC_FIXTURES
    ladder_gate: str = "Synthetic fixtures"
    decision_run_id: str = ""
    allow_new_llm: bool = False
    deterministic: bool = False


@dataclass
class CensusResult:
    context: CensusContext
    records: list[EmailCorpusDecisionRecord] = field(default_factory=list)
    classification_source_counts: dict[str, int] = field(default_factory=dict)
    new_llm_call_count: int = 0
    corpus_counts: dict[str, int] = field(default_factory=dict)
    processor_counts: dict[str, int] = field(default_factory=dict)
    reason_counts: dict[str, int] = field(default_factory=dict)
    review_buckets: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    current_corpus: dict[str, int] = field(default_factory=dict)
    index_impact: dict[str, int] = field(default_factory=dict)
    derived_card_impact: dict[str, int] = field(default_factory=dict)
    input_hash: str = ""
    total_elapsed_ms: int = 0
    phases: list[dict[str, Any]] = field(default_factory=list)
    artifact_paths: dict[str, str] = field(default_factory=dict)
    unchanged_count: int = 0
    newly_demoted_count: int = 0


def compute_input_hash(threads: list[EmailThreadRecord], *, policy_version: str) -> str:
    payload = {
        "policy_version": policy_version,
        "threads": sorted(
            [
                {
                    "thread_uid": t.thread_uid,
                    "gmail_thread_id": t.gmail_thread_id,
                    "thread_body_sha": t.thread_body_sha,
                    "triage_classification": t.triage_classification,
                    "triage_confidence": t.triage_confidence,
                }
                for t in threads
            ],
            key=lambda x: x["thread_uid"],
        ),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def summarize_records(records: list[EmailCorpusDecisionRecord]) -> CensusResult:
    corpus_counts: dict[str, int] = {}
    processor_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    unchanged = 0
    newly_demoted = 0
    chunks_hidden = 0
    embeddings_excluded = 0
    derived_preserved = 0
    derived_review = 0

    for rec in records:
        corpus_counts[rec.corpus_decision] = corpus_counts.get(rec.corpus_decision, 0) + 1
        processor_counts[rec.processor_decision] = processor_counts.get(rec.processor_decision, 0) + 1
        reason_counts[rec.decision_reason] = reason_counts.get(rec.decision_reason, 0) + 1
        source_counts[rec.classification_source] = source_counts.get(rec.classification_source, 0) + 1

        if rec.previous_corpus_state == "active" and rec.corpus_decision in {"suppressed", "quarantine"}:
            newly_demoted += 1
            chunks_hidden += 1 + len(rec.message_uids) + len(rec.attachment_uids)
            embeddings_excluded += 1 + len(rec.message_uids)
        elif rec.previous_corpus_state == rec.corpus_decision:
            unchanged += 1

        if rec.derived_uids:
            if rec.corpus_decision == "active":
                derived_preserved += len(rec.derived_uids)
            elif rec.corpus_decision == "quarantine":
                derived_review += len(rec.derived_uids)

    return CensusResult(
        context=CensusContext(),
        records=records,
        classification_source_counts=source_counts,
        corpus_counts=corpus_counts,
        processor_counts=processor_counts,
        reason_counts=reason_counts,
        review_buckets=bucket_samples(records),
        current_corpus={
            "email_threads": len(records),
            "email_messages": sum(len(r.message_uids) for r in records),
            "email_attachments": sum(len(r.attachment_uids) for r in records),
        },
        index_impact={
            "chunks_hidden_estimate": chunks_hidden,
            "embeddings_excluded_estimate": embeddings_excluded,
        },
        derived_card_impact={
            "derived_cards_preserved_active": derived_preserved,
            "derived_cards_needing_review": derived_review,
        },
        unchanged_count=unchanged,
        newly_demoted_count=newly_demoted,
    )


def run_email_census_dry_run(
    threads: list[EmailThreadRecord],
    *,
    context: CensusContext,
    card_classification_rows: list[dict[str, Any]] | None = None,
    classify_index_path: Path | None = None,
    llm_classify_fn: Callable | None = None,
    register_gate: GateRegistry | None = None,
    repo_root: Path | None = None,
) -> CensusResult:
    """Evaluate promotion policy for threads without mutating vault or index."""

    t0 = time.perf_counter()
    card_map = load_card_classifications_from_rows(card_classification_rows or [])

    classify_index = None
    if classify_index_path is not None and classify_index_path.is_file():
        from archive_sync.llm_enrichment.classify_index import ClassifyIndex

        classify_index = ClassifyIndex(classify_index_path)
    elif context.vault_path:
        classify_index = open_classify_index(Path(context.vault_path))

    loader = ClassificationReuseLoader(
        card_classifications=card_map,
        classify_index=classify_index,
        allow_new_llm=context.allow_new_llm,
        llm_classify_fn=llm_classify_fn,
    )
    batch = DecisionBatch()
    decision_run_id = context.decision_run_id or "section-b-dry-run-fixture"

    for thread in sorted(threads, key=lambda t: t.thread_uid):
        reused = loader.resolve(thread)
        batch.evaluate_thread(thread, reused, decision_run_id=decision_run_id)

    if classify_index is not None:
        classify_index.close()

    result = summarize_records(batch.records)
    result.context = context
    result.classification_source_counts = dict(loader.source_counts)
    result.new_llm_call_count = loader.new_llm_call_count
    result.input_hash = compute_input_hash(threads, policy_version=EMAIL_PROMOTION_POLICY_VERSION)
    result.total_elapsed_ms = int((time.perf_counter() - t0) * 1000)
    result.phases = [
        {"name": "classification_reuse", "status": "passed", "elapsed_ms": result.total_elapsed_ms},
        {"name": "policy_evaluation", "status": "passed", "elapsed_ms": 0},
    ]

    if repo_root is not None:
        result.artifact_paths = write_census_artifacts(repo_root, result)

    if register_gate is not None and result.artifact_paths:
        register_gate.complete_run(
            decision_run_id,
            status=GATE_RUN_STATUS_PASSED,
            report_path=result.artifact_paths.get("report", ""),
            summary_path=result.artifact_paths.get("summary", ""),
        )

    return result


def write_census_artifacts(repo_root: Path, result: CensusResult) -> dict[str, str]:
    ctx = result.context
    gate = SECTION_B_CENSUS_ARTIFACT_GATE
    run_id = ctx.decision_run_id or "section-b-dry-run"

    report = GateRunReport(
        run_id=run_id,
        gate=gate,
        ladder_gate=ctx.ladder_gate,
        archive_instance=ctx.archive_instance,
        vault_path=ctx.vault_path,
        index_schema=ctx.index_schema,
        engine_mode=ctx.engine_mode,
        policy_version=EMAIL_PROMOTION_POLICY_VERSION,
        decision_run_id=run_id,
        overall_status="passed",
        total_elapsed_ms=result.total_elapsed_ms,
        classification_source_counts=result.classification_source_counts,
        new_llm_call_count=result.new_llm_call_count,
        corpus_counts=result.corpus_counts,
        embedding_affected_count=result.index_impact.get("embeddings_excluded_estimate", 0),
        next_recommended_gate="local_seed_staging_apply",
        completion_state=SECTION_B_COMPLETION_STATE,
    )
    report.details = {
        "processor_counts": result.processor_counts,
        "reason_counts": result.reason_counts,
        "current_corpus": result.current_corpus,
        "index_impact": result.index_impact,
        "derived_card_impact": result.derived_card_impact,
        "unchanged_count": result.unchanged_count,
        "newly_demoted_count": result.newly_demoted_count,
        "input_hash": result.input_hash,
        "review_bucket_counts": {k: len(v) for k, v in result.review_buckets.items()},
        "safety": {
            "production_mutation": False,
            "dry_run_only": True,
            "apply_unlocked": False,
        },
    }

    paths = write_gate_report(
        repo_root,
        report,
        write_samples=[s for samples in result.review_buckets.values() for s in samples],
    )
    decisions_path = Path(paths["report"]).parent / "decisions.jsonl"
    write_decision_records_jsonl(decisions_path, result.records)
    paths["decisions"] = str(decisions_path)

    summary_path = Path(paths["summary"])
    summary_path.write_text(render_census_summary(result, report), encoding="utf-8")
    paths["summary"] = str(summary_path)
    return paths


def _frontmatter_rows_from_cache(vault: Path) -> list[dict[str, Any]]:
    """One Rust (or single-cursor) dump of email_thread + email_message frontmatter."""

    from archive_cli.vault_cache import VaultScanCache

    scan_cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    if cache_path.is_file():
        try:
            import archive_crate

            return list(
                archive_crate.frontmatter_dicts_from_cache(
                    str(cache_path),
                    types=["email_thread", "email_message"],
                )
            )
        except Exception:
            pass
    by_type, _rel_by_uid, uid_by_path, _uid_by_stem, frontmatter_by_uid = scan_cache.slice_lookup_tables()
    rows: list[dict[str, Any]] = []
    for card_type in ("email_thread", "email_message"):
        for rel in by_type.get(card_type) or []:
            uid = uid_by_path.get(rel, "")
            fm = dict(frontmatter_by_uid.get(uid) or {})
            if not fm:
                continue
            rows.append({"rel_path": rel, "frontmatter": fm})
    return rows


def load_threads_from_vault_cache(vault_path: Path) -> list[EmailThreadRecord]:
    """Load email_thread records via one cache dump (Rust cache_iter preferred).

    Resolves outbound signals from linked email_message cards (From=owner or
    direction=outbound). Owner ∈ thread.participants alone is never enough.
    """

    vault = Path(vault_path)
    rows = _frontmatter_rows_from_cache(vault)
    fm_by_uid: dict[str, dict[str, Any]] = {}
    fm_by_stem: dict[str, dict[str, Any]] = {}
    thread_rows: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        rel = str(row.get("rel_path") or "")
        fm = dict(row.get("frontmatter") or {})
        uid = str(fm.get("uid") or Path(rel).stem)
        if uid:
            fm_by_uid[uid] = fm
        stem = Path(rel).stem
        if stem:
            fm_by_stem[stem] = fm
        card_type = str(fm.get("type") or "")
        if card_type == "email_thread" or (not card_type and "EmailThread" in rel.replace("\\", "/")):
            thread_rows.append((rel, fm))

    threads: list[EmailThreadRecord] = []
    for rel, fm in sorted(thread_rows, key=lambda item: item[0]):
        message_from: list[str] = []
        message_dirs: list[str] = []
        for raw_ref in fm.get("messages") or []:
            ref = str(raw_ref).strip()
            if ref.startswith("[[") and ref.endswith("]]"):
                ref = ref[2:-2].strip()
            uid = ref.split("|", 1)[0].strip()
            msg_fm = fm_by_uid.get(uid) or fm_by_stem.get(uid)
            if not msg_fm:
                continue
            from_email = str(msg_fm.get("from_email") or msg_fm.get("from") or "").strip().lower()
            if from_email:
                message_from.append(from_email)
            direction = str(msg_fm.get("direction") or "").strip().lower()
            if direction:
                message_dirs.append(direction)
        threads.append(
            thread_from_frontmatter(
                rel,
                fm,
                message_from_emails=tuple(message_from),
                message_directions=tuple(message_dirs),
            )
        )
    return threads


def load_card_classifications_from_db(conn: Any, schema: str) -> list[dict[str, Any]]:
    try:
        rows = conn.execute(
            f"""
            SELECT card_uid, classification, confidence, card_types, classify_model
            FROM {schema}.card_classifications
            """
        ).fetchall()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
        else:
            out.append(
                {
                    "card_uid": row[0],
                    "classification": row[1],
                    "confidence": row[2],
                    "card_types": row[3],
                    "classify_model": row[4] if len(row) > 4 else "",
                }
            )
    return out
