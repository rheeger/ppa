"""Load and validate staged email corpus decision records."""

from __future__ import annotations

import json
from pathlib import Path

from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .constants import SECTION_B_CENSUS_ARTIFACT_GATE
from .decisions import EmailCorpusDecisionRecord


def record_from_dict(data: dict[str, object]) -> EmailCorpusDecisionRecord:
    return EmailCorpusDecisionRecord(
        decision_run_id=str(data.get("decision_run_id") or ""),
        source_key=str(data.get("source_key") or ""),
        account_email=str(data.get("account_email") or ""),
        gmail_thread_id=str(data.get("gmail_thread_id") or ""),
        gmail_history_id=str(data.get("gmail_history_id") or ""),
        thread_body_sha=str(data.get("thread_body_sha") or ""),
        thread_uid=str(data.get("thread_uid") or ""),
        message_uids=tuple(str(x) for x in (data.get("message_uids") or [])),
        attachment_uids=tuple(str(x) for x in (data.get("attachment_uids") or [])),
        derived_uids=tuple(str(x) for x in (data.get("derived_uids") or [])),
        classification=str(data["classification"]) if data.get("classification") is not None else None,
        canonical_classification=str(data.get("canonical_classification") or ""),
        confidence=float(data.get("confidence") or 0.0),
        card_types=tuple(str(x) for x in (data.get("card_types") or [])),
        classification_source=str(data.get("classification_source") or ""),
        classify_prompt_version=str(data.get("classify_prompt_version") or ""),
        classify_model=str(data.get("classify_model") or ""),
        policy_version=str(data.get("policy_version") or EMAIL_PROMOTION_POLICY_VERSION),
        previous_corpus_state=str(data.get("previous_corpus_state") or "active"),
        corpus_decision=str(data.get("corpus_decision") or ""),
        processor_decision=str(data.get("processor_decision") or ""),
        decision_reason=str(data.get("decision_reason") or ""),
        decision_signals=tuple(str(x) for x in (data.get("decision_signals") or [])),
        applied_at=str(data.get("applied_at") or ""),
    )


def load_decision_records_jsonl(path: Path | str) -> list[EmailCorpusDecisionRecord]:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(p)
    records: list[EmailCorpusDecisionRecord] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(record_from_dict(json.loads(line)))
    return records


def decisions_artifact_path(repo_root: Path, decision_run_id: str) -> Path:
    return (
        repo_root
        / "logs"
        / "validation-gates"
        / f"gate-{SECTION_B_CENSUS_ARTIFACT_GATE}"
        / decision_run_id
        / "decisions.jsonl"
    )


def validate_decision_records(
    records: list[EmailCorpusDecisionRecord],
    *,
    decision_run_id: str,
    expected_policy_version: str = EMAIL_PROMOTION_POLICY_VERSION,
) -> None:
    if not records:
        raise ValueError("decision records are empty")
    for rec in records:
        if rec.decision_run_id != decision_run_id:
            raise ValueError(
                f"decision_run_id mismatch: record {rec.thread_uid} has {rec.decision_run_id}"
            )
        if rec.policy_version != expected_policy_version:
            raise ValueError(
                f"policy_version mismatch for {rec.thread_uid}: {rec.policy_version}"
            )
        if not rec.corpus_decision:
            raise ValueError(f"missing corpus_decision for {rec.thread_uid}")
        if not rec.processor_decision:
            raise ValueError(f"missing processor_decision for {rec.thread_uid}")
        if not rec.classification_source:
            raise ValueError(f"missing classification_source for {rec.thread_uid}")
