"""Review bucket assignment for corpus hygiene dry-run samples."""

from __future__ import annotations

from archive_sync.llm_enrichment.email_promotion_policy import SUPPRESS_CONFIDENCE_MIN

from .constants import REVIEW_BUCKETS
from .decisions import EmailCorpusDecisionRecord


def _has_attachment_signal(record: EmailCorpusDecisionRecord) -> bool:
    signals = set(record.decision_signals)
    return "has_attachments" in signals or "attachment_would_suppress" in signals


def assign_review_bucket(record: EmailCorpusDecisionRecord) -> str | None:
    """Return the primary review bucket for a decision, if any."""

    canonical = record.canonical_classification
    conf = record.confidence
    signals = set(record.decision_signals)

    if record.classification_source == "missing" or (canonical == "unknown" and record.corpus_decision == "quarantine"):
        return "unknown_classification"

    if record.corpus_decision == "quarantine":
        if record.decision_reason == "derived_cards_would_suppress" or record.derived_uids:
            return "suppressed_with_derived_cards"
        if _has_attachment_signal(record):
            return "suppressed_with_attachments"
        return "quarantine_conflicts"

    if record.corpus_decision == "active" and (
        record.decision_reason.startswith("manual_override")
        or record.decision_reason in {"owner_participation", "starred_or_important", "back_and_forth_participation"}
    ):
        return "active_overrides"

    if record.corpus_decision == "suppressed":
        if _has_attachment_signal(record):
            return "suppressed_with_attachments"
        if canonical in {"automated", "noise"}:
            return "automated_noise"
        if canonical == "marketing" and conf >= SUPPRESS_CONFIDENCE_MIN:
            return "high_confidence_marketing"
        if "category_promotions" in signals:
            return "promotions_label_suppression"

    return None


def bucket_samples(
    records: list[EmailCorpusDecisionRecord],
    *,
    limit_per_bucket: int = 50,
) -> dict[str, list[dict[str, object]]]:
    """Group sample summaries by review bucket."""

    buckets: dict[str, list[dict[str, object]]] = {name: [] for name in REVIEW_BUCKETS}
    for record in records:
        bucket = assign_review_bucket(record)
        if bucket is None:
            continue
        if len(buckets[bucket]) >= limit_per_bucket:
            continue
        buckets[bucket].append(sample_summary(record))
    return buckets


def sample_summary(record: EmailCorpusDecisionRecord) -> dict[str, object]:
    return {
        "gmail_thread_id": record.gmail_thread_id,
        "thread_uid": record.thread_uid,
        "classification": record.classification,
        "canonical_classification": record.canonical_classification,
        "confidence": record.confidence,
        "classification_source": record.classification_source,
        "corpus_decision": record.corpus_decision,
        "processor_decision": record.processor_decision,
        "decision_reason": record.decision_reason,
        "derived_uids": list(record.derived_uids),
        "policy_version": record.policy_version,
        "decision_run_id": record.decision_run_id,
    }
