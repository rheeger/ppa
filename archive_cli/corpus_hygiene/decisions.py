"""Email corpus decision records for dry-run and future apply."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from archive_sync.llm_enrichment.email_promotion_policy import (
    EMAIL_PROMOTION_POLICY_VERSION,
    EmailPromotionDecision,
    EmailPromotionInput,
    EmailPromotionPolicy,
    ManualOverrides,
)

from .classification_reuse import EmailThreadRecord, ReusedClassification


@dataclass(frozen=True, slots=True)
class EmailCorpusDecisionRecord:
    decision_run_id: str
    source_key: str
    account_email: str
    gmail_thread_id: str
    gmail_history_id: str
    thread_body_sha: str
    thread_uid: str
    message_uids: tuple[str, ...]
    attachment_uids: tuple[str, ...]
    derived_uids: tuple[str, ...]
    classification: str | None
    canonical_classification: str
    confidence: float
    card_types: tuple[str, ...]
    classification_source: str
    classify_prompt_version: str
    classify_model: str
    policy_version: str
    previous_corpus_state: str
    corpus_decision: str
    processor_decision: str
    decision_reason: str
    decision_signals: tuple[str, ...]
    applied_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["decision_signals"] = list(self.decision_signals)
        d["message_uids"] = list(self.message_uids)
        d["attachment_uids"] = list(self.attachment_uids)
        d["derived_uids"] = list(self.derived_uids)
        d["card_types"] = list(self.card_types)
        return d


def promotion_input_from_thread(
    thread: EmailThreadRecord,
    reused: ReusedClassification,
    *,
    manual_overrides: ManualOverrides | None = None,
) -> EmailPromotionInput:
    return EmailPromotionInput(
        source_key=thread.source_key or f"gmail-messages:{thread.account_email}",
        gmail_thread_id=thread.gmail_thread_id,
        gmail_history_id=thread.gmail_history_id,
        thread_body_sha=thread.thread_body_sha,
        subject=thread.subject,
        from_emails=thread.from_emails,
        participant_emails=thread.participant_emails,
        owner_email=thread.owner_email,
        label_ids=thread.label_ids,
        message_count=thread.message_count,
        first_message_at=thread.first_message_at,
        last_message_at=thread.last_message_at,
        has_attachments=thread.has_attachments,
        calendar_event_hints=thread.calendar_event_hints,
        classification=reused.classification or None,
        confidence=reused.confidence,
        card_types=reused.card_types,
        classification_source=reused.classification_source,
        classify_prompt_version=reused.classify_prompt_version,
        classify_model=reused.classify_model,
        manual_overrides=manual_overrides or ManualOverrides(),
        owner_sent_message=thread.owner_sent_message,
        owner_replied=thread.owner_replied,
        has_derived_cards=bool(thread.derived_uids),
    )


def build_decision_record(
    thread: EmailThreadRecord,
    reused: ReusedClassification,
    policy_decision: EmailPromotionDecision,
    *,
    decision_run_id: str,
) -> EmailCorpusDecisionRecord:
    return EmailCorpusDecisionRecord(
        decision_run_id=decision_run_id,
        source_key=thread.source_key or policy_decision.source_key,
        account_email=thread.account_email,
        gmail_thread_id=thread.gmail_thread_id,
        gmail_history_id=thread.gmail_history_id,
        thread_body_sha=thread.thread_body_sha,
        thread_uid=thread.thread_uid,
        message_uids=thread.message_uids,
        attachment_uids=thread.attachment_uids,
        derived_uids=thread.derived_uids,
        classification=policy_decision.classification,
        canonical_classification=policy_decision.canonical_classification,
        confidence=policy_decision.confidence,
        card_types=policy_decision.card_types,
        classification_source=reused.classification_source,
        classify_prompt_version=reused.classify_prompt_version,
        classify_model=reused.classify_model,
        policy_version=EMAIL_PROMOTION_POLICY_VERSION,
        previous_corpus_state=thread.previous_corpus_state,
        corpus_decision=policy_decision.corpus_decision.value,
        processor_decision=policy_decision.processor_decision.value,
        decision_reason=policy_decision.decision_reason,
        decision_signals=policy_decision.decision_signals,
        applied_at="",
    )


@dataclass
class DecisionBatch:
    records: list[EmailCorpusDecisionRecord] = field(default_factory=list)
    policy: EmailPromotionPolicy = field(default_factory=EmailPromotionPolicy)

    def evaluate_thread(
        self,
        thread: EmailThreadRecord,
        reused: ReusedClassification,
        *,
        decision_run_id: str,
        manual_overrides: ManualOverrides | None = None,
    ) -> EmailCorpusDecisionRecord:
        inp = promotion_input_from_thread(thread, reused, manual_overrides=manual_overrides)
        decision = self.policy.evaluate(inp)
        record = build_decision_record(thread, reused, decision, decision_run_id=decision_run_id)
        self.records.append(record)
        return record
