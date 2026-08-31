"""Classify-before-promotion gate for Gmail sync batches."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from archive_cli.corpus_hygiene.classification_reuse import ReusedClassification
from archive_cli.corpus_hygiene.decisions import (
    DecisionBatch,
    EmailCorpusDecisionRecord,
    build_decision_record,
    promotion_input_from_thread,
)
from archive_cli.corpus_hygiene.state_store import CORPUS_STATE_ACTIVE, CORPUS_STATE_QUARANTINE, CORPUS_STATE_SUPPRESSED
from archive_sync.llm_enrichment.email_promotion_policy import (
    CorpusDecision,
    EmailPromotionDecision,
    EmailPromotionPolicy,
)

from .classification_resolve import GmailClassificationResolver
from .ledger import PromotionLedger
from .metrics import GmailPromotionBatchMetrics
from .thread_record import thread_record_from_gmail_items


class PromotionOutcome(StrEnum):
    PROMOTE_CARDS = "promote_cards"
    SUPPRESS = "suppress"
    QUARANTINE = "quarantine"
    DEMOTION_RECOMMENDED = "demotion_recommended"


def promotion_gate_enabled(kwargs: dict[str, Any]) -> bool:
    raw = kwargs.get("gmail_promotion_gate")
    if raw is None:
        raw = os.environ.get("PPA_GMAIL_PROMOTION_GATE", "")
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class GmailPromotionResult:
    outcome: PromotionOutcome
    record: EmailCorpusDecisionRecord
    commit_cursor: bool = True
    emit_cards: bool = True
    dirty_card_uids: tuple[str, ...] = ()
    classification_source: str = ""


@dataclass
class GmailPromotionGate:
    ledger: PromotionLedger
    resolver: GmailClassificationResolver
    decision_run_id: str
    policy: EmailPromotionPolicy | None = None
    metrics: GmailPromotionBatchMetrics | None = None
    fail_on_missing_classification: bool = False

    def __post_init__(self) -> None:
        if self.policy is None:
            self.policy = EmailPromotionPolicy()
        if self.metrics is None:
            self.metrics = GmailPromotionBatchMetrics()

    def evaluate_loaded_thread(
        self,
        thread_record: dict[str, Any],
        message_records: list[dict[str, Any]],
        *,
        account_email: str,
        own_emails: set[str],
        vault_has_active_card: bool,
    ) -> GmailPromotionResult:
        assert self.metrics is not None
        self.metrics.observed += 1

        ledger_state = self.ledger.get_thread_state(str(thread_record.get("thread_id", "")))
        previous = CORPUS_STATE_ACTIVE if vault_has_active_card else ledger_state
        if vault_has_active_card:
            previous = CORPUS_STATE_ACTIVE
        elif ledger_state in {CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE}:
            previous = ledger_state

        thread = thread_record_from_gmail_items(
            thread_record,
            message_records,
            account_email=account_email,
            own_emails=own_emails,
            previous_corpus_state=previous,
        )

        reused = self.resolver.resolve(thread)
        if self.fail_on_missing_classification and reused.classification_source == "missing":
            self.metrics.classification_failures += 1
            rec = self._synthetic_failure_record(thread, reused)
            return GmailPromotionResult(
                outcome=PromotionOutcome.SUPPRESS,
                record=rec,
                commit_cursor=False,
                emit_cards=False,
            )

        batch = DecisionBatch(policy=self.policy)
        policy_decision = batch.policy.evaluate(promotion_input_from_thread(thread, reused))
        record = build_decision_record(thread, reused, policy_decision, decision_run_id=self.decision_run_id)

        return self._finalize_outcome(
            thread=thread,
            record=record,
            policy_decision=policy_decision,
            reused=reused,
            vault_has_active_card=vault_has_active_card,
            previous=previous,
        )

    def persist_decision(self, result: GmailPromotionResult) -> None:
        self.ledger.persist(result.record)

    def _finalize_outcome(
        self,
        *,
        thread: Any,
        record: EmailCorpusDecisionRecord,
        policy_decision: EmailPromotionDecision,
        reused: ReusedClassification,
        vault_has_active_card: bool,
        previous: str,
    ) -> GmailPromotionResult:
        assert self.metrics is not None
        corpus = policy_decision.corpus_decision.value
        dirty: tuple[str, ...] = ()

        if vault_has_active_card and corpus in {CorpusDecision.SUPPRESSED.value, CorpusDecision.QUARANTINE.value}:
            self.metrics.demotion_recommended += 1
            signals = tuple(record.decision_signals) + (
                "routine_sync_keep_active",
                f"recommended_corpus_state:{corpus}",
            )
            record = replace(
                record,
                corpus_decision=CORPUS_STATE_ACTIVE,
                decision_signals=signals,
                decision_reason="routine_sync_demotion_recommended",
            )
            dirty = (record.thread_uid, *record.message_uids)
            self.metrics.promoted += 1
            return GmailPromotionResult(
                outcome=PromotionOutcome.DEMOTION_RECOMMENDED,
                record=record,
                emit_cards=True,
                dirty_card_uids=dirty,
                classification_source=reused.classification_source,
            )

        if corpus == CorpusDecision.SUPPRESSED.value:
            self.metrics.suppressed += 1
            if previous in {CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE}:
                pass
            return GmailPromotionResult(
                outcome=PromotionOutcome.SUPPRESS,
                record=record,
                emit_cards=False,
                commit_cursor=True,
                classification_source=reused.classification_source,
            )

        if corpus == CorpusDecision.QUARANTINE.value:
            self.metrics.quarantined += 1
            dirty = (record.thread_uid, *record.message_uids, *record.attachment_uids)
            self.metrics.dirty_card_uids.extend(dirty)
            return GmailPromotionResult(
                outcome=PromotionOutcome.QUARANTINE,
                record=record,
                emit_cards=True,
                commit_cursor=True,
                dirty_card_uids=dirty,
                classification_source=reused.classification_source,
            )

        # Active promotion
        self.metrics.promoted += 1
        if previous in {CORPUS_STATE_SUPPRESSED, CORPUS_STATE_QUARANTINE}:
            self.metrics.re_promoted += 1
        dirty = (record.thread_uid, *record.message_uids, *record.attachment_uids)
        self.metrics.dirty_card_uids.extend(dirty)
        return GmailPromotionResult(
            outcome=PromotionOutcome.PROMOTE_CARDS,
            record=record,
            emit_cards=True,
            dirty_card_uids=dirty,
            classification_source=reused.classification_source,
        )

    def _synthetic_failure_record(self, thread: Any, reused: ReusedClassification) -> EmailCorpusDecisionRecord:
        from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

        return EmailCorpusDecisionRecord(
            decision_run_id=self.decision_run_id,
            source_key=thread.source_key,
            account_email=thread.account_email,
            gmail_thread_id=thread.gmail_thread_id,
            gmail_history_id=thread.gmail_history_id,
            thread_body_sha=thread.thread_body_sha,
            thread_uid=thread.thread_uid,
            message_uids=thread.message_uids,
            attachment_uids=thread.attachment_uids,
            derived_uids=thread.derived_uids,
            classification=None,
            canonical_classification="unknown",
            confidence=0.0,
            card_types=(),
            classification_source=reused.classification_source,
            classify_prompt_version="",
            classify_model="",
            policy_version=EMAIL_PROMOTION_POLICY_VERSION,
            previous_corpus_state=thread.previous_corpus_state,
            corpus_decision="suppressed",
            processor_decision="suppressed_no_processing",
            decision_reason="classification_unresolved",
            decision_signals=("classification_failure",),
        )
