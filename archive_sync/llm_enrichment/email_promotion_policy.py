"""v2.5 email corpus promotion policy — shared semantics for cleanup and future Gmail sync.

Pure decision logic: consumes existing classification outputs and Gmail metadata;
does not call LLMs or mutate vault/DB state.

Completion scope (v2.5 Section A)
---------------------------------
This module is **Section A / Validation Gate 1 (synthetic fixtures) complete**.
It proves policy semantics in isolation. The validation gate framework lives in
``archive_cli.validation_gates``. Section B apply, slice apply, seed apply, Arnold dry-run apply,
and Arnold apply remain blocked until the relevant validation gates pass.

Rule ordering (earlier rules win)
---------------------------------
1. Manual overrides (applied after base evaluation; records pre-override recommendation)
2. Owner action overrides
3. Transactional promotion
4. Personal promotion
5. Marketing / automated / noise suppression
6. Quarantine

Owner reply + marketing (locked behavior)
-----------------------------------------
The Section A plan listed "classifier says marketing/noise but owner replied" as a
possible quarantine case under rule 6. Because rules are ordered and rule 2 runs
first, **owner participation always wins over marketing suppression**:

- ``corpus_decision = active``
- ``processor_decision = thread_enrichment``
- ``decision_reason = owner_participation``

This applies when the owner sent a message or replied, even if classification is
``marketing`` / ``automated`` / ``noise`` and Gmail labels include
``CATEGORY_PROMOTIONS``. Passive starred/important + promotions + marketing does
**not** count as owner participation; that path quarantines
(``starred_marketing_conflict``) unless an operator manual override applies.

Contrast: unstarred, no owner participation, high-confidence marketing → suppressed.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from archive_sync.llm_enrichment.known_senders import (
    NOISE_DOMAINS,
    TRANSACTIONAL_DOMAINS,
    _email_domain,
)

EMAIL_PROMOTION_POLICY_VERSION = "email-promotion-v1"

# Section A synthetic-fixture gate only. Not sufficient for Section B apply or Arnold.
SECTION_A_COMPLETION_STATE = "section_a_gate_1_complete"
SECTION_G_FRAMEWORK_STATE = "validation_gates_complete"
SECTION_A_GATE_NAME = "synthetic-fixtures"
SECTION_A_RUN_ID = "section-a-synthetic"
SECTION_A_VALIDATION_MATRIX_GATE = "Synthetic fixtures"

SUPPRESS_CONFIDENCE_MIN = 0.75
PROMOTE_TRANSACTIONAL_MIN = 0.50
QUARANTINE_BELOW = 0.50

CANONICAL_CATEGORIES = frozenset(
    {"transactional", "personal", "marketing", "automated", "noise", "unknown"}
)

_CATEGORY_ALIASES: dict[str, str] = {
    "transactional": "transactional",
    "transactional_receipt": "transactional",
    "booking_confirmation": "transactional",
    "shipping_notification": "transactional",
    "subscription_event": "transactional",
    "purchase_receipt": "transactional",
    "payroll_notification": "transactional",
    "transaction": "transactional",
    "txn": "transactional",
    "commerce": "transactional",
    "commercial": "transactional",
    "personal": "personal",
    "person_to_person": "personal",
    "marketing": "marketing",
    "promotion": "marketing",
    "automated": "automated",
    "automated_notification": "automated",
    "noise": "noise",
    "skip": "noise",
}


class CorpusDecision(StrEnum):
    ACTIVE = "active"
    SUPPRESSED = "suppressed"
    QUARANTINE = "quarantine"


class ProcessorDecision(StrEnum):
    TYPED_EXTRACTION = "typed_extraction"
    THREAD_ENRICHMENT = "thread_enrichment"
    NO_DOWNSTREAM_PROCESSING = "no_downstream_processing"
    SUPPRESSED_NO_PROCESSING = "suppressed_no_processing"
    QUARANTINE_REVIEW = "quarantine_review"


@dataclass(frozen=True)
class ManualOverrides:
    """Operator-configured policy inputs (Section A rule 1)."""

    always_active_thread: bool = False
    always_suppress_thread: bool = False
    always_active_senders: frozenset[str] = field(default_factory=frozenset)
    always_suppress_senders: frozenset[str] = field(default_factory=frozenset)
    always_keep_starred: bool = False
    always_keep_important: bool = False


@dataclass(frozen=True)
class EmailPromotionInput:
    """Normalized policy input — classification reuse layer fills this shape."""

    source_key: str = ""
    gmail_thread_id: str = ""
    gmail_history_id: str = ""
    thread_body_sha: str = ""
    subject: str = ""
    from_emails: tuple[str, ...] = ()
    participant_emails: tuple[str, ...] = ()
    owner_email: str = ""
    label_ids: tuple[str, ...] = ()
    message_count: int = 0
    first_message_at: str = ""
    last_message_at: str = ""
    has_attachments: bool = False
    calendar_event_hints: bool = False
    classification: str | None = None
    confidence: float = 0.0
    card_types: tuple[str, ...] = ()
    classification_source: str = ""
    classify_prompt_version: str = ""
    classify_model: str = ""
    manual_overrides: ManualOverrides = field(default_factory=ManualOverrides)
    owner_sent_message: bool = False
    owner_replied: bool = False
    has_derived_cards: bool = False
    known_transactional_card_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class EmailPromotionDecision:
    """Auditable policy output for dry-run diffs and future reinterpretation."""

    source_key: str
    external_id: str
    external_history_id: str
    content_hash: str
    policy_version: str
    classification: str | None
    canonical_classification: str
    confidence: float
    card_types: tuple[str, ...]
    classification_source: str
    corpus_decision: CorpusDecision
    processor_decision: ProcessorDecision
    decision_reason: str
    decision_signals: tuple[str, ...]
    evaluated_at: str
    pre_override_corpus_decision: CorpusDecision | None = None
    pre_override_processor_decision: ProcessorDecision | None = None
    pre_override_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["corpus_decision"] = self.corpus_decision.value
        d["processor_decision"] = self.processor_decision.value
        if self.pre_override_corpus_decision is not None:
            d["pre_override_corpus_decision"] = self.pre_override_corpus_decision.value
        if self.pre_override_processor_decision is not None:
            d["pre_override_processor_decision"] = self.pre_override_processor_decision.value
        return d


def normalize_canonical_classification(
    raw: str | None,
    *,
    confidence: float = 0.0,
) -> str:
    """Map raw classifier labels to canonical categories (Section A)."""

    label = (raw or "").strip().lower()
    if not label:
        if confidence < QUARANTINE_BELOW:
            return "noise"
        return "unknown"
    if label.startswith("transaction") and label != "transactional":
        return "transactional"
    return _CATEGORY_ALIASES.get(label, label if label in CANONICAL_CATEGORIES else "unknown")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _labels(inp: EmailPromotionInput) -> frozenset[str]:
    return frozenset(inp.label_ids)


def _sender_matches(scope_values: frozenset[str], from_emails: tuple[str, ...]) -> bool:
    if not scope_values:
        return False
    scopes = {s.strip().lower() for s in scope_values if s.strip()}
    for fe in from_emails:
        fe_l = fe.strip().lower()
        dom = _email_domain(fe)
        if fe_l in scopes or dom in scopes:
            return True
    return False


def _known_transactional_card_types(from_emails: tuple[str, ...]) -> list[str]:
    merged: list[str] = []
    for fe in from_emails:
        types = TRANSACTIONAL_DOMAINS.get(_email_domain(fe))
        if not types:
            continue
        for t in types:
            if t not in merged:
                merged.append(t)
    return merged


def _is_bulk_sender(from_emails: tuple[str, ...]) -> bool:
    if not from_emails:
        return False
    return all(_email_domain(fe) in NOISE_DOMAINS for fe in from_emails)


def _has_owner_participation(inp: EmailPromotionInput) -> bool:
    return inp.owner_sent_message or inp.owner_replied


def _has_back_and_forth(inp: EmailPromotionInput) -> bool:
    """Multi-party multi-message thread with real owner outbound.

    ``message_count >= 2`` and ``participants >= 2`` alone is not enough —
    mailbox owners appear on inbound mail, and notification storms (e.g. GitHub)
    look multi-party without any owner participation. Require the same outbound
    signals as owner participation (From=owner / direction=outbound / flags).
    """

    participants = {e.strip().lower() for e in inp.participant_emails if e.strip()}
    return (
        inp.message_count >= 2
        and len(participants) >= 2
        and _has_owner_participation(inp)
    )

def _is_high_confidence_noise(canonical: str, confidence: float) -> bool:
    return canonical == "noise" and confidence >= SUPPRESS_CONFIDENCE_MIN


def _starred_or_important(labels: frozenset[str]) -> bool:
    return "STARRED" in labels or "IMPORTANT" in labels


def _promotions_marketing_conflict(
    inp: EmailPromotionInput,
    canonical: str,
) -> bool:
    labels = _labels(inp)
    if "CATEGORY_PROMOTIONS" not in labels:
        return False
    if not _starred_or_important(labels):
        return False
    return canonical in {"marketing", "automated", "noise"}


def _effective_card_types(inp: EmailPromotionInput) -> tuple[str, ...]:
    if inp.card_types:
        return inp.card_types
    if inp.known_transactional_card_types:
        return inp.known_transactional_card_types
    known = _known_transactional_card_types(inp.from_emails)
    return tuple(known)


@dataclass
class _RuleOutcome:
    corpus_decision: CorpusDecision
    processor_decision: ProcessorDecision
    decision_reason: str
    decision_signals: list[str]


class EmailPromotionPolicy:
    """Ordered rule evaluation for Gmail thread corpus membership.

    See module docstring for rule order and locked owner-reply + marketing behavior.
    """

    def evaluate(self, inp: EmailPromotionInput) -> EmailPromotionDecision:
        canonical = normalize_canonical_classification(
            inp.classification,
            confidence=inp.confidence,
        )
        card_types = _effective_card_types(inp)
        labels = _labels(inp)
        signals: list[str] = []

        base = self._evaluate_rules(inp, canonical, card_types, labels, signals)
        outcome = base

        forced = self._apply_manual_overrides(inp, canonical, labels, base, signals)
        if forced is not None:
            outcome, pre = forced
            return self._build_decision(
                inp,
                canonical,
                card_types,
                outcome,
                signals,
                pre_override=pre,
            )

        return self._build_decision(inp, canonical, card_types, outcome, signals)

    def _evaluate_rules(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        card_types: tuple[str, ...],
        labels: frozenset[str],
        signals: list[str],
    ) -> _RuleOutcome:
        # Rule 2: owner action overrides.
        owner = self._owner_action_override(inp, canonical, labels, signals)
        if owner is not None:
            return owner

        # Rule 3: transactional promotion.
        transactional = self._transactional_promotion(inp, canonical, card_types, signals)
        if transactional is not None:
            return transactional

        # Rule 4: personal promotion.
        personal = self._personal_promotion(inp, canonical, labels, signals)
        if personal is not None:
            return personal

        # Rule 5: marketing / automated / noise suppression.
        suppressed = self._marketing_suppression(inp, canonical, labels, signals)
        if suppressed is not None:
            return suppressed

        # Rule 6: quarantine paths.
        quarantine = self._quarantine_check(inp, canonical, card_types, labels, signals)
        if quarantine is not None:
            return quarantine

        signals.append("no_matching_rule")
        return _RuleOutcome(
            CorpusDecision.ACTIVE,
            ProcessorDecision.NO_DOWNSTREAM_PROCESSING,
            "default_active_no_downstream",
            signals.copy(),
        )

    def _owner_action_override(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        labels: frozenset[str],
        signals: list[str],
    ) -> _RuleOutcome | None:
        # Rule 2 — owner participation beats marketing/automated/noise suppression (rule 5)
        # and beats the rule-6 quarantine alternative for "marketing + owner replied".
        # Multi-party threads with owner outbound use back_and_forth (still active +
        # enrichment). Bare multi-message/multi-participant without outbound does NOT
        # promote — notification storms and inbound-only promos fall through.
        if _has_back_and_forth(inp):
            signals.append("back_and_forth_participation")
            if inp.owner_sent_message:
                signals.append("owner_sent_message")
            if inp.owner_replied:
                signals.append("owner_replied")
            proc = (
                ProcessorDecision.THREAD_ENRICHMENT
                if canonical in {"personal", "marketing", "automated", "noise", "unknown"}
                else ProcessorDecision.NO_DOWNSTREAM_PROCESSING
            )
            return _RuleOutcome(
                CorpusDecision.ACTIVE,
                proc,
                "back_and_forth_participation",
                signals.copy(),
            )

        if _has_owner_participation(inp):
            reason = "owner_participation"
            if inp.owner_sent_message:
                signals.append("owner_sent_message")
            if inp.owner_replied:
                signals.append("owner_replied")
            proc = (
                ProcessorDecision.THREAD_ENRICHMENT
                if canonical in {"personal", "marketing", "automated", "noise", "unknown"}
                else ProcessorDecision.NO_DOWNSTREAM_PROCESSING
            )
            return _RuleOutcome(CorpusDecision.ACTIVE, proc, reason, signals.copy())

        if _starred_or_important(labels) and not _is_high_confidence_noise(canonical, inp.confidence):
            if not _promotions_marketing_conflict(inp, canonical):
                signals.append("starred_or_important")
                return _RuleOutcome(
                    CorpusDecision.ACTIVE,
                    ProcessorDecision.THREAD_ENRICHMENT,
                    "starred_or_important",
                    signals.copy(),
                )

        if inp.calendar_event_hints:
            signals.append("calendar_event_hint")
            return _RuleOutcome(
                CorpusDecision.ACTIVE,
                ProcessorDecision.THREAD_ENRICHMENT,
                "calendar_event_hint",
                signals.copy(),
            )
        return None

    def _transactional_promotion(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        card_types: tuple[str, ...],
        signals: list[str],
    ) -> _RuleOutcome | None:
        known_types = _known_transactional_card_types(inp.from_emails)
        if known_types and canonical != "marketing":
            signals.append("known_transactional_domain")
            return _RuleOutcome(
                CorpusDecision.ACTIVE,
                ProcessorDecision.TYPED_EXTRACTION,
                "transactional_extractable",
                signals.copy(),
            )

        if canonical == "transactional":
            signals.append("transactional_classification")
            return _RuleOutcome(
                CorpusDecision.ACTIVE,
                ProcessorDecision.TYPED_EXTRACTION,
                "transactional_extractable",
                signals.copy(),
            )

        if (
            card_types
            and inp.confidence >= PROMOTE_TRANSACTIONAL_MIN
            and canonical not in {"marketing", "noise"}
        ):
            signals.append("extractable_card_types")
            return _RuleOutcome(
                CorpusDecision.ACTIVE,
                ProcessorDecision.TYPED_EXTRACTION,
                "transactional_extractable",
                signals.copy(),
            )
        return None

    def _personal_promotion(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        labels: frozenset[str],
        signals: list[str],
    ) -> _RuleOutcome | None:
        if canonical != "personal":
            return None

        has_signal = (
            _has_owner_participation(inp)
            or "CATEGORY_PERSONAL" in labels
            or not _is_bulk_sender(inp.from_emails)
            or inp.message_count >= 2
        )
        if has_signal:
            if _has_owner_participation(inp):
                signals.append("owner_participation")
            if "CATEGORY_PERSONAL" in labels:
                signals.append("category_personal")
            if inp.message_count >= 2:
                signals.append("multi_message_thread")
            return _RuleOutcome(
                CorpusDecision.ACTIVE,
                ProcessorDecision.THREAD_ENRICHMENT,
                "owner_participation" if _has_owner_participation(inp) else "personal_correspondence",
                signals.copy(),
            )

        if inp.confidence < QUARANTINE_BELOW or inp.message_count <= 1:
            signals.append("personal_weak_signals")
            return _RuleOutcome(
                CorpusDecision.QUARANTINE,
                ProcessorDecision.QUARANTINE_REVIEW,
                "personal_weak_signals_quarantine",
                signals.copy(),
            )
        return None

    def _would_suppress_marketing(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        labels: frozenset[str],
    ) -> bool:
        if canonical not in {"marketing", "automated", "noise"}:
            if "CATEGORY_PROMOTIONS" in labels and canonical in {"marketing", "automated", "noise", "unknown"}:
                return inp.confidence >= SUPPRESS_CONFIDENCE_MIN
            return False
        if inp.confidence < SUPPRESS_CONFIDENCE_MIN:
            return False
        if _has_owner_participation(inp):
            return False
        if _starred_or_important(labels):
            return False
        if inp.calendar_event_hints:
            return False
        return True

    def _marketing_suppression(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        labels: frozenset[str],
        signals: list[str],
    ) -> _RuleOutcome | None:
        if not self._would_suppress_marketing(inp, canonical, labels):
            return None

        # Defer to rule 6 when suppression would drop linked derived cards or attachments.
        if inp.has_attachments or inp.has_derived_cards:
            return None

        if "CATEGORY_PROMOTIONS" in labels:
            signals.append("category_promotions")
        signals.append(f"{canonical}_classification")
        reason = (
            "marketing_classification_with_promotions_label"
            if "CATEGORY_PROMOTIONS" in labels and canonical == "marketing"
            else f"{canonical}_high_confidence"
        )
        return _RuleOutcome(
            CorpusDecision.SUPPRESSED,
            ProcessorDecision.SUPPRESSED_NO_PROCESSING,
            reason,
            signals.copy(),
        )

    def _quarantine_check(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        card_types: tuple[str, ...],
        labels: frozenset[str],
        signals: list[str],
    ) -> _RuleOutcome | None:
        if inp.confidence < QUARANTINE_BELOW and canonical in {"automated", "marketing", "noise", "unknown"}:
            if inp.has_attachments:
                signals.append("low_confidence")
                signals.append("has_attachments")
                return _RuleOutcome(
                    CorpusDecision.QUARANTINE,
                    ProcessorDecision.QUARANTINE_REVIEW,
                    "low_confidence_with_attachment",
                    signals.copy(),
                )
            signals.append("low_confidence")
            return _RuleOutcome(
                CorpusDecision.QUARANTINE,
                ProcessorDecision.QUARANTINE_REVIEW,
                "low_confidence",
                signals.copy(),
            )

        if _promotions_marketing_conflict(inp, canonical):
            signals.append("starred_marketing_conflict")
            return _RuleOutcome(
                CorpusDecision.QUARANTINE,
                ProcessorDecision.QUARANTINE_REVIEW,
                "starred_marketing_conflict",
                signals.copy(),
            )

        would_suppress = self._would_suppress_marketing(inp, canonical, labels)
        if would_suppress and inp.has_attachments:
            signals.append("attachment_would_suppress")
            return _RuleOutcome(
                CorpusDecision.QUARANTINE,
                ProcessorDecision.QUARANTINE_REVIEW,
                "low_confidence_with_attachment"
                if inp.confidence < SUPPRESS_CONFIDENCE_MIN
                else "attachment_would_suppress",
                signals.copy(),
            )

        if would_suppress and inp.has_derived_cards:
            signals.append("derived_cards_would_suppress")
            return _RuleOutcome(
                CorpusDecision.QUARANTINE,
                ProcessorDecision.QUARANTINE_REVIEW,
                "derived_cards_would_suppress",
                signals.copy(),
            )

        if canonical == "unknown":
            signals.append("unknown_classification")
            return _RuleOutcome(
                CorpusDecision.QUARANTINE,
                ProcessorDecision.QUARANTINE_REVIEW,
                "missing_classification",
                signals.copy(),
            )

        return None

    def _apply_manual_overrides(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        labels: frozenset[str],
        base: _RuleOutcome,
        signals: list[str],
    ) -> tuple[_RuleOutcome, _RuleOutcome] | None:
        ov = inp.manual_overrides
        pre = base

        if ov.always_active_thread:
            signals.append("manual_always_active_thread")
            return (
                _RuleOutcome(
                    CorpusDecision.ACTIVE,
                    base.processor_decision
                    if base.corpus_decision == CorpusDecision.ACTIVE
                    else ProcessorDecision.THREAD_ENRICHMENT,
                    "manual_override_always_active_thread",
                    signals.copy(),
                ),
                pre,
            )

        if ov.always_suppress_thread and not inp.has_derived_cards:
            signals.append("manual_always_suppress_thread")
            return (
                _RuleOutcome(
                    CorpusDecision.SUPPRESSED,
                    ProcessorDecision.SUPPRESSED_NO_PROCESSING,
                    "manual_override_always_suppress_thread",
                    signals.copy(),
                ),
                pre,
            )

        if _sender_matches(ov.always_active_senders, inp.from_emails):
            signals.append("manual_always_active_sender")
            return (
                _RuleOutcome(
                    CorpusDecision.ACTIVE,
                    ProcessorDecision.THREAD_ENRICHMENT,
                    "manual_override_always_active_sender",
                    signals.copy(),
                ),
                pre,
            )

        if _sender_matches(ov.always_suppress_senders, inp.from_emails):
            signals.append("manual_always_suppress_sender")
            return (
                _RuleOutcome(
                    CorpusDecision.SUPPRESSED,
                    ProcessorDecision.SUPPRESSED_NO_PROCESSING,
                    "manual_override_always_suppress_sender",
                    signals.copy(),
                ),
                pre,
            )

        if ov.always_keep_starred and "STARRED" in labels:
            signals.append("manual_always_keep_starred")
            return (
                _RuleOutcome(
                    CorpusDecision.ACTIVE,
                    ProcessorDecision.THREAD_ENRICHMENT,
                    "manual_override_always_keep_starred",
                    signals.copy(),
                ),
                pre,
            )

        if ov.always_keep_important and "IMPORTANT" in labels:
            signals.append("manual_always_keep_important")
            return (
                _RuleOutcome(
                    CorpusDecision.ACTIVE,
                    ProcessorDecision.THREAD_ENRICHMENT,
                    "manual_override_always_keep_important",
                    signals.copy(),
                ),
                pre,
            )

        return None

    def _build_decision(
        self,
        inp: EmailPromotionInput,
        canonical: str,
        card_types: tuple[str, ...],
        outcome: _RuleOutcome,
        signals: list[str],
        *,
        pre_override: _RuleOutcome | None = None,
    ) -> EmailPromotionDecision:
        return EmailPromotionDecision(
            source_key=inp.source_key,
            external_id=inp.gmail_thread_id,
            external_history_id=inp.gmail_history_id,
            content_hash=inp.thread_body_sha,
            policy_version=EMAIL_PROMOTION_POLICY_VERSION,
            classification=inp.classification,
            canonical_classification=canonical,
            confidence=inp.confidence,
            card_types=card_types,
            classification_source=inp.classification_source,
            corpus_decision=outcome.corpus_decision,
            processor_decision=outcome.processor_decision,
            decision_reason=outcome.decision_reason,
            decision_signals=tuple(dict.fromkeys(outcome.decision_signals)),
            evaluated_at=_utc_now_iso(),
            pre_override_corpus_decision=pre_override.corpus_decision if pre_override else None,
            pre_override_processor_decision=pre_override.processor_decision if pre_override else None,
            pre_override_reason=pre_override.decision_reason if pre_override else None,
        )


def summarize_decisions(decisions: list[EmailPromotionDecision]) -> dict[str, Any]:
    """Aggregate counts for policy dry-run reports."""

    by_corpus: dict[str, int] = {}
    by_processor: dict[str, int] = {}
    by_canonical: dict[str, int] = {}
    by_source: dict[str, int] = {}
    by_reason: dict[str, int] = {}
    override_count = 0

    for d in decisions:
        by_corpus[d.corpus_decision.value] = by_corpus.get(d.corpus_decision.value, 0) + 1
        by_processor[d.processor_decision.value] = by_processor.get(d.processor_decision.value, 0) + 1
        by_canonical[d.canonical_classification] = by_canonical.get(d.canonical_classification, 0) + 1
        src = d.classification_source or "(unknown)"
        by_source[src] = by_source.get(src, 0) + 1
        by_reason[d.decision_reason] = by_reason.get(d.decision_reason, 0) + 1
        if d.pre_override_corpus_decision is not None:
            override_count += 1

    return {
        "total_evaluated": len(decisions),
        "by_corpus_decision": by_corpus,
        "by_processor_decision": by_processor,
        "by_canonical_classification": by_canonical,
        "by_classification_source": by_source,
        "by_decision_reason": by_reason,
        "override_count": override_count,
    }


def build_policy_report(
    decisions: list[EmailPromotionDecision],
    *,
    gate: str = SECTION_A_GATE_NAME,
    run_id: str = SECTION_A_RUN_ID,
    engine_mode: str = "n/a",
) -> dict[str, Any]:
    """Section A policy decision report shape (Gate 1 synthetic fixtures)."""

    report: dict[str, Any] = {
        "run_id": run_id,
        "gate": gate,
        "ladder_gate": SECTION_A_VALIDATION_MATRIX_GATE,
        "policy_version": EMAIL_PROMOTION_POLICY_VERSION,
        "engine_mode": engine_mode,
        "thresholds": {
            "SUPPRESS_CONFIDENCE_MIN": SUPPRESS_CONFIDENCE_MIN,
            "PROMOTE_TRANSACTIONAL_MIN": PROMOTE_TRANSACTIONAL_MIN,
            "QUARANTINE_BELOW": QUARANTINE_BELOW,
        },
        "summary": summarize_decisions(decisions),
        "decisions": [d.to_dict() for d in decisions],
        "safety": {
            "production_mutation": False,
            "vault_access": False,
            "llm_calls": False,
        },
    }
    if gate == SECTION_A_GATE_NAME:
        report["completion_state"] = SECTION_A_COMPLETION_STATE
        report["gate_framework_state"] = SECTION_G_FRAMEWORK_STATE
        report["section_g_framework"] = SECTION_G_FRAMEWORK_STATE
        report["section_b_apply_unlocked"] = False
        report["next_recommended_gate"] = "small_slice"
        report["blocked_without_section_g"] = []
        from archive_cli.validation_gates.constants import VALIDATION_GATE_LOG_ROOT

        report["artifact_root"] = f"ppa/logs/{VALIDATION_GATE_LOG_ROOT}/gate-{gate}/{run_id}/"
    return report


def gate_artifact_dir(
    repo_root: str | Any,
    *,
    gate: str = SECTION_A_GATE_NAME,
    run_id: str = SECTION_A_RUN_ID,
) -> Any:
    """Return validation gate artifact directory for this run."""

    from archive_cli.validation_gates.report import gate_artifact_dir as _gate_artifact_dir

    return _gate_artifact_dir(repo_root, gate=gate, run_id=run_id)


def write_samples_jsonl(path: str | Any, decisions: list[EmailPromotionDecision]) -> None:
    from pathlib import Path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(d.to_dict(), sort_keys=True) for d in decisions]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_section_a_gate_artifacts(
    repo_root: str | Any,
    decisions: list[EmailPromotionDecision],
    *,
    run_id: str = SECTION_A_RUN_ID,
) -> dict[str, str]:
    """Write Gate 1 report.json, summary.md, and samples.jsonl under validation gate logs."""

    report = build_policy_report(decisions, run_id=run_id)
    out_dir = gate_artifact_dir(repo_root, run_id=run_id)
    report_path = out_dir / "report.json"
    summary_path = out_dir / "summary.md"
    samples_path = out_dir / "samples.jsonl"

    write_policy_report(report_path, report)
    write_samples_jsonl(samples_path, decisions)

    summary_lines = [
        "# Section A — Gate 1 Synthetic Fixtures",
        "",
        f"**Completion state:** `{SECTION_A_COMPLETION_STATE}`",
        f"**Validation gate framework:** `{SECTION_G_FRAMEWORK_STATE}`",
        f"**Section B apply unlocked:** no",
        "",
        f"- policy_version: `{EMAIL_PROMOTION_POLICY_VERSION}`",
        f"- ladder_gate: {SECTION_A_VALIDATION_MATRIX_GATE}",
        f"- run_id: `{run_id}`",
        f"- total_evaluated: {report['summary']['total_evaluated']}",
        "",
        "## Locked behavior: owner reply + marketing",
        "",
        "Rule 2 (owner participation) intentionally wins over rule 5 suppression and",
        "over the rule-6 quarantine alternative listed for marketing/noise + owner reply.",
        "Result: `active` + `thread_enrichment`, reason `owner_participation`.",
        "Passive STARRED + CATEGORY_PROMOTIONS + marketing quarantines (`starred_marketing_conflict`).",
        "",
        "## Corpus decisions",
        "",
    ]
    for k, v in sorted(report["summary"]["by_corpus_decision"].items()):
        summary_lines.append(f"- {k}: {v}")
    summary_lines.extend(["", "## Required examples", ""])
    for d in decisions:
        summary_lines.append(
            f"- `{d.external_id}`: {d.corpus_decision.value} / "
            f"{d.processor_decision.value} ({d.decision_reason})"
        )
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "report": str(report_path),
        "summary": str(summary_path),
        "samples": str(samples_path),
    }


def write_policy_report(path: str | Any, report: dict[str, Any]) -> None:
    from pathlib import Path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
