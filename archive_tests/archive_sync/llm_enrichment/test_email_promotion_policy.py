"""Section A — EmailPromotionPolicy fixture tests (no vault, no LLM)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_sync.llm_enrichment.email_promotion_policy import (
    EMAIL_PROMOTION_POLICY_VERSION,
    SECTION_A_COMPLETION_STATE,
    SECTION_A_GATE_NAME,
    SECTION_A_RUN_ID,
    SECTION_A_VALIDATION_MATRIX_GATE,
    SECTION_G_FRAMEWORK_STATE,
    CorpusDecision,
    EmailPromotionInput,
    EmailPromotionPolicy,
    ManualOverrides,
    ProcessorDecision,
    QUARANTINE_BELOW,
    SUPPRESS_CONFIDENCE_MIN,
    build_policy_report,
    normalize_canonical_classification,
    write_policy_report,
    write_section_a_gate_artifacts,
)

_POLICY = EmailPromotionPolicy()
_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "v2_5"
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _decide(**kwargs: object):
    inp = EmailPromotionInput(**kwargs)  # type: ignore[arg-type]
    return _POLICY.evaluate(inp)


# ---------------------------------------------------------------------------
# Required Section A examples
# ---------------------------------------------------------------------------


def test_marketing_newsletter_suppressed() -> None:
    d = _decide(
        source_key="gmail-messages:owner@example.com",
        gmail_thread_id="t-marketing",
        label_ids=("CATEGORY_PROMOTIONS",),
        classification="marketing",
        confidence=0.91,
        classification_source="classify_index",
    )
    assert d.corpus_decision == CorpusDecision.SUPPRESSED
    assert d.processor_decision == ProcessorDecision.SUPPRESSED_NO_PROCESSING
    assert d.decision_reason == "marketing_classification_with_promotions_label"


def test_transactional_receipt_active_typed_extraction() -> None:
    d = _decide(
        gmail_thread_id="t-receipt",
        from_emails=("receipts@doordash.com",),
        classification="transactional",
        card_types=("meal_order",),
        confidence=0.84,
        classification_source="card_classifications",
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.processor_decision == ProcessorDecision.TYPED_EXTRACTION
    assert d.decision_reason == "transactional_extractable"
    assert d.canonical_classification == "transactional"
    assert "meal_order" in d.card_types


def test_personal_reply_thread_active_enrichment() -> None:
    d = _decide(
        gmail_thread_id="t-personal",
        classification="personal",
        confidence=0.88,
        owner_sent_message=True,
        participant_emails=("owner@example.com", "friend@gmail.com"),
        message_count=4,
        classification_source="classify_index",
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.processor_decision == ProcessorDecision.THREAD_ENRICHMENT
    # Multi-party + owner outbound → back_and_forth (still active enrichment)
    assert d.decision_reason == "back_and_forth_participation"


def test_github_notification_storm_not_back_and_forth() -> None:
    """Multi-message GitHub notifications without owner outbound must not go active via back_and_forth."""

    d = _decide(
        gmail_thread_id="t-github-storm",
        classification="",
        confidence=0.0,
        classification_source="missing",
        participant_emails=(
            "notifications@github.com",
            "endaoment-frontend@noreply.github.com",
            "subscribed@noreply.github.com",
        ),
        from_emails=("notifications@github.com",),
        message_count=22,
        owner_sent_message=False,
        owner_replied=False,
    )
    assert d.decision_reason != "back_and_forth_participation"
    assert d.corpus_decision != CorpusDecision.ACTIVE or d.decision_reason != "back_and_forth_participation"
    # Missing/unknown → quarantine
    assert d.corpus_decision == CorpusDecision.QUARANTINE
    assert d.decision_reason in {"missing_classification", "low_confidence", "low_confidence_with_attachment"}


def test_bay_clubs_inbound_promo_not_back_and_forth() -> None:
    """Two inbound promo messages with owner only in participants ≠ back_and_forth."""

    d = _decide(
        gmail_thread_id="t-bay-clubs",
        classification="",
        confidence=0.0,
        classification_source="missing",
        label_ids=("CATEGORY_PROMOTIONS",),
        participant_emails=("bayclubs@mail-bayclubs.com", "owner@example.com"),
        from_emails=("bayclubs@mail-bayclubs.com",),
        message_count=2,
        owner_email="owner@example.com",
        owner_sent_message=False,
        owner_replied=False,
    )
    assert d.decision_reason != "back_and_forth_participation"
    assert d.corpus_decision == CorpusDecision.QUARANTINE


def test_real_reply_thread_stays_active_via_back_and_forth() -> None:
    """Owner outbound + multi-party multi-message → active (back_and_forth)."""

    d = _decide(
        gmail_thread_id="t-aspiriant-like",
        classification="",
        confidence=0.0,
        classification_source="missing",
        participant_emails=("lsanchez@aspiriant.com", "owner@example.com", "hdietz@aspiriant.com"),
        from_emails=("lsanchez@aspiriant.com", "owner@example.com"),
        message_count=4,
        owner_email="owner@example.com",
        owner_sent_message=True,
        owner_replied=False,
        label_ids=("IMPORTANT", "CATEGORY_PERSONAL", "SENT"),
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.decision_reason == "back_and_forth_participation"
    assert "owner_sent_message" in d.decision_signals


def test_starred_promotional_thread_quarantine() -> None:
    d = _decide(
        gmail_thread_id="t-starred-promo",
        label_ids=("CATEGORY_PROMOTIONS", "STARRED"),
        classification="marketing",
        confidence=0.91,
        classification_source="classify_index",
    )
    assert d.corpus_decision == CorpusDecision.QUARANTINE
    assert d.processor_decision == ProcessorDecision.QUARANTINE_REVIEW
    assert d.decision_reason == "starred_marketing_conflict"


def test_low_confidence_attachment_thread_quarantine() -> None:
    d = _decide(
        gmail_thread_id="t-attach",
        classification="automated",
        confidence=0.42,
        has_attachments=True,
        classification_source="classify_index",
    )
    assert d.corpus_decision == CorpusDecision.QUARANTINE
    assert d.processor_decision == ProcessorDecision.QUARANTINE_REVIEW
    assert d.decision_reason == "low_confidence_with_attachment"


# ---------------------------------------------------------------------------
# Category normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("transactional_receipt", "transactional"),
        ("booking_confirmation", "transactional"),
        ("purchase_receipt", "transactional"),
        ("person_to_person", "personal"),
        ("promotion", "marketing"),
        ("automated_notification", "automated"),
        ("skip", "noise"),
        ("", "noise"),
        (None, "noise"),
    ],
)
def test_normalize_canonical_classification(raw: str | None, expected: str) -> None:
    conf = 0.0 if raw in ("", None) else 0.8
    assert normalize_canonical_classification(raw, confidence=conf) == expected


def test_missing_classification_high_confidence_maps_to_unknown() -> None:
    assert normalize_canonical_classification(None, confidence=0.8) == "unknown"
    assert normalize_canonical_classification("", confidence=0.8) == "unknown"


def test_empty_classifier_low_confidence_maps_to_noise() -> None:
    assert normalize_canonical_classification("", confidence=0.2) == "noise"


# ---------------------------------------------------------------------------
# Rule ordering and invariants
# ---------------------------------------------------------------------------


def test_personal_active_without_typed_extraction() -> None:
    """Personal correspondence stays active even when not typed-extractable."""
    d = _decide(
        classification="personal",
        confidence=0.82,
        owner_sent_message=True,
        message_count=3,
        card_types=(),
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.processor_decision == ProcessorDecision.THREAD_ENRICHMENT
    assert d.processor_decision != ProcessorDecision.TYPED_EXTRACTION


def test_high_confidence_marketing_suppressed_without_overrides() -> None:
    d = _decide(
        classification="marketing",
        confidence=SUPPRESS_CONFIDENCE_MIN,
        label_ids=(),
    )
    assert d.corpus_decision == CorpusDecision.SUPPRESSED


def test_owner_reply_marketing_rule2_wins_over_rule6_quarantine() -> None:
    """Plan rule 6 lists marketing+owner-reply as quarantine; rule 2 wins → active."""
    d = _decide(
        classification="marketing",
        confidence=0.92,
        owner_replied=True,
        label_ids=("CATEGORY_PROMOTIONS",),
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.processor_decision == ProcessorDecision.THREAD_ENRICHMENT
    assert d.decision_reason == "owner_participation"
    assert "owner_replied" in d.decision_signals


def test_owner_reply_overrides_marketing_suppression_to_active() -> None:
    """Alias for locked owner-reply + marketing behavior (rule 2 over rule 5/6)."""
    test_owner_reply_marketing_rule2_wins_over_rule6_quarantine()


def test_manual_override_records_pre_override_recommendation() -> None:
    d = _decide(
        classification="marketing",
        confidence=0.95,
        label_ids=("CATEGORY_PROMOTIONS",),
        manual_overrides=ManualOverrides(always_active_thread=True),
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.pre_override_corpus_decision == CorpusDecision.SUPPRESSED
    assert d.decision_reason == "manual_override_always_active_thread"


def test_rule_order_starred_conflict_beats_suppression() -> None:
    """STARRED blocks suppression; conflict lands in quarantine not active."""
    d = _decide(
        classification="marketing",
        confidence=0.95,
        label_ids=("CATEGORY_PROMOTIONS", "STARRED"),
    )
    assert d.corpus_decision == CorpusDecision.QUARANTINE
    assert d.corpus_decision != CorpusDecision.SUPPRESSED


def test_transactional_known_domain_without_classifier_category() -> None:
    d = _decide(
        from_emails=("noreply@doordash.com",),
        classification=None,
        confidence=0.0,
        card_types=(),
    )
    assert d.corpus_decision == CorpusDecision.ACTIVE
    assert d.processor_decision == ProcessorDecision.TYPED_EXTRACTION


def test_derived_cards_quarantine_instead_of_suppress() -> None:
    d = _decide(
        classification="marketing",
        confidence=0.9,
        has_derived_cards=True,
    )
    assert d.corpus_decision == CorpusDecision.QUARANTINE
    assert d.decision_reason == "derived_cards_would_suppress"


def test_policy_version_constant_documented() -> None:
    assert EMAIL_PROMOTION_POLICY_VERSION == "email-promotion-v1"


def test_quarantine_threshold_constant() -> None:
    assert QUARANTINE_BELOW == 0.50


# ---------------------------------------------------------------------------
# Golden fixture report (Gate 1 synthetic)
# ---------------------------------------------------------------------------


def _required_example_inputs() -> list[EmailPromotionInput]:
    return [
        EmailPromotionInput(
            source_key="gmail-messages:owner@example.com",
            gmail_thread_id="t-marketing",
            label_ids=("CATEGORY_PROMOTIONS",),
            classification="marketing",
            confidence=0.91,
            classification_source="classify_index",
        ),
        EmailPromotionInput(
            gmail_thread_id="t-receipt",
            from_emails=("receipts@doordash.com",),
            classification="transactional",
            card_types=("meal_order",),
            confidence=0.84,
            classification_source="card_classifications",
        ),
        EmailPromotionInput(
            gmail_thread_id="t-personal",
            classification="personal",
            confidence=0.88,
            owner_sent_message=True,
            participant_emails=("owner@example.com", "friend@gmail.com"),
            message_count=4,
            classification_source="classify_index",
        ),
        EmailPromotionInput(
            gmail_thread_id="t-starred-promo",
            label_ids=("CATEGORY_PROMOTIONS", "STARRED"),
            classification="marketing",
            confidence=0.91,
            classification_source="classify_index",
        ),
        EmailPromotionInput(
            gmail_thread_id="t-attach",
            classification="automated",
            confidence=0.42,
            has_attachments=True,
            classification_source="classify_index",
        ),
    ]


def test_build_policy_report_shape() -> None:
    decisions = [_POLICY.evaluate(i) for i in _required_example_inputs()]
    report = build_policy_report(decisions)
    assert report["policy_version"] == EMAIL_PROMOTION_POLICY_VERSION
    assert report["gate"] == SECTION_A_GATE_NAME
    assert report["ladder_gate"] == SECTION_A_VALIDATION_MATRIX_GATE
    assert report["run_id"] == SECTION_A_RUN_ID
    assert report["completion_state"] == SECTION_A_COMPLETION_STATE
    assert report["section_g_framework"] == SECTION_G_FRAMEWORK_STATE
    assert report["section_b_apply_unlocked"] is False
    assert report["next_recommended_gate"] == "small_slice"
    assert report["blocked_without_section_g"] == []
    assert report["engine_mode"] == "n/a"
    assert report["safety"]["production_mutation"] is False
    assert report["safety"]["vault_access"] is False
    assert report["safety"]["llm_calls"] is False
    assert report["summary"]["total_evaluated"] == 5
    assert set(report["summary"]["by_corpus_decision"]) == {"active", "suppressed", "quarantine"}
    first = report["decisions"][0]
    for key in (
        "corpus_decision",
        "processor_decision",
        "decision_reason",
        "decision_signals",
        "policy_version",
        "classification_source",
        "canonical_classification",
    ):
        assert key in first


def test_write_section_a_completion_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_ts = "2026-06-01T02:01:24+00:00"
    monkeypatch.setattr(
        "archive_sync.llm_enrichment.email_promotion_policy._utc_now_iso",
        lambda: fixed_ts,
    )
    decisions = [_POLICY.evaluate(i) for i in _required_example_inputs()]
    report = build_policy_report(decisions)

    samples_path = _FIXTURES_DIR / "email_promotion_sample_decisions.json"
    report_path = _FIXTURES_DIR / "email_promotion_policy_report.json"

    samples_path.parent.mkdir(parents=True, exist_ok=True)
    samples_path.write_text(
        json.dumps([d.to_dict() for d in decisions], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_policy_report(report_path, report)

    gate_paths = write_section_a_gate_artifacts(_REPO_ROOT, decisions)
    assert Path(gate_paths["report"]).is_file()
    assert Path(gate_paths["summary"]).is_file()
    assert Path(gate_paths["samples"]).is_file()

    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    assert loaded["policy_version"] == EMAIL_PROMOTION_POLICY_VERSION
    assert loaded["run_id"] == SECTION_A_RUN_ID
    assert len(loaded["decisions"]) == 5
