"""Section B corpus hygiene constants."""

from __future__ import annotations

from archive_cli.validation_gates.constants import GATE_SYNTHETIC_FIXTURES

SECTION_B_COMPLETION_STATE = "section_b_dry_run_complete"
SECTION_B_APPLY_COMPLETION_STATE = "section_b_apply_rollback_complete"
SECTION_B_DRY_RUN_GATE = GATE_SYNTHETIC_FIXTURES
SECTION_B_CENSUS_ARTIFACT_GATE = "corpus_hygiene_email_dry_run"
SECTION_B_APPLY_ARTIFACT_GATE = "corpus_hygiene_email_apply"
SECTION_B_ROLLBACK_ARTIFACT_GATE = "corpus_hygiene_email_rollback"

CLASSIFICATION_SOURCES: tuple[str, ...] = (
    "card_classifications",
    "classify_index",
    "frontmatter",
    "stage0",
    "new_llm",
    "missing",
)

REVIEW_BUCKETS: tuple[str, ...] = (
    "high_confidence_marketing",
    "promotions_label_suppression",
    "automated_noise",
    "suppressed_with_attachments",
    "suppressed_with_derived_cards",
    "quarantine_conflicts",
    "active_overrides",
    "unknown_classification",
)
