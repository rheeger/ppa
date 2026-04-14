"""Tests for extraction_confidence scoring."""

from __future__ import annotations

from archive_sync.extractors.field_metrics import compute_extraction_confidence


def test_confidence_all_fields_populated():
    fm = {
        "restaurant": "Mixt",
        "items": [{"name": "x"}],
        "total": 12.0,
    }
    assert compute_extraction_confidence("meal_order", fm) == 1.0


def test_confidence_half_fields_populated():
    fm = {
        "restaurant": "Mixt",
        "items": [],
        "total": 12.0,
    }
    assert compute_extraction_confidence("meal_order", fm) == 0.67


def test_confidence_no_critical_fields_defined():
    assert compute_extraction_confidence("unknown_type_xyz", {}) == 1.0


def test_confidence_written_to_frontmatter(extractor_vault, tmp_path, sample_email_card):
    from archive_sync.extractors.registry import build_default_registry
    from archive_sync.extractors.runner import ExtractionRunner
    from archive_tests.archive_sync.extractors.conftest import write_email_to_vault

    fm, body = sample_email_card(
        "hfa-email-message-conf",
        "orders@doordash.com",
        "Your order from Mixt",
        "Subtotal $10\nTotal $12",
        sent_at="2023-06-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/conf.md", fm, body)
    staging = tmp_path / "st"
    runner = ExtractionRunner(
        vault_path=extractor_vault,
        registry=build_default_registry(),
        staging_dir=str(staging),
        workers=1,
        batch_size=10,
        sender_filter="doordash",
    )
    runner.run()
    cards = list(staging.rglob("*.md"))
    assert cards
    text = cards[0].read_text(encoding="utf-8")
    assert "extraction_confidence:" in text
