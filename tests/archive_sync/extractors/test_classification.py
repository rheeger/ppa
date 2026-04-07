"""Tests for should_extract / summary_only_fallback defaults."""

from __future__ import annotations

from archive_sync.extractors.base import EmailExtractor, TemplateVersion


def test_should_extract_rejects_marketing_subject():
    class E(EmailExtractor):
        sender_patterns = [r".*@x\.com$"]
        output_card_type = "meal_order"
        reject_subject_patterns = [r"(?i).*\bdeal\b.*"]

        def template_versions(self):
            return []

    ex = E()
    assert not ex.should_extract("Big deal today", "order from joe subtotal")


def test_should_extract_accepts_receipt_body_when_indicators_set():
    class E(EmailExtractor):
        sender_patterns = [r".*@x\.com$"]
        output_card_type = "meal_order"
        receipt_indicators = ["subtotal", "total"]

        def template_versions(self):
            return []

    ex = E()
    assert ex.should_extract("Your order", "Subtotal: $5\nTotal: $6")


def test_should_extract_default_accepts_all():
    class E(EmailExtractor):
        sender_patterns = [r".*@x\.com$"]
        output_card_type = "meal_order"

        def template_versions(self):
            return []

    ex = E()
    assert ex.should_extract("anything", "anything")


def test_summary_only_fallback_default_returns_empty():
    class E(EmailExtractor):
        sender_patterns = []
        output_card_type = "meal_order"

        def template_versions(self):
            def empty(fm, body):
                return []

            return [TemplateVersion("a", ("2000-01-01", "2099-12-31"), empty)]

    ex = E()
    out = ex.extract({"sent_at": "2024-01-01"}, "x", "hfa-email-message-x", "p.md")
    assert out == []
