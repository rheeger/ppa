"""Tests for archive_sync.extractors.base."""

from __future__ import annotations

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.extractors.base import EmailExtractor, ExtractionResult, TemplateVersion, _in_date_range
from hfa.schema import MealOrderCard


def test_template_version_date_range():
    tv = TemplateVersion("x", ("2020-01-01", "2021-12-31"), lambda fm, b: [])
    assert tv.date_range == ("2020-01-01", "2021-12-31")


def test_matches_sender_pattern():
    class E(EmailExtractor):
        sender_patterns = [r".*@doordash\.com$"]
        output_card_type = "meal_order"

        def template_versions(self):
            return []

        def summary_only_fallback(self, fm, body, suid, srp):
            return []

    assert E().matches("x@doordash.com", "hi")
    assert not E().matches("x@uber.com", "hi")


def test_matches_subject_pattern_narrows():
    class E(EmailExtractor):
        sender_patterns = [r".*@uber\.com$"]
        subject_patterns = [r".*ride.*"]
        output_card_type = "ride"

        def template_versions(self):
            return []

        def summary_only_fallback(self, fm, body, suid, srp):
            return []

    assert E().matches("x@uber.com", "Your ride receipt")
    assert not E().matches("x@uber.com", "Newsletter")


def test_generate_derived_uid_deterministic():
    class E(EmailExtractor):
        sender_patterns = []
        output_card_type = "meal_order"

        def template_versions(self):
            return []

        def summary_only_fallback(self, fm, body, suid, srp):
            return []

    ex = E()
    a = ex.generate_derived_uid("hfa-email-message-abc", "r1")
    b = ex.generate_derived_uid("hfa-email-message-abc", "r1")
    assert a == b
    assert a.startswith("hfa-meal_order-")


def test_generate_derived_uid_unique_across_discriminators():
    class E(EmailExtractor):
        sender_patterns = []
        output_card_type = "meal_order"

        def template_versions(self):
            return []

        def summary_only_fallback(self, fm, body, suid, srp):
            return []

    ex = E()
    assert ex.generate_derived_uid("hfa-email-message-abc", "a") != ex.generate_derived_uid(
        "hfa-email-message-abc", "b"
    )


def test_extract_tries_templates_newest_first():
    class E(EmailExtractor):
        sender_patterns = []
        output_card_type = "meal_order"

        def template_versions(self):
            def new_p(fm, body):
                if "USE_NEW" in body:
                    return [
                        {
                            "_discriminator": "new",
                            "service": "S",
                            "restaurant": "NewTemplate",
                            "items": [],
                            "_body": "n",
                        }
                    ]
                return []

            def old_p(fm, body):
                if "USE_OLD" in body:
                    return [
                        {
                            "_discriminator": "old",
                            "service": "S",
                            "restaurant": "OldTemplate",
                            "items": [],
                            "_body": "o",
                        }
                    ]
                return []

            return [
                TemplateVersion("newest", ("2000-01-01", "2099-12-31"), new_p),
                TemplateVersion("older", ("2000-01-01", "2099-12-31"), old_p),
            ]

        def summary_only_fallback(self, fm, body, suid, srp):
            return []

    ex = E()
    fm = {"sent_at": "2024-01-01T12:00:00Z"}
    out = ex.extract(fm, "USE_NEW\nUSE_OLD", "hfa-email-message-x", "Email/x.md")
    assert len(out) == 1
    assert out[0].card.restaurant == "NewTemplate"


def test_extract_falls_back_to_summary_only():
    class E(EmailExtractor):
        sender_patterns = []
        output_card_type = "meal_order"

        def template_versions(self):
            def empty(fm, body):
                return []

            return [TemplateVersion("a", ("2000-01-01", "2099-12-31"), empty)]

        def summary_only_fallback(self, fm, body, suid, srp):
            uid = self.generate_derived_uid(suid, f"summary:{suid}")
            card = MealOrderCard(
                uid=uid,
                type="meal_order",
                source=["email_extraction"],
                source_id=uid,
                created="2024-01-01",
                updated="2024-01-01",
                summary="fb",
                service="S",
                restaurant="Fallback",
                source_email=f"[[{suid}]]",
            )
            prov = deterministic_provenance(card, "email_extraction")
            return [ExtractionResult(card, prov, "b", suid, "p.md")]

    ex = E()
    out = ex.extract({"sent_at": "2024-01-01"}, "x", "hfa-email-message-x", "p.md")
    assert len(out) == 1
    assert out[0].card.restaurant == "Fallback"


def test_in_date_range_inclusive():
    assert _in_date_range("2024-06-15T00:00:00Z", "2024-01-01", "2024-12-31")
    assert not _in_date_range("2023-06-15T00:00:00Z", "2024-01-01", "2024-12-31")
