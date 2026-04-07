"""Tests for ExtractionRunner."""

from __future__ import annotations

from pathlib import Path

from archive_sync.extractors.base import EmailExtractor, TemplateVersion
from archive_sync.extractors.registry import ExtractorRegistry
from archive_sync.extractors.runner import ExtractionRunner
from hfa.schema import PersonCard
from hfa.uid import generate_uid
from hfa.vault import write_card
from tests.archive_sync.extractors.conftest import write_email_to_vault


class AcmeExtractor(EmailExtractor):
    output_card_type = "meal_order"
    sender_patterns = [r".*@acme\.com$"]

    def template_versions(self):
        def p(fm, body):
            return [
                {
                    "_discriminator": "diner",
                    "service": "Acme",
                    "restaurant": "Diner",
                    "items": [],
                    "_body": "# Acme\n",
                }
            ]

        return [TemplateVersion("t", ("2000-01-01", "2099-12-31"), p)]

    def summary_only_fallback(self, fm, body, suid, srp):
        return []


class BetaExtractor(EmailExtractor):
    output_card_type = "meal_order"
    sender_patterns = [r".*@beta\.com$"]

    def template_versions(self):
        def p(fm, body):
            return [
                {
                    "_discriminator": "b",
                    "service": "Beta",
                    "restaurant": "B",
                    "items": [],
                    "_body": "# B\n",
                }
            ]

        return [TemplateVersion("t", ("2000-01-01", "2099-12-31"), p)]

    def summary_only_fallback(self, fm, body, suid, srp):
        return []


def _write_email(vault: str, sample_email_card, uid: str, from_addr: str, subj: str, body: str, relp: str):
    fm, b = sample_email_card(uid, from_addr, subj, body)
    write_email_to_vault(vault, relp, fm, b)


def test_scan_identifies_email_messages_only(extractor_vault, sample_email_card):
    uid = generate_uid("person", "test", "p1")
    p = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=uid,
        created="2024-03-15",
        updated="2024-03-15",
        summary="P",
        first_name="Pat",
        last_name="Smith",
    )
    from archive_sync.adapters.base import deterministic_provenance

    write_card(extractor_vault, "People/pat-smith.md", p, "", deterministic_provenance(p, "test"))
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "sub",
        "x",
        "Email/2024-03/hfa-email-message-e1.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    m = ExtractionRunner(extractor_vault, reg).run()
    assert m.total_emails_scanned == 1
    assert m.matched_emails == 1


def test_match_dispatches_to_correct_extractor(extractor_vault, sample_email_card):
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "s",
        "b",
        "Email/2024-03/e1.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    m = ExtractionRunner(extractor_vault, reg).run()
    assert m.matched_emails == 1
    assert m.per_extractor.get("acme", {}).get("matched") == 1


def test_dry_run_produces_counts_without_writing(extractor_vault, sample_email_card, tmp_path):
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "s",
        "b",
        "Email/2024-03/e1.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    m = ExtractionRunner(extractor_vault, reg, dry_run=True).run()
    assert m.extracted_cards == 1
    assert not list(Path(extractor_vault).rglob("hfa-meal_order-*.md"))


def test_sender_filter_restricts_to_one_extractor(extractor_vault, sample_email_card):
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "s",
        "b",
        "Email/2024-03/e1.md",
    )
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e2",
        "b@beta.com",
        "s",
        "b",
        "Email/2024-03/e2.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    reg.register(BetaExtractor())
    m = ExtractionRunner(extractor_vault, reg, sender_filter="acme").run()
    assert m.matched_emails == 1
    assert "beta" not in m.per_extractor or m.per_extractor["beta"].get("matched", 0) == 0


def test_limit_stops_after_n_matches(extractor_vault, sample_email_card):
    for i in range(10):
        _write_email(
            extractor_vault,
            sample_email_card,
            f"hfa-email-message-e{i}",
            "a@acme.com",
            "s",
            "b",
            f"Email/2024-03/e{i}.md",
        )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    m = ExtractionRunner(extractor_vault, reg, limit=5).run()
    assert m.matched_emails == 5


def test_staging_dir_writes_to_staging(extractor_vault, sample_email_card, tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "s",
        "b",
        "Email/2024-03/e1.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    ExtractionRunner(extractor_vault, reg, staging_dir=str(staging)).run()
    assert list(staging.rglob("hfa-meal_order-*.md"))
    assert not list(Path(extractor_vault).rglob("hfa-meal_order-*.md"))


def test_idempotency_skips_existing(extractor_vault, sample_email_card, tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "s",
        "b",
        "Email/2024-03/e1.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    r1 = ExtractionRunner(extractor_vault, reg, staging_dir=str(staging)).run()
    assert r1.extracted_cards == 1
    r2 = ExtractionRunner(extractor_vault, reg, staging_dir=str(staging)).run()
    assert r2.skipped_existing == 1
    assert r2.extracted_cards == 0


def test_metrics_are_accurate(extractor_vault, sample_email_card):
    _write_email(
        extractor_vault,
        sample_email_card,
        "hfa-email-message-e1",
        "a@acme.com",
        "s",
        "b",
        "Email/2024-03/e1.md",
    )
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    m = ExtractionRunner(extractor_vault, reg).run()
    assert m.matched_emails == 1
    assert m.extracted_cards == 1
    assert m.per_extractor["acme"]["matched"] == 1
    assert m.per_extractor["acme"]["extracted"] == 1
