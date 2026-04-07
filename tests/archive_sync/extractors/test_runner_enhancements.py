"""Phase 3 runner enhancements: metrics file, sampling, progress, yield."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from archive_sync.extractors.base import EmailExtractor, TemplateVersion
from archive_sync.extractors.registry import ExtractorRegistry
from archive_sync.extractors.runner import (ExtractionRunner,
                                            _uid_in_vault_percent_sample)
from tests.archive_sync.extractors.conftest import write_email_to_vault


class AcmeXExtractor(EmailExtractor):
    output_card_type = "meal_order"
    sender_patterns = [r".*@acme\.com$"]

    def template_versions(self):
        def p(fm, body):
            return [
                {
                    "_discriminator": "d",
                    "service": "Acme",
                    "restaurant": "Diner",
                    "items": [],
                    "_body": "# Acme\n",
                }
            ]

        return [TemplateVersion("t", ("2000-01-01", "2099-12-31"), p)]

    def summary_only_fallback(self, fm, body, suid, srp):
        return []


def test_metrics_json_written_to_staging(extractor_vault, sample_email_card, tmp_path):
    staging = tmp_path / "stg"
    staging.mkdir()
    fm, b = sample_email_card("hfa-email-message-e1", "a@acme.com", "s", "b")
    write_email_to_vault(extractor_vault, "Email/2024-03/e1.md", fm, b)
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    ExtractionRunner(extractor_vault, reg, staging_dir=str(staging)).run()
    p = staging / "_metrics.json"
    assert p.is_file()
    data = json.loads(p.read_text(encoding="utf-8"))
    assert "yield_by_extractor" in data
    assert "cards_per_second" in data
    assert "rejected_emails" in data
    assert "field_population" in data


def test_yield_by_extractor_computed(extractor_vault, sample_email_card):
    fm, b = sample_email_card("hfa-email-message-e1", "a@acme.com", "s", "b")
    write_email_to_vault(extractor_vault, "Email/2024-03/e1.md", fm, b)
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    m = ExtractionRunner(extractor_vault, reg, dry_run=True).run()
    assert m.yield_by_extractor.get("acme_x", 0) == 1.0


def test_cards_per_second_computed(extractor_vault, sample_email_card):
    fm, b = sample_email_card("hfa-email-message-e1", "a@acme.com", "s", "b")
    write_email_to_vault(extractor_vault, "Email/2024-03/e1.md", fm, b)
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    m = ExtractionRunner(extractor_vault, reg, dry_run=True).run()
    assert m.cards_per_second >= 0.0


def test_progress_logging_interval(extractor_vault, sample_email_card, caplog):
    caplog.set_level(logging.INFO)
    fm, b = sample_email_card("hfa-email-message-e1", "a@acme.com", "s", "b")
    write_email_to_vault(extractor_vault, "Email/2024-03/e1.md", fm, b)
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    ExtractionRunner(extractor_vault, reg, dry_run=True, progress_every=1).run()
    assert any("extract" in r.message.lower() for r in caplog.records)


def test_limit_vault_percent_samples_correctly(extractor_vault, sample_email_card):
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    uids = [f"hfa-email-message-{i:04d}" for i in range(40)]
    expected = sum(1 for u in uids if _uid_in_vault_percent_sample(u, 50.0))
    for i, uid in enumerate(uids):
        fm, b = sample_email_card(uid, "a@acme.com", "s", "b")
        write_email_to_vault(extractor_vault, f"Email/2024-03/e{i}.md", fm, b)
    m = ExtractionRunner(extractor_vault, reg, dry_run=True, vault_percent=50.0).run()
    assert m.matched_emails == expected


def test_limit_vault_percent_deterministic(extractor_vault, sample_email_card):
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    for i in range(15):
        uid = f"hfa-email-message-{i:04d}"
        fm, b = sample_email_card(uid, "a@acme.com", "s", "b")
        write_email_to_vault(extractor_vault, f"Email/2024-03/e{i}.md", fm, b)
    m1 = ExtractionRunner(extractor_vault, reg, dry_run=True, vault_percent=33.0).run()
    m2 = ExtractionRunner(extractor_vault, reg, dry_run=True, vault_percent=33.0).run()
    assert m1.matched_emails == m2.matched_emails


def test_full_report_staging_summary_non_empty(extractor_vault, sample_email_card, tmp_path):
    """After staging extraction, staging_report sees meal_order cards."""
    from archive_mcp.commands.staging import (format_staging_report_markdown,
                                              staging_report)

    staging = tmp_path / "stg"
    staging.mkdir()
    fm, b = sample_email_card("hfa-email-message-e1", "a@acme.com", "s", "b")
    write_email_to_vault(extractor_vault, "Email/2024-03/e1.md", fm, b)
    reg = ExtractorRegistry()
    reg.register(AcmeXExtractor())
    ExtractionRunner(extractor_vault, reg, staging_dir=str(staging)).run()
    rep = staging_report(str(staging))
    assert rep.total_cards >= 1
    md = format_staging_report_markdown(rep)
    assert "meal_order" in md
