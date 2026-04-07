"""End-to-end extraction idempotency (Phase 3)."""

from __future__ import annotations

from archive_sync.extractors.doordash import DoordashExtractor
from archive_sync.extractors.promoter import promote_staging
from archive_sync.extractors.registry import ExtractorRegistry
from archive_sync.extractors.runner import ExtractionRunner
from tests.archive_sync.extractors.conftest import write_email_to_vault
from tests.archive_sync.extractors.test_doordash import DOORDASH_2024_BODY
from tests.archive_sync.extractors.test_runner import AcmeExtractor


def test_full_extraction_idempotent(extractor_vault, sample_email_card, tmp_path):
    """Extract → promote → extract again yields no new staged .md cards."""
    fm, b = sample_email_card("hfa-email-message-e1", "a@acme.com", "s", "b")
    write_email_to_vault(extractor_vault, "Email/2024-03/e1.md", fm, b)
    reg = ExtractorRegistry()
    reg.register(AcmeExtractor())
    s1 = tmp_path / "s1"
    s2 = tmp_path / "s2"
    s1.mkdir()
    s2.mkdir()
    ExtractionRunner(extractor_vault, reg, staging_dir=str(s1)).run()
    promote_staging(extractor_vault, str(s1))
    ExtractionRunner(extractor_vault, reg, staging_dir=str(s2)).run()
    md_files = [p for p in s2.rglob("*.md") if not p.name.startswith("_")]
    assert md_files == []


def test_per_extractor_content_stability(sample_email_card):
    ex = DoordashExtractor()
    fm, body = sample_email_card(
        "hfa-email-message-dd-stab",
        "noreply@doordash.com",
        "Order",
        DOORDASH_2024_BODY,
        sent_at="2024-06-01T12:00:00Z",
    )
    a = ex.extract(fm, body, fm["uid"], "Email/x.md")
    b = ex.extract(fm, body, fm["uid"], "Email/x.md")
    assert a[0].card.uid == b[0].card.uid
    assert a[0].card.model_dump() == b[0].card.model_dump()
    assert a[0].body.strip() == b[0].body.strip()
