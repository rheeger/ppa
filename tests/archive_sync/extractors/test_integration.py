"""End-to-end extraction + entity resolution."""

from __future__ import annotations

from archive_sync.extractors.entity_resolution import iter_derived_card_dicts, run_entity_resolution
from archive_sync.extractors.registry import build_default_registry
from archive_sync.extractors.runner import ExtractionRunner
from hfa.schema import validate_card_strict
from hfa.vault import read_note
from tests.archive_sync.extractors.conftest import write_email_to_vault
from tests.archive_sync.extractors.test_amazon import AMAZON_ORDER_BODY
from tests.archive_sync.extractors.test_doordash import DOORDASH_2024_BODY
from tests.archive_sync.extractors.test_instacart import INSTACART_BODY
from tests.archive_sync.extractors.test_uber_rides import UBER_TRIP_BODY
from tests.archive_sync.extractors.test_united import UNITED_BODY


def test_full_extraction_pipeline(extractor_vault, sample_email_card, tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()

    rows = [
        ("hfa-email-message-i0", "receipts@doordash.com", "Your DoorDash order", DOORDASH_2024_BODY),
        ("hfa-email-message-i1", "orders@doordash.com", "Receipt", DOORDASH_2024_BODY),
        ("hfa-email-message-i2", "noreply@uber.com", "Your trip receipt", UBER_TRIP_BODY),
        ("hfa-email-message-i3", "noreply@uber.com", "Ride receipt", UBER_TRIP_BODY),
        (
            "hfa-email-message-i4",
            "auto-confirm@amazon.com",
            "Your Amazon.com order confirmation",
            AMAZON_ORDER_BODY,
        ),
        (
            "hfa-email-message-i5",
            "auto-confirm@amazon.com",
            "Your Amazon.com order confirmation",
            AMAZON_ORDER_BODY,
        ),
        ("hfa-email-message-i6", "customers@instacart.com", "Receipt", INSTACART_BODY),
        ("hfa-email-message-i7", "orders@instacart.com", "Order", INSTACART_BODY),
        ("hfa-email-message-i8", "notifications@united.com", "United", UNITED_BODY),
        ("hfa-email-message-i9", "notifications@united.com", "Trip", UNITED_BODY),
    ]
    for i, (uid, from_email, subject, body) in enumerate(rows):
        fm, b = sample_email_card(uid, from_email, subject, body)
        write_email_to_vault(extractor_vault, f"Email/2024-03/e{i}.md", fm, b)

    registry = build_default_registry()
    r1 = ExtractionRunner(
        extractor_vault,
        registry,
        staging_dir=str(staging),
        workers=2,
    ).run()
    assert r1.matched_emails == 10
    assert r1.extracted_cards == 10

    derived = list(staging.rglob("hfa-*.md"))
    assert len(derived) == 10
    for p in derived:
        rel = str(p.relative_to(staging))
        fm, body, _ = read_note(staging, rel)
        card = validate_card_strict(fm)
        assert card.source_email.startswith("[[hfa-email-message-")
        assert body.strip()

    er = run_entity_resolution(str(staging), entity_filter="place", dry_run=False)
    assert er["places_created"] >= 1

    r2 = ExtractionRunner(
        extractor_vault,
        registry,
        staging_dir=str(staging),
        workers=2,
    ).run()
    assert r2.skipped_existing == 10
    assert r2.extracted_cards == 0


def test_extraction_with_entity_merge(extractor_vault, sample_email_card, tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    for i in range(3):
        fm, b = sample_email_card(
            f"hfa-email-message-m{i}",
            "receipts@doordash.com",
            "Order",
            """Order from Same Restaurant
- Item x 1 $5.00
Subtotal: $5.00
Total: $6.00
Deliver to: 1 St, Brooklyn, NY
""",
        )
        write_email_to_vault(extractor_vault, f"Email/2024-03/m{i}.md", fm, b)
    reg = build_default_registry()
    ExtractionRunner(extractor_vault, reg, staging_dir=str(staging)).run()
    cards = iter_derived_card_dicts(str(staging))
    from archive_sync.extractors.entity_resolution import OrgResolver, PlaceResolver

    pr = PlaceResolver(str(staging)).resolve(cards, dry_run=False)
    assert pr.places_created == 1
    or_ = OrgResolver(str(staging)).resolve(cards, dry_run=False)
    assert or_.orgs_created == 1
