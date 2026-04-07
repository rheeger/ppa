"""Tests for sender-census CLI."""

from __future__ import annotations

from archive_mcp.commands.census import run_sender_census
from tests.archive_sync.extractors.conftest import write_email_to_vault


def test_census_finds_emails_by_domain(extractor_vault, sample_email_card):
    fm1, body1 = sample_email_card(
        "hfa-email-message-dd1",
        "orders@doordash.com",
        "Your order from Mixt",
        "Subtotal $10\nTotal $12",
        sent_at="2023-06-01T12:00:00-08:00",
    )
    fm2, body2 = sample_email_card(
        "hfa-email-message-oth",
        "other@example.com",
        "Hello",
        "x",
        sent_at="2023-06-02T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/dd1.md", fm1, body1)
    write_email_to_vault(extractor_vault, "Email/oth.md", fm2, body2)
    text = run_sender_census(
        vault_path=extractor_vault,
        domain="doordash.com",
        sample_size=10,
        out_path="",
        include_keyword_hits=False,
        top_from_addresses=0,
        top_exact_subjects=0,
        top_subject_shapes=0,
    )
    assert "hfa-email-message-dd1" in text
    assert "hfa-email-message-oth" not in text


def test_census_matches_subdomains(extractor_vault, sample_email_card):
    fm, body = sample_email_card(
        "hfa-email-message-sub",
        "noreply@messages.doordash.com",
        "Your order from Pizza",
        "Total $20",
        sent_at="2020-01-15T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/sub.md", fm, body)
    text = run_sender_census(
        vault_path=extractor_vault,
        domain="doordash.com",
        sample_size=10,
        out_path="",
        include_keyword_hits=False,
        top_from_addresses=0,
        top_exact_subjects=0,
        top_subject_shapes=0,
    )
    assert "hfa-email-message-sub" in text


def test_census_sample_distributes_across_dates(extractor_vault, sample_email_card):
    for i, year in enumerate((2019, 2022, 2025)):
        fm, body = sample_email_card(
            f"hfa-email-message-y{i}",
            "a@doordash.com",
            f"Order {i}",
            "Total $5",
            sent_at=f"{year}-06-01T12:00:00-08:00",
        )
        write_email_to_vault(extractor_vault, f"Email/y{i}.md", fm, body)
    text = run_sender_census(
        vault_path=extractor_vault,
        domain="doordash.com",
        sample_size=3,
        out_path="",
        include_keyword_hits=False,
        top_from_addresses=0,
        top_exact_subjects=0,
        top_subject_shapes=0,
    )
    assert "2019" in text and "2022" in text and "2025" in text


def test_census_categorizes_by_subject_pattern(extractor_vault, sample_email_card):
    fm1, b1 = sample_email_card(
        "hfa-email-message-r1",
        "x@doordash.com",
        "Your receipt for order",
        "x",
        sent_at="2021-01-01T12:00:00-08:00",
    )
    fm2, b2 = sample_email_card(
        "hfa-email-message-p1",
        "x@doordash.com",
        "50% off your next order",
        "x",
        sent_at="2021-02-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/r1.md", fm1, b1)
    write_email_to_vault(extractor_vault, "Email/p1.md", fm2, b2)
    text = run_sender_census(
        vault_path=extractor_vault,
        domain="doordash.com",
        sample_size=10,
        out_path="",
        include_keyword_hits=False,
        top_from_addresses=0,
        top_exact_subjects=0,
        top_subject_shapes=0,
    )
    assert "Receipt" in text or "receipt" in text.lower()


def test_census_writes_to_file(extractor_vault, sample_email_card, tmp_path):
    fm, body = sample_email_card(
        "hfa-email-message-w1",
        "z@doordash.com",
        "Order ok",
        "x",
        sent_at="2024-01-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/w1.md", fm, body)
    out = tmp_path / "census.md"
    run_sender_census(
        vault_path=extractor_vault,
        domain="doordash.com",
        sample_size=5,
        out_path=str(out),
        include_keyword_hits=False,
        top_from_addresses=0,
        top_exact_subjects=0,
        top_subject_shapes=0,
    )
    assert out.is_file()
    assert "doordash.com" in out.read_text(encoding="utf-8")
