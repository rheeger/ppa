"""DoorDash extractor tests."""

from __future__ import annotations

from archive_sync.extractors.doordash import DoordashExtractor

DOORDASH_2020_BODY = """Order from Joe's Pizza

- Margherita x 1 $14.00
- Salad x 2 $18.00

Subtotal: $32.00
Tax: $2.50
Delivery Fee: $3.00
Tip: $5.00
Total: $42.50

Deliver to: 100 Main St, Brooklyn, NY 11201
"""

DOORDASH_2024_BODY = """Order from Thai House
DoorDash receipt

- Pad Thai x 1 $16.00

Subtotal: $16.00
Tax: $1.50
Total: $19.00
"""

# Post-2024 compact confirmation: no "Order from" block; restaurant + total on credits / estimated lines
DOORDASH_COMPACT_BODY = """DoorDash Order Confirmation

Thanks for your order, Robbie

Paid with Visa Ending in 2633 and/or credits The Sycamore Kitchen Total: $41.60

Estimated Total
$41.60
"""


class TestDoordashExtractor:
    def test_sender_pattern_matches(self):
        ex = DoordashExtractor()
        assert ex.matches("x@doordash.com", "hi")
        assert ex.matches("x@messages.doordash.com", "hi")

    def test_sender_pattern_rejects_non_doordash(self):
        ex = DoordashExtractor()
        assert not ex.matches("x@uber.com", "hi")

    def test_parse_plaintext_era(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd1",
            "receipts@doordash.com",
            "Receipt",
            DOORDASH_2020_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Joe's Pizza" in out[0].card.restaurant
        assert out[0].card.total > 0

    def test_parse_second_era(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd2",
            "noreply@doordash.com",
            "Order",
            DOORDASH_2024_BODY,
            sent_at="2024-06-01T12:00:00Z",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Thai" in out[0].card.restaurant

    def test_compact_confirmation_restaurant_from_credits_and_estimated_total(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd-compact",
            "noreply@doordash.com",
            "DoorDash Order Confirmation",
            DOORDASH_COMPACT_BODY,
            sent_at="2026-01-15T12:00:00Z",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Sycamore" in out[0].card.restaurant
        assert abs(out[0].card.total - 41.60) < 0.01

    def test_no_stub_when_unparseable(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd3",
            "a@doordash.com",
            "Receipt",
            "unstructured noise without line items",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 0

    def test_idempotency(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd4",
            "a@doordash.com",
            "R",
            DOORDASH_2024_BODY,
        )
        a = ex.extract(fm, body, fm["uid"], "Email/x.md")
        b = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert a[0].card.uid == b[0].card.uid

    def test_provenance_all_deterministic(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd5",
            "a@doordash.com",
            "R",
            DOORDASH_2024_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert all(e.method == "deterministic" for e in out[0].provenance.values())

    def test_body_contains_restaurant_and_items(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd6",
            "a@doordash.com",
            "R",
            DOORDASH_2020_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert "Joe's" in out[0].body or "Pizza" in out[0].body

    def test_source_email_field_populated(self, sample_email_card):
        ex = DoordashExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-dd7",
            "a@doordash.com",
            "R",
            DOORDASH_2024_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert out[0].card.source_email == "[[hfa-email-message-dd7]]"
