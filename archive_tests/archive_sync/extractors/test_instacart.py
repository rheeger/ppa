"""Instacart extractor tests."""

from __future__ import annotations

from archive_sync.extractors.instacart import InstacartExtractor

INSTACART_BODY = """Thanks for shopping with Instacart

From: Whole Foods Market

Subtotal: $55.00
Delivery Fee: $5.99
Total: $62.50
"""

INSTACART_DELIVERED_BODY = """Thanks for ordering from Instacart!

Your order from Wegmans was delivered on 10/23 @ 4:22 PM

Order Totals: 184.53
"""

INSTACART_MULTILINE_TOTAL_BODY = """From: Whole Foods Market

Payment Receipt
Subtotal
$99.00
Tax
$8.79
Total
$107.79
"""


class TestInstacartExtractor:
    def test_parse(self, sample_email_card):
        ex = InstacartExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ic1",
            "customers@instacart.com",
            "Receipt",
            INSTACART_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Whole Foods" in out[0].card.store

    def test_store_from_subject_when_body_sparse(self, sample_email_card):
        ex = InstacartExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ic2",
            "customers@instacart.com",
            "Your order from Trader Joe's is on the way",
            "Subtotal: $20.00\nTotal: $24.00\n",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Trader Joe" in out[0].card.store

    def test_delivered_email_order_totals_and_store(self, sample_email_card):
        ex = InstacartExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ic-delivered",
            "customers@instacart.com",
            "Your order was delivered",
            INSTACART_DELIVERED_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Wegmans" in out[0].card.store
        assert abs(out[0].card.total - 184.53) < 0.01

    def test_multiline_total_after_label(self, sample_email_card):
        ex = InstacartExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ic-ml",
            "customers@instacart.com",
            "Instacart+ receipt",
            INSTACART_MULTILINE_TOTAL_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert abs(out[0].card.total - 107.79) < 0.01
