"""Amazon purchase extractor tests."""

from __future__ import annotations

from archive_sync.extractors.amazon import AmazonExtractor

AMAZON_ORDER_BODY = """Your Amazon.com order #112-1234567-1234567

- USB Cable x 1 $12.99

Subtotal: $12.99
Tax: $1.04
Order Total: $14.03

Ship to: 9 Main St, Austin, TX
"""

AMAZON_QTY_LINES_BODY = """Order #999-8877665-1234567

1  Widget Pro Name Here  $19.99
2  Batteries AA 8pk  $8.49

    Order Total:
$31.47
"""


class TestAmazonExtractor:
    def test_subject_gate(self):
        ex = AmazonExtractor()
        assert ex.matches("auto-confirm@amazon.com", "Your Amazon.com order confirmation")
        assert ex.matches("auto-confirm@amazon.com", "Your Amazon.com delivery confirmation")
        assert not ex.matches("auto-confirm@amazon.com", "Deals we think you will love")
        assert not ex.matches("auto-confirm@amazon.com", "Your shipment confirmation")

    def test_shipment_subject_goes_to_shipping_not_purchase(self):
        """'shipment confirmation' should NOT match Amazon purchase extractor — that's ShippingExtractor's job."""
        from archive_sync.extractors.registry import build_default_registry

        reg = build_default_registry()
        m = reg.match("ship-confirm@amazon.com", "Your shipment confirmation")
        assert m is not None
        assert m.extractor_id == "shipping"

    def test_parse_order(self, sample_email_card):
        ex = AmazonExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-am1",
            "auto-confirm@amazon.com",
            "Your Amazon.com order of X has shipped",
            AMAZON_ORDER_BODY,
        )
        # Use order confirmation style subject so purchase extractor matches.
        fm = {**fm, "subject": "Your Amazon.com order confirmation"}
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.order_number
        assert out[0].card.vendor == "Amazon"

    def test_order_id_from_subject_when_body_sparse(self, sample_email_card):
        ex = AmazonExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-am2",
            "auto-confirm@amazon.com",
            "Your Amazon.com order 112-9988776-1234567 confirmation",
            """Thanks for your order.

Subtotal: $5.00
Order Total: $5.99
""",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "112-9988776-1234567" in out[0].card.order_number

    def test_rejects_rate_your_purchase_subject(self, sample_email_card):
        ex = AmazonExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-am3",
            "auto-confirm@amazon.com",
            "Rate your recent Amazon purchase",
            "Subtotal: $1\nOrder Total: $1\n",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 0

    def test_qty_prefixed_lines_and_multiline_order_total(self, sample_email_card):
        ex = AmazonExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-am4",
            "auto-confirm@amazon.com",
            "Your Amazon.com order confirmation",
            AMAZON_QTY_LINES_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        names = [it["name"] for it in out[0].card.items]
        assert any("Widget" in n for n in names)
        assert abs(out[0].card.total - 31.47) < 0.01
