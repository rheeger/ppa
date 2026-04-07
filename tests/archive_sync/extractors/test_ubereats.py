"""Uber Eats extractor tests."""

from __future__ import annotations

from archive_sync.extractors.ubereats import UberEatsExtractor

EATS_BODY = """Order from Sushi Place — Uber Eats

Subtotal: $25.00
Tax: $2.00
Total: $28.00
"""

# html2text-style single line: restaurant + logistics + footer (real vault shape)
EATS_BODY_HORIZONTAL = """Your order from Mixt - Valencia                     Picked up from    903 Valencia St, San Francisco, CA 94110, USA                              Delivered to    94 Jack London Alley, San Francisco, CA 94107, USA                                                                                             Delivered by Joao                        Rate order             Rate order                                                                       .eats_footer_table{width:100%!important}                 Contact support

Subtotal: $20.01
Tax: $1.70
Total: $21.71
"""

# Older / html2text receipts: qty + spaces + item + $price (see samples_seed/ubereats/2020)
EATS_BODY_QTY_NAME_PRICE = """Order from MIXT - Valencia

1   ORCHARD      $16.62
1   DESIGN YOUR OWN SALAD      $12.59

Subtotal: $29.21
Tax: $2.48
Total: $30.23
"""


class TestUberEatsExtractor:
    def test_ubereats_address(self, sample_email_card):
        ex = UberEatsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ue1",
            "ubereats@uber.com",
            "Receipt",
            EATS_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Sushi" in out[0].card.restaurant

    def test_restaurant_from_subject_before_body(self, sample_email_card):
        ex = UberEatsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ue-subj",
            "ubereats@uber.com",
            "Your Uber Eats order with Taqueria El Sol",
            "unrelated body without order from line\n\nSubtotal: $10\nTotal: $12\n",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Taqueria" in out[0].card.restaurant
        assert "Sol" in out[0].card.restaurant

    def test_horizontal_layout_trims_to_restaurant_name(self, sample_email_card):
        ex = UberEatsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ue-horiz",
            "ubereats@uber.com",
            "Receipt",
            EATS_BODY_HORIZONTAL,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.restaurant.strip() == "Mixt - Valencia"
        assert "Picked up" not in out[0].card.restaurant

    def test_generic_uber_with_eats_subject(self, sample_email_card):
        ex = UberEatsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ue2",
            "noreply@uber.com",
            "Your Uber Eats order with Sushi Place",
            EATS_BODY,
        )
        assert ex.matches(fm["from_email"], fm["subject"])
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1

    def test_parse_qty_name_price_lines(self, sample_email_card):
        ex = UberEatsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ue-qty",
            "ubereats@uber.com",
            "Receipt",
            EATS_BODY_QTY_NAME_PRICE,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        names = {it["name"] for it in out[0].card.items}
        assert any("ORCHARD" in n for n in names)
        assert any("DESIGN YOUR OWN" in n for n in names)
