"""Shipping extractor tests."""

from __future__ import annotations

from archive_sync.extractors.shipping import ShippingExtractor

UPS_BODY = """Your package is on the way

Tracking Number: 1Z999AA10123456784

Shipped on March 10, 2024
Estimated delivery: March 12, 2024
"""


class TestShippingExtractor:
    def test_ups_tracking(self, sample_email_card):
        ex = ShippingExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-sh1",
            "notify@ups.com",
            "UPS Update: package shipped",
            UPS_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.carrier == "UPS"
        assert out[0].card.tracking_number.startswith("1Z")

    def test_strips_boilerplate_estimated_delivery(self, sample_email_card):
        ex = ShippingExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-sh2",
            "notify@ups.com",
            "UPS package on the way",
            """Tracking Number: 1Z999AA10123456784

Estimated delivery: by, based on the selected service, destination and ship date. Limitations and exclusions apply.
""",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert not (out[0].card.estimated_delivery or "").strip()

    def test_delivered_timestamp_next_line(self, sample_email_card):
        ex = ShippingExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-sh3",
            "notify@ups.com",
            "Your package was delivered",
            """Hi Robert,
Your package was delivered.
Delivered
Saturday 12/20/2025  12:50 PM
UPS Ground
1Z89Y9F00392152289
""",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "2025" in (out[0].card.delivered_at or "")
