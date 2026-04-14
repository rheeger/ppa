"""Cross-provider regression test (Step 12).

Builds a multi-provider vault with emails from all 9 Tier 1-3 extractors,
runs the full extraction pipeline, and asserts no cross-contamination
(e.g. Uber Eats email producing a ride card, DoorDash promo producing a card).
"""

from __future__ import annotations

from archive_sync.extractors.preprocessing import clean_email_body
from archive_sync.extractors.registry import build_default_registry

EMAILS = [
    {
        "uid": "cp-dd-receipt",
        "from_email": "orders@doordash.com",
        "subject": "Your order from Burma Love",
        "body": "Your order from Burma Love\n\nSubtotal: $36.00\nTax: $3.06\nTotal: $44.06\n",
        "expect_type": "meal_order",
        "expect_count": 1,
    },
    {
        "uid": "cp-dd-promo",
        "from_email": "noreply@doordash.com",
        "subject": "$7 Off from your favorite restaurants this summer",
        "body": "Summer of DashPass -- save on delivery.\n",
        "expect_type": None,
        "expect_count": 0,
    },
    {
        "uid": "cp-ue-receipt",
        "from_email": "ubereats@uber.com",
        "subject": "Receipt for your Uber Eats order",
        "body": "Your order from Sushi Place\n\nSubtotal: $25.00\nTotal: $28.00\n",
        "expect_type": "meal_order",
        "expect_count": 1,
    },
    {
        "uid": "cp-uber-ride",
        "from_email": "noreply@uber.com",
        "subject": "Your Thursday trip with Uber",
        "body": "Thanks for riding\n\nTotal $67.64\n8:46 PM 155 W 65th St, New York, NY\n9:17 PM 187 Washington Park, Brooklyn, NY\n",
        "expect_type": "ride",
        "expect_count": 1,
    },
    {
        "uid": "cp-lyft-ride",
        "from_email": "receipts@lyft.com",
        "subject": "Your Lyft ride receipt",
        "body": "2024-05-04T21:15:00-07:00\n\n7:10 PM 2000 Mission St, SF, CA\n7:35 PM 450 Castro St, SF, CA\n\nTotal: $22.50\n",
        "expect_type": "ride",
        "expect_count": 1,
    },
    {
        "uid": "cp-united-flight",
        "from_email": "notifications@united.com",
        "subject": "Your trip confirmation",
        "body": "Confirmation Number: ABC7YZ\n\nNewark, NJ (EWR)   San Francisco, CA (SFO)\n\nTotal: $289.00 USD\n",
        "expect_type": "flight",
        "expect_count": 1,
    },
    {
        "uid": "cp-airbnb-booking",
        "from_email": "automated@airbnb.com",
        "subject": "Reservation confirmed",
        "body": "confirmation HMXYZ789AB\n\nTrip: Cozy loft downtown\n\nCheck-in: May 1, 2024\nCheck-out: May 5, 2024\nTotal: $890.00\n",
        "expect_type": "accommodation",
        "expect_count": 1,
    },
    {
        "uid": "cp-instacart-order",
        "from_email": "customers@instacart.com",
        "subject": "Your order from Whole Foods was delivered",
        "body": "Your order from Whole Foods was delivered\n\nOrder Totals: 95.50\n",
        "expect_type": "grocery_order",
        "expect_count": 1,
    },
    {
        "uid": "cp-ups-ship",
        "from_email": "notify@ups.com",
        "subject": "UPS Update: Your package is on the way",
        "body": "Tracking Number: 1Z999AA10123456784\n\nShipped on April 1, 2024\n",
        "expect_type": "shipment",
        "expect_count": 1,
    },
    {
        "uid": "cp-national-rental",
        "from_email": "email@nationalcar.com",
        "subject": "Reservation confirmed",
        "body": "Confirmation number: NATL998877\n\nPickup location: LAX Terminal 4\nPickup date: April 12, 2024\n\nTotal: $210.00\n",
        "expect_type": "car_rental",
        "expect_count": 1,
    },
    {
        "uid": "cp-uber-eats-promo",
        "from_email": "ubereats@uber.com",
        "subject": "50% off your next Uber Eats order",
        "body": "Enjoy 50% off your next order. Limited time offer.\n",
        "expect_type": None,
        "expect_count": 0,
    },
    {
        "uid": "cp-random-sender",
        "from_email": "hello@randomcompany.com",
        "subject": "Your invoice",
        "body": "Invoice #12345\nTotal: $500.00\n",
        "expect_type": None,
        "expect_count": 0,
    },
]


def _make_fm(email: dict) -> dict:
    return {
        "uid": email["uid"],
        "type": "email_message",
        "source": ["gmail"],
        "source_id": f"gmail.{email['uid']}",
        "created": "2024-06-01",
        "updated": "2024-06-01",
        "summary": email["subject"],
        "gmail_message_id": f"msgid-{email['uid']}",
        "gmail_thread_id": f"thread-{email['uid']}",
        "account_email": "me@example.com",
        "from_email": email["from_email"],
        "to_emails": ["me@example.com"],
        "subject": email["subject"],
        "sent_at": "2024-06-01T12:00:00-07:00",
        "people": [],
        "orgs": [],
        "tags": [],
    }


class TestCrossProvider:
    def test_all_providers_route_correctly(self):
        """Each email routes to exactly one extractor (or none) with no cross-contamination."""
        reg = build_default_registry()
        for email in EMAILS:
            fm = _make_fm(email)
            ext = reg.match(email["from_email"], email["subject"])

            if email["expect_count"] == 0:
                if ext is None:
                    continue
                body = clean_email_body(email["body"])
                results = ext.extract(fm, body, email["uid"], "test/cross.md")
                assert len(results) == 0, (
                    f"{email['uid']}: expected 0 cards but got {len(results)} "
                    f"from {ext.extractor_id}"
                )
            else:
                assert ext is not None, f"{email['uid']}: expected match but got None"
                body = clean_email_body(email["body"])
                results = ext.extract(fm, body, email["uid"], "test/cross.md")
                assert len(results) == email["expect_count"], (
                    f"{email['uid']}: expected {email['expect_count']} cards "
                    f"but got {len(results)} from {ext.extractor_id}"
                )
                if email["expect_type"]:
                    for r in results:
                        assert r.card.type == email["expect_type"], (
                            f"{email['uid']}: expected type {email['expect_type']} "
                            f"but got {r.card.type} from {ext.extractor_id}"
                        )

    def test_no_uber_eats_produces_ride(self):
        """Uber Eats emails must never produce ride cards."""
        reg = build_default_registry()
        for email in EMAILS:
            if "ubereats" not in email["from_email"]:
                continue
            ext = reg.match(email["from_email"], email["subject"])
            if ext is None:
                continue
            fm = _make_fm(email)
            body = clean_email_body(email["body"])
            results = ext.extract(fm, body, email["uid"], "test/cross.md")
            for r in results:
                assert r.card.type != "ride", (
                    f"{email['uid']}: Uber Eats email produced ride card"
                )

    def test_no_uber_ride_produces_meal_order(self):
        """Uber ride emails must never produce meal_order cards."""
        reg = build_default_registry()
        for email in EMAILS:
            if email["from_email"] != "noreply@uber.com":
                continue
            if "trip" not in email["subject"].lower() and "ride" not in email["subject"].lower():
                continue
            ext = reg.match(email["from_email"], email["subject"])
            if ext is None:
                continue
            fm = _make_fm(email)
            body = clean_email_body(email["body"])
            results = ext.extract(fm, body, email["uid"], "test/cross.md")
            for r in results:
                assert r.card.type != "meal_order", (
                    f"{email['uid']}: Uber ride email produced meal_order card"
                )

    def test_registry_order_uber_eats_before_rides(self):
        """Uber Eats must match before Uber Rides for Uber Eats subjects."""
        reg = build_default_registry()
        ext = reg.match("ubereats@uber.com", "Your Uber Eats order with Sushi Place")
        assert ext is not None
        assert ext.extractor_id == "uber_eats"

    def test_amazon_shipment_goes_to_shipping(self):
        """Amazon shipment subjects go to shipping extractor, not amazon purchase."""
        reg = build_default_registry()
        ext = reg.match("ship-confirm@amazon.com", "Your shipment confirmation")
        assert ext is not None
        assert ext.extractor_id == "shipping"

    def test_zero_errors_across_all_providers(self):
        """No extractor should throw an exception on any of the test emails."""
        reg = build_default_registry()
        for email in EMAILS:
            ext = reg.match(email["from_email"], email["subject"])
            if ext is None:
                continue
            fm = _make_fm(email)
            body = clean_email_body(email["body"])
            try:
                ext.extract(fm, body, email["uid"], "test/cross.md")
            except Exception as exc:
                raise AssertionError(
                    f"{email['uid']}: {ext.extractor_id} raised {exc}"
                ) from exc
