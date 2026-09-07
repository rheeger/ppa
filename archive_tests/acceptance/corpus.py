"""Frozen P04-B product corpus: cards, relation labels, and independent queries.

Python is the source of truth. Committed JSON under ``data/`` is the frozen
contract; tests recompute the payloads and compare hashes. Expected neighbors
and relation memberships are authored here — they are not copied from current
ANN or search output.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from archive_tests.acceptance.fixtures import (
    CONTRACT_VERSION,
    CORPUS_SEED,
    PROVENANCE_SOURCE,
    init_vault,
    provenance_for,
)
from archive_tests.acceptance.oracle import build_adversarial_modulo_ivf_dataset, ivf_nlist, ivf_nprobe
from archive_vault.schema import CARD_TYPES, BaseCard
from archive_vault.vault import write_card

DATA_DIR = Path(__file__).resolve().parent / "data"
CREATED = "2026-04-11"
UPDATED = "2026-09-06"
SOURCE = [PROVENANCE_SOURCE]
PEOPLE_PRIMARY = "[[p04-alex-rivera-primary]]"
PEOPLE_NAMESAKE = "[[p04-alex-rivera-namesake]]"
PEOPLE_JORDAN = "[[p04-jordan-hale]]"
SHARED_EXTERNAL_ID = "ext-shared-p04b-001"
FORBIDDEN_TEXT = (
    "rheeger",
    "heeger",
    "endaoment",
    "jane smith",
    "jane-smith",
    "arnold friedman",
    "hf-archives-seed",
    "robbie heeger",
    "@gmail.com",
    "netflix",
    "united airlines",
)

RELATION_OWNERS = {
    "same-trip": "P10-C",
    "same-charge": "P10-C",
    "not-the-same-person": "P07-C",
    "reply-is-authorization": "P01-B1",
    "renewal-is-not-current-subscription": "P10-C",
}


def canonical_dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _uid(kind: str, slug: str) -> str:
    return f"hfa-{kind}-p04b{slug}"


def _token(slug: str) -> str:
    return f"P04B-TOKEN-{slug.upper()}"


def _card(
    uid: str,
    card_type: str,
    rel_path: str,
    body: str,
    families: list[str],
    fields: dict[str, Any],
    *,
    corpus_state: str = "active",
    edge_kind: str = "deterministic",
) -> dict[str, Any]:
    merged = {
        "source": list(SOURCE),
        "source_id": fields.pop("source_id", uid.replace("hfa-", "src-")),
        "created": fields.pop("created", CREATED),
        "updated": fields.pop("updated", UPDATED),
        "tags": fields.pop("tags", []) + [f"corpus_state:{corpus_state}", "p04b", "synthetic"],
        **fields,
    }
    return {
        "uid": uid,
        "type": card_type,
        "rel_path": rel_path,
        "body": body,
        "families": families,
        "corpus_state": corpus_state,
        "edge_kind": edge_kind,
        "fields": merged,
    }


def _story_cards() -> list[dict[str, Any]]:
    auth_thread = _uid("email-thread", "auth0001")
    neg_thread = _uid("email-thread", "neg00001")
    share_a = _uid("email-thread", "sharea01")
    share_b = _uid("email-thread", "shareb01")
    req = _uid("email-message", "req0001")
    reply = _uid("email-message", "reply01")
    stale = _uid("email-message", "stale01")
    active = _uid("email-message", "actv001")
    quarantine = _uid("email-message", "qarn001")
    suppressed = _uid("email-message", "supp001")
    dup_a = _uid("email-message", "dupsrc1")
    dup_b = _uid("email-message", "dupsrc2")
    booking = _uid("email-message", "book001")
    share_am = _uid("email-message", "shaream")
    share_bm = _uid("email-message", "sharebm")
    attach_bad = _uid("email-attachment", "bad0001")
    attach_rcpt = _uid("email-attachment", "rcpt001")
    person_a = _uid("person", "alex0001")
    person_b = _uid("person", "alex0002")
    person_j = _uid("person", "jord0001")
    flight_out = _uid("flight", "out0001")
    flight_in = _uid("flight", "in00001")
    flight_look = _uid("flight", "look001")
    hotel = _uid("accommodation", "hotl001")
    ride = _uid("ride", "airp001")
    charge = _uid("finance", "chg0001")
    look_fin = _uid("finance", "look001")
    corr = _uid("finance", "corr001")
    eur = _uid("finance", "eur0001")
    refund = _uid("finance", "ref0001")
    purchase = _uid("purchase", "hotl001")
    purchase_look = _uid("purchase", "look001")
    renew = _uid("subscription", "renw001")
    cancel = _uid("subscription", "cncl001")
    observe = _uid("observation", "trip001")

    return [
        _card(
            person_a,
            "person",
            "People/p04-alex-rivera-primary.md",
            f"Primary Alex Rivera. Alias A. Rivera. High-degree hub. {_token('ALEX-PRIMARY')}",
            ["high_degree", "aliases", "namesake"],
            {
                "summary": "Alex Rivera",
                "first_name": "Alex",
                "last_name": "Rivera",
                "aliases": ["A. Rivera", "Alex R."],
                "emails": ["alex@example.test"],
                "company": "Harbor Loft Collective",
                "title": "Trip organizer",
                "description": "Synthetic high-degree person for P04-B.",
            },
        ),
        _card(
            person_b,
            "person",
            "People/p04-alex-rivera-namesake.md",
            f"Distinct Alex Rivera. Different email and identity. {_token('ALEX-NAMESAKE')}",
            ["namesake", "not_the_same_person"],
            {
                "summary": "Alex Rivera",
                "first_name": "Alex",
                "last_name": "Rivera",
                "emails": ["alex.rivera.ops@example.test"],
                "company": "Northwind Ledger Co",
                "title": "Namesake control",
                "description": "Same display name, different person.",
            },
        ),
        _card(
            person_j,
            "person",
            "People/p04-jordan-hale.md",
            f"Jordan Hale travels with the primary Alex Rivera. {_token('JORDAN')}",
            ["same_trip"],
            {
                "summary": "Jordan Hale",
                "first_name": "Jordan",
                "last_name": "Hale",
                "emails": ["jordan.hale@example.test"],
                "title": "Trip companion",
            },
        ),
        _card(
            _uid("organization", "loft0001"),
            "organization",
            "Entities/Organizations/p04-harbor-loft.md",
            f"Harbor Loft Collective hosts the labeled NYC stay. {_token('ORG-LOFT')}",
            ["same_trip", "same_charge"],
            {
                "summary": "Harbor Loft Collective",
                "name": "Harbor Loft Collective",
                "org_type": "lodging",
                "domain": "harbor-loft.example.test",
            },
        ),
        _card(
            _uid("organization", "payr0001"),
            "organization",
            "Entities/Organizations/p04-northwind-ledger.md",
            f"Northwind Ledger Co is the namesake employer. {_token('ORG-PAY')}",
            ["namesake"],
            {
                "summary": "Northwind Ledger Co",
                "name": "Northwind Ledger Co",
                "org_type": "employer",
                "domain": "northwind-ledger.example.test",
            },
        ),
        _card(
            _uid("place", "loft0001"),
            "place",
            "Entities/Places/p04-harbor-loft-nyc.md",
            f"Harbor Loft NYC place card. {_token('PLACE-LOFT')}",
            ["same_trip"],
            {
                "summary": "Harbor Loft NYC",
                "name": "Harbor Loft NYC",
                "city": "New York",
                "country": "US",
                "place_type": "lodging",
            },
        ),
        _card(
            auth_thread,
            "email_thread",
            "EmailThreads/p04-auth-loft.md",
            f"Thread asking to book the loft. {_token('THREAD-AUTH')}",
            ["reply_authorization"],
            {
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "subject": "Can you book the loft?",
                "participants": ["alex@example.test", "desk@harbor-loft.example.test"],
                "messages": [f"[[{req}]]", f"[[{reply}]]"],
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            neg_thread,
            "email_thread",
            "EmailThreads/p04-other-account.md",
            f"Different account thread. Not authorization evidence. {_token('THREAD-NEG')}",
            ["reply_authorization"],
            {
                "gmail_thread_id": "thread-p04b-neg",
                "account_email": "alex.rivera.ops@example.test",
                "subject": "Office snacks",
                "participants": ["alex.rivera.ops@example.test"],
                "people": [PEOPLE_NAMESAKE],
            },
        ),
        _card(
            share_a,
            "email_thread",
            "EmailThreads/p04-shared-ext-a.md",
            f"Account A thread sharing external id {SHARED_EXTERNAL_ID}. {_token('SHARE-A')}",
            ["shared_external_id"],
            {
                "source_id": SHARED_EXTERNAL_ID,
                "gmail_thread_id": "thread-p04b-share-a",
                "account_email": "alex@example.test",
                "subject": "Shared external locator A",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            share_b,
            "email_thread",
            "EmailThreads/p04-shared-ext-b.md",
            f"Account B thread sharing external id {SHARED_EXTERNAL_ID}. {_token('SHARE-B')}",
            ["shared_external_id"],
            {
                "source_id": SHARED_EXTERNAL_ID,
                "gmail_thread_id": "thread-p04b-share-b",
                "account_email": "alex.rivera.ops@example.test",
                "subject": "Shared external locator B",
                "people": [PEOPLE_NAMESAKE],
            },
        ),
        _card(
            req,
            "email_message",
            "Email/p04-loft-request.md",
            f"Can you book the loft for April 11-14? {_token('AUTH-REQUEST')}",
            ["reply_authorization"],
            {
                "gmail_message_id": "msg-p04b-req",
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "thread": f"[[{auth_thread}]]",
                "subject": "Can you book the loft?",
                "from_email": "alex@example.test",
                "to_emails": ["desk@harbor-loft.example.test"],
                "sent_at": "2026-04-08T14:02:00Z",
                "snippet": "Can you book the loft?",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            reply,
            "email_message",
            "Email/p04-loft-reply.md",
            f"Yes, book it, I will cover it. {_token('AUTH-REPLY')}",
            ["reply_authorization"],
            {
                "gmail_message_id": "msg-p04b-reply",
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "thread": f"[[{auth_thread}]]",
                "in_reply_to": "msg-p04b-req",
                "references": ["msg-p04b-req"],
                "subject": "Re: Can you book the loft?",
                "from_email": "desk@harbor-loft.example.test",
                "to_emails": ["alex@example.test"],
                "sent_at": "2026-04-08T15:11:00Z",
                "snippet": "Yes, book it, I will cover it.",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            stale,
            "email_message",
            "Email/p04-stale-edited-reply.md",
            f"Edited later to 'never mind'. Stale authorization. {_token('AUTH-STALE')}",
            ["reply_authorization"],
            {
                "gmail_message_id": "msg-p04b-stale",
                "gmail_thread_id": "thread-p04b-neg",
                "account_email": "alex.rivera.ops@example.test",
                "thread": f"[[{neg_thread}]]",
                "subject": "Re: Office snacks",
                "from_email": "alex.rivera.ops@example.test",
                "sent_at": "2026-03-01T09:00:00Z",
                "snippet": "never mind",
                "people": [PEOPLE_NAMESAKE],
                "tags": ["stale_edit"],
            },
        ),
        _card(
            active,
            "email_message",
            "Email/p04-active-note.md",
            f"Active hygiene email. {_token('EMAIL-ACTIVE')}",
            ["corpus_state"],
            {
                "gmail_message_id": "msg-p04b-active",
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "subject": "Active trip note",
                "people": [PEOPLE_PRIMARY],
            },
            corpus_state="active",
        ),
        _card(
            quarantine,
            "email_message",
            "Email/p04-quarantine-note.md",
            f"Quarantined marketing blast. {_token('EMAIL-QUARANTINE')}",
            ["corpus_state"],
            {
                "gmail_message_id": "msg-p04b-qarn",
                "gmail_thread_id": "thread-p04b-neg",
                "account_email": "alex@example.test",
                "subject": "Quarantine fixture",
                "people": [PEOPLE_PRIMARY],
            },
            corpus_state="quarantine",
        ),
        _card(
            suppressed,
            "email_message",
            "Email/p04-suppressed-note.md",
            f"Suppressed bounce notice. {_token('EMAIL-SUPPRESSED')}",
            ["corpus_state"],
            {
                "gmail_message_id": "msg-p04b-supp",
                "gmail_thread_id": "thread-p04b-neg",
                "account_email": "alex@example.test",
                "subject": "Suppressed fixture",
                "people": [PEOPLE_PRIMARY],
            },
            corpus_state="suppressed",
        ),
        _card(
            dup_a,
            "email_message",
            "Email/p04-source-dup-a.md",
            f"Source duplicate copy A. {_token('DUP-SRC-A')}",
            ["source_duplicate"],
            {
                "gmail_message_id": "msg-p04b-dup-a",
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "message_id_header": "<shared-dup-p04b@example.test>",
                "subject": "Duplicate source receipt",
                "people": [PEOPLE_PRIMARY],
                "tags": ["source_duplicate"],
            },
        ),
        _card(
            dup_b,
            "email_message",
            "Email/p04-source-dup-b.md",
            f"Source duplicate copy B. {_token('DUP-SRC-B')}",
            ["source_duplicate"],
            {
                "gmail_message_id": "msg-p04b-dup-b",
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "message_id_header": "<shared-dup-p04b@example.test>",
                "subject": "Duplicate source receipt",
                "people": [PEOPLE_PRIMARY],
                "tags": ["source_duplicate"],
            },
        ),
        _card(
            booking,
            "email_message",
            "Email/p04-loft-booking.md",
            f"Booking confirmation for Harbor Loft NYC. {_token('BOOKING')}",
            ["same_trip", "derived_duplicate"],
            {
                "gmail_message_id": "msg-p04b-book",
                "gmail_thread_id": "thread-p04b-auth",
                "account_email": "alex@example.test",
                "thread": f"[[{auth_thread}]]",
                "subject": "Harbor Loft booking confirmed",
                "people": [PEOPLE_PRIMARY, PEOPLE_JORDAN],
            },
        ),
        _card(
            share_am,
            "email_message",
            "Email/p04-shared-ext-a-msg.md",
            f"Message on account A shared locator. {_token('SHARE-A-MSG')}",
            ["shared_external_id"],
            {
                "gmail_message_id": "msg-p04b-share-a",
                "gmail_thread_id": "thread-p04b-share-a",
                "account_email": "alex@example.test",
                "subject": "Shared locator A",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            share_bm,
            "email_message",
            "Email/p04-shared-ext-b-msg.md",
            f"Message on account B shared locator. {_token('SHARE-B-MSG')}",
            ["shared_external_id"],
            {
                "gmail_message_id": "msg-p04b-share-b",
                "gmail_thread_id": "thread-p04b-share-b",
                "account_email": "alex.rivera.ops@example.test",
                "subject": "Shared locator B",
                "people": [PEOPLE_NAMESAKE],
            },
        ),
        _card(
            attach_bad,
            "email_attachment",
            "EmailAttachments/p04-malformed.bin.md",
            f"Malformed attachment, extraction failed. {_token('ATTACH-BAD')}",
            ["malformed_attachment"],
            {
                "gmail_message_id": "msg-p04b-book",
                "gmail_thread_id": "thread-p04b-auth",
                "attachment_id": "att-p04b-bad",
                "filename": "broken.bin",
                "mime_type": "application/octet-stream",
                "extraction_status": "failed",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            attach_rcpt,
            "email_attachment",
            "EmailAttachments/p04-loft-receipt.md",
            f"Harbor Loft receipt attachment for 482.00 USD. {_token('ATTACH-RCPT')}",
            ["same_charge"],
            {
                "gmail_message_id": "msg-p04b-book",
                "gmail_thread_id": "thread-p04b-auth",
                "attachment_id": "att-p04b-rcpt",
                "filename": "harbor-loft-receipt.pdf",
                "mime_type": "application/pdf",
                "extraction_status": "ok",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            _uid("calendar-event", "midn001"),
            "calendar_event",
            "Calendar/p04-crossing-midnight.md",
            f"Red-eye briefing crossing midnight. {_token('CAL-MIDNIGHT')}",
            ["timezone_interval"],
            {
                "calendar_id": "cal-p04b-primary",
                "event_id": "evt-p04b-midnight",
                "title": "Crossing midnight briefing",
                "start_at": "2026-04-11T22:00:00-07:00",
                "end_at": "2026-04-12T01:30:00-07:00",
                "timezone": "America/Los_Angeles",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            _uid("calendar-event", "notz001"),
            "calendar_event",
            "Calendar/p04-missing-timezone.md",
            f"Local time with no timezone attached. {_token('CAL-NOTZ')}",
            ["timezone_interval"],
            {
                "calendar_id": "cal-p04b-primary",
                "event_id": "evt-p04b-notz",
                "title": "Missing timezone standup",
                "start_at": "2026-04-12T09:00:00",
                "end_at": "2026-04-12T09:30:00",
                "timezone": "",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            _uid("calendar-event", "intv001"),
            "calendar_event",
            "Calendar/p04-interval-block.md",
            f"Three-hour interval in America/New_York. {_token('CAL-INTERVAL')}",
            ["timezone_interval"],
            {
                "calendar_id": "cal-p04b-primary",
                "event_id": "evt-p04b-interval",
                "title": "Harbor Loft interval block",
                "start_at": "2026-04-12T13:00:00-04:00",
                "end_at": "2026-04-12T16:00:00-04:00",
                "timezone": "America/New_York",
                "people": [PEOPLE_PRIMARY, PEOPLE_JORDAN],
            },
        ),
        _card(
            flight_out,
            "flight",
            "Transactions/Flights/p04-sfo-jfk.md",
            f"Harbor Air SFO to JFK confirmation HA4B11. {_token('FLIGHT-OUT')}",
            ["same_trip", "multi_leg"],
            {
                "airline": "Harbor Air",
                "confirmation_code": "HA4B11",
                "origin_airport": "SFO",
                "destination_airport": "JFK",
                "departure_at": "2026-04-11T08:10:00-07:00",
                "arrival_at": "2026-04-11T16:40:00-04:00",
                "fare_amount": 318.0,
                "source_email": f"[[{booking}]]",
                "people": [PEOPLE_PRIMARY, PEOPLE_JORDAN],
            },
        ),
        _card(
            flight_in,
            "flight",
            "Transactions/Flights/p04-jfk-sfo.md",
            f"Harbor Air JFK to SFO confirmation HA4B11. {_token('FLIGHT-IN')}",
            ["same_trip", "multi_leg"],
            {
                "airline": "Harbor Air",
                "confirmation_code": "HA4B11",
                "origin_airport": "JFK",
                "destination_airport": "SFO",
                "departure_at": "2026-04-14T18:05:00-04:00",
                "arrival_at": "2026-04-14T21:20:00-07:00",
                "fare_amount": 302.0,
                "source_email": f"[[{booking}]]",
                "people": [PEOPLE_PRIMARY, PEOPLE_JORDAN],
            },
        ),
        _card(
            flight_look,
            "flight",
            "Transactions/Flights/p04-lookalike-sfo-jfk.md",
            f"Different confirmation HA9Z22 in May. Proximity lookalike. {_token('FLIGHT-LOOK')}",
            ["same_trip"],
            {
                "airline": "Harbor Air",
                "confirmation_code": "HA9Z22",
                "origin_airport": "SFO",
                "destination_airport": "JFK",
                "departure_at": "2026-05-20T07:40:00-07:00",
                "arrival_at": "2026-05-20T16:10:00-04:00",
                "fare_amount": 299.0,
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            hotel,
            "accommodation",
            "Transactions/Accommodations/p04-harbor-loft.md",
            f"Harbor Loft NYC 2026-04-11 to 2026-04-14. {_token('HOTEL')}",
            ["same_trip", "same_charge"],
            {
                "property_name": "Harbor Loft NYC",
                "check_in": "2026-04-11",
                "check_out": "2026-04-14",
                "confirmation_code": "LOFT4B",
                "total_cost": 482.0,
                "source_email": f"[[{booking}]]",
                "people": [PEOPLE_PRIMARY, PEOPLE_JORDAN],
            },
        ),
        _card(
            ride,
            "ride",
            "Transactions/Rides/p04-jfk-loft.md",
            f"Airport ride JFK to Harbor Loft. {_token('RIDE')}",
            ["same_trip"],
            {
                "service": "Harbor Ride",
                "pickup_location": "JFK",
                "dropoff_location": "Harbor Loft NYC",
                "pickup_at": "2026-04-11T17:10:00-04:00",
                "fare": 48.0,
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            charge,
            "finance",
            "Finance/p04-loft-charge.md",
            f"Card charge 482.00 USD matching Harbor Loft receipt. {_token('CHARGE')}",
            ["same_charge", "same_trip"],
            {
                "amount": 482.0,
                "currency": "USD",
                "counterparty": "Harbor Loft Collective",
                "account": "alex-primary-card",
                "note": "Supported actual charge for the labeled stay.",
                "source_email": f"[[{booking}]]",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            look_fin,
            "finance",
            "Finance/p04-lookalike-charge.md",
            f"Lookalike 480.00 USD. Not the loft charge. {_token('CHARGE-LOOK')}",
            ["same_charge"],
            {
                "amount": 480.0,
                "currency": "USD",
                "counterparty": "Harbor Supply Desk",
                "account": "alex-primary-card",
                "note": "Proximity lookalike, different merchant.",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            corr,
            "finance",
            "Finance/p04-corrected-amount.md",
            f"Coffee 42.50 USD. Manually corrected from 425.00. {_token('CORRECTED')}",
            ["manual_correction"],
            {
                "amount": 42.50,
                "currency": "USD",
                "counterparty": "Pebble Cart",
                "account": "alex-primary-card",
                "note": "Manual correction from 425.00 after receipt review.",
                "people": [PEOPLE_PRIMARY],
                "tags": ["manual_correction", "original_amount:425.00"],
            },
        ),
        _card(
            eur,
            "finance",
            "Finance/p04-eur-purchase.md",
            f"EUR 100.00 workshop materials. {_token('EUR')}",
            ["multi_currency_refund"],
            {
                "amount": 100.0,
                "currency": "EUR",
                "counterparty": "Pebble Press",
                "account": "alex-eur",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            refund,
            "finance",
            "Finance/p04-eur-refund.md",
            f"EUR -40.00 partial refund. {_token('REFUND')}",
            ["multi_currency_refund"],
            {
                "amount": -40.0,
                "currency": "EUR",
                "counterparty": "Pebble Press",
                "account": "alex-eur",
                "transaction_type": "refund",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            purchase,
            "purchase",
            "Transactions/Purchases/p04-loft-receipt.md",
            f"Derived purchase from booking email. Total 482.00. {_token('PURCHASE')}",
            ["same_charge", "derived_duplicate"],
            {
                "vendor": "Harbor Loft Collective",
                "total": 482.0,
                "order_number": "LOFT4B",
                "source_email": f"[[{booking}]]",
                "people": [PEOPLE_PRIMARY],
                "tags": ["derived_duplicate"],
            },
        ),
        _card(
            purchase_look,
            "purchase",
            "Transactions/Purchases/p04-lookalike-receipt.md",
            f"Lookalike 480.00 purchase. {_token('PURCHASE-LOOK')}",
            ["same_charge"],
            {
                "vendor": "Harbor Supply Desk",
                "total": 480.0,
                "order_number": "SUP480",
                "people": [PEOPLE_PRIMARY],
            },
        ),
        _card(
            renew,
            "subscription",
            "Transactions/Subscriptions/p04-pebble-renew.md",
            f"Pebble Mail renewal last observed 2025-11-01. {_token('RENEW')}",
            ["subscription_ambiguity"],
            {
                "service_name": "Pebble Mail",
                "plan_name": "monthly",
                "price": 9.0,
                "event_type": "renewal",
                "event_at": "2025-11-01",
                "people": [PEOPLE_PRIMARY],
                "created": "2025-11-01",
            },
        ),
        _card(
            cancel,
            "subscription",
            "Transactions/Subscriptions/p04-pebble-cancel.md",
            f"Pebble Mail cancel 2026-01-15. Renewal is not current. {_token('CANCEL')}",
            ["subscription_ambiguity"],
            {
                "service_name": "Pebble Mail",
                "plan_name": "monthly",
                "price": 9.0,
                "event_type": "cancel",
                "event_at": "2026-01-15",
                "people": [PEOPLE_PRIMARY],
                "created": "2026-01-15",
            },
        ),
        _card(
            observe,
            "observation",
            "Agent/p04-proposed-same-trip.md",
            f"Proposed same-trip grouping. Not a production linker edge. {_token('OBS-TRIP')}",
            ["inferred_edge", "same_trip"],
            {
                "domain": "travel",
                "observation_type": "proposed_same_trip",
                "confidence": 0.61,
                "evidence_uids": [flight_out, flight_in, hotel, ride, charge],
                "people": [PEOPLE_PRIMARY],
            },
            edge_kind="inferred",
        ),
    ]


def _coverage_cards() -> list[dict[str, Any]]:
    """One card for each remaining type, plus changed/deleted chunk docs."""

    specs = [
        (
            "medical_record",
            "Medical/p04-coverage-record.md",
            "medical-record",
            "cov0001",
            {"record_type": "note", "provider_name": "Harbor Clinic", "code_display": "routine"},
        ),
        (
            "vaccination",
            "Vaccinations/p04-coverage-vax.md",
            "vaccination",
            "cov0001",
            {"vaccine_name": "Pebble Vax", "status": "completed"},
        ),
        (
            "imessage_thread",
            "IMessageThreads/p04-coverage-im-thread.md",
            "imessage-thread",
            "cov0001",
            {"imessage_chat_id": "chat-p04b-001", "display_name": "P04B IM"},
        ),
        (
            "imessage_message",
            "IMessage/p04-coverage-im-msg.md",
            "imessage-message",
            "cov0001",
            {"imessage_message_id": "im-p04b-001", "subject": "On the way"},
        ),
        (
            "imessage_attachment",
            "IMessageAttachments/p04-coverage-im-att.md",
            "imessage-attachment",
            "cov0001",
            {"imessage_message_id": "im-p04b-001", "attachment_id": "im-att-p04b", "filename": "map.png"},
        ),
        (
            "beeper_thread",
            "BeeperThreads/p04-coverage-bp-thread.md",
            "beeper-thread",
            "cov0001",
            {"beeper_room_id": "!p04broom:example.test", "thread_title": "P04B beeper"},
        ),
        (
            "beeper_message",
            "Beeper/p04-coverage-bp-msg.md",
            "beeper-message",
            "cov0001",
            {"beeper_event_id": "bp-p04b-001", "sender_name": "Alex Rivera"},
        ),
        (
            "beeper_attachment",
            "BeeperAttachments/p04-coverage-bp-att.md",
            "beeper-attachment",
            "cov0001",
            {"beeper_event_id": "bp-p04b-001", "attachment_id": "bp-att-p04b", "filename": "clip.png"},
        ),
        (
            "media_asset",
            "Photos/p04-coverage-photo.md",
            "media-asset",
            "cov0001",
            {"photos_asset_id": "photo-p04b-001", "filename": "loft.jpg", "title": "Loft stoop"},
        ),
        (
            "meeting_transcript",
            "MeetingTranscripts/p04-coverage-meeting.md",
            "meeting-transcript",
            "cov0001",
            {"otter_meeting_id": "otter-p04b-001", "title": "Loft logistics"},
        ),
        (
            "git_repository",
            "GitRepos/p04-coverage-repo.md",
            "git-repository",
            "cov0001",
            {"github_repo_id": "42424201", "name_with_owner": "pebble-labs/p04b-fixture"},
        ),
        (
            "git_commit",
            "GitCommits/p04-coverage-commit.md",
            "git-commit",
            "cov0001",
            {"commit_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "message_headline": "p04b fixture commit"},
        ),
        (
            "git_thread",
            "GitThreads/p04-coverage-thread.md",
            "git-thread",
            "cov0001",
            {"github_thread_id": "gh-thread-p04b", "number": "7", "thread_type": "pull"},
        ),
        (
            "git_message",
            "GitMessages/p04-coverage-gmsg.md",
            "git-message",
            "cov0001",
            {"github_message_id": "gh-msg-p04b", "actor_login": "arivera-p04b"},
        ),
        (
            "meal_order",
            "Transactions/MealOrders/p04-coverage-meal.md",
            "meal-order",
            "cov0001",
            {"service": "Pebble Eats", "restaurant": "Loft Kitchen", "total": 36.0},
        ),
        (
            "grocery_order",
            "Transactions/Groceries/p04-coverage-grocery.md",
            "grocery-order",
            "cov0001",
            {"service": "Pebble Grocer", "store": "Harbor Market", "total": 22.0},
        ),
        (
            "car_rental",
            "Transactions/CarRentals/p04-coverage-car.md",
            "car-rental",
            "cov0001",
            {"company": "Harbor Wheels", "pickup_location": "JFK", "confirmation_code": "CAR4B"},
        ),
        (
            "shipment",
            "Transactions/Shipments/p04-coverage-ship.md",
            "shipment",
            "cov0001",
            {"carrier": "Pebble Post", "tracking_number": "PP4B0001"},
        ),
        (
            "event_ticket",
            "Transactions/EventTickets/p04-coverage-ticket.md",
            "event-ticket",
            "cov0001",
            {"event_name": "Harbor Recital", "venue": "Loft Hall", "price": 28.0},
        ),
        (
            "payroll",
            "Transactions/Payroll/p04-coverage-pay.md",
            "payroll",
            "cov0001",
            {"employer": "Harbor Loft Collective", "pay_date": "2026-04-15", "net_amount": 1200.0},
        ),
        (
            "knowledge",
            "Knowledge/p04-coverage-knowledge.md",
            "knowledge",
            "cov0001",
            {"domain": "travel", "standing_query": "Which bookings belong to the April loft trip?"},
        ),
    ]
    cards = []
    for card_type, rel_path, kind, slug, extra in specs:
        uid = _uid(kind, slug)
        token = _token(f"COV-{kind}")
        extra = {"summary": extra.get("summary") or f"P04-B coverage {card_type}", "people": [PEOPLE_PRIMARY], **extra}
        cards.append(
            _card(
                uid,
                card_type,
                rel_path,
                f"Coverage card for {card_type}. {token}",
                ["coverage", "high_degree"],
                extra,
            )
        )
    cards.append(
        _card(
            _uid("document", "cur0001"),
            "document",
            "Documents/p04-current-chunk.md",
            f"Current itinerary chunk. Replaces retired key chunk:p04b-doc-v1. {_token('DOC-CURRENT')}",
            ["changed_deleted_chunks"],
            {
                "title": "Current loft itinerary",
                "filename": "itinerary-v2.md",
                "document_type": "itinerary",
                "people": [PEOPLE_PRIMARY],
                "tags": ["chunk_revision:current"],
            },
        )
    )
    cards.append(
        _card(
            _uid("document", "del0001"),
            "document",
            "Documents/p04-deleted-chunk.md",
            f"Retired chunk key chunk:p04b-doc-v1 is deleted. {_token('DOC-DELETED')}",
            ["changed_deleted_chunks"],
            {
                "title": "Retired loft itinerary",
                "filename": "itinerary-v1.md",
                "document_type": "itinerary",
                "people": [PEOPLE_PRIMARY],
                "tags": ["chunk_revision:deleted", "chunk_key:chunk:p04b-doc-v1"],
            },
        )
    )
    return cards


def build_cards() -> list[dict[str, Any]]:
    cards = _story_cards() + _coverage_cards()
    seen: set[str] = set()
    for card in cards:
        if card["uid"] in seen:
            raise ValueError(f"duplicate uid {card['uid']}")
        seen.add(card["uid"])
    cards.sort(key=lambda item: item["uid"])
    return cards


def _relation(
    case_id: str,
    family: str,
    rationale: str,
    positive: list[str],
    negative: list[str],
    paths: list[str],
    operation: str,
    request: str,
    owner: str,
    expected_fields: list[str],
    evidence_kinds: dict[str, str],
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "family": family,
        "rationale": rationale,
        "positive_evidence_ids": positive,
        "negative_evidence_ids": negative,
        "required_paths": paths,
        "operation": operation,
        "request": request,
        "owner_slice": owner,
        "expected_output_fields": expected_fields,
        "evidence_kinds": evidence_kinds,
        "label_role": "oracle",
    }


def build_relations(cards: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    by_uid = {card["uid"]: card for card in (cards or build_cards())}
    flight_out = _uid("flight", "out0001")
    flight_in = _uid("flight", "in00001")
    flight_look = _uid("flight", "look001")
    hotel = _uid("accommodation", "hotl001")
    ride = _uid("ride", "airp001")
    charge = _uid("finance", "chg0001")
    look_fin = _uid("finance", "look001")
    purchase = _uid("purchase", "hotl001")
    purchase_look = _uid("purchase", "look001")
    attach_rcpt = _uid("email-attachment", "rcpt001")
    booking = _uid("email-message", "book001")
    req = _uid("email-message", "req0001")
    reply = _uid("email-message", "reply01")
    stale = _uid("email-message", "stale01")
    person_a = _uid("person", "alex0001")
    person_b = _uid("person", "alex0002")
    renew = _uid("subscription", "renw001")
    cancel = _uid("subscription", "cncl001")
    observe = _uid("observation", "trip001")
    relations = [
        _relation(
            "rel-p04b-same-trip",
            "same-trip",
            "April 11-14 Harbor Loft itinerary shares confirmation HA4B11/LOFT4B. The May SFO-JFK booking is a proximity lookalike.",
            [flight_out, flight_in, hotel, ride, charge, booking],
            [flight_look],
            [by_uid[flight_out]["rel_path"], by_uid[hotel]["rel_path"], by_uid[ride]["rel_path"]],
            "trip_workflow",
            "Assemble the April Harbor Loft trip and exclude lookalike bookings.",
            "P10-C",
            ["member_uids", "unmatched_evidence", "excluded_uids"],
            {
                flight_out: "source_reported",
                flight_in: "source_reported",
                hotel: "source_reported",
                ride: "source_reported",
                charge: "source_reported",
                booking: "source_reported",
                observe: "proposed_link",
            },
        ),
        _relation(
            "rel-p04b-same-charge",
            "same-charge",
            "One supported 482.00 USD loft charge plus its receipt. 480.00 is a lookalike merchant amount.",
            [charge, purchase, attach_rcpt, hotel],
            [look_fin, purchase_look],
            [by_uid[charge]["rel_path"], by_uid[purchase]["rel_path"], by_uid[attach_rcpt]["rel_path"]],
            "reconcile_trip_costs",
            "Count one supported actual charge for the loft stay.",
            "P10-C",
            ["supported_charge_uid", "receipt_uids", "excluded_uids", "amount", "currency"],
            {charge: "source_reported", purchase: "derived", attach_rcpt: "source_reported", hotel: "source_reported"},
        ),
        _relation(
            "rel-p04b-not-the-same-person",
            "not-the-same-person",
            "Two people share the display name Alex Rivera and must stay distinct. Shared external id does not merge them.",
            [person_a, person_b],
            [],
            [by_uid[person_a]["rel_path"], by_uid[person_b]["rel_path"]],
            "person_query",
            "Return labeled memberships for each Alex Rivera and exclude a merge.",
            "P07-C",
            ["member_uids", "excluded_merge", "display_name"],
            {person_a: "source_reported", person_b: "source_reported"},
        ),
        _relation(
            "rel-p04b-reply-is-authorization",
            "reply-is-authorization",
            "The loft reply authorizes the later charge only as supporting evidence. No authorized=true field is emitted.",
            [req, reply, charge, booking],
            [stale, _uid("email-thread", "neg00001")],
            [by_uid[req]["rel_path"], by_uid[reply]["rel_path"], by_uid[charge]["rel_path"]],
            "retrieve_authorization_context",
            "Return the answer-bearing reply plus its preceding request and the later charge.",
            "P01-B1",
            ["reply_uid", "request_uid", "thread_uid", "supporting_charge_uid", "forbidden_authorized_flag"],
            {req: "source_reported", reply: "source_reported", charge: "source_reported", booking: "source_reported"},
        ),
        _relation(
            "rel-p04b-renewal-is-not-current",
            "renewal-is-not-current-subscription",
            "2025-11-01 renewal is last-observed. 2026-01-15 cancel conflicts. Freshness does not prove current billing.",
            [renew, cancel],
            [],
            [by_uid[renew]["rel_path"], by_uid[cancel]["rel_path"]],
            "subscription_lifecycle",
            "Report last-observed or unknown; do not claim a current subscription.",
            "P10-C",
            ["lifecycle_state", "last_observed_event_uid", "conflict_uids", "freshness"],
            {renew: "source_reported", cancel: "source_reported"},
        ),
    ]
    return relations


def _query(
    query_id: str,
    split: str,
    kind: str,
    request: str,
    *,
    expected_uids: list[str] | None = None,
    excluded_uids: list[str] | None = None,
    amount: float | None = None,
    currency: str | None = None,
    owner: str = "P04-B",
    notes: str = "",
) -> dict[str, Any]:
    payload = {
        "query_id": query_id,
        "split": split,
        "kind": kind,
        "request": request,
        "owner_slice": owner,
        "notes": notes,
    }
    if expected_uids is not None:
        payload["expected_uids"] = expected_uids
    if excluded_uids is not None:
        payload["excluded_uids"] = excluded_uids
    if amount is not None:
        payload["expected_amount"] = amount
    if currency is not None:
        payload["expected_currency"] = currency
    payload["label_hash"] = sha256_text(canonical_dumps({k: v for k, v in payload.items() if k != "label_hash"}))
    return payload


def build_queries() -> list[dict[str, Any]]:
    person_a = _uid("person", "alex0001")
    person_b = _uid("person", "alex0002")
    flight_out = _uid("flight", "out0001")
    flight_look = _uid("flight", "look001")
    hotel = _uid("accommodation", "hotl001")
    charge = _uid("finance", "chg0001")
    look_fin = _uid("finance", "look001")
    reply = _uid("email-message", "reply01")
    req = _uid("email-message", "req0001")
    suppressed = _uid("email-message", "supp001")
    quarantine = _uid("email-message", "qarn001")
    renew = _uid("subscription", "renw001")
    cancel = _uid("subscription", "cncl001")
    midnight = _uid("calendar-event", "midn001")
    return [
        _query(
            "q-p04b-exact-alex-primary",
            "calibration",
            "exact_id",
            person_a,
            expected_uids=[person_a],
            notes="Exact UID membership. Rank is irrelevant.",
        ),
        _query(
            "q-p04b-lex-loft-stay",
            "calibration",
            "lexical",
            "Harbor Loft NYC stay April",
            expected_uids=[hotel, charge],
            excluded_uids=[flight_look, look_fin],
            notes="Lexical relevance may vary in rank; lookalikes stay excluded.",
            owner="P01-B1",
        ),
        _query(
            "q-p04b-agg-eur-net",
            "calibration",
            "aggregate",
            "Net EUR for Pebble Press",
            expected_uids=[_uid("finance", "eur0001"), _uid("finance", "ref0001")],
            amount=60.0,
            currency="EUR",
            notes="100.00 + (-40.00) = 60.00 authored arithmetic.",
            owner="P10-C",
        ),
        _query(
            "q-p04b-ann-modulo-adversary",
            "calibration",
            "ann",
            "adversarial_modulo_ivf",
            expected_uids=["ann-p04b-0065"],
            notes="Independent exact cosine top-1. Do not copy native IVF neighbors.",
            owner="P01-A",
        ),
        _query(
            "q-p04b-person-namesake",
            "calibration",
            "lexical",
            "Alex Rivera",
            expected_uids=[person_a, person_b],
            notes="Both namesakes are relevant; merge is forbidden by the relation label.",
            owner="P10-A",
        ),
        _query(
            "q-p04b-lex-midnight",
            "calibration",
            "lexical",
            _token("CAL-MIDNIGHT"),
            expected_uids=[midnight],
            notes="Unique token membership for the crossing-midnight event.",
        ),
        _query(
            "q-p04b-exact-flight-out",
            "held_out",
            "exact_id",
            flight_out,
            expected_uids=[flight_out],
            notes="Held-out exact identifier.",
        ),
        _query(
            "q-p04b-lex-auth-reply",
            "held_out",
            "lexical",
            _token("AUTH-REPLY"),
            expected_uids=[reply, req],
            excluded_uids=[_uid("email-message", "stale01")],
            notes="Authorization context: reply plus request. No authorized=true.",
            owner="P01-B1",
        ),
        _query(
            "q-p04b-agg-same-charge",
            "held_out",
            "aggregate",
            "Supported loft charge total",
            expected_uids=[charge],
            excluded_uids=[look_fin],
            amount=482.0,
            currency="USD",
            notes="Exact fixture amount 482.00, exclude 480.00 lookalike.",
            owner="P10-C",
        ),
        _query(
            "q-p04b-lex-suppressed-excluded",
            "held_out",
            "lexical",
            "active eligible trip mail",
            excluded_uids=[suppressed, quarantine],
            notes="Active-eligible filter must exclude suppressed and quarantine. Enforcement is P05.",
            owner="P05-B",
        ),
        _query(
            "q-p04b-renewal-lifecycle",
            "held_out",
            "relation",
            "Is Pebble Mail a current subscription?",
            expected_uids=[renew, cancel],
            notes="Last-observed/unknown. Renewal is not proof of current billing.",
            owner="P10-C",
        ),
        _query(
            "q-p04b-lex-lookalike-excluded",
            "held_out",
            "lexical",
            _token("HOTEL"),
            expected_uids=[hotel],
            excluded_uids=[flight_look],
            notes="Hotel token must not pull the May lookalike flight.",
            owner="P10-C",
        ),
    ]


def build_ann_manifest() -> dict[str, Any]:
    dataset = build_adversarial_modulo_ivf_dataset(seed=CORPUS_SEED)
    return {
        "seed": dataset.seed,
        "n": len(dataset.items),
        "dimension": dataset.dimension,
        "nlist": dataset.nlist,
        "nprobe": dataset.nprobe,
        "query": list(dataset.query),
        "true_neighbor_id": dataset.true_neighbor_id,
        "true_neighbor_index": dataset.true_neighbor_index,
        "unprobed_list": dataset.unprobed_list,
        "vector_sha256": dataset.vector_sha256,
        "construction": (
            "n=1089 => nlist=33, nprobe=32. True neighbor is index 65 (second member of list 32). "
            "List 32 first member is anti-aligned so the old first-member probe skips that list."
        ),
    }


def build_manifest(cards: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    cards = cards or build_cards()
    queries = build_queries()
    relations = build_relations(cards)
    types = sorted({card["type"] for card in cards})
    families: dict[str, list[str]] = {}
    for card in cards:
        for family in card["families"]:
            families.setdefault(family, []).append(card["uid"])
    for key in families:
        families[key] = sorted(families[key])
    payload = {
        "contract_version": CONTRACT_VERSION,
        "seed": CORPUS_SEED,
        "card_count": len(cards),
        "type_count": len(types),
        "types": types,
        "families": families,
        "cards": [
            {
                "uid": card["uid"],
                "type": card["type"],
                "rel_path": card["rel_path"],
                "families": card["families"],
                "corpus_state": card["corpus_state"],
                "edge_kind": card["edge_kind"],
                "token": next((part for part in card["body"].split() if part.startswith("P04B-TOKEN-")), ""),
            }
            for card in cards
        ],
        "ann": build_ann_manifest(),
        "query_splits": {
            "calibration": sorted(q["query_id"] for q in queries if q["split"] == "calibration"),
            "held_out": sorted(q["query_id"] for q in queries if q["split"] == "held_out"),
        },
        "relation_case_ids": [item["case_id"] for item in relations],
    }
    payload["hashes"] = {
        "cards": sha256_text(canonical_dumps(cards)),
        "queries": sha256_text(canonical_dumps(queries)),
        "relations": sha256_text(canonical_dumps(relations)),
        "ann_vectors": payload["ann"]["vector_sha256"],
    }
    return payload


def frozen_payloads() -> dict[str, Any]:
    cards = build_cards()
    return {
        "corpus_manifest": build_manifest(cards),
        "expected_queries": {
            "contract_version": CONTRACT_VERSION,
            "seed": CORPUS_SEED,
            "queries": build_queries(),
        },
        "expected_relations": {
            "contract_version": CONTRACT_VERSION,
            "seed": CORPUS_SEED,
            "relations": build_relations(cards),
        },
        "cards": cards,
    }


def write_frozen_data_files(data_dir: Path | None = None) -> dict[str, Path]:
    data_dir = Path(data_dir or DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    payloads = frozen_payloads()
    written = {}
    for name in ("corpus_manifest", "expected_queries", "expected_relations"):
        path = data_dir / f"{name}.json"
        path.write_text(canonical_dumps(payloads[name]), encoding="utf-8")
        written[name] = path
    return written


def load_json(name: str) -> dict[str, Any]:
    path = DATA_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


def instantiate_card(spec: Mapping[str, Any]) -> BaseCard:
    model = CARD_TYPES[str(spec["type"])]
    payload = {"uid": spec["uid"], "type": spec["type"], **dict(spec["fields"])}
    return model.model_validate(payload)


def materialize_corpus(vault: Path, *, owned_root: Path) -> dict[str, Any]:
    """Write every frozen card through ``write_card`` into an owned vault."""

    init_vault(vault, owned_root=owned_root)
    cards = build_cards()
    written = []
    for spec in cards:
        card = instantiate_card(spec)
        path = write_card(vault, spec["rel_path"], card, body=spec["body"], provenance=provenance_for(card))
        written.append({"uid": spec["uid"], "rel_path": spec["rel_path"], "path": str(path)})
    return {"card_count": len(written), "cards": written, "hashes": build_manifest(cards)["hashes"]}


def collect_corpus_text(cards: list[dict[str, Any]] | None = None) -> str:
    cards = cards or build_cards()
    parts = [
        canonical_dumps(frozen_payloads()[name])
        for name in ("corpus_manifest", "expected_queries", "expected_relations")
    ]
    for card in cards:
        parts.append(card["uid"])
        parts.append(card["rel_path"])
        parts.append(card["body"])
        parts.append(canonical_dumps(card["fields"]))
    return "\n".join(parts)


def forbidden_text_hits(text: str) -> list[str]:
    lowered = text.lower()
    return [token for token in FORBIDDEN_TEXT if token in lowered]


def assert_relation_closure(
    relations: list[Mapping[str, Any]],
    cards_by_uid: Mapping[str, Mapping[str, Any]],
) -> None:
    """Fail when required evidence is missing or a negative is labeled positive."""

    if not relations:
        raise AssertionError("relation set is empty")
    families = {item["family"] for item in relations}
    missing_families = set(RELATION_OWNERS) - families
    if missing_families:
        raise AssertionError(f"missing labeled families: {sorted(missing_families)}")
    for item in relations:
        case_id = item["case_id"]
        positives = list(item.get("positive_evidence_ids") or [])
        negatives = list(item.get("negative_evidence_ids") or [])
        if not positives:
            raise AssertionError(f"{case_id} has no positive evidence")
        overlap = set(positives) & set(negatives)
        if overlap:
            raise AssertionError(f"{case_id} labels {sorted(overlap)} as both positive and negative")
        for uid in positives + negatives:
            if uid not in cards_by_uid and not str(uid).startswith("ann-p04b-"):
                raise AssertionError(f"{case_id} references missing card {uid}")
        for rel_path in item.get("required_paths") or []:
            if not any(card.get("rel_path") == rel_path for card in cards_by_uid.values()):
                raise AssertionError(f"{case_id} required path {rel_path} is not in the corpus")
        owner = str(item.get("owner_slice") or "")
        family = str(item["family"])
        expected_owner = RELATION_OWNERS[family]
        if family == "not-the-same-person":
            if owner not in {"P07-C", "P08-B", "P10-A"}:
                raise AssertionError(f"{case_id} owner {owner} is not a namesake owner")
        elif family == "reply-is-authorization":
            if owner not in {"P01-B1", "P10-B", "P06-D"}:
                raise AssertionError(f"{case_id} owner {owner} is not an authorization owner")
        elif owner != expected_owner:
            raise AssertionError(f"{case_id} owner {owner} != {expected_owner}")
        if item.get("authorized") is True:
            raise AssertionError(f"{case_id} must not manufacture authorized=true")


def assert_query_splits(queries: list[Mapping[str, Any]]) -> None:
    calibration = {item["query_id"] for item in queries if item.get("split") == "calibration"}
    held_out = {item["query_id"] for item in queries if item.get("split") == "held_out"}
    if not calibration or not held_out:
        raise AssertionError("calibration and held-out splits must both be non-empty")
    overlap = calibration & held_out
    if overlap:
        raise AssertionError(f"calibration/held-out overlap: {sorted(overlap)}")
    hashes = [item.get("label_hash") for item in queries]
    if len(hashes) != len(set(hashes)):
        raise AssertionError("query label hashes must be unique")
    for item in queries:
        recomputed = sha256_text(canonical_dumps({k: v for k, v in item.items() if k != "label_hash"}))
        if recomputed != item.get("label_hash"):
            raise AssertionError(f"query {item.get('query_id')} label_hash does not match payload")


def cardinalities(cards: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    cards = cards or build_cards()
    queries = build_queries()
    relations = build_relations(cards)
    ann = build_adversarial_modulo_ivf_dataset(seed=CORPUS_SEED)
    by_type: dict[str, int] = {}
    for card in cards:
        by_type[card["type"]] = by_type.get(card["type"], 0) + 1
    return {
        "cards": len(cards),
        "types": len(by_type),
        "by_type": dict(sorted(by_type.items())),
        "relations": len(relations),
        "queries": len(queries),
        "calibration_queries": sum(1 for item in queries if item["split"] == "calibration"),
        "held_out_queries": sum(1 for item in queries if item["split"] == "held_out"),
        "ann_vectors": len(ann.items),
        "ann_nlist": ivf_nlist(len(ann.items)),
        "ann_nprobe": ivf_nprobe(ivf_nlist(len(ann.items))),
    }


def p01_consumption_notes() -> dict[str, str]:
    return {
        "calibration": "Tune ranking/fusion only on expected_queries.json split=calibration.",
        "held_out": "Evaluate once on split=held_out. If a held-out case is used for tuning, mark that set used and version a new holdout.",
        "ann_oracle": "Use archive_tests.acceptance.oracle.exact_nearest_neighbors. Never copy native IVF neighbors into expected_queries.",
        "adversary": "build_adversarial_modulo_ivf_dataset is the hard AC. nlist=33, nprobe=32, true neighbor ann-p04b-0065.",
        "citations": "Lexical/exact_id expected_uids are membership oracles. Rank may vary for lexical; exclusions are hard.",
    }


def p10_consumption_notes() -> dict[str, str]:
    return {
        "cases": "Key workflows by expected_relations.json case_id and owner_slice.",
        "membership": "Assert positive_evidence_ids are present and negative_evidence_ids are absent. Do not pass on 'some card of the right type'.",
        "labels": "Relation rows are oracles, not production edges. Do not pre-write the missing linker edge to make E2E pass.",
        "authorization": "Score supporting-evidence coverage. No API may emit authorized=true.",
        "subscription": "Lifecycle is last-observed/unknown when cancel/freshness conflict. Renewal is not current billing.",
    }
