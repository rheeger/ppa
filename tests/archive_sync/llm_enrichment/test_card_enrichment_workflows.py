"""Tests for Phase 2.875 email thread workflow helpers (no LLM)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.threads import ThreadStub
from archive_sync.llm_enrichment.workflows import calendar_event as wf_cal
from archive_sync.llm_enrichment.workflows import document as wf_doc
from archive_sync.llm_enrichment.workflows import email_thread as wf
from archive_sync.llm_enrichment.workflows import finance as wf_fin
from archive_sync.llm_enrichment.workflows import imessage_thread as wf_ims


def _stub(**kwargs: Any) -> ThreadStub:
    base: dict[str, Any] = {
        "uid": "u1",
        "rel_path": "a.md",
        "gmail_thread_id": "tid1",
        "sent_at": "2024-01-01T00:00:00Z",
        "from_email": "friend@example.com",
        "from_name": "",
        "subject": "Re: dinner",
        "snippet": "",
        "direction": "sent",
        "participant_emails": (),
    }
    base.update(kwargs)
    return ThreadStub(**base)


def test_gate_email_thread_card() -> None:
    assert wf.gate_email_thread_card({"message_count": 1}) is False
    assert wf.gate_email_thread_card({"message_count": 2}) is True


def test_strip_thread_summary_boilerplate() -> None:
    assert (
        wf.strip_thread_summary_boilerplate(
            "This thread consists of automated notifications from Anthem."
        )
        == "Automated notifications from Anthem."
    )
    assert (
        wf.strip_thread_summary_boilerplate("In this thread, Zach coordinates with Silvergate.")
        == "Zach coordinates with Silvergate."
    )
    assert wf.strip_thread_summary_boilerplate("Discussed Q1 plan.") == "Discussed Q1 plan."


def test_parse_email_thread_response_strips_boilerplate() -> None:
    fields, _, _ = wf.parse_email_thread_response(
        {
            "thread_summary": "This thread is about dinner with Jane on Friday.",
            "entity_mentions": [],
            "calendar_match": None,
        },
        source_uid="u",
        run_id="r",
    )
    assert fields["thread_summary"] == "About dinner with Jane on Friday."


def test_parse_email_thread_response() -> None:
    data = {
        "thread_summary": "Discussed Q1 plan.",
        "entity_mentions": [
            {"type": "person", "name": "Jane", "context": {"role": "PM"}},
            {"type": "place", "name": "Lilia", "context": {"city": "Brooklyn"}},
        ],
        "calendar_match": {
            "title_keywords": ["dinner"],
            "approximate_date": "2024-03-15",
            "attendee_emails": ["jane@example.com"],
        },
    }
    fields, ents, matches = wf.parse_email_thread_response(
        data,
        source_uid="hfa-email-thread-x",
        run_id="run-test",
    )
    assert fields["thread_summary"] == "Discussed Q1 plan."
    assert len(ents) == 2
    assert ents[0].entity_type == "person"
    assert len(matches) == 1
    assert matches[0].target_card_type == "calendar_event"


def test_parse_empty_summary_skips_field_updates() -> None:
    fields, _, _ = wf.parse_email_thread_response(
        {"thread_summary": "", "entity_mentions": [], "calendar_match": None},
        source_uid="u",
        run_id="r",
    )
    assert fields == {}


def test_prefilter_passive_no_outbound_no_signals() -> None:
    ok, reason = wf.prefilter_email_thread(
        [_stub(direction="inbound"), _stub(uid="u2", direction="inbound")],
        None,
    )
    assert ok is False
    assert reason == "no_sent_no_signals"


def test_prefilter_accepts_outbound_as_participation() -> None:
    ok, reason = wf.prefilter_email_thread(
        [_stub(direction="inbound"), _stub(uid="u2", direction="outbound")],
        None,
    )
    assert ok is True
    assert reason == "outbound"


def test_prefilter_skips_known_noise_domain() -> None:
    ok, reason = wf.prefilter_email_thread(
        [_stub(from_email="x@mailchimp.com", direction="sent")],
        None,
    )
    assert ok is False
    assert reason == "known_noise"


def test_prefilter_gmail_promotions() -> None:
    ok, reason = wf.prefilter_email_thread(
        [_stub(direction="outbound")],
        None,
        thread_label_ids=["CATEGORY_PROMOTIONS", "INBOX"],
    )
    assert ok is False
    assert reason == "gmail_promotions"


def test_prefilter_passive_important() -> None:
    ok, reason = wf.prefilter_email_thread(
        [_stub(direction="inbound"), _stub(uid="u2", direction="inbound")],
        None,
        thread_label_ids=["IMPORTANT", "INBOX"],
    )
    assert ok is True
    assert reason == "passive_important"


def test_prefilter_passive_substantial_triage() -> None:
    stubs = [
        _stub(uid=f"u{i}", direction="inbound", from_email=f"p{i}@biz.io", subject="Re: deal")
        for i in range(5)
    ]
    ok, reason = wf.prefilter_email_thread(stubs, None)
    assert ok is True
    assert reason == "passive_substantial"


def test_prefilter_classify_index_noise(tmp_path: Path) -> None:
    db = tmp_path / "ci.db"
    with ClassifyIndex(db) as idx:
        idx.put_classification(
            "tid-noise",
            "noise",
            0.9,
            [],
            message_count=2,
            first_subject="x",
            first_from_email="a@b.com",
        )
    ok, reason = wf.prefilter_email_thread(
        [_stub(gmail_thread_id="tid-noise", direction="sent")],
        ClassifyIndex(db),
    )
    assert ok is False
    assert reason == "classify_index"


def test_prefilter_passes_conversation_stub() -> None:
    ok, reason = wf.prefilter_email_thread(
        [_stub(from_email="colleague@startup.io", direction="sent", subject="Re: Q1")],
        None,
    )
    assert ok is True
    assert reason == "outbound"


def test_gate_imessage_thread_card() -> None:
    assert wf_ims.gate_imessage_thread_card({"message_count": 0}) is False
    assert wf_ims.gate_imessage_thread_card({"message_count": 1}) is True


def test_prefilter_imessage_requires_from_me() -> None:
    assert wf_ims.prefilter_imessage_thread([]) == (False, "no_message_stubs")
    assert wf_ims.prefilter_imessage_thread([{"is_from_me": False}]) == (False, "no_from_me")
    assert wf_ims.prefilter_imessage_thread([{"is_from_me": True}]) == (True, "from_me")


def test_thread_display_label_imessage_and_beeper() -> None:
    assert wf_ims.thread_display_label({"display_name": "Mom"}, card_type="imessage_thread") == "Mom"
    assert (
        wf_ims.thread_display_label({"thread_title": "Signal: Jane"}, card_type="beeper_thread")
        == "Signal: Jane"
    )


def test_compose_and_dedupe_imessage_conversations() -> None:
    convs = [
        {
            "date_range": ["2024-01-01", "2024-01-02"],
            "summary": "planned dinner",
            "people_mentioned": [],
            "places_mentioned": [],
            "topics": ["food"],
        },
        {
            "date_range": ["2024-01-01", "2024-01-02"],
            "summary": "planned dinner",
            "people_mentioned": [],
            "places_mentioned": [],
            "topics": ["food"],
        },
    ]
    deduped = wf_ims.dedupe_conversations(convs)
    assert len(deduped) == 1
    text = wf_ims.compose_thread_summary("Jane", deduped)
    assert "Jane" in text and "dinner" in text


def test_build_outputs_from_imessage_conversations() -> None:
    convs = [
        {
            "date_range": ["2024-03-01", "2024-03-01"],
            "summary": "Discussed the venue.",
            "people_mentioned": ["Alex"],
            "places_mentioned": ["Lilia"],
            "topics": [],
        }
    ]
    fu, ents = wf_ims.build_outputs_from_conversations(
        convs,
        display_label="Jane",
        source_uid="u1",
        source_card_type="imessage_thread",
        run_id="r1",
    )
    assert "thread_summary" in fu
    assert len(ents) == 2
    assert {e.entity_type for e in ents} == {"person", "place"}


def test_gate_finance_card() -> None:
    assert wf_fin.gate_finance_card({"counterparty": "", "amount": 10}) is False
    assert wf_fin.gate_finance_card({"counterparty": "Amazon", "amount": 0.5}) is False
    assert wf_fin.gate_finance_card({"counterparty": "Amazon", "amount": -12.0}) is True


def test_prefilter_finance_passes_all() -> None:
    assert wf_fin.prefilter_finance({"excluded": True}) == (True, "ok")
    assert wf_fin.prefilter_finance({"transaction_type": "fee"}) == (True, "ok")
    assert wf_fin.prefilter_finance({"counterparty": "atm withdrawal"}) == (True, "ok")
    assert wf_fin.prefilter_finance({"counterparty": "Whole Foods", "transaction_type": "debit"}) == (True, "ok")


def test_parse_finance_response() -> None:
    data = {
        "counterparty_type": "merchant",
        "entity_mentions": [
            {
                "type": "organization",
                "name": "Whole Foods",
                "context": {"domain": "wholefoods.com"},
                "confidence": 0.8,
            }
        ],
        "email_match": {
            "counterparty_keywords": ["Whole Foods"],
            "amount": -42.0,
            "date_range": ["2024-01-01", "2024-01-07"],
        },
    }
    fu, ents, matches = wf_fin.parse_finance_response(
        data,
        source_uid="hfa-finance-x",
        run_id="r1",
        existing_provider_tags=[],
    )
    assert "provider_tags" in fu
    assert "counterparty_type:merchant" in fu["provider_tags"]
    assert len(ents) == 1
    assert ents[0].workflow == "finance_enrichment"
    assert len(matches) == 1
    assert matches[0].field_to_write == "source_email"
    assert matches[0].target_card_type == "email_message"


def test_parse_finance_response_defaults_missing_entity_type() -> None:
    """When the LLM omits entity mention type, infer from counterparty_type."""
    data = {
        "counterparty_type": "merchant",
        "entity_mentions": [{"name": "Acme Corp", "context": {}}],
        "email_match": None,
    }
    _, ents, _ = wf_fin.parse_finance_response(
        data,
        source_uid="hfa-finance-x",
        run_id="r1",
        existing_provider_tags=[],
    )
    assert len(ents) == 1
    assert ents[0].entity_type == "organization"

    data2 = {
        "counterparty_type": "person",
        "entity_mentions": [{"name": "Jane Doe", "context": {}}],
        "email_match": None,
    }
    _, ents2, _ = wf_fin.parse_finance_response(
        data2,
        source_uid="hfa-finance-x",
        run_id="r1",
        existing_provider_tags=[],
    )
    assert len(ents2) == 1
    assert ents2[0].entity_type == "person"


def test_merge_counterparty_type_idempotent() -> None:
    existing = ["foo:bar", "counterparty_type:merchant"]
    merged = wf_fin.merge_counterparty_type_into_provider_tags(existing, "person")
    assert "counterparty_type:person" in merged
    assert "counterparty_type:merchant" not in merged
    assert "foo:bar" in merged


def test_gate_calendar_event_requires_location() -> None:
    assert wf_cal.gate_calendar_event({"location": ""}) is False
    assert wf_cal.gate_calendar_event({"location": "https://zoom.us/j/123"}) is False
    assert wf_cal.gate_calendar_event({"location": "https://meet.google.com/abc"}) is False
    assert wf_cal.gate_calendar_event({"location": "123 Main St, Brooklyn"}) is True
    assert wf_cal.gate_calendar_event({"location": "Lilia, 567 Union Ave"}) is True


def test_prefilter_calendar_event() -> None:
    assert wf_cal.prefilter_calendar_event({"status": "cancelled", "title": "X"})[0] is False
    assert wf_cal.prefilter_calendar_event({"title": "Holidays in US", "attendee_emails": ["a@b.com"]})[0] is False
    assert wf_cal.prefilter_calendar_event({"title": "Lunch", "attendee_emails": []})[0] is False
    assert wf_cal.prefilter_calendar_event({"title": "Lunch", "attendee_emails": ["a@b.com"]})[0] is True
    assert wf_cal.prefilter_calendar_event({"title": "Trip", "all_day": True, "attendee_emails": []})[0] is False


def test_parse_calendar_response_place_only() -> None:
    data = {
        "place_extraction": {
            "name": "Lilia",
            "address": "567 Union Ave",
            "city": "Brooklyn",
            "place_type": "restaurant",
        },
    }
    ents = wf_cal.parse_calendar_response(
        data,
        source_uid="hfa-cal-x",
        run_id="r1",
    )
    assert len(ents) == 1
    assert ents[0].entity_type == "place"
    assert ents[0].raw_text == "Lilia"
    assert ents[0].context["city"] == "Brooklyn"


def test_parse_calendar_response_null_place() -> None:
    data = {"place_extraction": None}
    ents = wf_cal.parse_calendar_response(data, source_uid="u", run_id="r")
    assert ents == []


def test_gate_document_always_passes() -> None:
    assert wf_doc.gate_document({"text_source": "plain", "extension": "rtf"}) is True
    assert wf_doc.gate_document({}) is True


def test_prefilter_document_always_passes() -> None:
    assert wf_doc.prefilter_document({"document_type": "spreadsheet", "extension": "csv"}) == (True, "ok")


def test_should_skip_populated_document_never_skips() -> None:
    """Long ingestion ``description`` is not trusted as enrichment; always run LLM."""

    assert wf_doc.should_skip_populated_document({"description": "x" * 200, "quality_flags": []}) is False
    assert wf_doc.should_skip_populated_document({"description": "", "quality_flags": ["title_from_filename"]}) is False


def test_parse_document_response() -> None:
    fm = {
        "description": "",
        "title": "statement.pdf",
        "quality_flags": ["title_from_filename"],
        "document_date": "",
    }
    body = "Account statement for period ending January 31 2024. Balance $100."
    data = {
        "summary": "Jan 2024 bank statement; closing balance $100.",
        "description": "Bank statement for Jan 2024; closing balance $100.",
        "title": "Jan 2024 bank statement",
        "document_date": "2024-01-31",
    }
    fu = wf_doc.parse_document_response(data, fm=fm, body=body)
    assert fu["description"].startswith("Bank statement")
    assert fu["title"] == "Jan 2024 bank statement"
    assert fu["document_date"] == "2024-01-31"
    assert fu["summary"] == "Jan 2024 bank statement; closing balance $100."
    assert fu["quality_flags"] == []


def test_parse_document_response_overwrites_description_and_fills_summary() -> None:
    fm = {
        "description": "A custom summary written by a human.",
        "title": "ok title",
        "quality_flags": [],
        "document_date": "",
    }
    body = "Completely different text in the document body with more content."
    fu = wf_doc.parse_document_response(
        {
            "summary": "Replacement line.",
            "description": "New description from LLM.",
            "title": None,
            "document_date": None,
        },
        fm=fm,
        body=body,
    )
    assert fu["description"] == "New description from LLM."
    assert fu["summary"] == "Replacement line."


def test_parse_document_response_summary_fallback_from_description() -> None:
    fm = {"description": "", "title": "t", "quality_flags": []}
    fu = wf_doc.parse_document_response(
        {"description": "First sentence here. Second sentence.", "summary": ""},
        fm=fm,
        body="x",
    )
    assert "summary" in fu
    assert fu["summary"].startswith("First sentence")


def test_document_content_hash_stable() -> None:
    fm = {"filename": "a.pdf", "document_type": "pdf", "extension": "pdf", "title": "t", "file_modified_at": "2024-01-01"}
    h1 = wf_doc.document_content_hash(fm, "hello world")
    h2 = wf_doc.document_content_hash(fm, "hello world")
    assert h1 == h2
    assert wf_doc.document_content_hash(fm, "different") != h1
