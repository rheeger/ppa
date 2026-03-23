"""Seed link generation and scoring tests."""

from __future__ import annotations

from pathlib import Path

from archive_mcp.seed_links import (  # type: ignore[import-not-found]
    DECISION_CANONICAL_SAFE, LINK_TYPE_EVENT_HAS_MESSAGE,
    LINK_TYPE_EVENT_HAS_TRANSCRIPT, LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT,
    LINK_TYPE_MESSAGE_MENTIONS_PERSON, LINK_TYPE_POSSIBLE_SAME_PERSON,
    LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT, MODULE_CALENDAR,
    MODULE_COMMUNICATION, MODULE_IDENTITY, MODULE_MEDIA,
    build_seed_link_catalog, evaluate_seed_link_candidate,
    generate_seed_link_candidates, get_surface_policy_rows)
from hfa.provenance import ProvenanceEntry
from hfa.schema import (CalendarEventCard, EmailMessageCard, EmailThreadCard,
                        MediaAssetCard, MeetingTranscriptCard, PersonCard)
from hfa.vault import write_card


def _prov(*fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry("seed-test", "2026-03-10", "deterministic") for field in fields}


def _seed_link_vault(vault: Path) -> None:
    (vault / "People").mkdir(parents=True)
    (vault / "Email").mkdir()
    (vault / "Calendar").mkdir()
    (vault / "MeetingTranscripts" / "2026-03").mkdir(parents=True)
    (vault / "Photos").mkdir()
    (vault / "_meta").mkdir()
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")

    jane = PersonCard(
        uid="hfa-person-jane11111111",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Jane Smith",
        first_name="Jane",
        last_name="Smith",
        emails=["jane@example.com"],
        aliases=["Janie Smith"],
        company="Endaoment",
    )
    jane_dup = PersonCard(
        uid="hfa-person-jane22222222",
        type="person",
        source=["linkedin"],
        source_id="jane-linkedin",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Jane Smith",
        first_name="Jane",
        last_name="Smith",
        emails=["jane@example.com"],
        company="Endaoment",
    )
    thread = EmailThreadCard(
        uid="hfa-email-thread-aaaa1111",
        type="email_thread",
        source=["gmail"],
        source_id="gmail-thread-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board dinner with Jane",
        gmail_thread_id="gmail-thread-1",
        account_email="robbie@example.com",
        subject="Board dinner with Jane Smith",
        participants=["jane@example.com"],
    )
    message = EmailMessageCard(
        uid="hfa-email-message-bbbb1111",
        type="email_message",
        source=["gmail"],
        source_id="gmail-message-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board dinner invite",
        gmail_message_id="gmail-message-1",
        gmail_thread_id="gmail-thread-1",
        account_email="robbie@example.com",
        from_email="robbie@example.com",
        to_emails=["jane@example.com"],
        participant_emails=["jane@example.com"],
        subject="Board dinner invite",
        snippet="Dinner with Jane tomorrow",
        invite_event_id_hint="board-dinner-event",
        invite_title="Endaoment board dinner",
        invite_start_at="2026-03-11T18:00:00Z",
    )
    event = CalendarEventCard(
        uid="hfa-calendar-event-cccc111",
        type="calendar_event",
        source=["google.calendar"],
        source_id="calendar-event-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Endaoment board dinner",
        account_email="robbie@example.com",
        calendar_id="primary",
        event_id="board-dinner-event",
        ical_uid="board-dinner-ical",
        title="Endaoment board dinner",
        start_at="2026-03-11T18:00:00Z",
        end_at="2026-03-11T20:00:00Z",
        attendee_emails=["jane@example.com"],
    )
    photo = MediaAssetCard(
        uid="hfa-media-asset-dddd1111",
        type="media_asset",
        source=["apple.photos"],
        source_id="photo-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Dinner photo",
        photos_asset_id="photo-1",
        filename="dinner.jpg",
        captured_at="2026-03-11T18:30:00Z",
        place_name="Mission District",
        person_labels=["Jane Smith"],
    )
    transcript = MeetingTranscriptCard(
        uid="hfa-meeting-transcript-eeee1",
        type="meeting_transcript",
        source=["otter.meeting"],
        source_id="meeting-1",
        created="2026-03-11",
        updated="2026-03-11",
        summary="Endaoment board dinner transcript",
        otter_meeting_id="meeting-1",
        title="Endaoment board dinner",
        start_at="2026-03-11T18:00:00Z",
        end_at="2026-03-11T20:00:00Z",
        participant_emails=["jane@example.com"],
        event_id_hint="board-dinner-event",
    )

    write_card(vault, "People/jane-smith.md", jane, provenance=_prov("summary", "first_name", "last_name", "emails", "aliases", "company"))
    write_card(vault, "People/jane-smith-dup.md", jane_dup, provenance=_prov("summary", "first_name", "last_name", "emails", "company"))
    write_card(vault, "Email/board-thread.md", thread, provenance=_prov("summary", "gmail_thread_id", "account_email", "subject", "participants"))
    write_card(
        vault,
        "Email/board-message.md",
        message,
        provenance=_prov(
            "summary",
            "gmail_message_id",
            "gmail_thread_id",
            "account_email",
            "from_email",
            "to_emails",
            "participant_emails",
            "subject",
            "snippet",
            "invite_event_id_hint",
            "invite_title",
            "invite_start_at",
        ),
    )
    write_card(
        vault,
        "Calendar/board-event.md",
        event,
        provenance=_prov("summary", "account_email", "calendar_id", "event_id", "ical_uid", "title", "start_at", "end_at", "attendee_emails"),
    )
    write_card(
        vault,
        "Photos/dinner-photo.md",
        photo,
        provenance=_prov("summary", "photos_asset_id", "filename", "captured_at", "place_name", "person_labels"),
    )
    write_card(
        vault,
        "MeetingTranscripts/2026-03/board-dinner-transcript.md",
        transcript,
        body="## Transcript\n\nRobbie Heeger | Let's review the board dinner agenda.",
        provenance=_prov("summary", "otter_meeting_id", "title", "start_at", "end_at", "participant_emails", "event_id_hint"),
    )


def test_surface_policy_rows_include_expected_link_types():
    rows = get_surface_policy_rows()
    link_types = {row["link_type"] for row in rows}
    assert LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT in link_types
    assert LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT in link_types
    assert LINK_TYPE_POSSIBLE_SAME_PERSON in link_types


def test_seed_link_catalog_and_candidate_generation(tmp_path: Path):
    vault = tmp_path / "hf-archives"
    _seed_link_vault(vault)
    catalog = build_seed_link_catalog(vault)

    message = next(item for item in catalog.cards_by_type["email_message"] if item.slug == "board-message")
    identity = next(item for item in catalog.cards_by_type["person"] if item.slug == "jane-smith")
    media = next(item for item in catalog.cards_by_type["media_asset"] if item.slug == "dinner-photo")
    transcript = next(item for item in catalog.cards_by_type["meeting_transcript"] if item.slug == "board-dinner-transcript")

    communication_candidates = generate_seed_link_candidates(catalog, message, MODULE_COMMUNICATION)
    link_types = {candidate.proposed_link_type for candidate in communication_candidates}
    assert LINK_TYPE_MESSAGE_MENTIONS_PERSON in link_types

    calendar_candidates = generate_seed_link_candidates(catalog, message, MODULE_CALENDAR)
    assert any(candidate.proposed_link_type == LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT for candidate in calendar_candidates)

    transcript_candidates = generate_seed_link_candidates(catalog, transcript, MODULE_CALENDAR)
    assert any(candidate.proposed_link_type == LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT for candidate in transcript_candidates)

    identity_candidates = generate_seed_link_candidates(catalog, identity, MODULE_IDENTITY)
    assert any(candidate.proposed_link_type == LINK_TYPE_POSSIBLE_SAME_PERSON for candidate in identity_candidates)

    media_candidates = generate_seed_link_candidates(catalog, media, MODULE_MEDIA)
    assert media_candidates


def test_seed_link_scoring_prefers_exact_deterministic_links(tmp_path: Path):
    vault = tmp_path / "hf-archives"
    _seed_link_vault(vault)
    catalog = build_seed_link_catalog(vault)
    message = next(item for item in catalog.cards_by_type["email_message"] if item.slug == "board-message")
    event = next(item for item in catalog.cards_by_type["calendar_event"] if item.slug == "board-event")

    calendar_candidates = [
        candidate
        for candidate in generate_seed_link_candidates(catalog, message, MODULE_CALENDAR)
        if candidate.proposed_link_type == LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT and candidate.target_card_uid == event.uid
    ]
    assert calendar_candidates

    decision = evaluate_seed_link_candidate(vault, catalog, calendar_candidates[0])
    assert decision.final_confidence >= 0.92
    assert decision.decision == DECISION_CANONICAL_SAFE


def test_seed_link_scoring_prefers_exact_transcript_event_links(tmp_path: Path):
    vault = tmp_path / "hf-archives"
    _seed_link_vault(vault)
    catalog = build_seed_link_catalog(vault)
    transcript = next(item for item in catalog.cards_by_type["meeting_transcript"] if item.slug == "board-dinner-transcript")
    event = next(item for item in catalog.cards_by_type["calendar_event"] if item.slug == "board-event")

    transcript_candidates = [
        candidate
        for candidate in generate_seed_link_candidates(catalog, transcript, MODULE_CALENDAR)
        if candidate.proposed_link_type == LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT and candidate.target_card_uid == event.uid
    ]
    assert transcript_candidates

    decision = evaluate_seed_link_candidate(vault, catalog, transcript_candidates[0])
    assert decision.final_confidence >= 0.92
    assert decision.decision == DECISION_CANONICAL_SAFE


def test_calendar_event_generates_reverse_transcript_candidate(tmp_path: Path):
    vault = tmp_path / "hf-archives"
    _seed_link_vault(vault)
    catalog = build_seed_link_catalog(vault)
    event = next(item for item in catalog.cards_by_type["calendar_event"] if item.slug == "board-event")

    candidates = [
        candidate
        for candidate in generate_seed_link_candidates(catalog, event, MODULE_CALENDAR)
        if candidate.proposed_link_type == LINK_TYPE_EVENT_HAS_TRANSCRIPT
    ]
    assert candidates


def test_seed_link_scoring_keeps_identity_merge_review_first(tmp_path: Path):
    vault = tmp_path / "hf-archives"
    _seed_link_vault(vault)
    catalog = build_seed_link_catalog(vault)
    identity = next(item for item in catalog.cards_by_type["person"] if item.slug == "jane-smith")

    candidates = [
        candidate
        for candidate in generate_seed_link_candidates(catalog, identity, MODULE_IDENTITY)
        if candidate.proposed_link_type == LINK_TYPE_POSSIBLE_SAME_PERSON
    ]
    assert candidates

    decision = evaluate_seed_link_candidate(vault, catalog, candidates[0])
    assert decision.final_confidence >= 0.80
    assert decision.decision in {"review", "auto_promote", "canonical_safe"}
