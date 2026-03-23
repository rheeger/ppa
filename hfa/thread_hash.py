"""Thread body hash helpers for conversation summaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hfa.provenance import compute_input_hash
from hfa.schema import (EmailThreadCard, IMessageThreadCard,
                        validate_card_permissive)
from hfa.vault import find_note_by_slug, read_note


def slug_from_wikilink(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("[[") and cleaned.endswith("]]"):
        return cleaned[2:-2].split("|", 1)[0].strip()
    return cleaned


def imessage_thread_messages_payload(card: IMessageThreadCard, vault_path: str | Path) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    root = Path(vault_path)
    for wikilink in card.messages:
        slug = slug_from_wikilink(wikilink)
        if not slug:
            continue
        note_path = find_note_by_slug(root, slug)
        if note_path is None:
            continue
        frontmatter, body, _ = read_note(root, str(note_path.relative_to(root)))
        payload.append(
            {
                "message_id": str(frontmatter.get("imessage_message_id", "")).strip(),
                "sent_at": str(frontmatter.get("sent_at", "")).strip(),
                "sender_handle": str(frontmatter.get("sender_handle", "")).strip(),
                "subject": str(frontmatter.get("subject", "")).strip(),
                "body": body,
                "type": validate_card_permissive(frontmatter).type,
            }
        )
    return payload


def compute_imessage_thread_body_sha_from_payload(payload: list[dict[str, Any]]) -> str:
    return compute_input_hash({"messages": payload})


def compute_imessage_thread_body_sha(card: IMessageThreadCard, vault_path: str | Path) -> str:
    return compute_imessage_thread_body_sha_from_payload(imessage_thread_messages_payload(card, vault_path))


def compute_email_message_body_sha_from_payload(payload: dict[str, Any]) -> str:
    return compute_input_hash(
        {
            "message_id": str(payload.get("message_id", "")).strip(),
            "sent_at": str(payload.get("sent_at", "")).strip(),
            "direction": str(payload.get("direction", "")).strip(),
            "from_email": str(payload.get("from_email", "")).strip(),
            "to_emails": list(payload.get("to_emails", []) or []),
            "cc_emails": list(payload.get("cc_emails", []) or []),
            "bcc_emails": list(payload.get("bcc_emails", []) or []),
            "reply_to_emails": list(payload.get("reply_to_emails", []) or []),
            "subject": str(payload.get("subject", "")).strip(),
            "body": str(payload.get("body", "")).strip(),
            "attachments": list(payload.get("attachment_ids", []) or []),
            "invite": {
                "ical_uid": str(payload.get("invite_ical_uid", "")).strip(),
                "event_id_hint": str(payload.get("invite_event_id_hint", "")).strip(),
                "method": str(payload.get("invite_method", "")).strip(),
                "title": str(payload.get("invite_title", "")).strip(),
                "start_at": str(payload.get("invite_start_at", "")).strip(),
                "end_at": str(payload.get("invite_end_at", "")).strip(),
            },
        }
    )


def compute_email_attachment_metadata_sha_from_payload(payload: dict[str, Any]) -> str:
    return compute_input_hash(
        {
            "message_id": str(payload.get("message_id", "")).strip(),
            "attachment_id": str(payload.get("attachment_id", "")).strip(),
            "filename": str(payload.get("filename", "")).strip(),
            "mime_type": str(payload.get("mime_type", "")).strip(),
            "size_bytes": int(payload.get("size_bytes", 0) or 0),
            "content_id": str(payload.get("content_id", "")).strip(),
            "is_inline": bool(payload.get("is_inline", False)),
        }
    )


def compute_email_thread_body_sha_from_payload(payload: list[dict[str, Any]]) -> str:
    return compute_input_hash({"messages": payload})


def email_thread_messages_payload(card: EmailThreadCard, vault_path: str | Path) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    root = Path(vault_path)
    for wikilink in card.messages:
        slug = slug_from_wikilink(wikilink)
        if not slug:
            continue
        note_path = find_note_by_slug(root, slug)
        if note_path is None:
            continue
        frontmatter, body, _ = read_note(root, str(note_path.relative_to(root)))
        payload.append(
            {
                "message_id": str(frontmatter.get("gmail_message_id", "")).strip(),
                "sent_at": str(frontmatter.get("sent_at", "")).strip(),
                "direction": str(frontmatter.get("direction", "")).strip(),
                "from_email": str(frontmatter.get("from_email", "")).strip(),
                "to_emails": list(frontmatter.get("to_emails", []) or []),
                "cc_emails": list(frontmatter.get("cc_emails", []) or []),
                "bcc_emails": list(frontmatter.get("bcc_emails", []) or []),
                "reply_to_emails": list(frontmatter.get("reply_to_emails", []) or []),
                "subject": str(frontmatter.get("subject", "")).strip(),
                "body": body,
                "attachment_ids": [slug_from_wikilink(value) for value in frontmatter.get("attachments", []) or []],
                "invite_ical_uid": str(frontmatter.get("invite_ical_uid", "")).strip(),
                "invite_event_id_hint": str(frontmatter.get("invite_event_id_hint", "")).strip(),
                "invite_method": str(frontmatter.get("invite_method", "")).strip(),
                "invite_title": str(frontmatter.get("invite_title", "")).strip(),
                "invite_start_at": str(frontmatter.get("invite_start_at", "")).strip(),
                "invite_end_at": str(frontmatter.get("invite_end_at", "")).strip(),
            }
        )
    return payload


def compute_email_thread_body_sha(card: EmailThreadCard, vault_path: str | Path) -> str:
    return compute_email_thread_body_sha_from_payload(email_thread_messages_payload(card, vault_path))


def compute_calendar_event_body_sha_from_payload(payload: dict[str, Any]) -> str:
    return compute_input_hash(
        {
            "calendar_id": str(payload.get("calendar_id", "")).strip(),
            "event_id": str(payload.get("event_id", "")).strip(),
            "ical_uid": str(payload.get("ical_uid", "")).strip(),
            "status": str(payload.get("status", "")).strip(),
            "title": str(payload.get("title", "")).strip(),
            "description": str(payload.get("description", "")).strip(),
            "location": str(payload.get("location", "")).strip(),
            "start_at": str(payload.get("start_at", "")).strip(),
            "end_at": str(payload.get("end_at", "")).strip(),
            "timezone": str(payload.get("timezone", "")).strip(),
            "organizer_email": str(payload.get("organizer_email", "")).strip(),
            "organizer_name": str(payload.get("organizer_name", "")).strip(),
            "attendee_emails": list(payload.get("attendee_emails", []) or []),
            "recurrence": list(payload.get("recurrence", []) or []),
            "conference_url": str(payload.get("conference_url", "")).strip(),
            "source_messages": list(payload.get("source_messages", []) or []),
            "source_threads": list(payload.get("source_threads", []) or []),
            "meeting_transcripts": list(payload.get("meeting_transcripts", []) or []),
            "all_day": bool(payload.get("all_day", False)),
        }
    )


def compute_meeting_transcript_body_sha_from_payload(payload: dict[str, Any]) -> str:
    return compute_input_hash(
        {
            "otter_meeting_id": str(payload.get("otter_meeting_id", "")).strip(),
            "otter_conversation_id": str(payload.get("otter_conversation_id", "")).strip(),
            "title": str(payload.get("title", "")).strip(),
            "status": str(payload.get("status", "")).strip(),
            "start_at": str(payload.get("start_at", "")).strip(),
            "end_at": str(payload.get("end_at", "")).strip(),
            "duration_seconds": int(payload.get("duration_seconds", 0) or 0),
            "speaker_names": list(payload.get("speaker_names", []) or []),
            "speaker_emails": list(payload.get("speaker_emails", []) or []),
            "participant_names": list(payload.get("participant_names", []) or []),
            "participant_emails": list(payload.get("participant_emails", []) or []),
            "host_name": str(payload.get("host_name", "")).strip(),
            "host_email": str(payload.get("host_email", "")).strip(),
            "conference_url": str(payload.get("conference_url", "")).strip(),
            "event_id_hint": str(payload.get("event_id_hint", "")).strip(),
            "ical_uid": str(payload.get("ical_uid", "")).strip(),
            "summary": str(payload.get("summary_text", "")).strip(),
            "action_items": list(payload.get("action_items", []) or []),
            "transcript": str(payload.get("transcript", "")).strip(),
        }
    )
