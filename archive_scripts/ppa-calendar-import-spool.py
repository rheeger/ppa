#!/usr/bin/env python3
"""Single-writer Calendar import from extracted spool files."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from archive_sync.adapters.calendar_events import CalendarEventsAdapter
from archive_vault.identity import IdentityCache
from archive_vault.thread_hash import compute_calendar_event_body_sha_from_payload
from archive_vault.vault import write_card


def _read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default
    return payload if isinstance(payload, dict) else default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _collect_event_files(spool_dir: Path) -> list[Path]:
    candidates = sorted(spool_dir.glob("**/events/*.json"))
    deduped: dict[str, Path] = {}
    for path in candidates:
        deduped.setdefault(path.stem, path)
    return [deduped[key] for key in sorted(deduped)]


def _collect_manifest_files(spool_dir: Path) -> list[Path]:
    return sorted(spool_dir.glob("**/_meta/manifest.json"))


def _assert_manifest_consistency(spool_dir: Path, *, account_email: str, calendar_id: str) -> None:
    manifests = _collect_manifest_files(spool_dir)
    if not manifests:
        return
    expected_account = account_email.strip().lower()
    expected_calendar = calendar_id.strip()
    for manifest_path in manifests:
        payload = _read_json(manifest_path, {})
        manifest_account = str(payload.get("account_email", "")).strip().lower()
        manifest_calendar = str(payload.get("calendar_id", "")).strip()
        if not manifest_account:
            raise RuntimeError(f"Spool manifest missing account_email: {manifest_path}")
        if not manifest_calendar:
            raise RuntimeError(f"Spool manifest missing calendar_id: {manifest_path}")
        if manifest_account != expected_account or manifest_calendar != expected_calendar:
            raise RuntimeError(
                "Spool manifest mismatch in "
                f"{manifest_path}: expected ({expected_account}, {expected_calendar}), "
                f"found ({manifest_account}, {manifest_calendar})"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-writer Calendar import from spool files")
    parser.add_argument("--vault", required=True)
    parser.add_argument("--spool-dir", required=True)
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--calendar-id", default="primary")
    parser.add_argument("--checkpoint-every-events", type=int, default=25)
    parser.add_argument("--max-events", type=int, default=0, help="0 means no limit")
    args = parser.parse_args()

    vault = Path(args.vault).expanduser().resolve()
    spool_dir = Path(args.spool_dir).expanduser().resolve()
    meta_dir = spool_dir / "_meta"
    state_path = meta_dir / "import-state.json"
    adapter = CalendarEventsAdapter()
    account_email = args.account_email.strip().lower()
    calendar_id = args.calendar_id.strip()
    _assert_manifest_consistency(spool_dir, account_email=account_email, calendar_id=calendar_id)
    identity_cache = IdentityCache(vault)
    message_by_ical_uid, thread_by_ical_uid, message_by_event_id, thread_by_event_id = adapter._invite_lookup(
        str(vault),
        account_email=account_email,
    )
    transcript_by_ical_uid, transcript_by_event_id = adapter._meeting_transcript_lookup(str(vault))

    event_files = _collect_event_files(spool_dir)
    state = _read_json(
        state_path,
        {
            "next_index": 0,
            "processed_events": 0,
            "created_event_cards": 0,
            "complete": False,
        },
    )
    if state.get("complete"):
        print(json.dumps({"status": "already_complete", **state}, indent=2))
        return 0

    next_index = int(state.get("next_index", 0) or 0)
    processed_events = int(state.get("processed_events", 0) or 0)
    created = Counter({"calendar_event": int(state.get("created_event_cards", 0) or 0)})
    events_since_checkpoint = 0

    print(
        json.dumps(
            {
                "mode": "calendar-spool-import",
                "next_index": next_index,
                "event_file_count": len(event_files),
                "processed_events": processed_events,
            },
            indent=2,
        ),
        flush=True,
    )

    def checkpoint(*, complete: bool = False) -> None:
        _write_json(
            state_path,
            {
                "next_index": next_index,
                "processed_events": processed_events,
                "created_event_cards": created["calendar_event"],
                "complete": complete,
            },
        )

    while next_index < len(event_files):
        if args.max_events and processed_events >= args.max_events:
            checkpoint()
            print(
                json.dumps({"status": "paused", "reason": "max_events", "next_index": next_index}, indent=2), flush=True
            )
            return 0

        event = json.loads(event_files[next_index].read_text(encoding="utf-8"))
        event_id = str(event.get("id", "")).strip()
        if not event_id:
            next_index += 1
            continue
        event_etag = str(event.get("etag", "")).strip()
        organizer_email = str((event.get("organizer") or {}).get("email", "")).strip().lower()
        organizer_name = str((event.get("organizer") or {}).get("displayName", "")).strip()
        attendee_emails = [
            str(attendee.get("email", "")).strip().lower()
            for attendee in event.get("attendees", []) or []
            if str(attendee.get("email", "")).strip()
        ]
        all_emails = [email for email in [organizer_email, *attendee_emails] if email]
        ical_uid = str(event.get("iCalUID", "")).strip()
        source_messages: list[str] = []
        source_threads: list[str] = []
        meeting_transcripts: list[str] = []
        if ical_uid:
            source_messages.extend(message_by_ical_uid.get(ical_uid, []))
            source_threads.extend(thread_by_ical_uid.get(ical_uid, []))
            meeting_transcripts.extend(transcript_by_ical_uid.get(ical_uid, []))
        if event_id:
            source_messages.extend(message_by_event_id.get(event_id, []))
            source_threads.extend(thread_by_event_id.get(event_id, []))
            meeting_transcripts.extend(transcript_by_event_id.get(event_id, []))
        deduped_messages = list(dict.fromkeys(source_messages))
        deduped_threads = list(dict.fromkeys(source_threads))
        deduped_meeting_transcripts = list(dict.fromkeys(meeting_transcripts))
        conference_url = str(event.get("hangoutLink", "")).strip()
        if not conference_url:
            conference_data = event.get("conferenceData") or {}
            for entry in conference_data.get("entryPoints", []) or []:
                uri = str(entry.get("uri", "")).strip()
                if uri:
                    conference_url = uri
                    break
        start = event.get("start") or {}
        end = event.get("end") or {}
        start_at = str(start.get("dateTime") or start.get("date") or "").strip()
        end_at = str(end.get("dateTime") or end.get("date") or "").strip()
        timezone = str(start.get("timeZone") or end.get("timeZone") or "").strip()
        normalized = {
            "event_id": event_id,
            "calendar_id": calendar_id,
            "account_email": account_email,
            "event_etag": event_etag,
            "ical_uid": ical_uid,
            "status": str(event.get("status", "")).strip(),
            "title": str(event.get("summary", "")).strip(),
            "description": str(event.get("description", "")).strip(),
            "location": str(event.get("location", "")).strip(),
            "start_at": start_at,
            "end_at": end_at,
            "timezone": timezone,
            "organizer_email": organizer_email,
            "organizer_name": organizer_name,
            "attendee_emails": attendee_emails,
            "recurrence": [str(item).strip() for item in event.get("recurrence", []) or [] if str(item).strip()],
            "conference_url": conference_url,
            "source_messages": deduped_messages,
            "source_threads": deduped_threads,
            "meeting_transcripts": deduped_meeting_transcripts,
            "people": adapter._resolve_people(identity_cache, all_emails),
            "all_day": bool(start.get("date") and not start.get("dateTime")),
            "created": (start_at or "1970-01-01")[:10],
        }
        normalized["event_body_sha"] = compute_calendar_event_body_sha_from_payload(
            {
                "calendar_id": calendar_id,
                "event_id": event_id,
                "ical_uid": ical_uid,
                "status": normalized["status"],
                "title": normalized["title"],
                "description": normalized["description"],
                "location": normalized["location"],
                "start_at": start_at,
                "end_at": end_at,
                "timezone": timezone,
                "organizer_email": organizer_email,
                "organizer_name": organizer_name,
                "attendee_emails": attendee_emails,
                "recurrence": normalized["recurrence"],
                "conference_url": conference_url,
                "source_messages": deduped_messages,
                "source_threads": deduped_threads,
                "meeting_transcripts": deduped_meeting_transcripts,
                "all_day": normalized["all_day"],
            }
        )

        card, provenance, body = adapter.to_card(normalized)
        rel_path = Path(adapter._card_rel_path(vault, card))
        abs_path = vault / rel_path
        if abs_path.exists():
            adapter.merge_card(vault, rel_path, card, body, provenance)
        else:
            write_card(vault, rel_path, card, body=body, provenance=provenance)
            created["calendar_event"] += 1

        processed_events += 1
        next_index += 1
        events_since_checkpoint += 1

        if events_since_checkpoint >= args.checkpoint_every_events:
            checkpoint()
            print(
                json.dumps(
                    {
                        "progress": {
                            "next_index": next_index,
                            "processed_events": processed_events,
                            "created_event_cards": created["calendar_event"],
                        }
                    },
                    indent=2,
                ),
                flush=True,
            )
            events_since_checkpoint = 0

    checkpoint(complete=True)
    print(
        json.dumps(
            {
                "status": "complete",
                "processed_events": processed_events,
                "created_event_cards": created["calendar_event"],
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
