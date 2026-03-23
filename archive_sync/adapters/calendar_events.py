"""Calendar event archive adapter using the gws CLI."""

from __future__ import annotations

import json
import subprocess
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

from .base import BaseAdapter, FetchedBatch, deterministic_provenance
from arnoldlib.google_cli_auth import (CALENDAR_READONLY_SCOPES,
                                       account_name_from_email,
                                       build_google_cli_token_manager)
from hfa.identity import IdentityCache
from hfa.schema import CalendarEventCard
from hfa.thread_hash import compute_calendar_event_body_sha_from_payload
from hfa.uid import generate_uid
from hfa.vault import iter_notes, read_note

EVENT_SOURCE = "calendar.event"


def _normalize_account_email(account_email: str) -> str:
    return account_email.strip().lower()


def _event_identity(account_email: str, calendar_id: str, event_id: str) -> str:
    normalized_account = _normalize_account_email(account_email)
    normalized_calendar_id = calendar_id.strip()
    normalized_event_id = event_id.strip()
    base_identity = f"{normalized_calendar_id}:{normalized_event_id}"
    return f"{normalized_account}:{base_identity}" if normalized_account else base_identity


def _event_uid(account_email: str, calendar_id: str, event_id: str) -> str:
    return generate_uid("calendar-event", EVENT_SOURCE, _event_identity(account_email, calendar_id, event_id))


def _wikilink_from_uid(uid: str) -> str:
    return f"[[{uid}]]"


def _clean(value: str) -> str:
    return " ".join(value.strip().split())


class CalendarEventsAdapter(BaseAdapter):
    source_id = "calendar-events"
    preload_existing_uid_index = False

    def _ensure_token_manager(self, account_email: str) -> None:
        account = account_email.strip().lower()
        token_key = ("calendar", account)
        if getattr(self, "_token_manager_key", None) == token_key:
            return
        try:
            self._token_manager = build_google_cli_token_manager(
                account_email=account,
                scopes=CALENDAR_READONLY_SCOPES,
            )
        except RuntimeError:
            self._token_manager = None
        self._token_manager_key = token_key

    def get_cursor_key(self, **kwargs) -> str:
        account_email = str(kwargs.get("account_email", "")).strip().lower()
        calendar_id = str(kwargs.get("calendar_id", "primary")).strip().lower()
        suffix = ":".join(part for part in [account_email, calendar_id] if part)
        return f"{self.source_id}:{suffix}" if suffix else self.source_id

    def _gws(self, args: list[str]) -> dict[str, Any]:
        env = None
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is not None:
            env = token_manager.build_env()
        proc = subprocess.run(["gws", *args], capture_output=True, text=True, check=False, env=env)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "gws command failed")
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid gws JSON output: {exc}") from exc

    def _calendar_events_list_http(self, params: dict[str, Any]) -> dict[str, Any]:
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is None:
            raise RuntimeError("Calendar HTTP fallback requires a token manager")
        query = urllib.parse.urlencode({key: value for key, value in params.items() if value not in (None, "")})
        url = f"https://www.googleapis.com/calendar/v3/calendars/{params['calendarId']}/events?{query}"

        def _request(force_refresh: bool = False) -> dict[str, Any]:
            token = token_manager.get_access_token(force_refresh=force_refresh)
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))

        try:
            return _request()
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                return _request(force_refresh=True)
            raise RuntimeError(exc.read().decode("utf-8") or str(exc)) from exc

    def _calendar_events_list_proxy(self, account_email: str, params: dict[str, Any]) -> dict[str, Any]:
        from arnoldlib.auth import build_service_proxied
        from arnoldlib.bootstrap import bootstrap
        from arnoldlib.gate import _auto_issue_ticket

        account_name = account_name_from_email(account_email)
        if not account_name:
            raise RuntimeError(f"Unknown managed account for calendar proxy: {account_email}")
        bootstrap()
        ticket = _auto_issue_ticket(
            f"google.refresh_token.{account_name}",
            "google.calendar.search",
            account_name,
            "archive-sync",
        )
        service = build_service_proxied(
            account_name,
            "calendar",
            "v3",
            ticket=str(ticket["ticket"]),
            action="google.calendar.search",
            requested_by="archive-sync",
        )
        return service.events().list(**params).execute()

    def _should_fallback_to_http(self, message: str) -> bool:
        return any(
            marker in message
            for marker in (
                "accessNotConfigured",
                "API not enabled for your GCP project",
                "calendar-json.googleapis.com",
                "serviceusage.services.use",
                "required permission to use project",
                "claude-gmail-mcp",
                '"reason": "forbidden"',
                '"reason":"forbidden"',
            )
        )

    def _list_events(self, params: dict[str, Any], *, account_email: str = "") -> dict[str, Any]:
        normalized_account = account_email.strip().lower()
        if normalized_account:
            try:
                return self._calendar_events_list_proxy(normalized_account, params)
            except Exception:
                pass
        try:
            return self._gws(["calendar", "events", "list", "--params", json.dumps(params)])
        except RuntimeError as exc:
            message = str(exc)
            if not self._should_fallback_to_http(message):
                raise
            return self._calendar_events_list_http(params)

    def _invite_lookup(
        self,
        vault_path: str,
        *,
        account_email: str = "",
    ) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
        message_by_ical_uid: dict[str, list[str]] = {}
        thread_by_ical_uid: dict[str, list[str]] = {}
        message_by_event_id: dict[str, list[str]] = {}
        thread_by_event_id: dict[str, list[str]] = {}
        vault = Path(vault_path)
        normalized_account = account_email.strip().lower()
        rg_patterns = {
            "Email": r"invite_ical_uid|invite_event_id_hint",
            "EmailThreads": r"invite_ical_uids|invite_event_id_hints",
        }

        def _strip_yaml_scalar(value: str) -> str:
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                return value[1:-1]
            return value

        def _parse_inline_list(value: str) -> list[str]:
            raw = value.strip()
            if not (raw.startswith("[") and raw.endswith("]")):
                return []
            inner = raw[1:-1].strip()
            if not inner:
                return []
            return [_strip_yaml_scalar(part) for part in inner.split(",") if _strip_yaml_scalar(part)]

        def _read_invite_frontmatter(
            path: Path,
            *,
            card_type: str,
        ) -> tuple[str, str | list[str], str | list[str], str]:
            scalar_keys = ("invite_ical_uid", "invite_event_id_hint")
            list_keys = ("invite_ical_uids", "invite_event_id_hints")
            values: dict[str, str | list[str]] = {
                scalar_keys[0]: "",
                scalar_keys[1]: "",
                list_keys[0]: [],
                list_keys[1]: [],
                "account_email": "",
            }
            current_list_key: str | None = None
            try:
                with path.open("r", encoding="utf-8", errors="ignore") as handle:
                    if handle.readline().strip() != "---":
                        return (
                            str(values["invite_ical_uid"]),
                            values["invite_ical_uids"],
                            str(values["invite_event_id_hint"]),
                            values["invite_event_id_hints"],
                            str(values["account_email"]),
                        )
                    for raw_line in handle:
                        line = raw_line.rstrip("\n")
                        stripped = line.strip()
                        if stripped == "---":
                            break
                        if current_list_key is not None:
                            if stripped.startswith("- "):
                                cast_list = values[current_list_key]
                                if isinstance(cast_list, list):
                                    item = _strip_yaml_scalar(stripped[2:])
                                    if item:
                                        cast_list.append(item)
                                continue
                            current_list_key = None
                        if ":" not in line:
                            continue
                        key, raw_value = line.split(":", 1)
                        key = key.strip()
                        raw_value = raw_value.strip()
                        if key in scalar_keys:
                            values[key] = _strip_yaml_scalar(raw_value)
                        elif key == "account_email":
                            values[key] = _strip_yaml_scalar(raw_value).lower()
                        elif key in list_keys:
                            if raw_value.startswith("["):
                                values[key] = _parse_inline_list(raw_value)
                            elif not raw_value:
                                values[key] = []
                                current_list_key = key
            except FileNotFoundError:
                pass
            if card_type == "email_message":
                return (
                    str(values["invite_ical_uid"]),
                    [],
                    str(values["invite_event_id_hint"]),
                    [],
                    str(values["account_email"]),
                )
            return (
                "",
                values["invite_ical_uids"],
                "",
                values["invite_event_id_hints"],
                str(values["account_email"]),
            )

        def _candidate_paths(root: Path, *, pattern: str) -> list[Path]:
            try:
                proc = subprocess.run(
                    ["rg", "-l", "--glob", "*.md", pattern, str(root)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except Exception:
                proc = None
            if proc is None:
                return sorted(root.rglob("*.md"))
            if proc.returncode not in (0, 1):
                return sorted(root.rglob("*.md"))
            if proc.returncode == 1 or not proc.stdout.strip():
                return []
            return [Path(line) for line in proc.stdout.splitlines() if line.strip()]

        for root_name, card_type in (("Email", "email_message"), ("EmailThreads", "email_thread")):
            root = vault / root_name
            if not root.exists():
                continue
            for abs_path in _candidate_paths(root, pattern=rg_patterns[root_name]):
                rel_path = abs_path.relative_to(vault)
                wikilink = f"[[{Path(rel_path).stem}]]"
                if card_type == "email_message":
                    invite_ical_uid, _, invite_event_id_hint, _, note_account_email = _read_invite_frontmatter(
                        abs_path,
                        card_type=card_type,
                    )
                    if normalized_account and note_account_email != normalized_account:
                        continue
                    if invite_ical_uid:
                        message_by_ical_uid.setdefault(invite_ical_uid, []).append(wikilink)
                    if invite_event_id_hint:
                        message_by_event_id.setdefault(invite_event_id_hint, []).append(wikilink)
                else:
                    _, invite_ical_uids, _, invite_event_id_hints, note_account_email = _read_invite_frontmatter(
                        abs_path,
                        card_type=card_type,
                    )
                    if normalized_account and note_account_email != normalized_account:
                        continue
                    for invite_ical_uid in invite_ical_uids:
                        value = str(invite_ical_uid).strip()
                        if value:
                            thread_by_ical_uid.setdefault(value, []).append(wikilink)
                    for invite_event_id_hint in invite_event_id_hints:
                        value = str(invite_event_id_hint).strip()
                        if value:
                            thread_by_event_id.setdefault(value, []).append(wikilink)
        return message_by_ical_uid, thread_by_ical_uid, message_by_event_id, thread_by_event_id

    def _meeting_transcript_lookup(self, vault_path: str) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
        transcript_by_ical_uid: dict[str, list[str]] = {}
        transcript_by_event_id: dict[str, list[str]] = {}
        vault = Path(vault_path)
        root = vault / "MeetingTranscripts"
        if not root.exists():
            return transcript_by_ical_uid, transcript_by_event_id
        for abs_path in sorted(root.rglob("*.md")):
            rel_path = abs_path.relative_to(vault)
            wikilink = f"[[{Path(rel_path).stem}]]"
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if str(frontmatter.get("type", "")).strip() != "meeting_transcript":
                continue
            ical_uid = str(frontmatter.get("ical_uid", "")).strip()
            event_id_hint = str(frontmatter.get("event_id_hint", "")).strip()
            if ical_uid:
                transcript_by_ical_uid.setdefault(ical_uid, []).append(wikilink)
            if event_id_hint:
                transcript_by_event_id.setdefault(event_id_hint, []).append(wikilink)
        return transcript_by_ical_uid, transcript_by_event_id

    def _load_existing_event_state(
        self,
        vault_path: str,
        *,
        account_email: str,
        calendar_id: str,
    ) -> dict[str, dict[str, str]]:
        existing: dict[str, dict[str, str]] = {}
        normalized_account = account_email.strip().lower()
        normalized_calendar_id = calendar_id.strip().lower()
        for rel_path, _ in iter_notes(vault_path):
            if not rel_path.parts or rel_path.parts[0] != "Calendar":
                continue
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if str(frontmatter.get("type", "")).strip() != "calendar_event":
                continue
            if normalized_account and str(frontmatter.get("account_email", "")).strip().lower() != normalized_account:
                continue
            if normalized_calendar_id and str(frontmatter.get("calendar_id", "")).strip().lower() != normalized_calendar_id:
                continue
            event_id = str(frontmatter.get("event_id", "")).strip()
            if not event_id:
                continue
            existing[event_id] = {
                "event_etag": str(frontmatter.get("event_etag", "")).strip(),
                "event_body_sha": str(frontmatter.get("event_body_sha", "")).strip(),
            }
        return existing

    def _resolve_people(self, cache: IdentityCache, emails: list[str]) -> list[str]:
        links: list[str] = []
        for email_value in emails:
            resolved = cache.resolve("email", email_value)
            if resolved and resolved not in links:
                links.append(resolved)
        return links

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ):
        self._last_fetch_skipped_count = 0
        self._last_fetch_skip_details = {}
        items = self.fetch(vault_path, cursor, config=config, **kwargs)
        yield FetchedBatch(
            items=items,
            sequence=0,
            skipped_count=int(getattr(self, "_last_fetch_skipped_count", 0) or 0),
            skip_details=dict(getattr(self, "_last_fetch_skip_details", {})),
        )

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        account_email: str = "",
        calendar_id: str = "primary",
        max_events: int | None = 100,
        query: str | None = None,
        time_min: str | None = None,
        time_max: str | None = None,
        quick_update: bool = False,
        **kwargs,
    ) -> list[dict[str, Any]]:
        self._ensure_token_manager(account_email)
        identity_cache = IdentityCache(vault_path)
        message_by_ical_uid, thread_by_ical_uid, message_by_event_id, thread_by_event_id = self._invite_lookup(
            vault_path,
            account_email=account_email,
        )
        transcript_by_ical_uid, transcript_by_event_id = self._meeting_transcript_lookup(vault_path)
        quick_update_enabled = bool(quick_update and bool(getattr(config, "calendar_event_body_sha_cache_enabled", True)))
        existing_event_state = (
            self._load_existing_event_state(
                vault_path,
                account_email=account_email,
                calendar_id=calendar_id,
            )
            if quick_update_enabled
            else {}
        )
        items: list[dict[str, Any]] = []
        page_token = cursor.get("page_token")
        emitted_events = 0
        skipped_unchanged_events = int(cursor.get("skipped_unchanged_events", 0) or 0)
        page_size = max(1, min(int(max_events or 100), 100))
        self._last_fetch_skipped_count = 0
        self._last_fetch_skip_details = {"skipped_unchanged_events": 0}

        while True:
            params: dict[str, Any] = {
                "calendarId": calendar_id,
                "maxResults": page_size,
                "singleEvents": True,
                "orderBy": "startTime",
            }
            if page_token:
                params["pageToken"] = page_token
            if query:
                params["q"] = query
            if time_min:
                params["timeMin"] = time_min
            if time_max:
                params["timeMax"] = time_max
            response = self._list_events(params, account_email=account_email)
            events = response.get("items", []) or []
            if not events:
                page_token = None
                break
            for event in events:
                event_id = str(event.get("id", "")).strip()
                if not event_id:
                    continue
                event_etag = str(event.get("etag", "")).strip()
                if quick_update_enabled:
                    existing = existing_event_state.get(event_id, {})
                    if event_etag and event_etag == str(existing.get("event_etag", "")).strip():
                        skipped_unchanged_events += 1
                        self._last_fetch_skipped_count += 1
                        self._last_fetch_skip_details["skipped_unchanged_events"] = skipped_unchanged_events
                        continue
                organizer_email = str((event.get("organizer") or {}).get("email", "")).strip().lower()
                organizer_name = str((event.get("organizer") or {}).get("displayName", "")).strip()
                attendee_emails = [
                    str(attendee.get("email", "")).strip().lower()
                    for attendee in event.get("attendees", []) or []
                    if str(attendee.get("email", "")).strip()
                ]
                all_emails = [email for email in [organizer_email, *attendee_emails] if email]
                ical_uid = str(event.get("iCalUID", "")).strip()
                source_messages = []
                source_threads = []
                meeting_transcripts = []
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
                event_body_sha = compute_calendar_event_body_sha_from_payload(
                    {
                        "calendar_id": calendar_id,
                        "event_id": event_id,
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
                        "all_day": bool(start.get("date") and not start.get("dateTime")),
                    }
                )
                items.append(
                    {
                        "event_id": event_id,
                        "calendar_id": calendar_id,
                        "account_email": account_email.lower().strip(),
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
                        "people": self._resolve_people(identity_cache, all_emails),
                        "all_day": bool(start.get("date") and not start.get("dateTime")),
                        "event_body_sha": event_body_sha,
                        "created": (start_at or date.today().isoformat())[:10],
                    }
                )
                emitted_events += 1
                if max_events is not None and emitted_events >= max_events:
                    cursor.update(
                        {
                            "page_token": response.get("nextPageToken"),
                            "emitted_events": emitted_events,
                            "skipped_unchanged_events": skipped_unchanged_events,
                        }
                    )
                    return items
            page_token = response.get("nextPageToken")
            cursor.update(
                {
                    "page_token": page_token,
                    "emitted_events": emitted_events,
                    "skipped_unchanged_events": skipped_unchanged_events,
                }
            )
            if not page_token:
                break

        cursor.update(
            {
                "page_token": None,
                "emitted_events": emitted_events,
                "skipped_unchanged_events": skipped_unchanged_events,
            }
        )
        return items

    def to_card(self, item: dict[str, Any]):
        event_id = str(item.get("event_id", "")).strip()
        calendar_id = str(item.get("calendar_id", "primary")).strip()
        account_email = str(item.get("account_email", "")).strip()
        today = date.today().isoformat()
        card = CalendarEventCard(
            uid=_event_uid(account_email, calendar_id, event_id),
            type="calendar_event",
            source=[EVENT_SOURCE],
            source_id=_event_identity(account_email, calendar_id, event_id),
            created=str(item.get("created", "")).strip() or today,
            updated=today,
            summary=str(item.get("title", "")).strip() or event_id,
            people=list(item.get("people", [])),
            account_email=account_email,
            calendar_id=calendar_id,
            event_id=event_id,
            event_etag=str(item.get("event_etag", "")).strip(),
            ical_uid=str(item.get("ical_uid", "")).strip(),
            status=str(item.get("status", "")).strip(),
            title=str(item.get("title", "")).strip(),
            description=str(item.get("description", "")).strip(),
            location=str(item.get("location", "")).strip(),
            start_at=str(item.get("start_at", "")).strip(),
            end_at=str(item.get("end_at", "")).strip(),
            timezone=str(item.get("timezone", "")).strip(),
            organizer_email=str(item.get("organizer_email", "")).strip(),
            organizer_name=str(item.get("organizer_name", "")).strip(),
            attendee_emails=list(item.get("attendee_emails", [])),
            recurrence=list(item.get("recurrence", [])),
            conference_url=str(item.get("conference_url", "")).strip(),
            source_messages=list(item.get("source_messages", [])),
            source_threads=list(item.get("source_threads", [])),
            meeting_transcripts=list(item.get("meeting_transcripts", [])),
            all_day=bool(item.get("all_day", False)),
            event_body_sha=str(item.get("event_body_sha", "")).strip(),
        )
        provenance = deterministic_provenance(card, EVENT_SOURCE)
        return card, provenance, ""

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
