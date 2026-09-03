"""Calendar event archive adapter using the gws CLI."""

from __future__ import annotations

import json
import logging
import subprocess
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any

from archive_auth import account_name_from_email, build_google_cli_token_manager
from archive_vault.identity import IdentityCache
from archive_vault.schema import CalendarEventCard
from archive_vault.thread_hash import compute_calendar_event_body_sha_from_payload
from archive_vault.uid import generate_uid

from .base import BaseAdapter, FetchedBatch, deterministic_provenance
from .datetime_canon import to_utc_z_iso

logger = logging.getLogger("ppa.calendar")

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
    enable_person_resolution = False

    def should_enable_person_resolution(self, **kwargs) -> bool:
        return False

    def _ensure_token_manager(self, account_email: str) -> None:
        account = account_email.strip().lower()
        token_key = ("calendar", account)
        if getattr(self, "_token_manager_key", None) == token_key:
            return
        try:
            # Service profile (`calendar`) matches local refresh grants. Hard-coded
            # CALENDAR_READONLY_SCOPES mint HTTP 400 invalid_scope against those tokens.
            self._token_manager = build_google_cli_token_manager(
                account_email=account,
                services=["calendar"],
            )
        except (RuntimeError, TypeError, ValueError, OSError):
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
            try:
                env = token_manager.build_env()
            except Exception:
                # Mint failure must not block gws' own credentials / HTTP fallback.
                env = None
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
        encoded: dict[str, str] = {}
        for key, value in params.items():
            if value in (None, ""):
                continue
            if isinstance(value, bool):
                encoded[key] = "true" if value else "false"
            else:
                encoded[key] = str(value)
        calendar_id = urllib.parse.quote(str(params["calendarId"]), safe="@.")
        query = urllib.parse.urlencode(encoded)
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events?{query}"

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
            body = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
            raise RuntimeError(body or str(exc)) from exc

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
                "HTTP Error 400",
                "Bad Request",
                "invalid_scope",
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
        except Exception as exc:
            # gws / urllib may surface urllib.error.HTTPError for bad scope tokens
            message = f"{type(exc).__name__}: {exc}"
            if self._token_manager is not None and self._should_fallback_to_http(message):
                return self._calendar_events_list_http(params)
            raise

    def _calendar_frontmatter_rows_from_cache(self, vault_path: str) -> list[dict[str, Any]]:
        """One Rust (or single-cursor) dump of calendar-related frontmatter. Builds cache on miss."""

        from archive_cli.vault_cache import VaultScanCache
        from archive_sync.cli_logging import format_mins_secs

        cached_vault = getattr(self, "_calendar_lookup_vault", "")
        cached_rows = getattr(self, "_calendar_lookup_rows", None)
        if cached_vault == vault_path and cached_rows is not None:
            return cached_rows

        started = perf_counter()
        vault = Path(vault_path)
        scan_cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
        cache_path = VaultScanCache.cache_path_for_vault(vault)
        types = ["email_thread", "email_message", "meeting_transcript", "calendar_event"]
        if cache_path.is_file():
            try:
                import archive_crate

                rows = list(
                    archive_crate.frontmatter_dicts_from_cache(
                        str(cache_path),
                        types=types,
                    )
                )
                logger.info(
                    "calendar lookup rows=%s elapsed=%s source=rust",
                    len(rows),
                    format_mins_secs(perf_counter() - started),
                )
                self._calendar_lookup_vault = vault_path
                self._calendar_lookup_rows = rows
                return rows
            except Exception:
                pass
        by_type, _rel_by_uid, uid_by_path, _uid_by_stem, frontmatter_by_uid = scan_cache.slice_lookup_tables()
        rows: list[dict[str, Any]] = []
        for card_type in types:
            for rel in by_type.get(card_type) or []:
                uid = uid_by_path.get(rel, "")
                fm = dict(frontmatter_by_uid.get(uid) or {})
                if not fm:
                    continue
                rows.append({"rel_path": rel, "frontmatter": fm})
        logger.info(
            "calendar lookup rows=%s elapsed=%s",
            len(rows),
            format_mins_secs(perf_counter() - started),
        )
        self._calendar_lookup_vault = vault_path
        self._calendar_lookup_rows = rows
        return rows

    @staticmethod
    def _string_list(raw: Any) -> list[str]:
        if isinstance(raw, (list, tuple)):
            return [str(item).strip() for item in raw if str(item).strip()]
        value = str(raw or "").strip()
        return [value] if value else []

    def _invite_lookup(
        self,
        vault_path: str,
        *,
        account_email: str = "",
        rows: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
        message_by_ical_uid: dict[str, list[str]] = {}
        thread_by_ical_uid: dict[str, list[str]] = {}
        message_by_event_id: dict[str, list[str]] = {}
        thread_by_event_id: dict[str, list[str]] = {}
        normalized_account = account_email.strip().lower()
        dumped = rows if rows is not None else self._calendar_frontmatter_rows_from_cache(vault_path)
        for row in dumped:
            rel_path = str(row.get("rel_path") or "")
            frontmatter = dict(row.get("frontmatter") or {})
            card_type = str(frontmatter.get("type") or "").strip()
            if card_type not in {"email_message", "email_thread"}:
                continue
            note_account = str(frontmatter.get("account_email") or "").strip().lower()
            if normalized_account and note_account != normalized_account:
                continue
            wikilink = f"[[{Path(rel_path).stem}]]"
            if card_type == "email_message":
                invite_ical_uid = str(frontmatter.get("invite_ical_uid") or "").strip()
                invite_event_id_hint = str(frontmatter.get("invite_event_id_hint") or "").strip()
                if invite_ical_uid:
                    message_by_ical_uid.setdefault(invite_ical_uid, []).append(wikilink)
                if invite_event_id_hint:
                    message_by_event_id.setdefault(invite_event_id_hint, []).append(wikilink)
                continue
            for invite_ical_uid in self._string_list(frontmatter.get("invite_ical_uids")):
                thread_by_ical_uid.setdefault(invite_ical_uid, []).append(wikilink)
            for invite_event_id_hint in self._string_list(frontmatter.get("invite_event_id_hints")):
                thread_by_event_id.setdefault(invite_event_id_hint, []).append(wikilink)
        return message_by_ical_uid, thread_by_ical_uid, message_by_event_id, thread_by_event_id

    def _meeting_transcript_lookup(
        self,
        vault_path: str,
        *,
        rows: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
        transcript_by_ical_uid: dict[str, list[str]] = {}
        transcript_by_event_id: dict[str, list[str]] = {}
        dumped = rows if rows is not None else self._calendar_frontmatter_rows_from_cache(vault_path)
        for row in dumped:
            rel_path = str(row.get("rel_path") or "")
            frontmatter = dict(row.get("frontmatter") or {})
            if str(frontmatter.get("type") or "").strip() != "meeting_transcript":
                continue
            wikilink = f"[[{Path(rel_path).stem}]]"
            ical_uid = str(frontmatter.get("ical_uid") or "").strip()
            event_id_hint = str(frontmatter.get("event_id_hint") or "").strip()
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
        rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, dict[str, str]]:
        existing: dict[str, dict[str, str]] = {}
        normalized_account = account_email.strip().lower()
        normalized_calendar_id = calendar_id.strip().lower()
        dumped = rows if rows is not None else self._calendar_frontmatter_rows_from_cache(vault_path)
        for row in dumped:
            frontmatter = dict(row.get("frontmatter") or {})
            if str(frontmatter.get("type") or "").strip() != "calendar_event":
                continue
            if normalized_account and str(frontmatter.get("account_email") or "").strip().lower() != normalized_account:
                continue
            if (
                normalized_calendar_id
                and str(frontmatter.get("calendar_id") or "").strip().lower() != normalized_calendar_id
            ):
                continue
            event_id = str(frontmatter.get("event_id") or "").strip()
            if not event_id:
                continue
            existing[event_id] = {
                "event_etag": str(frontmatter.get("event_etag") or "").strip(),
                "event_body_sha": str(frontmatter.get("event_body_sha") or "").strip(),
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
        cache_rows = self._calendar_frontmatter_rows_from_cache(vault_path)
        message_by_ical_uid, thread_by_ical_uid, message_by_event_id, thread_by_event_id = self._invite_lookup(
            vault_path,
            account_email=account_email,
            rows=cache_rows,
        )
        transcript_by_ical_uid, transcript_by_event_id = self._meeting_transcript_lookup(
            vault_path,
            rows=cache_rows,
        )
        quick_update_enabled = bool(
            quick_update and bool(getattr(config, "calendar_event_body_sha_cache_enabled", True))
        )
        existing_event_state = (
            self._load_existing_event_state(
                vault_path,
                account_email=account_email,
                calendar_id=calendar_id,
                rows=cache_rows,
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

        # Google Calendar API: orderBy=startTime requires timeMin (HTTP 400 otherwise).
        effective_time_min = time_min
        if not effective_time_min and not cursor.get("sync_token") and not cursor.get("syncToken"):
            from datetime import datetime, timedelta, timezone

            effective_time_min = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")

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
            if effective_time_min:
                params["timeMin"] = effective_time_min
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
                        "recurrence": [
                            str(item).strip() for item in event.get("recurrence", []) or [] if str(item).strip()
                        ],
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
                        "recurrence": [
                            str(item).strip() for item in event.get("recurrence", []) or [] if str(item).strip()
                        ],
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
            start_at=to_utc_z_iso(str(item.get("start_at", "")).strip()),
            end_at=to_utc_z_iso(str(item.get("end_at", "")).strip()),
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
