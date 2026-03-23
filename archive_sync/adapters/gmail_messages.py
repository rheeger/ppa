"""Gmail message/thread/archive adapter using the gws CLI."""

from __future__ import annotations

import base64
import html
import json
import os
import random
import re
import subprocess
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from email.header import decode_header
from email.utils import getaddresses, parsedate_to_datetime
from typing import Any

from .base import BaseAdapter, FetchedBatch, deterministic_provenance
from .gmail_correspondents import load_own_aliases
from ppa_google_auth import build_google_cli_token_manager
from hfa.identity import IdentityCache
from hfa.schema import EmailAttachmentCard, EmailMessageCard, EmailThreadCard
from hfa.thread_hash import (
    compute_email_attachment_metadata_sha_from_payload,
    compute_email_message_body_sha_from_payload,
    compute_email_thread_body_sha_from_payload)
from hfa.uid import generate_uid
from hfa.vault import iter_notes, read_note

THREAD_SOURCE = "gmail.thread"
MESSAGE_SOURCE = "gmail.message"
ATTACHMENT_SOURCE = "gmail.attachment"
WIKILINK_RE = re.compile(r"\[\[([^\]]+)\]\]")
TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
META_ITEMPROP_RE = re.compile(r'itemprop="(?P<key>[^"]+)"[^>]*content="(?P<value>[^"]+)"', re.IGNORECASE)
TIME_ITEMPROP_RE = re.compile(r'itemprop="(?P<key>startDate|endDate)"[^>]*datetime="(?P<value>[^"]+)"', re.IGNORECASE)
NAME_ITEMPROP_RE = re.compile(r'itemprop="name"[^>]*>(?P<value>.*?)</', re.IGNORECASE | re.DOTALL)
CALENDAR_SUBJECT_RE = re.compile(
    r"^(?P<prefix>updated invitation|invitation|cancelled|canceled|reminder):\s*(?P<title>.+?)(?:\s+@\s+(?P<when>.+))?$",
    re.IGNORECASE,
)
CALENDAR_MIME_TYPES = {"text/calendar", "application/ics", "text/ics"}
TRANSIENT_GMAIL_STATUS_RE = re.compile(r"\b(?:429|500|502|503|504)\b")


def _normalize_account_email(account_email: str) -> str:
    return account_email.strip().lower()


def _thread_identity(account_email: str, thread_id: str) -> str:
    normalized_account = _normalize_account_email(account_email)
    normalized_thread_id = thread_id.strip()
    return f"{normalized_account}:{normalized_thread_id}" if normalized_account else normalized_thread_id


def _message_identity(account_email: str, message_id: str) -> str:
    normalized_account = _normalize_account_email(account_email)
    normalized_message_id = message_id.strip()
    return f"{normalized_account}:{normalized_message_id}" if normalized_account else normalized_message_id


def _attachment_identity(account_email: str, message_id: str, attachment_id: str) -> str:
    normalized_account = _normalize_account_email(account_email)
    normalized_message_id = message_id.strip()
    normalized_attachment_id = attachment_id.strip()
    base_identity = f"{normalized_message_id}:{normalized_attachment_id}"
    return f"{normalized_account}:{base_identity}" if normalized_account else base_identity


def _thread_uid(account_email: str, thread_id: str) -> str:
    return generate_uid("email-thread", THREAD_SOURCE, _thread_identity(account_email, thread_id))


def _message_uid(account_email: str, message_id: str) -> str:
    return generate_uid("email-message", MESSAGE_SOURCE, _message_identity(account_email, message_id))


def _attachment_uid(account_email: str, message_id: str, attachment_id: str) -> str:
    return generate_uid("email-attachment", ATTACHMENT_SOURCE, _attachment_identity(account_email, message_id, attachment_id))


def _wikilink_from_uid(uid: str) -> str:
    return f"[[{uid}]]"


def _clean_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value.strip())


def _header_values(headers: list[dict[str, Any]], name: str) -> list[str]:
    target = name.lower()
    return [str(header.get("value", "")).strip() for header in headers if str(header.get("name", "")).lower() == target]


def _header_value(headers: list[dict[str, Any]], name: str) -> str:
    values = _header_values(headers, name)
    return values[0] if values else ""


def _extract_addresses(values: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for raw_name, raw_email in getaddresses(values):
        email_value = raw_email.strip().lower()
        if not email_value or "@" not in email_value:
            continue
        out.append((raw_name.strip(), email_value))
    return out


def _iter_parts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    parts = [payload]
    for part in payload.get("parts", []) or []:
        parts.extend(_iter_parts(part))
    return parts


def _decode_body_data(data: str) -> str:
    if not data:
        return ""
    padding = "=" * (-len(data) % 4)
    try:
        raw = base64.urlsafe_b64decode((data + padding).encode("utf-8"))
    except Exception:
        return ""
    return raw.decode("utf-8", errors="replace")


def _decode_mime_header(value: str) -> str:
    if not value:
        return ""
    decoded: list[str] = []
    for chunk, encoding in decode_header(value):
        if isinstance(chunk, bytes):
            decoded.append(chunk.decode(encoding or "utf-8", errors="replace"))
        else:
            decoded.append(chunk)
    return "".join(decoded)


def _strip_html(value: str) -> str:
    text = TAG_RE.sub(" ", html.unescape(value or ""))
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


def _parse_iso_datetime(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _extract_text_body(payload: dict[str, Any]) -> str:
    text_plain = ""
    text_html = ""
    for part in _iter_parts(payload):
        mime_type = str(part.get("mimeType", "")).lower()
        body_data = _decode_body_data(str((part.get("body") or {}).get("data", "")))
        if not body_data:
            continue
        if mime_type == "text/plain" and not text_plain:
            text_plain = body_data.strip()
        elif mime_type == "text/html" and not text_html:
            text_html = _strip_html(body_data)
    return text_plain or text_html


def _parse_reference_header(value: str) -> list[str]:
    return [token.strip("<>") for token in value.split() if token.strip()]


def _parse_datetime_header(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    try:
        parsed = parsedate_to_datetime(cleaned)
    except (TypeError, ValueError):
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _parse_internal_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return ""
    parsed = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _date_bucket(value: str) -> str:
    if len(value) >= 10:
        return value[:10]
    return date.today().isoformat()


def _parse_ics_datetime(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        if fmt == "%Y%m%d":
            return parsed.date().isoformat()
        parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    return ""


def _parse_ics(data: str) -> dict[str, str]:
    if not data.strip():
        return {}
    unfolded: list[str] = []
    for line in data.splitlines():
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line.strip())
    all_values: dict[str, list[str]] = {}
    for line in unfolded:
        if ":" not in line:
            continue
        key_part, raw_value = line.split(":", 1)
        key = key_part.split(";", 1)[0].upper()
        all_values.setdefault(key, []).append(raw_value.strip())
    in_vevent = False
    event_values: dict[str, list[str]] = {}
    for line in unfolded:
        upper = line.upper()
        if upper == "BEGIN:VEVENT":
            in_vevent = True
            continue
        if upper == "END:VEVENT":
            in_vevent = False
            continue
        if not in_vevent or ":" not in line:
            continue
        key_part, raw_value = line.split(":", 1)
        key = key_part.split(";", 1)[0].upper()
        event_values.setdefault(key, []).append(raw_value.strip())
    return {
        "invite_ical_uid": (event_values.get("UID") or [""])[0],
        "invite_method": (all_values.get("METHOD") or [""])[0],
        "invite_title": (event_values.get("SUMMARY") or [""])[0],
        "invite_start_at": _parse_ics_datetime((event_values.get("DTSTART") or [""])[0]),
        "invite_end_at": _parse_ics_datetime((event_values.get("DTEND") or [""])[0]),
    }


def _parse_google_calendar_html(html_body: str, headers: list[dict[str, Any]]) -> dict[str, str]:
    if not html_body:
        return {}
    matches = {match.group("key"): html.unescape(match.group("value")) for match in META_ITEMPROP_RE.finditer(html_body)}
    time_values = {match.group("key"): html.unescape(match.group("value")) for match in TIME_ITEMPROP_RE.finditer(html_body)}
    subject = _decode_mime_header(_header_value(headers, "Subject"))
    invite: dict[str, str] = {}
    subject_match = CALENDAR_SUBJECT_RE.match(subject)
    if subject_match:
        prefix = subject_match.group("prefix").lower()
        title = subject_match.group("title").strip()
        if title:
            invite["invite_title"] = title
        if prefix == "reminder":
            invite["invite_method"] = "REMINDER"
        elif prefix in {"cancelled", "canceled"}:
            invite["invite_method"] = "CANCEL"
        else:
            invite["invite_method"] = "REQUEST"
    if matches.get("eventId/googleCalendar"):
        invite["invite_event_id_hint"] = matches["eventId/googleCalendar"]
    if matches.get("name") and not invite.get("invite_title"):
        invite["invite_title"] = matches["name"]
    elif not invite.get("invite_title"):
        name_match = NAME_ITEMPROP_RE.search(html_body)
        if name_match:
            invite["invite_title"] = _strip_html(name_match.group("value"))
    if time_values.get("startDate"):
        invite["invite_start_at"] = _parse_iso_datetime(time_values["startDate"])
    if time_values.get("endDate"):
        invite["invite_end_at"] = _parse_iso_datetime(time_values["endDate"])
    plain_html = _strip_html(html_body).lower()
    sender_values = _extract_addresses(_header_values(headers, "Sender") + _header_values(headers, "From"))
    sender_emails = {email for _, email in sender_values}
    is_google_calendar = "calendar-notification@google.com" in sender_emails or "invitation from google calendar" in plain_html
    has_calendar_language = any(
        phrase in plain_html
        for phrase in (
            "you have been invited",
            "you have an upcoming appointment",
            "invitation from google calendar",
            "powered by google calendar appointment scheduling",
        )
    )
    if not (invite or is_google_calendar or has_calendar_language):
        return {}
    if is_google_calendar and not invite.get("invite_method"):
        invite["invite_method"] = "REQUEST" if "you have been invited" in plain_html else "REMINDER"
    return {key: value for key, value in invite.items() if value}


def _extract_attachments(payload: dict[str, Any]) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for part in _iter_parts(payload):
        filename = str(part.get("filename", "")).strip()
        body = part.get("body") or {}
        attachment_id = str(body.get("attachmentId", "")).strip()
        if not filename and not attachment_id:
            continue
        headers = part.get("headers", []) or []
        disposition = _header_value(headers, "Content-Disposition").lower()
        content_id = _header_value(headers, "Content-ID").strip().strip("<>")
        part_id = str(part.get("partId", "")).strip()
        effective_attachment_id = attachment_id or part_id or filename or "attachment"
        attachments.append(
            {
                "attachment_id": effective_attachment_id,
                "filename": filename or effective_attachment_id,
                "mime_type": str(part.get("mimeType", "")).strip(),
                "size_bytes": int(body.get("size", 0) or 0),
                "content_id": content_id,
                "is_inline": "inline" in disposition or bool(content_id),
            }
        )
    return attachments


class GmailMessagesAdapter(BaseAdapter):
    source_id = "gmail-messages"
    preload_existing_uid_index = False

    def _ensure_token_manager(self, account_email: str) -> None:
        account = account_email.strip().lower()
        token_key = ("gmail", account)
        if getattr(self, "_token_manager_key", None) == token_key:
            return
        try:
            self._token_manager = build_google_cli_token_manager(
                account_email=account,
                services=["gmail"],
            )
        except RuntimeError:
            self._token_manager = None
        self._token_manager_key = token_key

    def get_cursor_key(self, **kwargs) -> str:
        account_email = str(kwargs.get("account_email", "")).strip().lower()
        return f"{self.source_id}:{account_email}" if account_email else self.source_id

    def _gws(self, args: list[str]) -> dict[str, Any]:
        env = None
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is not None:
            env = token_manager.build_env()
        proc = subprocess.run(["gws", *args], capture_output=True, text=True, check=False, env=env)
        if proc.returncode != 0:
            message = proc.stderr.strip() or proc.stdout.strip() or "gws command failed"
            if self._should_fallback_to_http(message, args):
                return self._gmail_http_json(args)
            raise RuntimeError(message)
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid gws JSON output: {exc}") from exc

    def _should_fallback_to_http(self, message: str, args: list[str]) -> bool:
        if len(args) < 4 or args[0] != "gmail" or args[1] != "users":
            return False
        return any(
            marker in message
            for marker in (
                "serviceusage.services.use",
                "required permission to use project",
                "claude-gmail-mcp",
                '"reason": "forbidden"',
                '"reason":"forbidden"',
            )
        )

    def _gmail_http_json(self, args: list[str]) -> dict[str, Any]:
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is None:
            raise RuntimeError("Gmail HTTP fallback requires a token manager")
        params = json.loads(args[-1]) if args[-2:] and args[-2] == "--params" else {}
        if args[:4] == ["gmail", "users", "threads", "list"]:
            query = urllib.parse.urlencode({key: value for key, value in params.items() if value not in (None, "")})
            url = f"https://gmail.googleapis.com/gmail/v1/users/me/threads?{query}"
            return self._gmail_http_request_json(url, token_manager=token_manager)
        if args[:4] == ["gmail", "users", "threads", "get"]:
            thread_id = urllib.parse.quote(str(params.get("id", "")).strip(), safe="")
            query = urllib.parse.urlencode(
                {key: value for key, value in params.items() if key not in {"id", "userId"} and value not in (None, "")}
            )
            url = f"https://gmail.googleapis.com/gmail/v1/users/me/threads/{thread_id}"
            if query:
                url = f"{url}?{query}"
            return self._gmail_http_request_json(url, token_manager=token_manager)
        if args[:4] == ["gmail", "users", "messages", "attachments"]:
            message_id = urllib.parse.quote(str(params.get("messageId", "")).strip(), safe="")
            attachment_id = urllib.parse.quote(str(params.get("id", "")).strip(), safe="")
            url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/attachments/{attachment_id}"
            return self._gmail_http_request_json(url, token_manager=token_manager)
        raise RuntimeError("Unsupported Gmail HTTP fallback command")

    def _gmail_http_request_json(self, url: str, *, token_manager) -> dict[str, Any]:
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

    def _gws_with_retry(self, args: list[str], *, attempts: int = 8) -> dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(1, max(1, attempts) + 1):
            try:
                return self._gws(args)
            except Exception as exc:
                last_exc = exc
                message = str(exc)
                is_quota_error = any(
                    marker in message
                    for marker in (
                        "rateLimitExceeded",
                        "Quota exceeded",
                        "quota metric",
                        '"code": 403',
                    )
                )
                is_failed_precondition = any(
                    marker in message
                    for marker in (
                        "failedPrecondition",
                        "Precondition check failed",
                    )
                )
                is_transient = bool(TRANSIENT_GMAIL_STATUS_RE.search(message)) or is_quota_error or is_failed_precondition
                if attempt >= attempts or not is_transient:
                    raise
                if is_quota_error:
                    sleep_seconds = min(90.0, 5.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)
                elif is_failed_precondition:
                    sleep_seconds = min(45.0, 3.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)
                else:
                    sleep_seconds = min(8.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.25)
                time.sleep(sleep_seconds)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("gws retry failed without an exception")

    def _worker_count(self, explicit_value: int | None, *, env_var: str, default: int) -> int:
        raw_value = explicit_value if explicit_value is not None else os.environ.get(env_var)
        try:
            worker_count = int(raw_value) if raw_value is not None else int(default)
        except (TypeError, ValueError):
            worker_count = int(default)
        return max(1, worker_count)

    def _resolve_people(self, cache: IdentityCache, emails: list[str]) -> list[str]:
        links: list[str] = []
        for email_value in emails:
            resolved = cache.resolve("email", email_value)
            if resolved and resolved not in links:
                links.append(resolved)
        return links

    def _load_existing_quick_update_state(
        self,
        vault_path: str,
        *,
        account_email: str,
    ) -> tuple[dict[str, dict[str, str]], dict[str, str], dict[str, str]]:
        thread_state: dict[str, dict[str, str]] = {}
        message_hashes: dict[str, str] = {}
        attachment_hashes: dict[str, str] = {}
        normalized_account = account_email.strip().lower()
        for rel_path, _ in iter_notes(vault_path):
            if not rel_path.parts:
                continue
            top_level = rel_path.parts[0]
            if top_level not in {"EmailThreads", "Email", "EmailAttachments"}:
                continue
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if normalized_account and str(frontmatter.get("account_email", "")).strip().lower() != normalized_account:
                continue
            card_type = str(frontmatter.get("type", "")).strip()
            if card_type == "email_thread":
                thread_id = str(frontmatter.get("gmail_thread_id", "")).strip()
                if thread_id:
                    thread_state[thread_id] = {
                        "gmail_history_id": str(frontmatter.get("gmail_history_id", "")).strip(),
                        "thread_body_sha": str(frontmatter.get("thread_body_sha", "")).strip(),
                    }
            elif card_type == "email_message":
                message_id = str(frontmatter.get("gmail_message_id", "")).strip()
                if message_id:
                    message_hashes[message_id] = str(frontmatter.get("message_body_sha", "")).strip()
            elif card_type == "email_attachment":
                message_id = str(frontmatter.get("gmail_message_id", "")).strip()
                attachment_id = str(frontmatter.get("attachment_id", "")).strip()
                if message_id and attachment_id:
                    attachment_hashes[f"{message_id}:{attachment_id}"] = str(
                        frontmatter.get("attachment_metadata_sha", "")
                    ).strip()
        return thread_state, message_hashes, attachment_hashes

    def _filter_quick_update_records(
        self,
        *,
        thread_record: dict[str, Any],
        message_records: list[dict[str, Any]],
        attachment_records: list[dict[str, Any]],
        existing_thread_state: dict[str, dict[str, str]],
        existing_message_hashes: dict[str, str],
        existing_attachment_hashes: dict[str, str],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
        existing_thread = existing_thread_state.get(str(thread_record.get("thread_id", "")).strip(), {})
        incoming_thread_sha = str(thread_record.get("thread_body_sha", "")).strip()
        incoming_history_id = str(thread_record.get("gmail_history_id", "")).strip()
        existing_thread_sha = str(existing_thread.get("thread_body_sha", "")).strip()
        existing_history_id = str(existing_thread.get("gmail_history_id", "")).strip()
        skip_details = {
            "skipped_unchanged_threads": 0,
            "skipped_unchanged_messages": 0,
            "skipped_unchanged_attachments": 0,
        }

        if existing_thread and existing_thread_sha and existing_thread_sha == incoming_thread_sha:
            thread_only_records = [thread_record] if existing_history_id != incoming_history_id else []
            skip_details["skipped_unchanged_messages"] = len(message_records)
            skip_details["skipped_unchanged_attachments"] = len(attachment_records)
            if not thread_only_records:
                skip_details["skipped_unchanged_threads"] = 1
            return thread_only_records, [], [], skip_details

        filtered_messages = [
            record
            for record in message_records
            if existing_message_hashes.get(str(record.get("message_id", "")).strip()) != str(record.get("message_body_sha", "")).strip()
        ]
        filtered_attachments = [
            record
            for record in attachment_records
            if existing_attachment_hashes.get(
                f"{str(record.get('message_id', '')).strip()}:{str(record.get('attachment_id', '')).strip()}"
            )
            != str(record.get("attachment_metadata_sha", "")).strip()
        ]
        skip_details["skipped_unchanged_messages"] = len(message_records) - len(filtered_messages)
        skip_details["skipped_unchanged_attachments"] = len(attachment_records) - len(filtered_attachments)
        return [thread_record], filtered_messages, filtered_attachments, skip_details

    def _recompute_message_body_sha(self, record: dict[str, Any]) -> str:
        return compute_email_message_body_sha_from_payload(
            {
                "message_id": str(record.get("message_id", "")).strip(),
                "sent_at": str(record.get("sent_at", "")).strip(),
                "direction": str(record.get("direction", "")).strip(),
                "from_email": str(record.get("from_email", "")).strip(),
                "to_emails": list(record.get("to_emails", []) or []),
                "cc_emails": list(record.get("cc_emails", []) or []),
                "bcc_emails": list(record.get("bcc_emails", []) or []),
                "reply_to_emails": list(record.get("reply_to_emails", []) or []),
                "subject": str(record.get("subject", "")).strip(),
                "body": str(record.get("body", "")).strip(),
                "attachment_ids": list(record.get("attachment_ids", []) or []),
                "invite_ical_uid": str(record.get("invite_ical_uid", "")).strip(),
                "invite_event_id_hint": str(record.get("invite_event_id_hint", "")).strip(),
                "invite_method": str(record.get("invite_method", "")).strip(),
                "invite_title": str(record.get("invite_title", "")).strip(),
                "invite_start_at": str(record.get("invite_start_at", "")).strip(),
                "invite_end_at": str(record.get("invite_end_at", "")).strip(),
            }
        )

    def _apply_attachment_cap(
        self,
        *,
        thread_record: dict[str, Any],
        message_records: list[dict[str, Any]],
        attachment_records: list[dict[str, Any]],
        emitted_attachments: int,
        max_attachments: int | None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        if max_attachments is None:
            return thread_record, message_records, attachment_records
        remaining = max(0, max_attachments - emitted_attachments)
        if len(attachment_records) <= remaining:
            return thread_record, message_records, attachment_records

        account_email = str(thread_record.get("account_email", "")).strip()
        kept_attachment_records = list(attachment_records[:remaining])
        kept_attachment_ids_by_message: dict[str, list[str]] = {}
        kept_attachment_links_by_message: dict[str, list[str]] = {}
        for record in kept_attachment_records:
            message_id = str(record.get("message_id", "")).strip()
            attachment_id = str(record.get("attachment_id", "")).strip()
            kept_attachment_ids_by_message.setdefault(message_id, []).append(attachment_id)
            kept_attachment_links_by_message.setdefault(message_id, []).append(
                _wikilink_from_uid(_attachment_uid(account_email, message_id, attachment_id))
            )

        adjusted_messages: list[dict[str, Any]] = []
        for record in message_records:
            updated = dict(record)
            message_id = str(updated.get("message_id", "")).strip()
            updated["attachment_ids"] = kept_attachment_ids_by_message.get(message_id, [])
            updated["attachments"] = kept_attachment_links_by_message.get(message_id, [])
            updated["message_body_sha"] = self._recompute_message_body_sha(updated)
            adjusted_messages.append(updated)

        adjusted_thread = dict(thread_record)
        adjusted_thread["thread_body_sha"] = compute_email_thread_body_sha_from_payload(
            [
                {
                    "message_id": str(record.get("message_id", "")).strip(),
                    "sent_at": str(record.get("sent_at", "")).strip(),
                    "message_body_sha": str(record.get("message_body_sha", "")).strip(),
                }
                for record in adjusted_messages
            ]
        )
        return adjusted_thread, adjusted_messages, kept_attachment_records

    def _fetch_attachment_body(self, message_id: str, attachment_id: str) -> str:
        if not message_id or not attachment_id:
            return ""
        payload = self._gws_with_retry(
            [
                "gmail",
                "users",
                "messages",
                "attachments",
                "get",
                "--params",
                json.dumps({"userId": "me", "messageId": message_id, "id": attachment_id}),
            ]
        )
        return _decode_body_data(str(payload.get("data", "")))

    def _calendar_attachment_refs(self, message: dict[str, Any]) -> list[tuple[str, str]]:
        message_id = str(message.get("id", "")).strip()
        if not message_id:
            return []
        refs: list[tuple[str, str]] = []
        payload = message.get("payload", {}) or {}
        for part in _iter_parts(payload):
            mime_type = str(part.get("mimeType", "")).lower()
            filename = str(part.get("filename", "")).lower()
            body_info = part.get("body") or {}
            attachment_id = str(body_info.get("attachmentId", "")).strip()
            is_calendar_part = mime_type in CALENDAR_MIME_TYPES or filename.endswith(".ics")
            if is_calendar_part and attachment_id:
                refs.append((message_id, attachment_id))
        return refs

    def _prefetch_calendar_attachment_bodies(
        self,
        thread: dict[str, Any],
        *,
        attachment_workers: int,
    ) -> dict[str, str]:
        jobs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for message in thread.get("messages", []) or []:
            for message_id, attachment_id in self._calendar_attachment_refs(message):
                key = f"{message_id}:{attachment_id}"
                if key in seen:
                    continue
                seen.add(key)
                jobs.append((message_id, attachment_id))
        if not jobs:
            return {}

        worker_count = max(1, min(int(attachment_workers), len(jobs)))
        if worker_count == 1:
            return {f"{message_id}:{attachment_id}": self._fetch_attachment_body(message_id, attachment_id) for message_id, attachment_id in jobs}

        def _load(job: tuple[str, str]) -> tuple[str, str]:
            message_id, attachment_id = job
            return f"{message_id}:{attachment_id}", self._fetch_attachment_body(message_id, attachment_id)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            return dict(executor.map(_load, jobs))

    def _extract_invite_data(
        self,
        message: dict[str, Any],
        headers: list[dict[str, Any]],
        *,
        attachment_bodies: dict[str, str] | None = None,
    ) -> dict[str, str]:
        payload = message.get("payload", {}) or {}
        event_id_hint = _header_value(headers, "X-Goog-Calendar-EventId").strip()
        for part in _iter_parts(payload):
            mime_type = str(part.get("mimeType", "")).lower()
            filename = str(part.get("filename", "")).lower()
            body_info = part.get("body") or {}
            body_data = _decode_body_data(str(body_info.get("data", "")))
            attachment_id = str(body_info.get("attachmentId", "")).strip()
            is_calendar_part = mime_type in CALENDAR_MIME_TYPES or filename.endswith(".ics")
            if not is_calendar_part:
                continue
            if not body_data and attachment_id:
                message_id = str(message.get("id", "")).strip()
                cache_key = f"{message_id}:{attachment_id}"
                body_data = (attachment_bodies or {}).get(cache_key, "")
                if not body_data:
                    body_data = self._fetch_attachment_body(message_id, attachment_id)
            invite = _parse_ics(body_data)
            if event_id_hint:
                invite["invite_event_id_hint"] = event_id_hint
            if not invite.get("invite_event_id_hint") and invite.get("invite_ical_uid", "").endswith("@google.com"):
                invite["invite_event_id_hint"] = invite["invite_ical_uid"].split("@", 1)[0]
            return {key: value for key, value in invite.items() if value}

        html_bodies = [
            _decode_body_data(str((part.get("body") or {}).get("data", "")))
            for part in _iter_parts(payload)
            if str(part.get("mimeType", "")).lower() == "text/html"
        ]
        for html_body in html_bodies:
            invite = _parse_google_calendar_html(html_body, headers)
            if event_id_hint and not invite.get("invite_event_id_hint"):
                invite["invite_event_id_hint"] = event_id_hint
            if invite:
                return {key: value for key, value in invite.items() if value}

        return {"invite_event_id_hint": event_id_hint} if event_id_hint else {}

    def _message_records(
        self,
        message: dict[str, Any],
        *,
        account_email: str,
        own_emails: set[str],
        identity_cache: IdentityCache,
        thread_uid: str,
        attachment_bodies: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        headers = message.get("payload", {}).get("headers", []) or []
        from_pairs = _extract_addresses(_header_values(headers, "From"))
        to_pairs = _extract_addresses(_header_values(headers, "To"))
        cc_pairs = _extract_addresses(_header_values(headers, "Cc"))
        bcc_pairs = _extract_addresses(_header_values(headers, "Bcc"))
        reply_to_pairs = _extract_addresses(_header_values(headers, "Reply-To"))
        from_name, from_email = from_pairs[0] if from_pairs else ("", "")
        to_emails = [email for _, email in to_pairs]
        cc_emails = [email for _, email in cc_pairs]
        bcc_emails = [email for _, email in bcc_pairs]
        reply_to_emails = [email for _, email in reply_to_pairs]
        participant_emails: list[str] = []
        for email_value in [from_email, *to_emails, *cc_emails, *bcc_emails, *reply_to_emails]:
            if email_value and email_value not in participant_emails:
                participant_emails.append(email_value)
        sent_at = _parse_internal_date(message.get("internalDate")) or _parse_datetime_header(_header_value(headers, "Date"))
        people_links = self._resolve_people(identity_cache, participant_emails)
        body = _extract_text_body(message.get("payload", {}) or {})
        attachments = _extract_attachments(message.get("payload", {}) or {})
        attachment_links = [
            _wikilink_from_uid(
                _attachment_uid(account_email, str(message.get("id", "")), str(attachment["attachment_id"]))
            )
            for attachment in attachments
        ]
        attachment_ids = [str(attachment["attachment_id"]).strip() for attachment in attachments]
        invite_data = self._extract_invite_data(message, headers, attachment_bodies=attachment_bodies)
        direction = "outbound" if from_email in own_emails else "inbound"
        references = _parse_reference_header(_header_value(headers, "References"))
        message_id_header = _decode_mime_header(_header_value(headers, "Message-ID")).strip("<>")
        in_reply_to = _header_value(headers, "In-Reply-To").strip("<>")
        message_body_sha = compute_email_message_body_sha_from_payload(
            {
                "message_id": str(message.get("id", "")).strip(),
                "sent_at": sent_at,
                "direction": direction,
                "from_email": from_email,
                "to_emails": to_emails,
                "cc_emails": cc_emails,
                "bcc_emails": bcc_emails,
                "reply_to_emails": reply_to_emails,
                "subject": _decode_mime_header(_header_value(headers, "Subject")),
                "body": body,
                "attachment_ids": attachment_ids,
                **invite_data,
            }
        )
        message_record = {
            "kind": "message",
            "message_id": str(message.get("id", "")).strip(),
            "thread_id": str(message.get("threadId", "")).strip(),
            "account_email": account_email,
            "thread": _wikilink_from_uid(thread_uid),
            "direction": direction,
            "from_name": from_name,
            "from_email": from_email,
            "to_emails": to_emails,
            "cc_emails": cc_emails,
            "bcc_emails": bcc_emails,
            "reply_to_emails": reply_to_emails,
            "participant_emails": participant_emails,
            "sent_at": sent_at,
            "subject": _decode_mime_header(_header_value(headers, "Subject")),
            "snippet": str(message.get("snippet", "")).strip(),
            "label_ids": [str(label).strip() for label in message.get("labelIds", []) if str(label).strip()],
            "message_id_header": message_id_header,
            "in_reply_to": in_reply_to,
            "references": references,
            "has_attachments": bool(attachments),
            "attachments": attachment_links,
            "attachment_ids": attachment_ids,
            "calendar_events": [],
            "invite_ical_uid": invite_data.get("invite_ical_uid", ""),
            "invite_event_id_hint": invite_data.get("invite_event_id_hint", ""),
            "invite_method": invite_data.get("invite_method", ""),
            "invite_title": invite_data.get("invite_title", ""),
            "invite_start_at": invite_data.get("invite_start_at", ""),
            "invite_end_at": invite_data.get("invite_end_at", ""),
            "message_body_sha": message_body_sha,
            "people": people_links,
            "body": body,
            "created": _date_bucket(sent_at),
        }
        attachment_records = [
            {
                "kind": "attachment",
                "message_id": message_record["message_id"],
                "thread_id": message_record["thread_id"],
                "attachment_id": attachment["attachment_id"],
                "account_email": account_email,
                "message": _wikilink_from_uid(_message_uid(account_email, message_record["message_id"])),
                "thread": _wikilink_from_uid(thread_uid),
                "filename": attachment["filename"],
                "mime_type": attachment["mime_type"],
                "size_bytes": attachment["size_bytes"],
                "content_id": attachment["content_id"],
                "is_inline": attachment["is_inline"],
                "attachment_metadata_sha": compute_email_attachment_metadata_sha_from_payload(
                    {
                        "message_id": message_record["message_id"],
                        "attachment_id": attachment["attachment_id"],
                        "filename": attachment["filename"],
                        "mime_type": attachment["mime_type"],
                        "size_bytes": attachment["size_bytes"],
                        "content_id": attachment["content_id"],
                        "is_inline": attachment["is_inline"],
                    }
                ),
                "people": people_links,
                "created": message_record["created"],
            }
            for attachment in attachments
        ]
        return message_record, attachment_records

    def _thread_records(
        self,
        thread: dict[str, Any],
        *,
        account_email: str,
        own_emails: set[str],
        identity_cache: IdentityCache,
        attachment_workers: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        thread_id = str(thread.get("id", "")).strip()
        thread_uid = _thread_uid(account_email, thread_id)
        attachment_bodies = self._prefetch_calendar_attachment_bodies(thread, attachment_workers=attachment_workers)
        message_items: list[dict[str, Any]] = []
        attachment_items: list[dict[str, Any]] = []
        participants: list[str] = []
        people_links: list[str] = []
        label_ids: list[str] = []
        message_links: list[str] = []
        invite_ical_uids: list[str] = []
        invite_event_id_hints: list[str] = []
        timestamps: list[str] = []
        thread_hash_payload: list[dict[str, Any]] = []
        has_attachments = False
        subject = ""

        def _message_sort_key(item: dict[str, Any]) -> int:
            try:
                return int(item.get("internalDate", 0) or 0)
            except (TypeError, ValueError):
                return 0

        for message in sorted(thread.get("messages", []) or [], key=_message_sort_key):
            message_record, attachment_records = self._message_records(
                message,
                account_email=account_email,
                own_emails=own_emails,
                identity_cache=identity_cache,
                thread_uid=thread_uid,
                attachment_bodies=attachment_bodies,
            )
            if not subject and message_record["subject"]:
                subject = message_record["subject"]
            for value in message_record["participant_emails"]:
                if value not in participants:
                    participants.append(value)
            for value in message_record["people"]:
                if value not in people_links:
                    people_links.append(value)
            for value in message_record["label_ids"]:
                if value not in label_ids:
                    label_ids.append(value)
            message_link = _wikilink_from_uid(_message_uid(account_email, message_record["message_id"]))
            if message_link not in message_links:
                message_links.append(message_link)
            if message_record["sent_at"]:
                timestamps.append(message_record["sent_at"])
            if message_record["invite_ical_uid"] and message_record["invite_ical_uid"] not in invite_ical_uids:
                invite_ical_uids.append(message_record["invite_ical_uid"])
            if message_record["invite_event_id_hint"] and message_record["invite_event_id_hint"] not in invite_event_id_hints:
                invite_event_id_hints.append(message_record["invite_event_id_hint"])
            has_attachments = has_attachments or message_record["has_attachments"]
            thread_hash_payload.append(
                {
                    "message_id": message_record["message_id"],
                    "sent_at": message_record["sent_at"],
                    "message_body_sha": message_record["message_body_sha"],
                }
            )
            message_items.append(message_record)
            attachment_items.extend(attachment_records)

        first_message_at = timestamps[0] if timestamps else ""
        last_message_at = timestamps[-1] if timestamps else ""
        thread_record = {
            "kind": "thread",
            "thread_id": thread_id,
            "gmail_history_id": str(thread.get("historyId", "")).strip(),
            "account_email": account_email,
            "subject": subject,
            "participants": participants,
            "label_ids": label_ids,
            "messages": message_links,
            "calendar_events": [],
            "first_message_at": first_message_at,
            "last_message_at": last_message_at,
            "message_count": len(message_links),
            "has_attachments": has_attachments,
            "invite_ical_uids": invite_ical_uids,
            "invite_event_id_hints": invite_event_id_hints,
            "thread_body_sha": compute_email_thread_body_sha_from_payload(thread_hash_payload),
            "people": people_links,
            "created": _date_bucket(first_message_at or date.today().isoformat()),
        }
        return thread_record, message_items, attachment_items

    def _limits_reached(
        self,
        *,
        emitted_threads: int,
        emitted_messages: int,
        emitted_attachments: int,
        max_threads: int | None,
        max_messages: int | None,
        max_attachments: int | None,
    ) -> bool:
        checks = []
        if max_threads is not None:
            checks.append(emitted_threads >= max_threads)
        if max_messages is not None:
            checks.append(emitted_messages >= max_messages)
        if max_attachments is not None:
            checks.append(emitted_attachments >= max_attachments)
        return bool(checks) and any(checks)

    def _load_thread_batch(
        self,
        thread_id: str,
        *,
        account_email: str,
        own_emails: set[str],
        identity_cache: IdentityCache,
        attachment_workers: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        thread_data = self._gws_with_retry(
            [
                "gmail",
                "users",
                "threads",
                "get",
                "--params",
                json.dumps({"userId": "me", "id": thread_id, "format": "full"}),
            ]
        )
        return self._thread_records(
            thread_data,
            account_email=account_email,
            own_emails=own_emails,
            identity_cache=identity_cache,
            attachment_workers=attachment_workers,
        )

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        account_email: str = "",
        query: str | None = None,
        max_threads: int | None = 100,
        max_messages: int | None = 100,
        max_attachments: int | None = 100,
        page_size: int = 25,
        workers: int | None = None,
        attachment_workers: int | None = None,
        quick_update: bool = False,
        **kwargs,
    ):
        self._ensure_token_manager(account_email)
        account = account_email.lower().strip()
        own_emails = {account}
        own_emails.update(load_own_aliases(vault_path))
        identity_cache = IdentityCache(vault_path)
        hash_cache_enabled = bool(getattr(config, "gmail_thread_body_sha_cache_enabled", True))
        quick_update_enabled = bool(quick_update and hash_cache_enabled)
        existing_thread_state: dict[str, dict[str, str]] = {}
        existing_message_hashes: dict[str, str] = {}
        existing_attachment_hashes: dict[str, str] = {}
        if quick_update_enabled:
            existing_thread_state, existing_message_hashes, existing_attachment_hashes = self._load_existing_quick_update_state(
                vault_path,
                account_email=account,
            )
        emitted_threads = 0
        emitted_messages = 0
        emitted_attachments = 0
        scanned_threads = int(cursor.get("scanned_threads", 0) or 0)
        skipped_unchanged_threads = int(cursor.get("skipped_unchanged_threads", 0) or 0)
        skipped_unchanged_messages = int(cursor.get("skipped_unchanged_messages", 0) or 0)
        skipped_unchanged_attachments = int(cursor.get("skipped_unchanged_attachments", 0) or 0)
        next_page_token = cursor.get("page_token")
        page_thread_ids = list(cursor.get("page_thread_ids") or [])
        page_next_token = cursor.get("page_next_token")
        page_index = int(cursor.get("page_index", 0) or 0)
        capped_page_size = max(1, min(int(page_size or 25), 100))
        fetch_workers = self._worker_count(
            workers,
            env_var="HFA_GMAIL_WORKERS",
            default=32,
        )
        calendar_attachment_workers = self._worker_count(
            attachment_workers,
            env_var="HFA_GMAIL_ATTACHMENT_WORKERS",
            default=min(4, fetch_workers),
        )
        sequence = 0

        while True:
            if self._limits_reached(
                emitted_threads=emitted_threads,
                emitted_messages=emitted_messages,
                emitted_attachments=emitted_attachments,
                max_threads=max_threads,
                max_messages=max_messages,
                max_attachments=max_attachments,
            ):
                return

            if page_thread_ids and page_index >= len(page_thread_ids):
                if not page_next_token:
                    break
                page_thread_ids = []
                page_index = 0
                next_page_token = page_next_token
                page_next_token = None
                continue

            current_page_token = next_page_token
            if not page_thread_ids:
                params: dict[str, Any] = {"userId": "me", "maxResults": capped_page_size}
                if current_page_token:
                    params["pageToken"] = current_page_token
                if query:
                    params["q"] = query
                list_data = self._gws_with_retry(["gmail", "users", "threads", "list", "--params", json.dumps(params)])
                page_threads = list_data.get("threads", []) or []
                page_thread_ids = []
                page_skip_details = {
                    "skipped_unchanged_threads": 0,
                    "skipped_unchanged_messages": 0,
                    "skipped_unchanged_attachments": 0,
                }
                for thread in page_threads:
                    thread_id = str(thread.get("id", "")).strip()
                    if not thread_id:
                        continue
                    history_id = str(thread.get("historyId", "")).strip()
                    if quick_update_enabled:
                        existing_thread = existing_thread_state.get(thread_id, {})
                        if history_id and history_id == str(existing_thread.get("gmail_history_id", "")).strip():
                            scanned_threads += 1
                            skipped_unchanged_threads += 1
                            page_skip_details["skipped_unchanged_threads"] += 1
                            continue
                    page_thread_ids.append(thread_id)
                page_next_token = list_data.get("nextPageToken")
                page_index = 0
                if not page_thread_ids:
                    yield FetchedBatch(
                        items=[],
                        cursor_patch={
                            "page_thread_ids": [],
                            "page_index": 0,
                            "page_next_token": None,
                            "page_token": page_next_token,
                            "scanned_threads": scanned_threads,
                            "emitted_threads": emitted_threads,
                            "emitted_messages": emitted_messages,
                            "emitted_attachments": emitted_attachments,
                            "skipped_unchanged_threads": skipped_unchanged_threads,
                            "skipped_unchanged_messages": skipped_unchanged_messages,
                            "skipped_unchanged_attachments": skipped_unchanged_attachments,
                        },
                        sequence=sequence,
                        skipped_count=sum(page_skip_details.values()),
                        skip_details=page_skip_details,
                    )
                    sequence += 1
                    if not page_next_token:
                        break
                    next_page_token = page_next_token
                    page_next_token = None
                    continue

            with ThreadPoolExecutor(max_workers=max(1, min(fetch_workers, len(page_thread_ids) - page_index))) as executor:
                inflight: dict[int, Any] = {}
                next_submit_index = page_index

                def _submit_limit() -> int:
                    if max_threads is None:
                        return len(page_thread_ids)
                    remaining_threads = max(0, max_threads - emitted_threads)
                    return min(len(page_thread_ids), page_index + remaining_threads)

                def _submit(next_index: int) -> None:
                    thread_id = page_thread_ids[next_index]
                    inflight[next_index] = executor.submit(
                        self._load_thread_batch,
                        thread_id,
                        account_email=account,
                        own_emails=own_emails,
                        identity_cache=identity_cache,
                        attachment_workers=calendar_attachment_workers,
                    )

                while next_submit_index < _submit_limit() and len(inflight) < fetch_workers:
                    _submit(next_submit_index)
                    next_submit_index += 1

                while page_index < len(page_thread_ids):
                    future = inflight.pop(page_index)
                    thread_record, message_records, attachment_records = future.result()
                    scanned_threads += 1
                    batch_skip_details = {
                        "skipped_unchanged_threads": 0,
                        "skipped_unchanged_messages": 0,
                        "skipped_unchanged_attachments": 0,
                    }
                    thread_record, message_records, attachment_records = self._apply_attachment_cap(
                        thread_record=thread_record,
                        message_records=message_records,
                        attachment_records=attachment_records,
                        emitted_attachments=emitted_attachments,
                        max_attachments=max_attachments,
                    )
                    if quick_update_enabled:
                        thread_records, message_records, attachment_records, filter_skip_details = self._filter_quick_update_records(
                            thread_record=thread_record,
                            message_records=message_records,
                            attachment_records=attachment_records,
                            existing_thread_state=existing_thread_state,
                            existing_message_hashes=existing_message_hashes,
                            existing_attachment_hashes=existing_attachment_hashes,
                        )
                        for key, value in filter_skip_details.items():
                            batch_skip_details[key] += value
                    else:
                        thread_records = [thread_record]
                    skipped_unchanged_threads += batch_skip_details["skipped_unchanged_threads"]
                    skipped_unchanged_messages += batch_skip_details["skipped_unchanged_messages"]
                    skipped_unchanged_attachments += batch_skip_details["skipped_unchanged_attachments"]
                    batch_items: list[dict[str, Any]] = []

                    if (max_threads is None or emitted_threads < max_threads) and thread_records:
                        batch_items.extend(thread_records)
                        emitted_threads += 1
                    for message_record in message_records:
                        if max_messages is not None and emitted_messages >= max_messages:
                            break
                        batch_items.append(message_record)
                        emitted_messages += 1
                    for attachment_record in attachment_records:
                        if max_attachments is not None and emitted_attachments >= max_attachments:
                            break
                        batch_items.append(attachment_record)
                        emitted_attachments += 1

                    page_index += 1
                    yield FetchedBatch(
                        items=batch_items,
                        cursor_patch={
                            "page_thread_ids": page_thread_ids,
                            "page_index": page_index,
                            "page_next_token": page_next_token,
                            "page_token": current_page_token,
                            "scanned_threads": scanned_threads,
                            "emitted_threads": emitted_threads,
                            "emitted_messages": emitted_messages,
                            "emitted_attachments": emitted_attachments,
                            "skipped_unchanged_threads": skipped_unchanged_threads,
                            "skipped_unchanged_messages": skipped_unchanged_messages,
                            "skipped_unchanged_attachments": skipped_unchanged_attachments,
                        },
                        sequence=sequence,
                        skipped_count=sum(batch_skip_details.values()),
                        skip_details=batch_skip_details,
                    )
                    sequence += 1

                    if self._limits_reached(
                        emitted_threads=emitted_threads,
                        emitted_messages=emitted_messages,
                        emitted_attachments=emitted_attachments,
                        max_threads=max_threads,
                        max_messages=max_messages,
                        max_attachments=max_attachments,
                    ):
                        for pending in inflight.values():
                            pending.cancel()
                        executor.shutdown(wait=False, cancel_futures=True)
                        return

                    while next_submit_index < _submit_limit() and len(inflight) < fetch_workers:
                        _submit(next_submit_index)
                        next_submit_index += 1

        yield FetchedBatch(
            items=[],
            cursor_patch={
                "page_thread_ids": [],
                "page_index": 0,
                "page_next_token": None,
                "page_token": None,
                "scanned_threads": scanned_threads,
                "emitted_threads": emitted_threads,
                "emitted_messages": emitted_messages,
                "emitted_attachments": emitted_attachments,
                "skipped_unchanged_threads": skipped_unchanged_threads,
                "skipped_unchanged_messages": skipped_unchanged_messages,
                "skipped_unchanged_attachments": skipped_unchanged_attachments,
            },
            sequence=sequence,
        )

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        account_email: str = "",
        query: str | None = None,
        max_threads: int | None = 100,
        max_messages: int | None = 100,
        max_attachments: int | None = 100,
        page_size: int = 25,
        workers: int | None = None,
        attachment_workers: int | None = None,
        quick_update: bool = False,
        **kwargs,
    ) -> list[dict[str, Any]]:
        self._ensure_token_manager(account_email)
        items: list[dict[str, Any]] = []
        for batch in self.fetch_batches(
            vault_path,
            cursor,
            config=config,
            account_email=account_email,
            query=query,
            max_threads=max_threads,
            max_messages=max_messages,
            max_attachments=max_attachments,
            page_size=page_size,
            workers=workers,
            attachment_workers=attachment_workers,
            quick_update=quick_update,
            **kwargs,
        ):
            items.extend(batch.items)
            cursor.update(batch.cursor_patch)
        return items

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        kind = str(item.get("kind", "")).strip()
        if kind == "thread":
            thread_id = str(item.get("thread_id", "")).strip()
            account_email = str(item.get("account_email", "")).strip()
            source_id = _thread_identity(account_email, thread_id)
            card = EmailThreadCard(
                uid=_thread_uid(account_email, thread_id),
                type="email_thread",
                source=[THREAD_SOURCE],
                source_id=source_id,
                created=str(item.get("created", "")).strip() or today,
                updated=today,
                summary=str(item.get("subject", "")).strip() or thread_id,
                people=list(item.get("people", [])),
                gmail_thread_id=thread_id,
                gmail_history_id=str(item.get("gmail_history_id", "")).strip(),
                account_email=account_email,
                subject=str(item.get("subject", "")).strip(),
                participants=list(item.get("participants", [])),
                label_ids=list(item.get("label_ids", [])),
                messages=list(item.get("messages", [])),
                calendar_events=list(item.get("calendar_events", [])),
                first_message_at=str(item.get("first_message_at", "")).strip(),
                last_message_at=str(item.get("last_message_at", "")).strip(),
                message_count=int(item.get("message_count", 0) or 0),
                has_attachments=bool(item.get("has_attachments", False)),
                invite_ical_uids=list(item.get("invite_ical_uids", [])),
                invite_event_id_hints=list(item.get("invite_event_id_hints", [])),
                thread_body_sha=str(item.get("thread_body_sha", "")).strip(),
            )
            provenance = deterministic_provenance(card, THREAD_SOURCE)
            return card, provenance, ""

        if kind == "message":
            message_id = str(item.get("message_id", "")).strip()
            account_email = str(item.get("account_email", "")).strip()
            source_id = _message_identity(account_email, message_id)
            card = EmailMessageCard(
                uid=_message_uid(account_email, message_id),
                type="email_message",
                source=[MESSAGE_SOURCE],
                source_id=source_id,
                created=str(item.get("created", "")).strip() or today,
                updated=today,
                summary=str(item.get("subject", "")).strip() or str(item.get("snippet", "")).strip() or message_id,
                people=list(item.get("people", [])),
                gmail_message_id=message_id,
                gmail_thread_id=str(item.get("thread_id", "")).strip(),
                account_email=account_email,
                thread=str(item.get("thread", "")).strip(),
                direction=str(item.get("direction", "")).strip(),
                from_name=str(item.get("from_name", "")).strip(),
                from_email=str(item.get("from_email", "")).strip(),
                to_emails=list(item.get("to_emails", [])),
                cc_emails=list(item.get("cc_emails", [])),
                bcc_emails=list(item.get("bcc_emails", [])),
                reply_to_emails=list(item.get("reply_to_emails", [])),
                participant_emails=list(item.get("participant_emails", [])),
                sent_at=str(item.get("sent_at", "")).strip(),
                subject=str(item.get("subject", "")).strip(),
                snippet=str(item.get("snippet", "")).strip(),
                label_ids=list(item.get("label_ids", [])),
                message_id_header=str(item.get("message_id_header", "")).strip(),
                in_reply_to=str(item.get("in_reply_to", "")).strip(),
                references=list(item.get("references", [])),
                has_attachments=bool(item.get("has_attachments", False)),
                attachments=list(item.get("attachments", [])),
                calendar_events=list(item.get("calendar_events", [])),
                invite_ical_uid=str(item.get("invite_ical_uid", "")).strip(),
                invite_event_id_hint=str(item.get("invite_event_id_hint", "")).strip(),
                invite_method=str(item.get("invite_method", "")).strip(),
                invite_title=str(item.get("invite_title", "")).strip(),
                invite_start_at=str(item.get("invite_start_at", "")).strip(),
                invite_end_at=str(item.get("invite_end_at", "")).strip(),
                message_body_sha=str(item.get("message_body_sha", "")).strip(),
            )
            provenance = deterministic_provenance(card, MESSAGE_SOURCE)
            return card, provenance, str(item.get("body", "")).strip()

        if kind == "attachment":
            message_id = str(item.get("message_id", "")).strip()
            attachment_id = str(item.get("attachment_id", "")).strip()
            account_email = str(item.get("account_email", "")).strip()
            source_id = _attachment_identity(account_email, message_id, attachment_id)
            card = EmailAttachmentCard(
                uid=_attachment_uid(account_email, message_id, attachment_id),
                type="email_attachment",
                source=[ATTACHMENT_SOURCE],
                source_id=source_id,
                created=str(item.get("created", "")).strip() or today,
                updated=today,
                summary=str(item.get("filename", "")).strip() or attachment_id,
                people=list(item.get("people", [])),
                gmail_message_id=message_id,
                gmail_thread_id=str(item.get("thread_id", "")).strip(),
                attachment_id=attachment_id,
                account_email=account_email,
                message=str(item.get("message", "")).strip(),
                thread=str(item.get("thread", "")).strip(),
                filename=str(item.get("filename", "")).strip(),
                mime_type=str(item.get("mime_type", "")).strip(),
                size_bytes=int(item.get("size_bytes", 0) or 0),
                content_id=str(item.get("content_id", "")).strip(),
                is_inline=bool(item.get("is_inline", False)),
                attachment_metadata_sha=str(item.get("attachment_metadata_sha", "")).strip(),
            )
            provenance = deterministic_provenance(card, ATTACHMENT_SOURCE)
            return card, provenance, ""

        raise ValueError(f"Unsupported Gmail record kind: {kind}")

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
