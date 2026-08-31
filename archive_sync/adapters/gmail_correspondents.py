"""Gmail correspondents adapter (HTTP-first, vault-local history)."""

from __future__ import annotations

import json
import logging
import os
import random
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses
from pathlib import Path
from typing import Any

from archive_auth import ACCOUNTS, build_google_cli_token_manager
from archive_cli.index_config import get_gmail_api_workers
from archive_vault.schema import PersonCard
from archive_vault.sync_state import update_cursor
from archive_vault.uid import generate_uid

from .base import BaseAdapter, deterministic_provenance

logger = logging.getLogger("ppa.gmail_correspondents")

_GMAIL_BATCH_LIMIT = 100
_GMAIL_BATCH_URL = "https://www.googleapis.com/batch/gmail/v1"
_GMAIL_API_ROOT = "https://gmail.googleapis.com/gmail/v1/users/me"
_METADATA_HEADERS = ("From", "To", "Cc", "Bcc", "Reply-To")
_GMAIL_GET_FIELDS = "id,internalDate,payload/headers"

_SKIPPABLE_MESSAGE_MARKERS = (
    "failedPrecondition",
    "Precondition check failed",
    "notFound",
    '"code": 404',
    "backendError",
    '"code": 500',
    "Unknown Error",
)


def _is_skippable_message_error(message: str, status: int | None = None) -> bool:
    if status in {404, 500}:
        return True
    return any(marker in message for marker in _SKIPPABLE_MESSAGE_MARKERS)


_QUOTA_ERROR_MARKERS = (
    "rateLimitExceeded",
    "RATE_LIMIT_EXCEEDED",
    "Quota exceeded",
    "quota metric",
)
_GMAIL_HTTP_ATTEMPTS = 8


def _is_gmail_quota_error(message: str, status: int | None = None) -> bool:
    if status == 429:
        return True
    return any(marker in message for marker in _QUOTA_ERROR_MARKERS)


def _quota_backoff_seconds(attempt: int) -> float:
    return min(90.0, 5.0 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)


def _string_list(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    value = str(raw or "").strip()
    return [value] if value else []


def _http_error_message(exc: urllib.error.HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8", errors="replace")
    except Exception:
        body = ""
    return body or str(exc)


def _parse_application_http(payload: bytes | str) -> tuple[int, dict[str, Any]]:
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    text = text.replace("\r\n", "\n")
    head, _, body = text.partition("\n\n")
    status = 0
    first = head.split("\n", 1)[0].strip()
    match = re.match(r"HTTP/\S+\s+(\d+)", first)
    if match:
        status = int(match.group(1))
    body = body.strip()
    if not body:
        return status, {}
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return status, {}
    return status, parsed if isinstance(parsed, dict) else {}


def _watermark_date(watermark: str) -> date | None:
    text = str(watermark or "").strip()
    if len(text) >= 10 and text[4] in "-/" and text[7] in "-/":
        try:
            return date(int(text[0:4]), int(text[5:7]), int(text[8:10]))
        except ValueError:
            return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).date()


def gmail_after_query(watermark: str) -> str | None:
    """Build a Gmail search ``after:YYYY/MM/DD`` from ``last_sync``.

    Gmail ``after:`` is exclusive at midnight UTC and is day-granular. A
    watermark of ``2026-03-09T00:55:05`` becomes ``after:2026/03/09``, which
    lists messages with internalDate on 2026-03-10 00:00:00 UTC or later.
    Same-day mail after the watermark is therefore deferred until a later
    calendar day. Date-window form matches
    ``archive_scripts/ppa-gmail-extract-parallel.py``; gmail-messages itself
    increments via ``history_id`` skip rather than ``after:``.
    """

    day = _watermark_date(watermark)
    if day is None:
        return None
    return f"after:{day.year:04d}/{day.month:02d}/{day.day:02d}"


def resolve_correspondents_list_query(
    cursor: dict[str, Any],
    query: str | None = None,
) -> tuple[str | None, str]:
    """Choose the Gmail ``messages.list`` query for this fetch.

    Returns ``(list_query, mode)`` where mode is ``resume``, ``incremental``,
    or ``full``.

    Resume (``page_token`` set): keep the in-progress query. Never derive a
    new ``after:`` from ``last_sync`` mid-walk — Gmail page tokens are bound
    to the original list request. Prefer ``cursor['list_query']``, else the
    explicit ``query`` kwarg, else ``None`` (full-mailbox continuation,
    including jobs started before incremental listing existed).

    Fresh run: explicit ``query`` wins; else ``last_sync`` → ``after:``;
    else full mailbox (bootstrap).
    """

    explicit = str(query).strip() if query else ""
    if cursor.get("page_token"):
        stored = str(cursor.get("list_query") or "").strip()
        if stored:
            return stored, "resume"
        if explicit:
            return explicit, "resume"
        return None, "resume"
    if explicit:
        return explicit, "incremental" if "after:" in explicit.lower() else "full"
    last_sync = str(cursor.get("last_sync") or "").strip()
    derived = gmail_after_query(last_sync) if last_sync else None
    if derived:
        return derived, "incremental"
    return None, "full"


AUTOMATED_LOCAL_PREFIXES = {
    "alert",
    "alerts",
    "billing",
    "comment",
    "community",
    "contact",
    "donotreply",
    "do-not-reply",
    "hello",
    "info",
    "mail",
    "mailer-daemon",
    "newsletter",
    "no-reply",
    "noreply",
    "notification",
    "notifications",
    "push",
    "receipt",
    "receipts",
    "reply",
    "security",
    "subscribed",
    "support",
    "update",
    "updates",
}
AUTOMATED_DOMAINS = {
    "facebookmail.com",
    "googlegroups.com",
    "linkedin.com",
    "noreply.github.com",
    "reply.github.com",
    "reply.linkedin.com",
    "replies.uber.com",
    "substack.com",
}
AUTOMATED_DOMAIN_PREFIXES = {
    "about",
    "e",
    "email",
    "lists",
    "mail",
    "news",
    "notification",
    "notifications",
    "o",
    "promotion",
    "promotions",
    "reply",
    "replies",
    "welcome",
}
NON_PERSON_NAME_TOKENS = {
    "advisors",
    "alliance",
    "american",
    "animal",
    "air",
    "buy",
    "capital",
    "club",
    "community",
    "company",
    "cooking",
    "daily",
    "express",
    "facebook",
    "foundation",
    "from",
    "fund",
    "geographic",
    "group",
    "hospital",
    "hotels",
    "information",
    "institute",
    "lines",
    "linkedin",
    "mail",
    "management",
    "making",
    "national",
    "news",
    "on",
    "partners",
    "porter",
    "resident",
    "residents",
    "running",
    "team",
    "the",
    "university",
    "via",
    "moves",
}


def _split_display_name(name: str) -> tuple[str, str]:
    cleaned = " ".join(name.strip().split())
    if not cleaned:
        return "", ""
    parts = cleaned.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _looks_like_person_name(name: str) -> bool:
    cleaned = " ".join(name.strip().split())
    if not cleaned or any(char in cleaned for char in "@/"):
        return False
    if cleaned.isupper():
        return False
    tokens = [re.sub(r"[^A-Za-z'-]", "", token) for token in cleaned.split()]
    tokens = [token for token in tokens if token]
    if len(tokens) < 2 or len(tokens) > 4:
        return False
    lowered = {token.lower() for token in tokens}
    if lowered & NON_PERSON_NAME_TOKENS:
        return False
    return all(len(token) >= 2 and token.replace("-", "").replace("'", "").isalpha() for token in tokens)


def _looks_like_person_local_part(local: str) -> bool:
    cleaned = local.lower().strip()
    if not cleaned:
        return False
    base = cleaned.split("+", 1)[0]
    tokens = [token for token in re.split(r"[._+-]+", base) if token]
    if base in AUTOMATED_LOCAL_PREFIXES or any(token in AUTOMATED_LOCAL_PREFIXES for token in tokens):
        return False
    if any(base.startswith(f"{prefix}-") or base.startswith(f"{prefix}_") for prefix in AUTOMATED_LOCAL_PREFIXES):
        return False
    if base.startswith("reply+") or base.startswith("reply-"):
        return False
    if len(tokens) >= 2 and all(token.isalpha() and len(token) >= 2 for token in tokens[:3]):
        return True
    return False


def _is_automated_local(local: str) -> bool:
    cleaned = local.lower().strip()
    if not cleaned:
        return True
    base = cleaned.split("+", 1)[0]
    tokens = [token for token in re.split(r"[._+-]+", base) if token]
    if base in AUTOMATED_LOCAL_PREFIXES or any(token in AUTOMATED_LOCAL_PREFIXES for token in tokens):
        return True
    if any(base.startswith(f"{prefix}-") or base.startswith(f"{prefix}_") for prefix in AUTOMATED_LOCAL_PREFIXES):
        return True
    return base.startswith("reply+") or base.startswith("reply-")


def _should_keep_correspondent(name: str, email: str) -> bool:
    local, _, domain = email.partition("@")
    local = local.lower().strip()
    domain = domain.lower().strip()
    first_label = domain.split(".", 1)[0]
    if domain in AUTOMATED_DOMAINS:
        return False
    if first_label in AUTOMATED_DOMAIN_PREFIXES:
        return False
    if _is_automated_local(local):
        return False
    if _looks_like_person_name(name):
        return True
    return _looks_like_person_local_part(local)


def _extract_addresses_from_headers(headers: list[dict[str, str]]) -> list[tuple[str, str]]:
    values: list[str] = []
    keep = {"from", "to", "cc", "bcc", "reply-to"}
    for h in headers:
        name = (h.get("name") or "").lower()
        if name in keep:
            values.append(h.get("value", ""))
    pairs = getaddresses(values)
    out: list[tuple[str, str]] = []
    for n, e in pairs:
        em = (e or "").strip().lower()
        if not em or "@" not in em:
            continue
        out.append(((n or "").strip(), em))
    return out


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


def _managed_account_emails() -> set[str]:
    return {
        str(account.get("email", "")).strip().lower()
        for account in ACCOUNTS.values()
        if str(account.get("email", "")).strip()
    }


class GmailCorrespondentsAdapter(BaseAdapter):
    source_id = "gmail-correspondents"
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
        account_emails = sorted(
            {str(value).strip().lower() for value in (kwargs.get("account_emails") or []) if str(value).strip()}
        )
        if account_emails:
            return f"{self.source_id}:aggregate:{'+'.join(account_emails)}"
        account_email = str(kwargs.get("account_email", "")).strip().lower()
        return f"{self.source_id}:{account_email}" if account_email else self.source_id

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
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid gws JSON output: {e}") from e

    def _can_parallelize_gws(self) -> bool:
        bound = getattr(self._gws, "__func__", None)
        return bound is GmailCorrespondentsAdapter._gws

    def _can_use_http(self) -> bool:
        return getattr(self, "_token_manager", None) is not None and self._can_parallelize_gws()

    def _api_workers(self) -> int:
        if not self._can_parallelize_gws():
            return 1
        return get_gmail_api_workers()

    def _gmail_json(self, args: list[str]) -> dict[str, Any]:
        if self._can_use_http():
            return self._gmail_http_json(args)
        return self._gws(args)

    def _gmail_http_json(self, args: list[str]) -> dict[str, Any]:
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is None:
            raise RuntimeError("Gmail HTTP requires a token manager")
        params = json.loads(args[-1]) if args[-2:] and args[-2] == "--params" else {}
        if args[:4] == ["gmail", "users", "messages", "list"]:
            query = urllib.parse.urlencode(
                {key: value for key, value in params.items() if key not in {"userId"} and value not in (None, "")}
            )
            url = f"{_GMAIL_API_ROOT}/messages"
            if query:
                url = f"{url}?{query}"
            return self._gmail_http_request_json(url, token_manager=token_manager)
        if args[:4] == ["gmail", "users", "messages", "get"]:
            message_id = urllib.parse.quote(str(params.get("id", "")).strip(), safe="")
            query_params = [
                (key, value) for key, value in params.items() if key not in {"id", "userId"} and value not in (None, "")
            ]
            if str(params.get("format") or "").lower() == "metadata":
                query_params.append(("fields", _GMAIL_GET_FIELDS))
                query_params.extend(("metadataHeaders", header) for header in _METADATA_HEADERS)
            query = urllib.parse.urlencode(query_params, doseq=True)
            url = f"{_GMAIL_API_ROOT}/messages/{message_id}"
            if query:
                url = f"{url}?{query}"
            return self._gmail_http_request_json(url, token_manager=token_manager)
        raise RuntimeError("Unsupported Gmail HTTP command")

    def _gmail_http_request_json(self, url: str, *, token_manager) -> dict[str, Any]:
        def _request(force_refresh: bool = False) -> dict[str, Any]:
            token = token_manager.get_access_token(force_refresh=force_refresh)
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))

        last_exc: Exception | None = None
        for attempt in range(1, _GMAIL_HTTP_ATTEMPTS + 1):
            try:
                return _request()
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code == 401:
                    try:
                        return _request(force_refresh=True)
                    except urllib.error.HTTPError as retry_exc:
                        raise RuntimeError(_http_error_message(retry_exc)) from retry_exc
                message = _http_error_message(exc)
                if _is_gmail_quota_error(message, exc.code) and attempt < _GMAIL_HTTP_ATTEMPTS:
                    delay = _quota_backoff_seconds(attempt)
                    logger.warning(
                        "gmail http quota retry attempt=%s/%s delay=%.1fs",
                        attempt,
                        _GMAIL_HTTP_ATTEMPTS,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(message) from exc
        raise RuntimeError(
            _http_error_message(last_exc) if isinstance(last_exc, urllib.error.HTTPError) else str(last_exc)
        )

    def _parse_batch_response(self, content_type: str, raw: bytes) -> list[tuple[int, int, dict[str, Any]]]:
        header = f"Content-Type: {content_type}\r\n\r\n".encode("utf-8")
        parsed = BytesParser(policy=policy.default).parsebytes(header + raw)
        out: list[tuple[int, int, dict[str, Any]]] = []
        if not parsed.is_multipart():
            status, payload = _parse_application_http(raw)
            out.append((0, status, payload))
            return out
        for fallback_index, part in enumerate(parsed.iter_parts()):
            cid = str(part.get("Content-ID") or "")
            match = re.search(r"item-(\d+)", cid)
            index = int(match.group(1)) if match else fallback_index
            payload_bytes = part.get_payload(decode=True)
            if payload_bytes is None:
                inner = part.get_payload()
                payload_bytes = inner.encode("utf-8") if isinstance(inner, str) else b""
            status, data = _parse_application_http(payload_bytes or b"")
            out.append((index, status, data))
        return out

    def _gmail_http_batch_chunk(self, message_ids: list[str]) -> list[dict[str, Any]]:
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is None:
            raise RuntimeError("Gmail HTTP batch requires a token manager")
        boundary = f"batch_{os.urandom(8).hex()}"
        query = urllib.parse.urlencode(
            [("format", "metadata"), ("fields", _GMAIL_GET_FIELDS)]
            + [("metadataHeaders", header) for header in _METADATA_HEADERS]
        )
        parts: list[str] = []
        for index, message_id in enumerate(message_ids):
            quoted = urllib.parse.quote(message_id, safe="")
            parts.append(
                f"--{boundary}\r\n"
                "Content-Type: application/http\r\n"
                "Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: <item-{index}>\r\n"
                "\r\n"
                f"GET /gmail/v1/users/me/messages/{quoted}?{query}\r\n"
                "\r\n"
            )
        body = ("".join(parts) + f"--{boundary}--\r\n").encode("utf-8")

        def _post(force_refresh: bool = False) -> tuple[str, bytes]:
            token = token_manager.get_access_token(force_refresh=force_refresh)
            req = urllib.request.Request(_GMAIL_BATCH_URL, data=body, method="POST")
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header("Content-Type", f"multipart/mixed; boundary={boundary}")
            with urllib.request.urlopen(req, timeout=120) as resp:
                return str(resp.headers.get("Content-Type") or ""), resp.read()

        content_type = ""
        raw = b""
        last_exc: Exception | None = None
        for attempt in range(1, _GMAIL_HTTP_ATTEMPTS + 1):
            try:
                content_type, raw = _post()
                break
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code == 401:
                    try:
                        content_type, raw = _post(force_refresh=True)
                        break
                    except urllib.error.HTTPError as retry_exc:
                        raise RuntimeError(_http_error_message(retry_exc)) from retry_exc
                message = _http_error_message(exc)
                if _is_gmail_quota_error(message, exc.code) and attempt < _GMAIL_HTTP_ATTEMPTS:
                    delay = _quota_backoff_seconds(attempt)
                    logger.warning(
                        "gmail batch quota retry attempt=%s/%s delay=%.1fs",
                        attempt,
                        _GMAIL_HTTP_ATTEMPTS,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(message) from exc
        else:
            raise RuntimeError(
                _http_error_message(last_exc) if isinstance(last_exc, urllib.error.HTTPError) else str(last_exc)
            )

        parsed_parts = self._parse_batch_response(content_type, raw)
        results: list[dict[str, Any]] = [{} for _ in message_ids]
        retry_ids: list[tuple[int, str]] = []
        for index, status, payload in parsed_parts:
            if index < 0 or index >= len(message_ids):
                continue
            if status == 200 and payload:
                results[index] = payload
                continue
            error_text = json.dumps(payload) if payload else f"HTTP {status}"
            if status == 200:
                results[index] = payload
            elif _is_skippable_message_error(error_text, status):
                retry_ids.append((index, message_ids[index]))
            else:
                retry_ids.append((index, message_ids[index]))
        for index, message_id in retry_ids:
            results[index] = self._fetch_message_metadata(message_id)
        return results

    def _gmail_http_batch_get_metadata(self, message_ids: list[str]) -> list[dict[str, Any]]:
        if not message_ids:
            return []
        chunks = [
            message_ids[offset : offset + _GMAIL_BATCH_LIMIT]
            for offset in range(0, len(message_ids), _GMAIL_BATCH_LIMIT)
        ]

        def _run_chunk(chunk: list[str]) -> list[dict[str, Any]]:
            return self._gmail_http_batch_chunk(chunk)

        workers = min(self._api_workers(), len(chunks))
        if workers > 1 and len(chunks) > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                chunk_results = list(executor.map(_run_chunk, chunks))
        else:
            chunk_results = [_run_chunk(chunk) for chunk in chunks]
        fetched: list[dict[str, Any]] = []
        for chunk_result in chunk_results:
            fetched.extend(chunk_result)
        return fetched

    def _fetch_message_metadata(self, message_id: str) -> dict[str, Any]:
        get_params = {"userId": "me", "id": message_id, "format": "metadata"}
        args = ["gmail", "users", "messages", "get", "--params", json.dumps(get_params)]
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                return self._gmail_json(args)
            except Exception as exc:
                last_exc = exc
                if not _is_skippable_message_error(str(exc)):
                    raise
                if attempt < 3:
                    time.sleep(float(attempt))
        logger.warning("skip unreadable message id=%s error=%s", message_id, last_exc)
        return {}

    def _fetch_messages_metadata(self, message_ids: list[str]) -> list[dict[str, Any]]:
        if not message_ids:
            return []
        if self._can_use_http():
            try:
                return self._gmail_http_batch_get_metadata(message_ids)
            except Exception as exc:
                if _is_gmail_quota_error(str(exc)):
                    raise
                logger.warning("gmail batch get failed (%s); using parallel http gets", exc)
                workers = min(self._api_workers(), len(message_ids))
                if workers > 1 and len(message_ids) > 1:
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        return list(executor.map(self._fetch_message_metadata, message_ids))
                return [self._fetch_message_metadata(mid) for mid in message_ids]
        workers = min(self._api_workers(), len(message_ids))
        if workers > 1 and len(message_ids) > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                return list(executor.map(self._fetch_message_metadata, message_ids))
        return [self._fetch_message_metadata(mid) for mid in message_ids]

    def _read_local_message_fields(self, path: Path) -> dict[str, Any]:
        values: dict[str, Any] = {
            "account_email": "",
            "from_name": "",
            "from_email": "",
            "to_emails": [],
            "cc_emails": [],
            "bcc_emails": [],
            "reply_to_emails": [],
            "gmail_message_id": "",
            "sent_at": "",
        }
        current_list_key: str | None = None
        list_keys = {"to_emails", "cc_emails", "bcc_emails", "reply_to_emails"}
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                if handle.readline().strip() != "---":
                    return values
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
                    if key not in values:
                        continue
                    if key in list_keys:
                        if raw_value.startswith("["):
                            values[key] = _parse_inline_list(raw_value)
                        elif not raw_value:
                            values[key] = []
                            current_list_key = key
                    else:
                        values[key] = _strip_yaml_scalar(raw_value)
        except FileNotFoundError:
            return values
        return values

    @staticmethod
    def _payload_from_frontmatter(frontmatter: dict[str, Any]) -> dict[str, Any]:
        return {
            "account_email": str(frontmatter.get("account_email") or "").strip().lower(),
            "from_name": str(frontmatter.get("from_name") or "").strip(),
            "from_email": str(frontmatter.get("from_email") or "").strip().lower(),
            "to_emails": _string_list(frontmatter.get("to_emails")),
            "cc_emails": _string_list(frontmatter.get("cc_emails")),
            "bcc_emails": _string_list(frontmatter.get("bcc_emails")),
            "reply_to_emails": _string_list(frontmatter.get("reply_to_emails")),
            "gmail_message_id": str(frontmatter.get("gmail_message_id") or "").strip(),
            "sent_at": str(frontmatter.get("sent_at") or frontmatter.get("created") or "").strip(),
        }

    @staticmethod
    def _pairs_from_local_payload(payload: dict[str, Any]) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        from_email = str(payload.get("from_email") or "").strip().lower()
        from_name = str(payload.get("from_name") or "").strip()
        if from_email:
            pairs.append((from_name, from_email))
        for key in ("to_emails", "cc_emails", "bcc_emails", "reply_to_emails"):
            for email in payload.get(key, []) or []:
                normalized = str(email).strip().lower()
                if normalized:
                    pairs.append(("", normalized))
        return pairs

    @staticmethod
    def _ingest_pairs(
        counts: dict[str, dict[str, Any]],
        pairs: list[tuple[str, str]],
        own: set[str],
    ) -> None:
        per_message: dict[str, str] = {}
        for name, email in pairs:
            if not email:
                continue
            if email not in per_message:
                per_message[email] = name
            elif name and not per_message[email]:
                per_message[email] = name
        for email, name in per_message.items():
            if email in own:
                continue
            if not _should_keep_correspondent(name, email):
                continue
            row = counts[email]
            if name and not row["name"]:
                row["name"] = name
            row["email"] = email
            row["count"] += 1

    def _email_rows_from_scan_cache(self, scan_cache) -> list[dict[str, Any]]:
        by_type, _rel_by_uid, uid_by_path, _uid_by_stem, frontmatter_by_uid = scan_cache.slice_lookup_tables()
        rows: list[dict[str, Any]] = []
        for rel in by_type.get("email_message") or []:
            uid = uid_by_path.get(rel, "")
            fm = dict(frontmatter_by_uid.get(uid) or {})
            if not fm:
                continue
            rows.append({"rel_path": rel, "frontmatter": fm})
        return rows

    def _email_message_rows_from_cache(self, vault_path: str) -> list[dict[str, Any]]:
        from archive_cli.vault_cache import VaultScanCache

        vault = Path(vault_path)
        types = ["email_message"]
        scan_cache = None
        try:
            scan_cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
        except Exception as exc:
            logger.warning("vault cache build failed for correspondents: %s", exc)
        cache_path = VaultScanCache.cache_path_for_vault(vault)
        if cache_path.is_file():
            try:
                import archive_crate

                return list(archive_crate.frontmatter_dicts_from_cache(str(cache_path), types=types))
            except Exception:
                pass
        if scan_cache is None:
            return []
        return self._email_rows_from_scan_cache(scan_cache)

    def _iter_yaml_email_payloads(self, vault_path: str):
        email_root = Path(vault_path) / "Email"
        if not email_root.exists():
            return
        for path in email_root.rglob("*.md"):
            yield self._read_local_message_fields(path)

    def _vault_correspondent_state(
        self,
        vault_path: str,
        own: set[str],
        *,
        account_emails: set[str] | None = None,
        log=None,
        progress_every: int | None = None,
    ) -> tuple[dict[str, dict[str, Any]], int, set[str], str]:
        counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "", "email": "", "count": 0})
        scanned = 0
        message_ids: set[str] = set()
        max_sent_at = ""
        normalized_account_filters = {value.strip().lower() for value in (account_emails or set()) if value.strip()}
        rows = self._email_message_rows_from_cache(vault_path)
        if rows:
            payloads: list[dict[str, Any]] = []
            for row in rows:
                frontmatter = dict(row.get("frontmatter") or {})
                card_type = str(frontmatter.get("type") or "").strip()
                if card_type and card_type != "email_message":
                    continue
                payloads.append(self._payload_from_frontmatter(frontmatter))
            iterator = payloads
            source = "cache"
        else:
            iterator = self._iter_yaml_email_payloads(vault_path)
            source = "yaml"
        if log:
            log(f"vault-local scan start source={source}")
        for payload in iterator:
            payload_account = str(payload.get("account_email") or "").strip().lower()
            if normalized_account_filters and payload_account not in normalized_account_filters:
                continue
            message_id = str(payload.get("gmail_message_id") or "").strip()
            if message_id:
                message_ids.add(message_id)
            sent_at = str(payload.get("sent_at") or "").strip()
            if sent_at and sent_at > max_sent_at:
                max_sent_at = sent_at
            self._ingest_pairs(counts, self._pairs_from_local_payload(payload), own)
            scanned += 1
            if log and progress_every and scanned % max(1, int(progress_every)) == 0:
                log(
                    f"vault-local scan progress: scanned={scanned} unique_correspondents={len(counts)} "
                    f"account_filters={sorted(normalized_account_filters) if normalized_account_filters else ['all']}"
                )
        if log:
            log(
                f"vault-local scan done source={source} scanned={scanned} unique={len(counts)} "
                f"coverage={max_sent_at or '-'} known_ids={len(message_ids)}"
            )
        return counts, scanned, message_ids, max_sent_at

    def _fetch_from_local_messages(
        self,
        vault_path: str,
        own: set[str],
        *,
        account_emails: set[str] | None = None,
        log=None,
        progress_every: int | None = None,
    ) -> list[dict[str, Any]]:
        counts, scanned, _message_ids, _max_sent_at = self._vault_correspondent_state(
            vault_path,
            own,
            account_emails=account_emails,
            log=log,
            progress_every=progress_every,
        )
        items = sorted(counts.values(), key=lambda x: (-x["count"], x["email"]))
        for item in items:
            item["scanned_messages"] = scanned
            item["next_page_token"] = None
        return items

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        account_email: str = "",
        max_messages: int | None = None,
        query: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        self._ensure_token_manager(account_email)
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        persist_cursor = not bool(kwargs.get("dry_run"))
        cursor_key = self.get_cursor_key(account_email=account_email, **kwargs)
        started = time.perf_counter()
        list_query, list_mode = resolve_correspondents_list_query(cursor, query)
        watermark = str(cursor.get("last_sync") or "").strip()
        resuming = bool(cursor.get("page_token"))
        explicit_query = bool(str(query or "").strip())

        def _log(message: str) -> None:
            logger.info("%s", message)
            if verbose:
                print(f"{self.source_id}: {message}", flush=True)

        def _counts_payload() -> dict[str, dict[str, Any]]:
            return {
                email: {
                    "name": str(row.get("name") or ""),
                    "email": str(row.get("email") or email),
                    "count": int(row.get("count") or 0),
                }
                for email, row in counts.items()
            }

        def _checkpoint(next_token: str | None, scanned_count: int) -> None:
            cursor["page_token"] = next_token
            cursor["scanned_messages"] = scanned_count
            if next_token:
                cursor["correspondent_counts"] = _counts_payload()
                if list_query:
                    cursor["list_query"] = list_query
                else:
                    cursor.pop("list_query", None)
            else:
                cursor.pop("correspondent_counts", None)
                cursor.pop("list_query", None)
            if persist_cursor and vault_path:
                update_cursor(Path(vault_path), cursor_key, dict(cursor))

        requested_account_filters = {
            str(value).strip().lower()
            for value in [account_email, *(kwargs.get("account_emails") or [])]
            if str(value).strip()
        }
        own = set(requested_account_filters)
        own.update(_managed_account_emails())
        own_map = load_own_aliases(vault_path)
        own.update(a.lower() for a in own_map if "@" in a)

        counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "", "email": "", "count": 0})
        vault_message_ids: set[str] = set()
        vault_scanned = 0
        vault_max_sent_at = ""
        email_root_exists = (Path(vault_path) / "Email").exists()

        if email_root_exists and not resuming:
            local_counts, vault_scanned, vault_message_ids, vault_max_sent_at = self._vault_correspondent_state(
                vault_path,
                own,
                account_emails=requested_account_filters or None,
                log=_log,
                progress_every=progress_every,
            )
            counts.update(local_counts)

        if not account_email.strip() and email_root_exists:
            items = sorted(counts.values(), key=lambda x: (-x["count"], x["email"]))
            for item in items:
                item["scanned_messages"] = vault_scanned
                item["next_page_token"] = None
            cursor["page_token"] = None
            cursor["scanned_messages"] = vault_scanned
            _log(f"vault-local only scanned={vault_scanned} unique={len(counts)}")
            return items

        if not resuming and not explicit_query:
            vault_q = gmail_after_query(vault_max_sent_at) if vault_max_sent_at else None
            if vault_q and (list_query is None or vault_q > list_query):
                list_query = vault_q
                list_mode = "incremental"
                watermark = vault_max_sent_at or watermark

        if resuming:
            for email, row in (cursor.get("correspondent_counts") or {}).items():
                key = str(email or "").strip().lower()
                if not key:
                    continue
                counts[key]["name"] = str(row.get("name") or "")
                counts[key]["email"] = str(row.get("email") or key)
                counts[key]["count"] = int(row.get("count") or 0)

        _log(
            f"api fetch start account={account_email or '-'} mode={list_mode} "
            f"watermark={watermark or '-'} query={list_query or '(none)'} "
            f"vault_scanned={vault_scanned} scanned={cursor.get('scanned_messages', 0)} "
            f"page_token={'yes' if cursor.get('page_token') else 'no'} max_messages={max_messages or 'all'}"
        )

        page_token = cursor.get("page_token")
        scanned = int(cursor.get("scanned_messages", 0) or 0)
        batch_scanned = 0
        page_index = 0

        def _ingest_message(msg: dict[str, Any]) -> None:
            headers = msg.get("payload", {}).get("headers", [])
            self._ingest_pairs(counts, _extract_addresses_from_headers(headers), own)

        def _finalize(next_token: str | None) -> list[dict[str, Any]]:
            items = sorted(counts.values(), key=lambda x: (-x["count"], x["email"]))
            for item in items:
                item["scanned_messages"] = scanned
                item["next_page_token"] = next_token
            return items

        while True:
            params: dict[str, Any] = {"userId": "me", "maxResults": 500}
            if page_token:
                params["pageToken"] = page_token
            if list_query:
                params["q"] = list_query
            list_data = self._gmail_json(["gmail", "users", "messages", "list", "--params", json.dumps(params)])
            msgs = list_data.get("messages", [])
            if not msgs:
                page_token = None
                break
            page_index += 1
            listed_ids = [str(m.get("id") or "") for m in msgs if m.get("id")]
            if max_messages is not None:
                remaining = max(0, int(max_messages) - batch_scanned)
                listed_ids = listed_ids[:remaining]
                if not listed_ids:
                    next_token = list_data.get("nextPageToken")
                    _checkpoint(next_token, scanned)
                    return _finalize(next_token)
            skipped_vault = 0
            fetch_ids: list[str] = []
            for message_id in listed_ids:
                if message_id in vault_message_ids:
                    skipped_vault += 1
                    scanned += 1
                    batch_scanned += 1
                else:
                    fetch_ids.append(message_id)
            fetched = self._fetch_messages_metadata(fetch_ids)
            skipped_unreadable = 0
            for msg in fetched:
                if not msg.get("payload") and not msg.get("id"):
                    skipped_unreadable += 1
                else:
                    _ingest_message(msg)
                scanned += 1
                batch_scanned += 1
            next_token = list_data.get("nextPageToken")
            _checkpoint(next_token, scanned)
            skip_note = ""
            if skipped_unreadable:
                skip_note += f" skipped_unreadable={skipped_unreadable}"
            if skipped_vault:
                skip_note += f" skipped_vault={skipped_vault}"
            _log(
                f"api page={page_index} scanned={scanned} unique={len(counts)} "
                f"page_size={len(listed_ids)} fetched={len(fetch_ids)}{skip_note} "
                f"elapsed={time.perf_counter() - started:.1f}s"
            )
            if max_messages and batch_scanned >= max_messages:
                _log(f"api fetch cap reached scanned={scanned} unique={len(counts)}")
                return _finalize(next_token)

            page_token = next_token
            if not page_token:
                break

        _checkpoint(None, scanned)
        _log(
            f"api fetch done mode={list_mode} scanned={scanned} unique={len(counts)} "
            f"vault_scanned={vault_scanned} elapsed={time.perf_counter() - started:.1f}s"
        )
        return _finalize(None)

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        email = str(item.get("email", "")).strip().lower()
        name = str(item.get("name", "")).strip()
        first_name, last_name = _split_display_name(name)
        source_id = email or str(item.get("name", "")).strip() or "gmail-correspondent-unknown"
        card = PersonCard(
            uid=generate_uid("person", self.source_id, source_id),
            type="person",
            source=[self.source_id],
            source_id=source_id,
            created=today,
            updated=today,
            summary=name or email or "unknown",
            first_name=first_name,
            last_name=last_name,
            emails=[email] if email else [],
            tags=["email-correspondent", "gmail-correspondent"],
            emails_seen_count=int(item.get("count", 0) or 0),
        )
        provenance = deterministic_provenance(card, self.source_id)
        return card, provenance, ""


def load_own_aliases(vault_path: str) -> set[str]:
    path = os.path.join(vault_path, "_meta", "own-emails.json")
    if not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x).strip().lower() for x in data if isinstance(x, str)}
    except (OSError, json.JSONDecodeError):
        pass
    return set()
