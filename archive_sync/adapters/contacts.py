"""Contacts adapter — Apple/Google/VCF contacts to person cards."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger("ppa.contacts")

from archive_vault.schema import PersonCard
from archive_vault.uid import generate_uid

from .base import BaseAdapter, deterministic_provenance


def _vcf_unescape(value: str) -> str:
    return value.replace("\\n", " ").replace("\\,", ",").strip()


def _split_vcf_name(value: str) -> tuple[str, str]:
    parts = value.split(";")
    last_name = _vcf_unescape(parts[0]) if parts else ""
    first_name = _vcf_unescape(parts[1]) if len(parts) > 1 else ""
    return first_name, last_name


def _primary_org(value: str) -> str:
    return next((part.strip() for part in _vcf_unescape(value).split(";") if part.strip()), "")


def _normalize_partial_date(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{2}-\d{2}", raw):
        return raw
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%m-%d", "%Y-%m"):
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if fmt == "%m-%d":
            return parsed.strftime("%m-%d")
        if fmt == "%Y-%m":
            return parsed.strftime("%Y-%m")
        return parsed.date().isoformat()
    return ""


def _vcf_prop(upper: str, name: str) -> bool:
    return upper == name or upper.startswith(f"{name}:") or upper.startswith(f"{name};")


_CONTACTS_PERMISSION_MARKERS = (
    "not authorized",
    "not allowed",
    "permission",
    "(-1743)",
    "errAEPrivilegeError",
    "access not allowed",
    "osascript is not allowed",
)


class AppleContactsPermissionError(PermissionError):
    """Contacts.app refused the dump (TCC / Automation)."""


def _is_contacts_permission_error(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in _CONTACTS_PERMISSION_MARKERS)


def _contact_content_hash(row: dict[str, Any]) -> str:
    payload = {
        "name": row.get("name") or "",
        "first_name": row.get("first_name") or "",
        "last_name": row.get("last_name") or "",
        "emails": row.get("emails") or [],
        "phones": row.get("phones") or [],
        "company": row.get("company") or "",
        "title": row.get("title") or "",
        "birthday": row.get("birthday") or "",
        "description": row.get("description") or "",
        "aliases": row.get("aliases") or [],
        "websites": row.get("websites") or [],
        "linkedin": row.get("linkedin") or "",
        "twitter": row.get("twitter") or "",
        "github": row.get("github") or "",
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


# `vcard of every person` returns Apple Event -1741 on a real address book
# (reply too large). Count and a single vcard succeed. Batches of 50 work.
APPLE_CONTACTS_DUMP_BATCH = 50


def _osascript_error_text(result: subprocess.CompletedProcess[str]) -> str:
    return ((result.stderr or "") + "\n" + (result.stdout or "")).strip()


def _run_osascript(script: str, *, timeout: int) -> str:
    result = subprocess.run(
        ["osascript", "-e", script],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    err = _osascript_error_text(result)
    if result.returncode != 0:
        if _is_contacts_permission_error(err):
            raise AppleContactsPermissionError(f"Contacts permission denied: {err}")
        raise RuntimeError(f"Contacts.app dump failed: {err or result.returncode}")
    return (result.stdout or "").strip()


def _apple_contacts_count() -> int:
    script = (
        "with timeout of 60 seconds\n"
        '  tell application "Contacts"\n'
        "    count people\n"
        "  end tell\n"
        "end timeout\n"
    )
    raw = _run_osascript(script, timeout=70)
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Contacts.app count failed: {raw or 'empty'}") from exc


def _write_apple_vcard_range(start: int, end: int, dest: Path) -> None:
    posix = str(dest)
    script = (
        "with timeout of 180 seconds\n"
        '  tell application "Contacts"\n'
        f"    set cardList to vcard of people {start} thru {end}\n"
        "  end tell\n"
        "end timeout\n"
        'set text item delimiters to ""\n'
        "set cardText to cardList as text\n"
        f'set outFile to POSIX file "{posix}"\n'
        "set fileRef to open for access outFile with write permission\n"
        "set eof of fileRef to 0\n"
        "write cardText to fileRef\n"
        "close access fileRef\n"
    )
    _run_osascript(script, timeout=200)
    if not dest.is_file() or dest.stat().st_size == 0:
        raise RuntimeError(f"Contacts.app dump wrote no cards for people {start} thru {end}")


def _dump_range_resilient(start: int, end: int, tmp: Path, parts: list[bytes]) -> None:
    chunk = tmp / f"apple-batch-{start}-{end}.vcf"
    try:
        _write_apple_vcard_range(start, end, chunk)
        parts.append(chunk.read_bytes())
        return
    except AppleContactsPermissionError:
        raise
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        if start == end:
            logger.warning("apple contacts skip person index=%s: %s", start, exc)
            return
        message = str(exc).lower()
        if "-1741" in message or "-1712" in message or "timed out" in message:
            mid = (start + end) // 2
            _dump_range_resilient(start, mid, tmp, parts)
            _dump_range_resilient(mid + 1, end, tmp, parts)
            return
        raise
    finally:
        chunk.unlink(missing_ok=True)


def dump_apple_contacts_vcf(dest: Path) -> None:
    """Write unified Contacts.app vCards to dest. Raises on TCC denial."""

    dest.parent.mkdir(parents=True, exist_ok=True)
    count = _apple_contacts_count()
    logger.info(
        "apple contacts dump start people=%s batch=%s dest=%s",
        count,
        APPLE_CONTACTS_DUMP_BATCH,
        dest,
    )
    if count <= 0:
        dest.write_bytes(b"")
        return
    parts: list[bytes] = []
    with tempfile.TemporaryDirectory(prefix="ppa-apple-contacts-dump-") as tmp:
        tmp_path = Path(tmp)
        start = 1
        while start <= count:
            end = min(start + APPLE_CONTACTS_DUMP_BATCH - 1, count)
            logger.info("apple contacts dump batch %s-%s/%s", start, end, count)
            _dump_range_resilient(start, end, tmp_path, parts)
            start = end + 1
    dest.write_bytes(b"".join(parts))
    if not dest.is_file() or dest.stat().st_size == 0:
        raise RuntimeError("Contacts.app dump wrote no cards")


class ContactsAdapter(BaseAdapter):
    source_id = "contacts"

    def __init__(self) -> None:
        super().__init__()
        self._last_google_sync_token = ""
        self._person_etags: dict[str, str] = {}
        self._apple_contact_hashes: dict[str, str] = {}
        self._dump_apple_contacts_vcf = dump_apple_contacts_vcf

    def get_cursor_key(self, **kwargs) -> str:
        raw_sources = kwargs.get("sources") or []
        normalized = {
            "contacts.apple"
            if str(source).strip().lower() in {"apple", "vcf"}
            else f"contacts.{str(source).strip().lower()}"
            for source in raw_sources
            if str(source).strip()
        }
        if len(normalized) == 1:
            return next(iter(normalized))
        return self.source_id

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        sources: list[str] | None = None,
        vcf_paths: list[str] | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        selected = {item.strip().lower() for item in (sources or ["apple", "vcf", "google"]) if item.strip()}
        items: list[dict[str, Any]] = []
        max_items = kwargs.get("max_items")
        if "google" in selected:
            items.extend(self._fetch_google(cursor=cursor, max_items=max_items))
        if "apple" in selected:
            items.extend(self._fetch_apple(cursor=cursor, vcf_paths=vcf_paths, max_items=max_items))
        elif "vcf" in selected:
            items.extend(self._fetch_vcf_files(vcf_paths=vcf_paths))
        return items

    def _configured_vcf_paths(self) -> list[str]:
        raw = os.environ.get("HFA_CONTACTS_VCF_PATHS", "").strip()
        if not raw:
            return []
        return [path.strip() for path in raw.split(os.pathsep) if path.strip()]

    def _selected_google_accounts(self, available_accounts: dict[str, Any]) -> list[str]:
        explicit_names = [
            value.strip() for value in os.environ.get("HFA_GOOGLE_CONTACTS_ACCOUNTS", "").split(",") if value.strip()
        ]
        if explicit_names:
            return [name for name in explicit_names if name in available_accounts]

        account_email = os.environ.get("GOOGLE_ACCOUNT", "").strip().lower()
        if account_email:
            try:
                from archive_auth import account_name_from_email

                account_name = account_name_from_email(account_email)
            except Exception:
                account_name = None
            if account_name and account_name in available_accounts:
                return [account_name]
        return list(available_accounts)

    def _fetch_google_page_via_proxy(
        self,
        account: str,
        *,
        fields: str,
        page_token: str | None,
        sync_token: str | None = None,
        request_sync_token: bool = False,
    ) -> dict[str, Any]:
        from arnoldlib.auth import build_service_proxied
        from arnoldlib.gate import _auto_issue_ticket

        ticket = _auto_issue_ticket(
            f"google.refresh_token.{account}",
            "google.contacts.list",
            account,
            "archive-sync",
        )
        service = build_service_proxied(
            account,
            "people",
            "v1",
            ticket=str(ticket["ticket"]),
            action="google.contacts.list",
            requested_by="archive-sync",
        )
        kwargs: dict[str, Any] = {
            "resourceName": "people/me",
            "personFields": fields,
            "pageSize": 200,
            "pageToken": page_token,
            "requestSyncToken": bool(request_sync_token),
        }
        if sync_token:
            kwargs["syncToken"] = sync_token
        return service.people().connections().list(**kwargs).execute()

    def _fetch_google_page_via_direct(
        self,
        account: str,
        *,
        fields: str,
        page_token: str | None,
        sync_token: str | None = None,
        request_sync_token: bool = False,
    ) -> dict[str, Any]:
        from archive_auth import build_google_cli_token_manager

        manager = build_google_cli_token_manager(account_name=account, services=["contacts"])
        if manager is None:
            raise RuntimeError(f"No direct Google OAuth token manager available for account {account}")
        params = {
            "personFields": fields,
            "pageSize": "200",
            "resourceName": "people/me",
            "requestSyncToken": "true" if request_sync_token else "false",
        }
        if page_token:
            params["pageToken"] = page_token
        if sync_token:
            params["syncToken"] = sync_token
        url = "https://people.googleapis.com/v1/people/me/connections?" + urllib.parse.urlencode(params)

        def _request(*, force_refresh: bool = False) -> dict[str, Any]:
            token = manager.get_access_token(force_refresh=force_refresh)
            request = urllib.request.Request(url)
            request.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))

        try:
            return _request()
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                return _request(force_refresh=True)
            raise

    def _should_fallback_to_direct_google(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "passkey_gate_internal_token",
                "connection refused",
                "auto-issue failed",
                "action request failed",
            )
        )

    def _google_sync_expired(self, exc: Exception) -> bool:
        if isinstance(exc, urllib.error.HTTPError) and exc.code == 410:
            return True
        message = str(exc).lower()
        return "410" in message and ("sync" in message or "expired" in message or "gone" in message)

    def _fetch_google_page(
        self,
        account: str,
        *,
        fields: str,
        page_token: str | None,
        sync_token: str | None,
        request_sync_token: bool,
        has_arnoldlib: bool,
    ) -> dict[str, Any]:
        kwargs = {
            "fields": fields,
            "page_token": page_token,
            "sync_token": sync_token,
            "request_sync_token": request_sync_token,
        }
        if has_arnoldlib:
            try:
                return self._fetch_google_page_via_proxy(account, **kwargs)
            except Exception as exc:
                if not self._should_fallback_to_direct_google(exc):
                    raise
                return self._fetch_google_page_via_direct(account, **kwargs)
        return self._fetch_google_page_via_direct(account, **kwargs)

    def _fetch_google(self, cursor: dict[str, Any] | None = None, max_items: int | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        remaining = None if max_items in (None, "") else max(0, int(max_items))
        state = cursor if isinstance(cursor, dict) else {}
        stored_etags = {
            str(name).strip(): str(etag).strip()
            for name, etag in dict(state.get("person_etags") or {}).items()
            if str(name).strip() and str(etag).strip()
        }
        self._person_etags = dict(stored_etags)
        self._last_google_sync_token = str(state.get("sync_token") or "").strip()
        try:
            from archive_auth import ACCOUNTS

            _has_arnoldlib = True
            try:
                from arnoldlib.bootstrap import bootstrap

                bootstrap()
            except ImportError:
                _has_arnoldlib = False

            fields = "names,emailAddresses,phoneNumbers,organizations,birthdays,urls,biographies,nicknames"
            for account in self._selected_google_accounts(ACCOUNTS):
                if remaining is not None and remaining <= 0:
                    break
                sync_token = self._last_google_sync_token or None
                page_token = None
                while True:
                    try:
                        response = self._fetch_google_page(
                            account,
                            fields=fields,
                            page_token=page_token,
                            sync_token=sync_token if page_token is None else None,
                            request_sync_token=True,
                            has_arnoldlib=_has_arnoldlib,
                        )
                    except Exception as exc:
                        if sync_token and self._google_sync_expired(exc):
                            self._last_google_sync_token = ""
                            sync_token = None
                            page_token = None
                            continue
                        break
                    for person in response.get("connections", []):
                        resource_name = str(person.get("resourceName") or "").strip()
                        etag = str(person.get("etag") or "").strip()
                        if resource_name and etag and stored_etags.get(resource_name) == etag:
                            continue
                        if resource_name and etag:
                            self._person_etags[resource_name] = etag
                        rows.append(self._google_fields(person))
                        if remaining is not None:
                            remaining -= 1
                            if remaining <= 0:
                                next_sync = str(response.get("nextSyncToken") or "").strip()
                                if next_sync:
                                    self._last_google_sync_token = next_sync
                                return rows
                    page_token = response.get("nextPageToken")
                    next_sync = str(response.get("nextSyncToken") or "").strip()
                    if next_sync:
                        self._last_google_sync_token = next_sync
                    if not page_token:
                        break
        except Exception:
            return rows
        return rows

    def cursor_checkpoint(
        self,
        item: dict[str, Any],
        *,
        card=None,
        index: int = -1,
        processed_successfully: int = 0,
        result=None,
        **kwargs,
    ) -> dict[str, Any] | None:
        uid = str(item.get("apple_uid") or item.get("name") or item.get("source_path") or "").strip()
        digest = str(item.get("_content_hash") or "").strip()
        if uid and digest:
            self._apple_contact_hashes[uid] = digest
        return super().cursor_checkpoint(
            item,
            card=card,
            index=index,
            processed_successfully=processed_successfully,
            result=result,
            **kwargs,
        )

    def finalize_cursor(self, cursor: dict[str, Any], **kwargs) -> dict[str, Any] | None:
        patch = {"last_sync": datetime.now().isoformat()}
        if self._last_google_sync_token:
            patch["sync_token"] = self._last_google_sync_token
        if self._person_etags:
            patch["person_etags"] = dict(self._person_etags)
        if self._apple_contact_hashes:
            patch["contact_hashes"] = dict(self._apple_contact_hashes)
        return patch

    def _fetch_apple(
        self,
        *,
        cursor: dict[str, Any] | None = None,
        vcf_paths: list[str] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        configured = [path for path in (vcf_paths or self._configured_vcf_paths()) if str(path).strip()]
        if configured:
            rows = self._fetch_vcf_files(vcf_paths=configured)
        else:
            rows = self._fetch_apple_live()
        state = cursor if isinstance(cursor, dict) else {}
        stored = {
            str(name).strip(): str(digest).strip()
            for name, digest in dict(state.get("contact_hashes") or {}).items()
            if str(name).strip() and str(digest).strip()
        }
        self._apple_contact_hashes = dict(stored)
        remaining = None if max_items in (None, "") else max(0, int(max_items))
        out: list[dict[str, Any]] = []
        for row in rows:
            uid = str(row.get("apple_uid") or row.get("name") or row.get("source_path") or "").strip()
            digest = _contact_content_hash(row)
            if uid and stored.get(uid) == digest:
                self._apple_contact_hashes[uid] = digest
                continue
            if uid:
                row = dict(row)
                row["_content_hash"] = digest
            out.append(row)
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    break
        return out

    def _fetch_apple_live(self) -> list[dict[str, Any]]:
        with tempfile.TemporaryDirectory(prefix="ppa-apple-contacts-") as tmp:
            dest = Path(tmp) / "contacts.vcf"
            self._dump_apple_contacts_vcf(dest)
            if not dest.is_file():
                return []
            return self._parse_vcf(str(dest))

    def _fetch_vcf_files(
        self,
        *,
        vcf_paths: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        configured_paths = [path for path in (vcf_paths or self._configured_vcf_paths()) if str(path).strip()]
        if not configured_paths:
            return []
        candidates = configured_paths
        rows: list[dict[str, Any]] = []
        for path in sorted(set(candidates)):
            if os.path.isfile(path):
                rows.extend(self._parse_vcf(path))
        return rows

    def _parse_vcf(self, path: str) -> list[dict[str, Any]]:
        try:
            with open(path, encoding="utf-8", errors="ignore") as handle:
                raw = handle.read()
        except OSError:
            return []
        raw = re.sub(r"\n[ \t]", "", raw)
        blocks = re.findall(r"BEGIN:VCARD(.*?)END:VCARD", raw, flags=re.DOTALL | re.IGNORECASE)
        rows: list[dict[str, Any]] = []
        for block in blocks:
            name = ""
            first_name = ""
            last_name = ""
            emails: list[str] = []
            phones: list[str] = []
            aliases: list[str] = []
            websites: list[str] = []
            company = ""
            title = ""
            birthday = ""
            description = ""
            apple_uid = ""
            linkedin = ""
            twitter = ""
            github = ""
            show_as_company = False
            for line in block.splitlines():
                if ":" not in line:
                    continue
                upper = line.upper()
                value = line.split(":", 1)[1]
                if _vcf_prop(upper, "FN"):
                    name = _vcf_unescape(value)
                elif _vcf_prop(upper, "NOTE"):
                    description = _vcf_unescape(value)
                elif _vcf_prop(upper, "NICKNAME"):
                    nick = _vcf_unescape(value)
                    if nick:
                        aliases.append(nick)
                elif _vcf_prop(upper, "N"):
                    first_name, last_name = _split_vcf_name(value)
                elif _vcf_prop(upper, "EMAIL"):
                    match = re.search(r"([^:\s;]+@[^:\s;]+)", line)
                    if match:
                        emails.append(match.group(1).lower())
                elif _vcf_prop(upper, "TEL"):
                    phones.append(_vcf_unescape(value))
                elif _vcf_prop(upper, "ORG"):
                    company = _primary_org(value)
                elif _vcf_prop(upper, "TITLE"):
                    title = _vcf_unescape(value)
                elif _vcf_prop(upper, "BDAY"):
                    birthday = _normalize_partial_date(_vcf_unescape(value))
                elif _vcf_prop(upper, "UID") or _vcf_prop(upper, "X-ABUID"):
                    uid_value = _vcf_unescape(value).strip()
                    if uid_value and not apple_uid:
                        apple_uid = uid_value
                elif _vcf_prop(upper, "URL"):
                    url = _vcf_unescape(value).strip()
                    if url:
                        websites.append(url)
                        lower = url.lower()
                        if "linkedin" in lower and not linkedin:
                            linkedin = url
                        elif any(domain in lower for domain in ("twitter.com", "x.com")) and not twitter:
                            twitter = url
                        elif "github.com" in lower and not github:
                            github = url
                elif _vcf_prop(upper, "X-ABSHOWAS"):
                    show_as_company = value.strip().upper() == "COMPANY"
                elif "X-SOCIALPROFILE" in upper:
                    lower = line.lower()
                    social = line.split(":", 1)[-1].strip()
                    if "linkedin" in lower:
                        linkedin = social
                    elif "twitter" in lower or "x.com" in lower:
                        twitter = social
                    elif "github" in lower:
                        github = social
            if show_as_company:
                continue
            if name or emails or phones:
                rows.append(
                    {
                        "source": "contacts.apple",
                        "name": name or (emails[0] if emails else "unknown"),
                        "first_name": first_name,
                        "last_name": last_name,
                        "emails": list(dict.fromkeys(emails)),
                        "phones": list(dict.fromkeys(phones)),
                        "aliases": list(dict.fromkeys(aliases)),
                        "websites": list(dict.fromkeys(websites)),
                        "company": company,
                        "title": title,
                        "birthday": birthday,
                        "description": description,
                        "linkedin": linkedin,
                        "twitter": twitter,
                        "github": github,
                        "apple_uid": apple_uid,
                        "source_path": path,
                    }
                )
        return rows

    def _google_fields(self, person: dict[str, Any]) -> dict[str, Any]:
        names = person.get("names", [])
        emails = [entry.get("value", "").lower() for entry in person.get("emailAddresses", []) if entry.get("value")]
        phones = [entry.get("value", "") for entry in person.get("phoneNumbers", []) if entry.get("value")]
        organizations = person.get("organizations", [])
        birthdays = person.get("birthdays", [])
        urls = [entry.get("value", "") for entry in person.get("urls", []) if entry.get("value")]
        biographies = [entry.get("value", "") for entry in person.get("biographies", []) if entry.get("value")]
        nicknames = [entry.get("value", "") for entry in person.get("nicknames", []) if entry.get("value")]
        birthday = ""
        for entry in birthdays:
            date_value = entry.get("date", {})
            year = date_value.get("year")
            month = date_value.get("month")
            day = date_value.get("day")
            if year and month and day:
                birthday = f"{year:04d}-{month:02d}-{day:02d}"
                break
        linkedin = next((url for url in urls if "linkedin.com" in url.lower()), "")
        twitter = next((url for url in urls if any(domain in url.lower() for domain in ("twitter.com", "x.com"))), "")
        github = next((url for url in urls if "github.com" in url.lower()), "")
        companies = [entry.get("name", "") for entry in organizations if entry.get("name")]
        titles = [entry.get("title", "") for entry in organizations if entry.get("title")]
        return {
            "source": "contacts.google",
            "name": names[0].get("displayName", "") if names else "",
            "first_name": names[0].get("givenName", "") if names else "",
            "last_name": names[0].get("familyName", "") if names else "",
            "aliases": list(dict.fromkeys(nicknames)),
            "emails": list(dict.fromkeys(emails)),
            "phones": list(dict.fromkeys(phones)),
            "company": organizations[0].get("name", "") if organizations else "",
            "companies": list(dict.fromkeys(companies)),
            "title": organizations[0].get("title", "") if organizations else "",
            "titles": list(dict.fromkeys(titles)),
            "birthday": birthday,
            "linkedin": linkedin,
            "twitter": twitter,
            "github": github,
            "description": biographies[0].strip() if biographies else "",
            "resource_name": person.get("resourceName", ""),
        }

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        source = str(item.get("source", "contacts.apple"))
        emails = list(item.get("emails", []))
        source_id = (
            str(item.get("apple_uid") or item.get("resource_name") or "").strip()
            or (emails[0] if emails else "")
            or str(item.get("name", "")).strip()
            or f"{source}:{item.get('source_path', 'manual')}"
        )
        card = PersonCard(
            uid=generate_uid("person", source, source_id),
            type="person",
            source=[source],
            source_id=source_id,
            created=today,
            updated=today,
            summary=str(item.get("name", "")).strip() or (emails[0] if emails else "unknown"),
            first_name=str(item.get("first_name", "")).strip(),
            last_name=str(item.get("last_name", "")).strip(),
            aliases=list(item.get("aliases", [])),
            emails=emails,
            phones=list(item.get("phones", [])),
            birthday=str(item.get("birthday", "")).strip(),
            company=str(item.get("company", "")).strip(),
            companies=list(item.get("companies", []))
            or ([str(item.get("company", "")).strip()] if str(item.get("company", "")).strip() else []),
            title=str(item.get("title", "")).strip(),
            titles=list(item.get("titles", []))
            or ([str(item.get("title", "")).strip()] if str(item.get("title", "")).strip() else []),
            linkedin=str(item.get("linkedin", "")).strip(),
            twitter=str(item.get("twitter", "")).strip(),
            github=str(item.get("github", "")).strip(),
            websites=list(item.get("websites", [])),
            description=str(item.get("description", "")).strip(),
        )
        provenance = deterministic_provenance(card, source)
        return card, provenance, ""
