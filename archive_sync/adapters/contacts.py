"""Contacts adapter — Apple/Google/VCF contacts to person cards."""

from __future__ import annotations

import glob
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import date, datetime
from typing import Any

from .base import BaseAdapter, deterministic_provenance
from hfa.schema import PersonCard
from hfa.uid import generate_uid


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


class ContactsAdapter(BaseAdapter):
    source_id = "contacts"

    def get_cursor_key(self, **kwargs) -> str:
        raw_sources = kwargs.get("sources") or []
        normalized = {
            "contacts.apple" if str(source).strip().lower() in {"apple", "vcf"} else f"contacts.{str(source).strip().lower()}"
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
        if "google" in selected:
            items.extend(self._fetch_google())
        if {"apple", "vcf"} & selected:
            if vcf_paths:
                items.extend(self._fetch_vcf_files(vcf_paths=vcf_paths))
            else:
                items.extend(self._fetch_vcf_files())
        return items

    def _configured_vcf_paths(self) -> list[str]:
        raw = os.environ.get("HFA_CONTACTS_VCF_PATHS", "").strip()
        if not raw:
            return []
        return [path.strip() for path in raw.split(os.pathsep) if path.strip()]

    def _selected_google_accounts(self, available_accounts: dict[str, Any]) -> list[str]:
        explicit_names = [
            value.strip()
            for value in os.environ.get("HFA_GOOGLE_CONTACTS_ACCOUNTS", "").split(",")
            if value.strip()
        ]
        if explicit_names:
            return [name for name in explicit_names if name in available_accounts]

        account_email = os.environ.get("GOOGLE_ACCOUNT", "").strip().lower()
        if account_email:
            try:
                from ppa_google_auth import account_name_from_email

                account_name = account_name_from_email(account_email)
            except Exception:
                account_name = None
            if account_name and account_name in available_accounts:
                return [account_name]
        return list(available_accounts)

    def _fetch_google_page_via_proxy(self, account: str, *, fields: str, page_token: str | None) -> dict[str, Any]:
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
        return (
            service.people()
            .connections()
            .list(resourceName="people/me", personFields=fields, pageSize=200, pageToken=page_token)
            .execute()
        )

    def _fetch_google_page_via_direct(self, account: str, *, fields: str, page_token: str | None) -> dict[str, Any]:
        from ppa_google_auth import build_google_cli_token_manager

        manager = build_google_cli_token_manager(account_name=account, services=["contacts"])
        if manager is None:
            raise RuntimeError(f"No direct Google OAuth token manager available for account {account}")
        params = {
            "personFields": fields,
            "pageSize": "200",
            "resourceName": "people/me",
        }
        if page_token:
            params["pageToken"] = page_token
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

    def _fetch_google(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        try:
            from ppa_google_auth import ACCOUNTS

            _has_arnoldlib = True
            try:
                from arnoldlib.bootstrap import bootstrap
                bootstrap()
            except ImportError:
                _has_arnoldlib = False

            fields = "names,emailAddresses,phoneNumbers,organizations,birthdays,urls,biographies,nicknames"
            for account in self._selected_google_accounts(ACCOUNTS):
                page_token = None
                while True:
                    try:
                        if _has_arnoldlib:
                            try:
                                response = self._fetch_google_page_via_proxy(account, fields=fields, page_token=page_token)
                            except Exception as exc:
                                if not self._should_fallback_to_direct_google(exc):
                                    raise
                                response = self._fetch_google_page_via_direct(account, fields=fields, page_token=page_token)
                        else:
                            response = self._fetch_google_page_via_direct(account, fields=fields, page_token=page_token)
                    except Exception:
                        break
                    for person in response.get("connections", []):
                        rows.append(self._google_fields(person))
                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break
        except Exception:
            return []
        return rows

    def _fetch_vcf_files(self, *, vcf_paths: list[str] | None = None) -> list[dict[str, Any]]:
        configured_paths = [path for path in (vcf_paths or self._configured_vcf_paths()) if str(path).strip()]
        if configured_paths:
            candidates = configured_paths
        else:
            home = os.path.expanduser("~")
            candidates = [
                os.path.join(home, "Downloads", "apple-contacts-export.vcf"),
                os.path.join(home, "Downloads", "vcard-jenny-souza.vcf"),
                os.path.join(home, "Documents", "Health & Personal", "01_Documents", "06_Wedding", "Steven B_ Goldfarb.vcf"),
            ]
            candidates.extend(glob.glob(os.path.join(home, "Downloads", "*contacts*.vcf")))
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
            company = ""
            title = ""
            birthday = ""
            linkedin = ""
            twitter = ""
            github = ""
            show_as_company = False
            for line in block.splitlines():
                upper = line.upper()
                if upper.startswith("FN"):
                    _, value = line.split(":", 1)
                    name = _vcf_unescape(value)
                elif upper.startswith("N"):
                    _, value = line.split(":", 1)
                    first_name, last_name = _split_vcf_name(value)
                elif upper.startswith("EMAIL"):
                    match = re.search(r"([^:\s;]+@[^:\s;]+)", line)
                    if match:
                        emails.append(match.group(1).lower())
                elif upper.startswith("TEL"):
                    _, value = line.split(":", 1)
                    phones.append(_vcf_unescape(value))
                elif upper.startswith("ORG"):
                    _, value = line.split(":", 1)
                    company = _primary_org(value)
                elif upper.startswith("TITLE"):
                    _, value = line.split(":", 1)
                    title = _vcf_unescape(value)
                elif upper.startswith("BDAY"):
                    _, value = line.split(":", 1)
                    birthday = _normalize_partial_date(_vcf_unescape(value))
                elif upper.startswith("X-ABSHOWAS"):
                    _, value = line.split(":", 1)
                    show_as_company = value.strip().upper() == "COMPANY"
                elif "X-SOCIALPROFILE" in upper:
                    lower = line.lower()
                    value = line.split(":", 1)[-1].strip()
                    if "linkedin" in lower:
                        linkedin = value
                    elif "twitter" in lower or "x.com" in lower:
                        twitter = value
                    elif "github" in lower:
                        github = value
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
                        "company": company,
                        "title": title,
                        "birthday": birthday,
                        "linkedin": linkedin,
                        "twitter": twitter,
                        "github": github,
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
            str(item.get("resource_name", "")).strip()
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
            description=str(item.get("description", "")).strip(),
        )
        provenance = deterministic_provenance(card, source)
        return card, provenance, ""
