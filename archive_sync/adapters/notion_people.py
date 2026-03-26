"""Notion people CSV adapter."""

from __future__ import annotations

import csv
import os
import re
from datetime import date, datetime
from typing import Any

from hfa.schema import PersonCard
from hfa.uid import generate_uid

from .base import BaseAdapter, deterministic_provenance


def _clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _slug_tag(value: str) -> str:
    cleaned = _clean(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    return cleaned.strip("-")


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = _clean(value)
        if not cleaned:
            continue
        marker = cleaned.lower()
        if marker in seen:
            continue
        seen.add(marker)
        out.append(cleaned)
    return out


def _lower_row(row: dict[str, Any]) -> dict[str, str]:
    return {str(key).lower(): str(value or "") for key, value in row.items() if key is not None}


def _extract_urls(value: str) -> list[str]:
    return re.findall(r"https?://[^\s,)]+", value or "")


def _strip_notion_link_label(value: str) -> str:
    cleaned = _clean(value)
    if not cleaned:
        return ""
    return re.sub(r"\s*\(https?://[^)]+\)\s*$", "", cleaned)


def _split_full_name(name: str) -> tuple[str, str]:
    cleaned = _clean(name)
    if not cleaned:
        return "", ""
    parts = cleaned.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _collect_emails(lower: dict[str, str], fields: list[str]) -> list[str]:
    emails: list[str] = []
    for field in fields:
        value = _clean(lower.get(field, ""))
        if not value:
            continue
        for candidate in re.split(r"[,;]", value.replace("mailto:", "")):
            email = candidate.strip().lower()
            if "@" in email:
                emails.append(email)
    return _dedupe(emails)


def _collect_phones(lower: dict[str, str], fields: list[str]) -> list[str]:
    values: list[str] = []
    for field in fields:
        value = _clean(lower.get(field, ""))
        if value:
            values.extend(part.strip() for part in re.split(r"[,;]", value) if part.strip())
    return _dedupe(values)


def _parse_birthday(value: str) -> str:
    cleaned = _clean(value)
    if not cleaned:
        return ""
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _company_from_staff(lower: dict[str, str], emails: list[str]) -> str:
    explicit = _clean(lower.get("company", ""))
    if explicit:
        return explicit
    if any(email.endswith("@endaoment.org") for email in emails):
        return "Endaoment"
    company_email = _clean(lower.get("company email", "")).replace("mailto:", "").lower()
    if "@" in company_email:
        return company_email.split("@", 1)[1]
    return ""


def _websites_from_fields(lower: dict[str, str], fields: list[str]) -> list[str]:
    websites: list[str] = []
    for field in fields:
        value = lower.get(field, "")
        if not value:
            continue
        if field in {"website", "website type"}:
            if value.startswith("http://") or value.startswith("https://"):
                websites.append(value)
        websites.extend(_extract_urls(value))
    return _dedupe(websites)


def _people_row_to_item(row: dict[str, Any]) -> dict[str, Any]:
    lower = _lower_row(row)
    name = _clean(lower.get("full name") or lower.get("name"))
    if not name:
        first_name = _clean(lower.get("first name"))
        last_name = _clean(lower.get("last name"))
        name = " ".join(part for part in [first_name, last_name] if part)
    else:
        first_name, last_name = _split_full_name(name)
    emails = _collect_emails(lower, ["email", "personal email", "company email", "protonmail"])
    phones = _collect_phones(lower, ["phone", "primary phone number", "brasil phone number"])
    company = _strip_notion_link_label(lower.get("company") or lower.get("organization") or lower.get("org") or "")
    title = _clean(lower.get("title") or lower.get("role"))
    tags = _dedupe(
        ["notion"]
        + [tag for tag in re.split(r"[,;]", lower.get("tags", "")) if _clean(tag)]
        + [_clean(lower.get("contact type", ""))]
        + [_clean(lower.get("status", ""))]
        + [_clean(lower.get("priority", ""))]
    )
    return {
        "source": "notion",
        "name": name,
        "first_name": first_name,
        "last_name": last_name,
        "emails": emails,
        "phones": phones,
        "birthday": _parse_birthday(lower.get("birthday", "")),
        "company": company,
        "companies": _dedupe([company]),
        "title": title,
        "titles": _dedupe([title]),
        "linkedin": lower.get("linkedin", ""),
        "twitter": lower.get("twitter", ""),
        "instagram": lower.get("ig", ""),
        "telegram": lower.get("telegram", ""),
        "discord": lower.get("discord", ""),
        "websites": _websites_from_fields(lower, ["website", "calendly"]),
        "description": _clean(lower.get("description", "")),
        "relationship_type": _slug_tag(lower.get("contact type", "")),
        "tags": [tag for tag in (_slug_tag(tag) for tag in tags) if tag],
    }


def _staff_row_to_item(row: dict[str, Any]) -> dict[str, Any]:
    lower = _lower_row(row)
    name = _clean(lower.get("name"))
    first_name, last_name = _split_full_name(name)
    emails = _collect_emails(lower, ["company email", "personal email", "protonmail"])
    phones = _collect_phones(lower, ["primary phone number", "brasil phone number"])
    company = _company_from_staff(lower, emails)
    title = _clean(lower.get("role"))
    teams = [_strip_notion_link_label(team) for team in re.split(r"[,;]", lower.get("teams", "")) if _clean(team)]
    tags = _dedupe(
        ["notion", "staff"]
        + [lower.get("type", "")]
        + (["inactive"] if lower.get("no longer employed", "").lower() == "yes" else [])
        + teams
    )
    return {
        "source": "notion.staff",
        "name": name,
        "first_name": first_name,
        "last_name": last_name,
        "emails": emails,
        "phones": phones,
        "birthday": _parse_birthday(lower.get("birthday", "")),
        "company": company,
        "companies": _dedupe([company]),
        "title": title,
        "titles": _dedupe([title]),
        "linkedin": lower.get("linkedin", ""),
        "twitter": lower.get("twittter", ""),
        "instagram": lower.get("instagram", ""),
        "pronouns": _clean(lower.get("pronouns", "")),
        "reports_to": _clean(lower.get("reports to", "")),
        "websites": _websites_from_fields(lower, ["links", "headshot"]),
        "description": _clean(lower.get("pr bio") or lower.get("2024 bio ")),
        "tags": [tag for tag in (_slug_tag(tag) for tag in tags) if tag],
    }


def _item_to_card(item: dict[str, Any], source_name: str) -> tuple[PersonCard, dict[str, Any]]:
    today = date.today().isoformat()
    emails = list(item.get("emails", []))
    source = str(item.get("source", source_name))
    linkedin = _clean(str(item.get("linkedin", "")))
    source_id = (
        linkedin or (emails[0] if emails else "") or _clean(str(item.get("name", ""))) or f"{source_name}-unknown"
    )
    card = PersonCard(
        uid=generate_uid("person", source, source_id),
        type="person",
        source=[source],
        source_id=source_id,
        created=today,
        updated=today,
        summary=_clean(str(item.get("name", ""))) or (emails[0] if emails else "unknown"),
        first_name=_clean(str(item.get("first_name", ""))),
        last_name=_clean(str(item.get("last_name", ""))),
        emails=emails,
        phones=list(item.get("phones", [])),
        birthday=_clean(str(item.get("birthday", ""))),
        company=_clean(str(item.get("company", ""))),
        companies=list(item.get("companies", []))
        or ([_clean(str(item.get("company", "")))] if _clean(str(item.get("company", ""))) else []),
        title=_clean(str(item.get("title", ""))),
        titles=list(item.get("titles", []))
        or ([_clean(str(item.get("title", "")))] if _clean(str(item.get("title", ""))) else []),
        linkedin=linkedin,
        twitter=_clean(str(item.get("twitter", ""))),
        instagram=_clean(str(item.get("instagram", ""))),
        telegram=_clean(str(item.get("telegram", ""))),
        discord=_clean(str(item.get("discord", ""))),
        pronouns=_clean(str(item.get("pronouns", ""))),
        reports_to=_clean(str(item.get("reports_to", ""))),
        websites=list(item.get("websites", [])),
        description=_clean(str(item.get("description", ""))),
        relationship_type=_clean(str(item.get("relationship_type", ""))),
        tags=list(item.get("tags", [])),
    )
    return card, {"source": source}


class NotionPeopleAdapter(BaseAdapter):
    source_id = "notion-people"

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        csv_path: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        path = csv_path or os.path.join(os.path.expanduser("~"), "Downloads", "notion-people.csv")
        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        if not os.path.isfile(path) and os.path.isdir(downloads):
            for candidate in os.listdir(downloads):
                lower = candidate.lower()
                if "notion" in lower and "people" in lower and lower.endswith(".csv"):
                    path = os.path.join(downloads, candidate)
                    break
        if not os.path.isfile(path):
            return []

        rows: list[dict[str, Any]] = []
        with open(path, encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append(_people_row_to_item(row))
        return rows

    def to_card(self, item: dict[str, Any]):
        card, metadata = _item_to_card(item, "notion")
        provenance = deterministic_provenance(card, metadata["source"])
        return card, provenance, ""


class NotionStaffAdapter(BaseAdapter):
    source_id = "notion-staff"

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        csv_path: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        path = csv_path or os.path.join(os.path.expanduser("~"), "Downloads", "staff.csv")
        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        if not os.path.isfile(path) and os.path.isdir(downloads):
            for candidate in os.listdir(downloads):
                lower = candidate.lower()
                if "staff" in lower and lower.endswith(".csv"):
                    path = os.path.join(downloads, candidate)
                    break
        if not os.path.isfile(path):
            return []

        rows: list[dict[str, Any]] = []
        with open(path, encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append(_staff_row_to_item(row))
        return rows

    def to_card(self, item: dict[str, Any]):
        card, metadata = _item_to_card(item, "notion.staff")
        provenance = deterministic_provenance(card, metadata["source"])
        return card, provenance, ""
