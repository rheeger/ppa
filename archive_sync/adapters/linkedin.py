"""LinkedIn connections and profile export adapter."""

from __future__ import annotations

import csv
import os
import re
from collections import Counter
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from hfa.schema import PersonCard
from hfa.uid import generate_uid

from .base import BaseAdapter, deterministic_provenance


def _extract_linkedin_username(url: str) -> str:
    match = re.search(r"(?:https?://)?(?:[\w]+\.)?linkedin\.com/in/([^/?#]+)", url.strip())
    return match.group(1).lower() if match else ""


def _normalize_name(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _normalize_date(value: str, *formats: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return raw


def _normalize_connected_on(value: str) -> str:
    return _normalize_date(value, "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y")


def _normalize_birthday(value: str) -> str:
    normalized = _normalize_date(value, "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d")
    return normalized if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized) else ""


def _read_csv_rows(path: str | Path, *, header_prefixes: tuple[str, ...] | None = None) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.is_file():
        return []
    with csv_path.open(encoding="utf-8", errors="ignore") as handle:
        raw_lines = handle.readlines()
    header_index = 0
    if header_prefixes:
        for index, line in enumerate(raw_lines):
            normalized = line.strip().lower()
            if any(normalized.startswith(prefix) for prefix in header_prefixes):
                header_index = index
                break
    reader = csv.DictReader(StringIO("".join(raw_lines[header_index:])))
    rows: list[dict[str, str]] = []
    for row in reader:
        if not row:
            continue
        normalized_row = {str(key or "").strip(): (value or "").strip() for key, value in row.items()}
        if any(value for value in normalized_row.values()):
            rows.append(normalized_row)
    return rows


def _parse_list_field(value: str) -> list[str]:
    raw = value.strip()
    if not raw:
        return []
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    return [part.strip().strip("\"'") for part in raw.split(",") if part.strip().strip("\"'")]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _normalize_phone(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return raw
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if raw.startswith("+"):
        return f"+{digits}"
    return digits


def _resolve_export_paths(csv_path: str | None) -> tuple[Path | None, Path | None]:
    downloads = Path(os.path.expanduser("~")) / "Downloads"
    if csv_path:
        candidate = Path(os.path.expanduser(csv_path))
        if candidate.is_dir():
            return (
                candidate / "Connections.csv" if (candidate / "Connections.csv").is_file() else None,
                candidate,
            )
        return (candidate if candidate.is_file() else None), candidate.parent if candidate.parent.is_dir() else None

    default_connections = downloads / "LinkedInConnections.csv"
    if default_connections.is_file():
        return default_connections, downloads

    exported_connections = downloads / "Connections.csv"
    if exported_connections.is_file():
        return exported_connections, downloads

    if downloads.is_dir():
        directory_candidates = sorted(
            [
                candidate
                for candidate in downloads.iterdir()
                if candidate.is_dir()
                and "linkedin" in candidate.name.lower()
                and (candidate / "Connections.csv").is_file()
            ]
        )
        if directory_candidates:
            export_dir = directory_candidates[0]
            return export_dir / "Connections.csv", export_dir

        for candidate in sorted(downloads.iterdir()):
            if candidate.is_file() and "linkedin" in candidate.name.lower() and candidate.suffix.lower() == ".csv":
                return candidate, downloads

    return None, None


def _derive_self_linkedin_url(export_dir: Path, full_name: str) -> str:
    rows = _read_csv_rows(export_dir / "Invitations.csv")
    if not rows:
        return ""
    normalized_name = _normalize_name(full_name)
    if not normalized_name:
        return ""
    urls: Counter[str] = Counter()
    for row in rows:
        if _normalize_name(row.get("From", "")) == normalized_name:
            inviter_url = row.get("inviterProfileUrl", "").strip()
            if inviter_url:
                urls[inviter_url] += 1
        if _normalize_name(row.get("To", "")) == normalized_name:
            invitee_url = row.get("inviteeProfileUrl", "").strip()
            if invitee_url:
                urls[invitee_url] += 1
    return urls.most_common(1)[0][0] if urls else ""


def _headline_company_title(headline: str) -> tuple[str, str]:
    match = re.match(r"(?P<title>.+?)\s+at\s+(?P<company>.+)", headline.strip())
    if not match:
        return "", ""
    return match.group("company").strip(), match.group("title").strip()


def _format_date_range(started_on: str, finished_on: str) -> str:
    start = started_on.strip()
    finish = finished_on.strip() or "Present"
    if start and finish:
        return f"{start} - {finish}"
    return start or finish


def _format_position_row(row: dict[str, str]) -> str:
    company = row.get("Company Name", "").strip()
    title = row.get("Title", "").strip()
    location = row.get("Location", "").strip()
    date_range = _format_date_range(row.get("Started On", ""), row.get("Finished On", ""))
    parts = [" - ".join(part for part in [company, title] if part)]
    if location:
        parts.append(location)
    if date_range:
        parts.append(date_range)
    return " | ".join(part for part in parts if part)


def _format_education_row(row: dict[str, str]) -> str:
    school = row.get("School Name", "").strip()
    degree = row.get("Degree Name", "").strip()
    start = row.get("Start Date", "").strip()
    end = row.get("End Date", "").strip()
    date_range = " - ".join(part for part in [start, end] if part)
    parts = [" - ".join(part for part in [school, degree] if part)]
    if date_range:
        parts.append(date_range)
    return " | ".join(part for part in parts if part)


def _format_verification_row(row: dict[str, str]) -> str:
    verification_type = row.get("Verification type", "").strip()
    organization = row.get("Organization name", "").strip()
    provider = row.get("Verification service provider", "").strip()
    verified_date = row.get("Verified date", "").strip()
    parts = [verification_type]
    if organization and organization.upper() != "N/A":
        parts.append(organization)
    if provider and provider.upper() != "N/A":
        parts.append(f"via {provider}")
    if verified_date and verified_date.upper() != "N/A":
        parts.append(f"verified {verified_date}")
    return " - ".join(part for part in parts if part)


def _build_profile_body(
    *,
    headline: str,
    summary: str,
    industry: str,
    geo_location: str,
    zip_code: str,
    instant_messengers: list[str],
    positions: list[str],
    education: list[str],
    verifications: list[str],
) -> str:
    sections: list[str] = []

    profile_lines: list[str] = []
    if headline:
        profile_lines.append(f"Headline: {headline}")
    if summary:
        profile_lines.append(f"Summary: {summary}")
    if industry:
        profile_lines.append(f"Industry: {industry}")
    if geo_location:
        profile_lines.append(f"Location: {geo_location}")
    if zip_code:
        profile_lines.append(f"Zip Code: {zip_code}")
    if instant_messengers:
        profile_lines.append(f"Instant Messengers: {', '.join(instant_messengers)}")
    if profile_lines:
        sections.append("\n".join(profile_lines))

    if positions:
        sections.append("LinkedIn positions:\n" + "\n".join(f"- {entry}" for entry in positions))
    if education:
        sections.append("LinkedIn education:\n" + "\n".join(f"- {entry}" for entry in education))
    if verifications:
        sections.append("LinkedIn verifications:\n" + "\n".join(f"- {entry}" for entry in verifications))
    return "\n\n".join(section for section in sections if section)


def _build_profile_item(export_dir: Path) -> dict[str, Any] | None:
    profile_rows = _read_csv_rows(export_dir / "Profile.csv")
    if not profile_rows:
        return None

    profile = profile_rows[0]
    first_name = profile.get("First Name", "").strip() or profile.get("First name", "").strip()
    last_name = profile.get("Last Name", "").strip() or profile.get("Last name", "").strip()
    full_name = " ".join(part for part in [first_name, last_name] if part)

    headline = profile.get("Headline", "").strip()
    summary = profile.get("Summary", "").strip()
    industry = profile.get("Industry", "").strip()
    geo_location = profile.get("Geo Location", "").strip()
    zip_code = profile.get("Zip Code", "").strip()
    websites = _dedupe_preserve_order(_parse_list_field(profile.get("Websites", "")))
    twitter_handles = _parse_list_field(profile.get("Twitter Handles", ""))
    instant_messengers = _parse_list_field(profile.get("Instant Messengers", ""))
    birthday = _normalize_birthday(profile.get("Birth Date", ""))

    position_rows = _read_csv_rows(export_dir / "Positions.csv")
    companies = _dedupe_preserve_order(
        [row.get("Company Name", "").strip() for row in position_rows if row.get("Company Name", "").strip()]
    )
    titles = _dedupe_preserve_order(
        [row.get("Title", "").strip() for row in position_rows if row.get("Title", "").strip()]
    )
    current_position = next((row for row in position_rows if not row.get("Finished On", "").strip()), None)
    primary_position = current_position or (position_rows[0] if position_rows else {})
    company = primary_position.get("Company Name", "").strip()
    title = primary_position.get("Title", "").strip()
    if not company or not title:
        headline_company, headline_title = _headline_company_title(headline)
        company = company or headline_company
        title = title or headline_title

    email_rows = _read_csv_rows(export_dir / "Email Addresses.csv")
    emails = _dedupe_preserve_order(
        [row.get("Email Address", "").strip().lower() for row in email_rows if row.get("Email Address", "").strip()]
    )

    phone_rows = _read_csv_rows(export_dir / "PhoneNumbers.csv")
    whatsapp_rows = _read_csv_rows(export_dir / "Whatsapp Phone Numbers.csv")
    phones = _dedupe_preserve_order(
        [
            normalized_phone
            for normalized_phone in (_normalize_phone(row.get("Number", "")) for row in [*phone_rows, *whatsapp_rows])
            if normalized_phone
        ]
    )

    education_entries = [
        entry for entry in (_format_education_row(row) for row in _read_csv_rows(export_dir / "Education.csv")) if entry
    ]
    verification_entries = [
        entry
        for entry in (
            _format_verification_row(row) for row in _read_csv_rows(export_dir / "Verifications" / "Verifications.csv")
        )
        if entry
    ]
    position_entries = [entry for entry in (_format_position_row(row) for row in position_rows) if entry]

    linkedin_url = _derive_self_linkedin_url(export_dir, full_name)
    linkedin = _extract_linkedin_username(linkedin_url)
    description = summary or headline

    return {
        "source": "linkedin",
        "name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "emails": emails,
        "phones": phones,
        "birthday": birthday,
        "company": company,
        "companies": companies,
        "title": title,
        "titles": titles,
        "linkedin_url": linkedin_url,
        "linkedin": linkedin,
        "twitter": twitter_handles[0] if twitter_handles else "",
        "websites": websites,
        "description": description,
        "profile_body": _build_profile_body(
            headline=headline,
            summary=summary,
            industry=industry,
            geo_location=geo_location,
            zip_code=zip_code,
            instant_messengers=instant_messengers,
            positions=position_entries,
            education=education_entries,
            verifications=verification_entries,
        ),
    }


class LinkedInAdapter(BaseAdapter):
    source_id = "linkedin"
    preload_existing_uid_index = False
    parallel_person_matching = True
    parallel_person_match_default_workers = 8
    parallel_person_match_default_chunk_size = 128

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        csv_path: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        connections_path, export_dir = _resolve_export_paths(csv_path)
        rows: list[dict[str, Any]] = []
        if connections_path is not None:
            for row in _read_csv_rows(
                connections_path,
                header_prefixes=("first name,last name,", "firstname,lastname,"),
            ):
                first = (row.get("First Name") or row.get("FirstName") or row.get("First") or "").strip()
                last = (row.get("Last Name") or row.get("LastName") or row.get("Last") or "").strip()
                email = (row.get("Email Address") or row.get("Email") or row.get("EmailAddress") or "").strip().lower()
                linkedin_url = (row.get("URL") or row.get("Profile URL") or row.get("LinkedIn URL") or "").strip()
                rows.append(
                    {
                        "source": "linkedin",
                        "name": " ".join(part for part in [first, last] if part),
                        "first_name": first,
                        "last_name": last,
                        "emails": [email] if email else [],
                        "linkedin_url": linkedin_url,
                        "linkedin": _extract_linkedin_username(linkedin_url),
                        "company": (row.get("Company") or row.get("Organization") or "").strip(),
                        "title": (row.get("Position") or row.get("Title") or row.get("Role") or "").strip(),
                        "connected_on": _normalize_connected_on(
                            (row.get("Connected On") or row.get("ConnectedOn") or "").strip()
                        ),
                    }
                )
        if export_dir is not None:
            profile_item = _build_profile_item(export_dir)
            if profile_item is not None:
                rows.append(profile_item)
        return rows

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        emails = list(item.get("emails", []))
        phones = list(item.get("phones", []))
        linkedin_username = str(item.get("linkedin", "")).strip().lower()
        source_id = (
            linkedin_username
            or (emails[0] if emails else "")
            or str(item.get("name", "")).strip()
            or "linkedin-unknown"
        )
        company = str(item.get("company", "")).strip()
        title = str(item.get("title", "")).strip()
        companies = [value.strip() for value in item.get("companies", []) if str(value).strip()]
        titles = [value.strip() for value in item.get("titles", []) if str(value).strip()]
        card = PersonCard(
            uid=generate_uid("person", "linkedin", source_id),
            type="person",
            source=["linkedin"],
            source_id=source_id,
            created=today,
            updated=today,
            summary=str(item.get("name", "")).strip() or (emails[0] if emails else "unknown"),
            first_name=str(item.get("first_name", "")).strip(),
            last_name=str(item.get("last_name", "")).strip(),
            emails=emails,
            phones=phones,
            birthday=str(item.get("birthday", "")).strip(),
            company=company,
            companies=companies or ([company] if company else []),
            title=title,
            titles=titles or ([title] if title else []),
            linkedin=linkedin_username,
            linkedin_url=str(item.get("linkedin_url", "")).strip(),
            linkedin_connected_on=str(item.get("connected_on", "")).strip(),
            twitter=str(item.get("twitter", "")).strip(),
            websites=[value.strip() for value in item.get("websites", []) if str(value).strip()],
            description=str(item.get("description", "")).strip(),
            tags=["linkedin"],
        )
        provenance = deterministic_provenance(card, "linkedin")
        body = ""
        if item.get("profile_body"):
            body = str(item.get("profile_body", "")).strip()
        elif item.get("connected_on"):
            body = f"Connected on: {item['connected_on']}"
        return card, provenance, body
