"""Medical record import adapter for One Medical FHIR/CCD and Epic EHI TSV exports."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from archive_vault.identity_resolver import resolve_person
from archive_vault.schema import MedicalRecordCard, VaccinationCard
from archive_vault.uid import generate_uid
from archive_vault.vault import find_note_by_slug

from ..cli_logging import CliProgressReporter, log_cli_step
from .base import BaseAdapter, deterministic_provenance
from .epic_ehi import collect_epic_ehi_items

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - runtime dependency only
    PdfReader = None

MEDICAL_SOURCE = "medical-records"
FHIR_SOURCE = "onemedical.fhir"
CCD_SOURCE = "onemedical.ccd"
EPIC_CCD_SOURCE = "epic.ccd"
EPIC_EHI_SOURCE = "epic.ehi"
PDF_SOURCE = "vaccine.pdf"
FHIR_NAMESPACE = "onemedical"
CCD_NS = {"cda": "urn:hl7-org:v3"}
CCD_SECTION_ALIASES = {
    "condition": ("Problems",),
    "observation": ("Results", "Vital Signs", "Social History", "Mental Status"),
    "diagnostic_report": ("Results",),
    "document_reference": ("Notes",),
    "medication_request": ("Medications",),
    "medication_statement": ("Medications",),
    "encounter": ("Encounters",),
    "appointment": ("Encounters", "Plan of Treatment"),
    "procedure": ("History of Procedures",),
    "communication": ("Notes", "Plan of Treatment"),
    "service_request": ("Plan of Treatment",),
    "task": ("Plan of Treatment",),
    "care_plan": ("Plan of Treatment",),
    "questionnaire_response": ("Notes",),
    "coverage": ("Insurance Providers",),
    "coverageeligibilityresponse": ("Insurance Providers",),
    "consent": ("Notes",),
    "careteam": ("Encounters",),
    "provenance": ("Notes",),
    "immunization": ("Immunizations",),
    "immunizationrecommendation": ("Immunizations",),
}
FHIR_RECORD_TYPE_MAP = {
    "Condition": "condition",
    "Observation": "observation",
    "DiagnosticReport": "diagnostic_report",
    "DocumentReference": "document_reference",
    "MedicationRequest": "medication_request",
    "MedicationStatement": "medication_statement",
    "Encounter": "encounter",
    "Procedure": "procedure",
    "Communication": "communication",
    "ServiceRequest": "service_request",
    "Appointment": "appointment",
    "Task": "task",
    "CarePlan": "care_plan",
    "QuestionnaireResponse": "questionnaire_response",
    "Coverage": "coverage",
    "CoverageEligibilityResponse": "coverage_eligibility_response",
    "Consent": "consent",
    "CareTeam": "care_team",
    "Provenance": "provenance",
    "ImmunizationRecommendation": "immunization_recommendation",
}
DATE_PATTERNS = (
    "%Y-%m-%d",
    "%Y-%m",
    "%Y",
    "%m/%d/%Y",
    "%m/%Y",
)
PDF_DATE_RE = re.compile(r"(?:^|\s)(?P<date>\d{2}/\d{2}/\d{4}(?![\d/])|\d{2}/\d{4}(?![\d/])|\d{4}(?![\d/-]))")
LOT_RE = re.compile(r"Lot No:\s*([A-Za-z0-9-]+)")
EXPIRES_RE = re.compile(r"Expires at:\s*([0-9/]+)")
WHITESPACE_RE = re.compile(r"\s+")
HTML_TAG_RE = re.compile(r"<[^>]+>")
CCD_SECTION_RE = re.compile(r"<section\b.*?</section>", re.IGNORECASE | re.DOTALL)
CCD_TITLE_RE = re.compile(r"<title\b[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
CCD_TEXT_RE = re.compile(r"<text\b[^>]*>(.*?)</text>", re.IGNORECASE | re.DOTALL)
CCD_EFFECTIVE_TIME_RE = re.compile(r"<effectiveTime\b[^>]*value=\"([^\"]+)\"", re.IGNORECASE)
CCD_ID_RE = re.compile(r"<id\b[^>]*root=\"([^\"]+)\"", re.IGNORECASE)
CCD_CODE_RE = re.compile(
    r"<code\b[^>]*code=\"([^\"]*)\"[^>]*codeSystem=\"([^\"]*)\"[^>]*displayName=\"([^\"]*)\"",
    re.IGNORECASE,
)
CCD_GIVEN_RE = re.compile(r"<given\b[^>]*>(.*?)</given>", re.IGNORECASE | re.DOTALL)
CCD_FAMILY_RE = re.compile(r"<family\b[^>]*>(.*?)</family>", re.IGNORECASE | re.DOTALL)
PDF_SKIP_TOKENS = (
    "phone:",
    "fax:",
    "union street",
    "brooklyn, ny",
    "as of ",
    "patient name:",
    "immunization record",
)
PDF_INVALID_VACCINE_PREFIXES = ("fax", "phone", "lot no", "expires at", "brand", "administered at")


def _clean(value: Any) -> str:
    return WHITESPACE_RE.sub(" ", str(value or "").strip())


def _clean_list(values: list[Any] | tuple[Any, ...] | set[Any] | None) -> list[str]:
    cleaned: list[str] = []
    for value in values or []:
        text = _clean(value)
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _slug_to_wikilink(value: str) -> str:
    slug = value.strip()
    if slug.startswith("[[") and slug.endswith("]]"):
        return slug
    return f"[[{slug}]]"


def _file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_tsv_rows(path: str | Path | None) -> list[dict[str, str]]:
    if not path:
        return []
    rows: list[dict[str, str]] = []
    text = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
    if not text:
        return rows
    headers = text[0].split("\t")
    for raw_line in text[1:]:
        values = raw_line.split("\t")
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
        row = {header: values[index] if index < len(values) else "" for index, header in enumerate(headers)}
        rows.append(row)
    return rows


def _parse_date_like(value: str) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    if raw.endswith("Z"):
        return raw
    for token in ("T",):
        if token in raw:
            return raw
    for fmt in DATE_PATTERNS:
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if fmt == "%Y":
            return parsed.strftime("%Y")
        if fmt == "%Y-%m":
            return parsed.strftime("%Y-%m")
        if fmt == "%m/%Y":
            return parsed.strftime("%Y-%m")
        return parsed.strftime("%Y-%m-%d")
    return raw


def _valid_pdf_occurrence(value: str) -> bool:
    normalized = _parse_date_like(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        year = int(normalized[:4])
        return 1900 <= year <= 2100
    if re.fullmatch(r"\d{4}-\d{2}", normalized):
        year = int(normalized[:4])
        return 1900 <= year <= 2100
    if re.fullmatch(r"\d{4}", normalized):
        year = int(normalized)
        return 1900 <= year <= 2100
    return False


def _bucket_date(value: str, *, fallback: str | None = None) -> str:
    normalized = _parse_date_like(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return normalized
    if re.fullmatch(r"\d{4}-\d{2}", normalized):
        return f"{normalized}-01"
    if re.fullmatch(r"\d{4}", normalized):
        return f"{normalized}-01-01"
    return fallback or date.today().isoformat()


def _normalize_name_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean(value).lower()).strip()


def _is_plausible_vaccine_name(value: str) -> bool:
    lowered = _clean(value).lower()
    if not lowered:
        return False
    if lowered.startswith(PDF_INVALID_VACCINE_PREFIXES):
        return False
    if any(token in lowered for token in PDF_SKIP_TOKENS):
        return False
    return bool(re.search(r"[a-z]{3,}", lowered))


def _coding_parts(value: Any) -> tuple[str, str, str]:
    if not isinstance(value, dict):
        return "", "", ""
    coding = value.get("coding") or []
    first = coding[0] if isinstance(coding, list) and coding else {}
    text = _clean(value.get("text"))
    return (
        _clean(first.get("system")),
        _clean(first.get("code")),
        text or _clean(first.get("display")),
    )


def _display_from_reference(value: Any) -> str:
    if isinstance(value, dict):
        return _clean(value.get("display") or value.get("reference"))
    return _clean(value)


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _clean(value)
        if text:
            return text
    return ""


def _observation_value_parts(resource: dict[str, Any]) -> tuple[str, float, str]:
    quantity = resource.get("valueQuantity")
    if isinstance(quantity, dict):
        unit = _clean(quantity.get("unit") or quantity.get("code"))
        raw = quantity.get("value")
        try:
            numeric = float(raw)
        except (TypeError, ValueError):
            numeric = 0.0
        return (_clean(raw), numeric, unit)
    concept = resource.get("valueCodeableConcept")
    if isinstance(concept, dict):
        _system, _code, display = _coding_parts(concept)
        return (display, 0.0, "")
    for field_name in (
        "valueString",
        "valueBoolean",
        "valueInteger",
        "valueDateTime",
        "valueTime",
    ):
        if field_name in resource and resource[field_name] not in ("", None):
            return (_clean(resource[field_name]), 0.0, "")
    return ("", 0.0, "")


def _decode_document_reference(resource: dict[str, Any]) -> str:
    contents = resource.get("content") or []
    if not isinstance(contents, list) or not contents:
        return ""
    attachment = (contents[0] or {}).get("attachment") or {}
    data = attachment.get("data")
    if not data:
        return ""
    try:
        decoded = base64.b64decode(data)
    except Exception:
        return ""
    text = decoded.decode("utf-8", errors="ignore")
    if "html" in _clean(attachment.get("contentType")).lower():
        text = HTML_TAG_RE.sub(" ", unescape(text))
    return _clean(text)


def _questionnaire_value(resource: dict[str, Any]) -> str:
    answers: list[str] = []
    for item in resource.get("item") or []:
        for answer in item.get("answer") or []:
            for key, value in answer.items():
                if key.startswith("value") and value not in ("", None):
                    answers.append(_clean(value))
    return "; ".join(answer for answer in answers if answer)[:2000]


def _extract_ccd_sections(path: str | None) -> dict[str, str]:
    if not path:
        return {}
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        sections: dict[str, str] = {}
        for match in CCD_SECTION_RE.findall(text):
            title_match = CCD_TITLE_RE.search(match)
            body_match = CCD_TEXT_RE.search(match)
            title_text = _clean(unescape(HTML_TAG_RE.sub(" ", title_match.group(1) if title_match else "")))
            excerpt = _clean(unescape(HTML_TAG_RE.sub(" ", body_match.group(1) if body_match else "")))
            if title_text and excerpt:
                sections[title_text] = excerpt[:4000]
        return sections
    sections = {}
    for section in root.findall(".//cda:section", CCD_NS):
        title = section.find("cda:title", CCD_NS)
        text_node = section.find("cda:text", CCD_NS)
        title_text = _clean(title.text if title is not None else "")
        if not title_text or text_node is None:
            continue
        excerpt = _clean(" ".join(piece for piece in text_node.itertext() if _clean(piece)))
        if excerpt:
            sections[title_text] = excerpt[:4000]
    return sections


def _ccd_patient_identifiers_from_path(path: str | Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    given = [_clean(unescape(HTML_TAG_RE.sub(" ", item))) for item in CCD_GIVEN_RE.findall(text)]
    family = [_clean(unescape(HTML_TAG_RE.sub(" ", item))) for item in CCD_FAMILY_RE.findall(text)]
    first_name = next((item for item in given if item), "")
    last_name = next((item for item in family if item), "")
    summary = " ".join(part for part in (first_name, last_name) if part)
    return {
        "summary": summary,
        "first_name": first_name,
        "last_name": last_name,
        "emails": [],
        "phones": [],
    }


def _ccd_document_item(
    *,
    path: str | Path,
    person_link: str,
    source_hashes: dict[str, str],
) -> dict[str, Any]:
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="ignore")
    title_matches = CCD_TITLE_RE.findall(text)
    title = _clean(unescape(HTML_TAG_RE.sub(" ", title_matches[0] if title_matches else path.stem)))
    code_match = CCD_CODE_RE.search(text)
    code = _clean(code_match.group(1) if code_match else "")
    code_system = _clean(code_match.group(2) if code_match else "")
    code_display = _clean(code_match.group(3) if code_match else title)
    effective_time = _parse_date_like(
        _clean((CCD_EFFECTIVE_TIME_RE.search(text) or ["", ""])[1] if CCD_EFFECTIVE_TIME_RE.search(text) else "")
    )
    document_id = _clean((CCD_ID_RE.search(text) or ["", ""])[1] if CCD_ID_RE.search(text) else path.stem)
    sections = _extract_ccd_sections(str(path))
    section_titles = list(sections)
    section_excerpt = " | ".join(f"{name}: {excerpt[:200]}" for name, excerpt in list(sections.items())[:5])
    return {
        "kind": "medical_record",
        "source": [EPIC_CCD_SOURCE],
        "source_id": f"epic:ccd:{document_id}",
        "summary": title or code_display or path.stem,
        "created": _bucket_date(effective_time or date.today().isoformat()),
        "people": [person_link],
        "source_system": "epic",
        "source_format": "ccd_xml",
        "record_type": "ccd_document",
        "record_subtype": code_display or title,
        "status": "",
        "occurred_at": effective_time,
        "recorded_at": effective_time,
        "provider_name": "",
        "facility_name": "",
        "encounter_source_id": "",
        "code_system": code_system,
        "code": code,
        "code_display": code_display,
        "value_text": section_excerpt or title,
        "value_numeric": 0.0,
        "unit": "",
        "raw_source_ref": f"ccd:{path.name}",
        "details_json": {
            "ccd_document_title": title,
            "ccd_document_id": document_id,
            "ccd_context": {
                "section_titles": section_titles,
                "section_excerpt": section_excerpt[:4000],
            },
            "source_hashes": source_hashes,
        },
    }


def _looks_like_vaccine(text: str) -> bool:
    lowered = _clean(text).lower()
    return any(
        token in lowered
        for token in (
            "vaccine",
            "vacc.",
            "vaccination",
            "immunization",
            "hepatitis b",
            "dtap",
            "hib",
            "rotavirus",
            "pneumococcal",
            "polio",
            "influenza",
            "mmr",
            "varicella",
        )
    )


def _ccd_context(record_type: str, sections: dict[str, str]) -> dict[str, Any]:
    titles = CCD_SECTION_ALIASES.get(record_type, ())
    excerpts = [_clean(sections.get(title, "")) for title in titles if _clean(sections.get(title, ""))]
    if not excerpts:
        return {}
    return {
        "section_titles": list(titles),
        "section_excerpt": excerpts[0][:1200],
    }


def _patient_identifiers(resource: dict[str, Any]) -> dict[str, Any]:
    names = resource.get("name") or []
    primary_name = names[0] if isinstance(names, list) and names else {}
    given_names = primary_name.get("given") or []
    telecom = resource.get("telecom") or []
    emails = [entry.get("value") for entry in telecom if entry.get("system") == "email"]
    phones = [entry.get("value") for entry in telecom if entry.get("system") == "phone"]
    summary = " ".join(
        part for part in [_clean(given_names[0] if given_names else ""), _clean(primary_name.get("family"))] if part
    )
    return {
        "summary": summary,
        "first_name": _clean(given_names[0] if given_names else ""),
        "last_name": _clean(primary_name.get("family")),
        "emails": _clean_list(emails),
        "phones": _clean_list(phones),
    }


def _normalize_person_wikilink(vault_path: str, person_wikilink: str) -> str:
    wikilink = _slug_to_wikilink(person_wikilink.strip().strip("[]"))
    slug = wikilink[2:-2]
    if find_note_by_slug(vault_path, slug) is None:
        raise ValueError(f"Person note not found for {wikilink}")
    return wikilink


def _resolve_target_person(vault_path: str, patient: dict[str, Any] | None, person_wikilink: str | None) -> str:
    if person_wikilink:
        return _normalize_person_wikilink(vault_path, person_wikilink)
    if not patient:
        raise ValueError("FHIR bundle missing Patient resource and no person_wikilink override was supplied")
    result = resolve_person(vault_path, _patient_identifiers(patient))
    if result.action == "merge" and result.wikilink:
        return result.wikilink
    raise ValueError(f"Unable to resolve target person from FHIR Patient resource: action={result.action}")


def _render_medical_body(item: dict[str, Any]) -> str:
    lines = [
        f"Record type: {_clean(item.get('record_type'))}",
        f"Occurred at: {_clean(item.get('occurred_at'))}",
        f"Recorded at: {_clean(item.get('recorded_at'))}",
        f"Status: {_clean(item.get('status'))}",
        f"Code: {_clean(item.get('code_display')) or _clean(item.get('code'))}",
        f"Value: {_clean(item.get('value_text'))}",
        f"Provider: {_clean(item.get('provider_name'))}",
        f"Facility: {_clean(item.get('facility_name'))}",
        f"Person: {_clean_list(item.get('people'))[0] if _clean_list(item.get('people')) else ''}",
        f"Source ref: {_clean(item.get('raw_source_ref'))}",
    ]
    ccd_context = ((item.get("details_json") or {}).get("ccd_context") or {}).get("section_excerpt", "")
    if ccd_context:
        lines.append(f"CCD context: {ccd_context}")
    return "\n".join(line for line in lines if line.split(": ", 1)[-1])


def _render_vaccination_body(item: dict[str, Any]) -> str:
    lines = [
        f"Vaccine: {_clean(item.get('vaccine_name'))}",
        f"Occurred at: {_clean(item.get('occurred_at'))}",
        f"Status: {_clean(item.get('status'))}",
        f"Brand: {_clean(item.get('brand_name'))}",
        f"Manufacturer: {_clean(item.get('manufacturer'))}",
        f"Lot: {_clean(item.get('lot_number'))}",
        f"Expires: {_clean(item.get('expiration_date'))}",
        f"Administered at: {_clean(item.get('administered_at'))}",
        f"Performer: {_clean(item.get('performer_name'))}",
        f"Location: {_clean(item.get('location'))}",
        f"Person: {_clean_list(item.get('people'))[0] if _clean_list(item.get('people')) else ''}",
        f"Source ref: {_clean(item.get('raw_source_ref'))}",
    ]
    return "\n".join(line for line in lines if line.split(": ", 1)[-1])


def _parse_vaccine_pdf_text(text: str) -> list[dict[str, str]]:
    lines = [_clean(line) for line in text.splitlines() if _clean(line)]
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in lines:
        lowered = line.lower()
        if (
            lowered.startswith("immunization record")
            or lowered.startswith("as of ")
            or lowered.startswith("patient name:")
        ):
            continue
        if line.startswith("-- ") or line == "●" or any(token in lowered for token in PDF_SKIP_TOKENS):
            continue
        if (
            lowered.startswith("vaccine ")
            or lowered.startswith("brand:") is False
            and lowered == "date administered at"
        ):
            continue
        if line.startswith("Brand:"):
            current["brand_name"] = _clean(line.split("Brand:", 1)[1].split("Lot No:", 1)[0])
            lot_match = LOT_RE.search(line)
            if lot_match:
                current["lot_number"] = _clean(lot_match.group(1))
            expires_match = EXPIRES_RE.search(line)
            if expires_match:
                current["expiration_date"] = _parse_date_like(expires_match.group(1))
            continue
        if line.startswith("Expires at:"):
            current["expiration_date"] = _parse_date_like(line.split("Expires at:", 1)[1])
            continue
        date_match = PDF_DATE_RE.search(line)
        if date_match:
            date_text = _parse_date_like(date_match.group("date"))
            if not _valid_pdf_occurrence(date_text):
                current = {}
                continue
            if current.get("vaccine_name"):
                prefix = current["vaccine_name"]
            else:
                prefix = _clean(line[: date_match.start("date")])
            suffix = _clean(line[date_match.end("date") :])
            if prefix:
                current["vaccine_name"] = prefix
            current["occurred_at"] = date_text
            current["location"] = _clean(" ".join(part for part in [current.get("location", ""), suffix] if part))
            if _is_plausible_vaccine_name(current.get("vaccine_name", "")):
                entries.append(
                    {
                        "occurred_at": current.get("occurred_at", ""),
                        "vaccine_name": current.get("vaccine_name", ""),
                        "brand_name": current.get("brand_name", ""),
                        "lot_number": current.get("lot_number", ""),
                        "expiration_date": current.get("expiration_date", ""),
                        "location": current.get("location", ""),
                    }
                )
            current = {}
            continue
        if _is_plausible_vaccine_name(line):
            current["vaccine_name"] = _clean(" ".join(part for part in [current.get("vaccine_name", ""), line] if part))
    return entries


def _read_vaccine_pdf_entries(path: str | None) -> list[dict[str, str]]:
    if not path:
        return []
    if PdfReader is None:  # pragma: no cover - exercised in live use
        raise RuntimeError("pypdf is required to parse vaccine PDFs")
    reader = PdfReader(path)
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return _parse_vaccine_pdf_text(text)


def _extract_fhir_resources(path: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    entries = payload.get("entry") if isinstance(payload, dict) else []
    resources: list[dict[str, Any]] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        resource = entry if "resourceType" in entry else entry.get("resource", {})
        if isinstance(resource, dict) and resource.get("resourceType"):
            resources.append(resource)
    return resources


def _medical_item_from_resource(
    resource: dict[str, Any],
    *,
    person_link: str,
    ccd_sections: dict[str, str],
    source_hashes: dict[str, str],
) -> dict[str, Any]:
    resource_type = _clean(resource.get("resourceType"))
    record_type = FHIR_RECORD_TYPE_MAP.get(resource_type, _normalize_name_key(resource_type).replace(" ", "_"))
    record_subtype = ""
    code_system = ""
    code = ""
    code_display = ""
    value_text = ""
    value_numeric = 0.0
    unit = ""
    provider_name = ""
    facility_name = ""
    encounter_source_id = ""
    occurred_at = ""
    recorded_at = ""
    status = _first_nonempty(
        ((resource.get("status") or {}).get("coding") or [{}])[0].get("display")
        if isinstance(resource.get("status"), dict)
        else resource.get("status"),
        ((resource.get("clinicalStatus") or {}).get("coding") or [{}])[0].get("code")
        if isinstance(resource.get("clinicalStatus"), dict)
        else "",
    )

    if resource_type == "Condition":
        code_system, code, code_display = _coding_parts(resource.get("code"))
        record_subtype = _first_nonempty(
            ((resource.get("category") or [{}])[0].get("text")),
            ((resource.get("category") or [{}])[0].get("coding") or [{}])[0].get("code"),
        )
        occurred_at = _parse_date_like(_first_nonempty(resource.get("onsetDateTime"), resource.get("recordedDate")))
        recorded_at = _parse_date_like(resource.get("recordedDate"))
        value_text = _clean(resource.get("abatementString"))
        provider_name = _display_from_reference(resource.get("recorder"))
    elif resource_type == "Observation":
        code_system, code, code_display = _coding_parts(resource.get("code"))
        record_subtype = _first_nonempty(
            ((resource.get("category") or [{}])[0].get("text")),
            ((resource.get("category") or [{}])[0].get("coding") or [{}])[0].get("code"),
        )
        occurred_at = _parse_date_like(resource.get("effectiveDateTime"))
        recorded_at = _parse_date_like(_first_nonempty(resource.get("issued"), resource.get("effectiveDateTime")))
        value_text, value_numeric, unit = _observation_value_parts(resource)
    elif resource_type == "DiagnosticReport":
        code_system, code, code_display = _coding_parts(resource.get("code"))
        record_subtype = _first_nonempty(
            ((resource.get("category") or [{}])[0].get("coding") or [{}])[0].get("code"),
            ((resource.get("category") or [{}])[0].get("text")),
        )
        occurred_at = _parse_date_like(resource.get("effectiveDateTime"))
        recorded_at = _parse_date_like(_first_nonempty(resource.get("issued"), resource.get("effectiveDateTime")))
        value_text = "; ".join(
            _clean(result.get("reference"))
            for result in resource.get("result") or []
            if _clean(result.get("reference"))
        )
    elif resource_type == "DocumentReference":
        code_system, code, code_display = _coding_parts(resource.get("type"))
        record_subtype = _first_nonempty(
            ((resource.get("category") or [{}])[0].get("coding") or [{}])[0].get("code"),
            ((resource.get("category") or [{}])[0].get("text")),
        )
        occurred_at = _parse_date_like(resource.get("date"))
        recorded_at = occurred_at
        value_text = _first_nonempty(resource.get("description"), _decode_document_reference(resource))
        encounter_source_id = _clean((((resource.get("context") or {}).get("encounter") or [{}])[0].get("reference")))
    elif resource_type == "MedicationRequest":
        code_system, code, code_display = _coding_parts(resource.get("medicationCodeableConcept") or {})
        occurred_at = _parse_date_like(_first_nonempty(resource.get("authoredOn"), resource.get("occurrenceDateTime")))
        recorded_at = occurred_at
        value_text = _clean((((resource.get("dosageInstruction") or [{}])[0]).get("text")))
        provider_name = _display_from_reference(resource.get("requester"))
    elif resource_type == "MedicationStatement":
        medication = resource.get("medicationCodeableConcept") or {}
        code_system, code, code_display = _coding_parts(medication)
        occurred_at = _parse_date_like(
            _first_nonempty(resource.get("effectiveDateTime"), ((resource.get("effectivePeriod") or {}).get("start")))
        )
        recorded_at = _parse_date_like(resource.get("dateAsserted"))
    elif resource_type == "Encounter":
        record_subtype = _first_nonempty(
            ((resource.get("class") or {}).get("code")),
            ((resource.get("type") or [{}])[0].get("coding") or [{}])[0].get("code"),
        )
        occurred_at = _parse_date_like(
            _first_nonempty(((resource.get("period") or {}).get("start")), ((resource.get("period") or {}).get("end")))
        )
        recorded_at = occurred_at
        provider_name = _display_from_reference(resource.get("serviceProvider"))
        value_text = _clean(
            ((resource.get("reasonCode") or [{}])[0].get("text"))
            or (((resource.get("reasonCode") or [{}])[0].get("coding") or [{}])[0].get("display"))
        )
    elif resource_type == "Procedure":
        code_system, code, code_display = _coding_parts(resource.get("code"))
        occurred_at = _parse_date_like(
            _first_nonempty(resource.get("performedDateTime"), ((resource.get("performedPeriod") or {}).get("start")))
        )
        recorded_at = occurred_at
    elif resource_type == "Communication":
        occurred_at = _parse_date_like(_first_nonempty(resource.get("sent"), resource.get("received")))
        recorded_at = occurred_at
        value_text = _clean((((resource.get("payload") or [{}])[0]).get("contentString")))
    elif resource_type == "ServiceRequest":
        code_system, code, code_display = _coding_parts(resource.get("code"))
        occurred_at = _parse_date_like(_first_nonempty(resource.get("occurrenceDateTime"), resource.get("authoredOn")))
        recorded_at = _parse_date_like(resource.get("authoredOn"))
    elif resource_type == "Appointment":
        record_subtype = _first_nonempty(
            resource.get("appointmentType", {}).get("text"), resource.get("serviceCategory", [{}])[0].get("text")
        )
        occurred_at = _parse_date_like(resource.get("start"))
        recorded_at = _parse_date_like(resource.get("created"))
        value_text = _clean(resource.get("description"))
    elif resource_type == "Task":
        code_system, code, code_display = _coding_parts(resource.get("code"))
        occurred_at = _parse_date_like(_first_nonempty(resource.get("authoredOn"), resource.get("lastModified")))
        recorded_at = _parse_date_like(resource.get("lastModified"))
        value_text = _clean(resource.get("description"))
    elif resource_type == "CarePlan":
        record_subtype = _first_nonempty(
            ((resource.get("category") or [{}])[0].get("text")),
            ((resource.get("category") or [{}])[0].get("coding") or [{}])[0].get("code"),
        )
        occurred_at = _parse_date_like(
            _first_nonempty(((resource.get("period") or {}).get("start")), resource.get("created"))
        )
        recorded_at = _parse_date_like(resource.get("created"))
        value_text = _clean(resource.get("description"))
    elif resource_type == "QuestionnaireResponse":
        record_subtype = _first_nonempty(resource.get("questionnaire"), resource.get("status"))
        occurred_at = _parse_date_like(resource.get("authored"))
        recorded_at = occurred_at
        value_text = _questionnaire_value(resource)
    elif resource_type == "Coverage":
        occurred_at = _parse_date_like(((resource.get("period") or {}).get("start")))
        recorded_at = occurred_at
        value_text = _display_from_reference(resource.get("payor"))
    elif resource_type == "CoverageEligibilityResponse":
        occurred_at = _parse_date_like(resource.get("created"))
        recorded_at = occurred_at
        value_text = _clean(resource.get("outcome"))
    elif resource_type == "Consent":
        occurred_at = _parse_date_like(resource.get("dateTime"))
        recorded_at = occurred_at
        value_text = _clean(resource.get("scope", {}).get("text"))
    elif resource_type == "CareTeam":
        occurred_at = _parse_date_like(
            _first_nonempty(((resource.get("period") or {}).get("start")), resource.get("status"))
        )
        recorded_at = occurred_at
        value_text = "; ".join(
            _display_from_reference(participant.get("member")) for participant in resource.get("participant") or []
        )
    elif resource_type == "Provenance":
        occurred_at = _parse_date_like(resource.get("recorded"))
        recorded_at = occurred_at
        value_text = "; ".join(
            _clean(target.get("reference"))
            for target in resource.get("target") or []
            if _clean(target.get("reference"))
        )
    elif resource_type == "ImmunizationRecommendation":
        occurred_at = _parse_date_like(resource.get("date"))
        recorded_at = occurred_at
        recommendation = (
            ((resource.get("recommendation") or [{}])[0]) if isinstance(resource.get("recommendation"), list) else {}
        )
        vaccine_code = (
            ((recommendation.get("vaccineCode") or [{}])[0])
            if isinstance(recommendation.get("vaccineCode"), list)
            else {}
        )
        code_system, code, code_display = _coding_parts(vaccine_code)
        value_text = _clean(((recommendation.get("forecastStatus") or {}).get("text")))

    ccd_context = _ccd_context(record_type, ccd_sections)
    details_json: dict[str, Any] = {
        "fhir_resource_type": resource_type,
        "fhir_id": _clean(resource.get("id")),
        "source_hashes": source_hashes,
    }
    if ccd_context:
        details_json["ccd_context"] = ccd_context
    return {
        "kind": "medical_record",
        "source": [FHIR_NAMESPACE, FHIR_SOURCE, *([CCD_SOURCE] if ccd_context else [])],
        "source_id": f"onemedical:{resource_type}:{_clean(resource.get('id'))}",
        "summary": _first_nonempty(code_display, value_text, resource.get("description"), resource_type),
        "created": _bucket_date(_first_nonempty(occurred_at, recorded_at)),
        "people": [person_link],
        "source_system": "onemedical",
        "source_format": "fhir_json",
        "record_type": record_type,
        "record_subtype": record_subtype,
        "status": status,
        "occurred_at": occurred_at,
        "recorded_at": recorded_at,
        "provider_name": provider_name,
        "facility_name": facility_name,
        "encounter_source_id": encounter_source_id,
        "code_system": code_system,
        "code": code,
        "code_display": code_display,
        "value_text": value_text,
        "value_numeric": value_numeric,
        "unit": unit,
        "raw_source_ref": f"fhir:{resource_type}:{_clean(resource.get('id'))}",
        "details_json": details_json,
    }


def _vaccination_item_from_resource(
    resource: dict[str, Any],
    *,
    person_link: str,
    source_hashes: dict[str, str],
) -> dict[str, Any]:
    code_system, code, code_display = _coding_parts(resource.get("vaccineCode"))
    performer = ((resource.get("performer") or [{}])[0]) if isinstance(resource.get("performer"), list) else {}
    details_json = {
        "fhir_resource_type": "Immunization",
        "fhir_id": _clean(resource.get("id")),
        "source_hashes": source_hashes,
    }
    return {
        "kind": "vaccination",
        "source": [FHIR_NAMESPACE, FHIR_SOURCE],
        "source_id": f"onemedical:Immunization:{_clean(resource.get('id'))}",
        "summary": code_display or _clean(resource.get("status")) or _clean(resource.get("id")),
        "created": _bucket_date(resource.get("occurrenceDateTime")),
        "people": [person_link],
        "source_system": "onemedical",
        "source_format": "fhir_json",
        "occurred_at": _parse_date_like(resource.get("occurrenceDateTime")),
        "vaccine_name": code_display,
        "cvx_code": code,
        "status": _clean(resource.get("status")),
        "manufacturer": _display_from_reference(resource.get("manufacturer")),
        "brand_name": "",
        "lot_number": _clean(resource.get("lotNumber")),
        "expiration_date": _parse_date_like(resource.get("expirationDate")),
        "administered_at": _display_from_reference((resource.get("location") or {})),
        "performer_name": _display_from_reference(performer.get("actor")),
        "location": _display_from_reference((resource.get("location") or {})),
        "raw_source_ref": f"fhir:Immunization:{_clean(resource.get('id'))}",
        "details_json": details_json,
    }


def _overlay_pdf_vaccination(item: dict[str, Any], pdf_entry: dict[str, str], pdf_hash: str) -> None:
    field_sources: dict[str, str] = item.setdefault("field_sources", {})
    details_json = dict(item.get("details_json") or {})
    details_json["pdf_overlay"] = {key: value for key, value in pdf_entry.items() if _clean(value)}
    details_json.setdefault("source_hashes", {})["vaccine_pdf"] = pdf_hash
    item["details_json"] = details_json
    if PDF_SOURCE not in item["source"]:
        item["source"].append(PDF_SOURCE)
    for field_name in ("brand_name", "lot_number", "expiration_date", "location"):
        if _clean(pdf_entry.get(field_name)) and not _clean(item.get(field_name)):
            item[field_name] = _clean(pdf_entry[field_name])
            field_sources[field_name] = PDF_SOURCE
    if _clean(pdf_entry.get("location")) and not _clean(item.get("administered_at")):
        item["administered_at"] = _clean(pdf_entry["location"])
        field_sources["administered_at"] = PDF_SOURCE


def _pdf_only_vaccination_item(
    entry: dict[str, str],
    *,
    person_link: str,
    pdf_hash: str,
) -> dict[str, Any]:
    occurred_at = _parse_date_like(entry.get("occurred_at", ""))
    vaccine_name = _clean(entry.get("vaccine_name"))
    return {
        "kind": "vaccination",
        "source": [FHIR_NAMESPACE, PDF_SOURCE],
        "source_id": f"vaccine-pdf:{occurred_at}:{_normalize_name_key(vaccine_name)}",
        "summary": vaccine_name,
        "created": _bucket_date(occurred_at),
        "people": [person_link],
        "source_system": "onemedical",
        "source_format": "vaccine_pdf",
        "occurred_at": occurred_at,
        "vaccine_name": vaccine_name,
        "cvx_code": "",
        "status": "",
        "manufacturer": "",
        "brand_name": _clean(entry.get("brand_name")),
        "lot_number": _clean(entry.get("lot_number")),
        "expiration_date": _parse_date_like(entry.get("expiration_date", "")),
        "administered_at": _clean(entry.get("location")),
        "performer_name": "",
        "location": _clean(entry.get("location")),
        "raw_source_ref": f"vaccine_pdf:{occurred_at}:{_normalize_name_key(vaccine_name)}",
        "details_json": {"source_hashes": {"vaccine_pdf": pdf_hash}, "pdf_overlay": dict(entry)},
        "field_sources": {
            "brand_name": PDF_SOURCE,
            "lot_number": PDF_SOURCE,
            "expiration_date": PDF_SOURCE,
            "administered_at": PDF_SOURCE,
            "location": PDF_SOURCE,
        },
    }


class MedicalRecordsAdapter(BaseAdapter):
    source_id = MEDICAL_SOURCE
    preload_existing_uid_index = False
    enable_person_resolution = False

    def should_enable_person_resolution(self, **kwargs) -> bool:
        return False

    def ingest_verbose(self, **kwargs) -> bool:
        raw_value = kwargs.get("verbose")
        if raw_value is None and "HFA_IMPORT_VERBOSE" not in os.environ:
            return True
        return super().ingest_verbose(**kwargs)

    def get_cursor_key(self, **kwargs) -> str:
        if kwargs.get("ehi_tables_dir_path"):
            return "medical-records:epic-ehi"
        return "medical-records:onemedical"

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        *,
        fhir_json_path: str | None = None,
        ccd_xml_path: str | None = None,
        ccd_dir_path: str | None = None,
        ehi_tables_dir_path: str | None = None,
        vaccine_pdf_path: str | None = None,
        person_wikilink: str | None = None,
        epic_pat_id: str | None = None,
        ehi_include_order_results: bool = True,
        ehi_include_adt: bool = True,
        **kwargs,
    ) -> list[dict[str, Any]]:
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        if not fhir_json_path and not ccd_xml_path and not ccd_dir_path and not ehi_tables_dir_path:
            raise ValueError("fhir_json_path or ccd_xml_path or ccd_dir_path or ehi_tables_dir_path is required")
        if ehi_tables_dir_path:
            log_cli_step(self.source_id, 1, 2, "prepare Epic EHI import", f"tables={ehi_tables_dir_path}")
            target_person = _normalize_person_wikilink(vault_path, person_wikilink or "")
            log_cli_step(self.source_id, 1, 2, "prepare Epic EHI import complete", f"person={target_person}")
            items, ehi_meta = collect_epic_ehi_items(
                ehi_tables_dir=ehi_tables_dir_path,
                person_link=target_person,
                source_id=self.source_id,
                verbose=verbose,
                progress_every=progress_every,
                epic_pat_id=epic_pat_id,
                include_order_results=bool(ehi_include_order_results),
                include_adt=bool(ehi_include_adt),
            )
            log_cli_step(
                self.source_id,
                2,
                2,
                "emit structured Epic EHI cards complete",
                f"items={len(items)} meta={ehi_meta.get('counts')}",
            )
            cursor.update(
                {
                    "ehi_tables_dir_path": str(Path(ehi_tables_dir_path).expanduser().resolve()),
                    "emitted_items": len(items),
                    "patient_wikilink": target_person,
                    "epic_ehi_patient_id": ehi_meta.get("patient_id"),
                    "epic_ehi_counts": ehi_meta.get("counts"),
                }
            )
            return items
        if not fhir_json_path:
            log_cli_step(
                self.source_id, 1, 3, "prepare CCD-only medical import", f"ccd_dir={ccd_dir_path or ccd_xml_path}"
            )
            ccd_paths: list[Path] = []
            if ccd_xml_path:
                ccd_paths.append(Path(ccd_xml_path))
            if ccd_dir_path:
                ccd_paths.extend(sorted(Path(ccd_dir_path).glob("*.XML")))
                ccd_paths.extend(sorted(Path(ccd_dir_path).glob("*.xml")))
            unique_paths: list[Path] = []
            seen_paths: set[Path] = set()
            for path in ccd_paths:
                resolved = path.expanduser().resolve()
                if resolved in seen_paths or not resolved.exists():
                    continue
                seen_paths.add(resolved)
                unique_paths.append(resolved)
            if not unique_paths:
                raise ValueError("No CCD XML files found for CCD-only medical import")
            target_person = _normalize_person_wikilink(vault_path, person_wikilink or "")
            log_cli_step(
                self.source_id,
                1,
                3,
                "prepare CCD-only medical import complete",
                f"person={target_person} ccd_documents={len(unique_paths)}",
            )
            reporter = CliProgressReporter(
                source_id=self.source_id,
                step_number=2,
                total_steps=3,
                stage="parse CCD documents",
                total_items=len(unique_paths),
                progress_every=progress_every,
                enabled=verbose,
            )
            log_cli_step(self.source_id, 2, 3, "parse CCD documents")
            items = [
                _ccd_document_item(
                    path=path,
                    person_link=target_person,
                    source_hashes={"ccd_xml": _file_sha256(path)},
                )
                for path in unique_paths
            ]
            for index in range(1, len(unique_paths) + 1):
                reporter.update(index)
            reporter.complete(len(unique_paths), extra=f"items={len(items)}")
            cursor.update(
                {
                    "ccd_document_count": len(unique_paths),
                    "emitted_items": len(items),
                    "patient_wikilink": target_person,
                }
            )
            log_cli_step(self.source_id, 3, 3, "emit CCD-only medical cards complete", f"items={len(items)}")
            return items
        log_cli_step(self.source_id, 1, 4, "prepare FHIR medical import", f"fhir={fhir_json_path}")
        resources = _extract_fhir_resources(fhir_json_path)
        patient = next((resource for resource in resources if resource.get("resourceType") == "Patient"), None)
        target_person = _resolve_target_person(vault_path, patient, person_wikilink)
        log_cli_step(
            self.source_id,
            1,
            4,
            "prepare FHIR medical import complete",
            f"person={target_person} resources={len(resources)}",
        )
        log_cli_step(self.source_id, 2, 4, "load CCD and vaccine overlays")
        ccd_sections = _extract_ccd_sections(ccd_xml_path)
        pdf_entries = _read_vaccine_pdf_entries(vaccine_pdf_path)
        log_cli_step(
            self.source_id,
            2,
            4,
            "load CCD and vaccine overlays complete",
            f"ccd_sections={len(ccd_sections)} pdf_entries={len(pdf_entries)}",
        )
        source_hashes = {"fhir_json": _file_sha256(fhir_json_path)}
        if ccd_xml_path:
            source_hashes["ccd_xml"] = _file_sha256(ccd_xml_path)
        pdf_hash = ""
        if vaccine_pdf_path:
            pdf_hash = _file_sha256(vaccine_pdf_path)
            source_hashes["vaccine_pdf"] = pdf_hash

        pdf_by_key: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        for entry in pdf_entries:
            key = (_parse_date_like(entry.get("occurred_at", "")), _normalize_name_key(entry.get("vaccine_name", "")))
            if key[0] and key[1]:
                pdf_by_key[key].append(entry)

        used_pdf_entries: set[tuple[str, str]] = set()
        items: list[dict[str, Any]] = []
        reporter = CliProgressReporter(
            source_id=self.source_id,
            step_number=3,
            total_steps=4,
            stage="parse FHIR resources",
            total_items=len(resources),
            progress_every=progress_every,
            enabled=verbose,
        )
        log_cli_step(self.source_id, 3, 4, "parse FHIR resources")
        for resource in resources:
            resource_type = _clean(resource.get("resourceType"))
            if resource_type == "Patient":
                reporter.update(len(items) + 1)
                continue
            if resource_type == "Immunization":
                item = _vaccination_item_from_resource(resource, person_link=target_person, source_hashes=source_hashes)
                key = (_parse_date_like(item.get("occurred_at", "")), _normalize_name_key(item.get("vaccine_name", "")))
                match = pdf_by_key.get(key)
                if match:
                    _overlay_pdf_vaccination(item, match[0], pdf_hash)
                    used_pdf_entries.add(key)
                items.append(item)
                reporter.update(len(items))
                continue
            items.append(
                _medical_item_from_resource(
                    resource,
                    person_link=target_person,
                    ccd_sections=ccd_sections,
                    source_hashes=source_hashes,
                )
            )
            reporter.update(len(items))

        for key, entries in pdf_by_key.items():
            if key in used_pdf_entries:
                continue
            for entry in entries:
                items.append(_pdf_only_vaccination_item(entry, person_link=target_person, pdf_hash=pdf_hash))
        reporter.complete(len(resources), extra=f"items={len(items)}")
        log_cli_step(self.source_id, 4, 4, "emit structured FHIR medical cards complete", f"items={len(items)}")

        cursor.update(
            {
                "fhir_resource_count": len(resources),
                "emitted_items": len(items),
                "patient_wikilink": target_person,
                "fhir_hash": source_hashes["fhir_json"],
            }
        )
        return items

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        field_sources = dict(item.get("field_sources") or {})
        provenance_source = (list(item.get("source", [])) or [FHIR_SOURCE])[0]
        if _clean(item.get("kind")) == "vaccination":
            card = VaccinationCard(
                uid=generate_uid("vaccination", self.source_id, _clean(item.get("source_id"))),
                type="vaccination",
                source=list(item.get("source", [])) or [FHIR_SOURCE],
                source_id=_clean(item.get("source_id")),
                created=_clean(item.get("created")) or today,
                updated=today,
                summary=_clean(item.get("summary")),
                tags=["medical", "vaccination"],
                people=list(item.get("people", [])),
                source_system=_clean(item.get("source_system")),
                source_format=_clean(item.get("source_format")),
                occurred_at=_clean(item.get("occurred_at")),
                vaccine_name=_clean(item.get("vaccine_name")),
                cvx_code=_clean(item.get("cvx_code")),
                status=_clean(item.get("status")),
                manufacturer=_clean(item.get("manufacturer")),
                brand_name=_clean(item.get("brand_name")),
                lot_number=_clean(item.get("lot_number")),
                expiration_date=_clean(item.get("expiration_date")),
                administered_at=_clean(item.get("administered_at")),
                performer_name=_clean(item.get("performer_name")),
                location=_clean(item.get("location")),
                raw_source_ref=_clean(item.get("raw_source_ref")),
                details_json=dict(item.get("details_json") or {}),
            )
            provenance = deterministic_provenance(card, provenance_source, field_sources=field_sources)
            return card, provenance, _render_vaccination_body(item)

        card = MedicalRecordCard(
            uid=generate_uid("medical-record", self.source_id, _clean(item.get("source_id"))),
            type="medical_record",
            source=list(item.get("source", [])) or [FHIR_SOURCE],
            source_id=_clean(item.get("source_id")),
            created=_clean(item.get("created")) or today,
            updated=today,
            summary=_clean(item.get("summary")),
            tags=["medical", _clean(item.get("record_type")).replace("_", "-")],
            people=list(item.get("people", [])),
            source_system=_clean(item.get("source_system")),
            source_format=_clean(item.get("source_format")),
            record_type=_clean(item.get("record_type")),
            record_subtype=_clean(item.get("record_subtype")),
            status=_clean(item.get("status")),
            occurred_at=_clean(item.get("occurred_at")),
            recorded_at=_clean(item.get("recorded_at")),
            provider_name=_clean(item.get("provider_name")),
            facility_name=_clean(item.get("facility_name")),
            encounter_source_id=_clean(item.get("encounter_source_id")),
            code_system=_clean(item.get("code_system")),
            code=_clean(item.get("code")),
            code_display=_clean(item.get("code_display")),
            value_text=_clean(item.get("value_text")),
            value_numeric=float(item.get("value_numeric", 0) or 0),
            unit=_clean(item.get("unit")),
            raw_source_ref=_clean(item.get("raw_source_ref")),
            details_json=dict(item.get("details_json") or {}),
        )
        provenance = deterministic_provenance(card, provenance_source, field_sources=field_sources)
        return card, provenance, _render_medical_body(item)

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
