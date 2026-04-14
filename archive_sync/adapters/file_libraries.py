"""Local file library adapter for HFA document imports."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import mimetypes
import os
import re
import signal
import subprocess
import threading
import zipfile
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from xml.etree import ElementTree

from archive_vault.identity import IdentityCache
from archive_vault.schema import DocumentCard
from archive_vault.uid import generate_uid
from archive_vault.vault import read_note

from .base import BaseAdapter, FetchedBatch, deterministic_provenance

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - exercised in live use
    PdfReader = None
else:  # pragma: no cover - runtime behavior only
    logging.getLogger("pypdf").setLevel(logging.ERROR)

FILE_LIBRARY_SOURCE = "file.library"
DEFAULT_BATCH_SIZE = 100
MAX_BODY_CHARS = 40000
MAX_EXTRACT_CHARS = 60000
MAX_FILE_BYTES = 200 * 1024 * 1024
MAX_TEXT_FILE_BYTES = 8 * 1024 * 1024
EXTRACTION_TIMEOUT_SECONDS = 10
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-])\d{3}[\s.\-]\d{4}\b")
ISO_DATE_RE = re.compile(r"\b((?:19|20)\d{2})[-_/](\d{2})[-_/](\d{2})\b")
YEAR_MONTH_RE = re.compile(r"\b((?:19|20)\d{2})[-_/](\d{2})\b")
MONTH_NAME_DATE_RE = re.compile(
    r"\b("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r")\s+(\d{1,2}),\s+((?:19|20)\d{2})\b",
    re.IGNORECASE,
)
MARKDOWN_PREFIX_RE = re.compile(r"^(?:#+|\*+|-+|\d+\.)\s*")
UPPERCASE_WORD_RE = re.compile(r"^[A-Z][A-Z'.-]*(?:\s+[A-Z][A-Z'.-]*){1,3}$")
TITLE_CASE_WORD_RE = re.compile(r"^[A-Z][a-zA-Z'.-]*(?:\s+[A-Z][a-zA-Z'.-]*){1,3}$")
NOISY_TITLE_BITS = (
    "this instrument and any securities issuable",
    "have not been registered under the securities act",
    "table of contents",
    "begin:vcalendar",
)
PERSON_NAME_STOPWORDS = {
    "bank",
    "banking",
    "team",
    "pharmacy",
    "calendar",
    "budget",
    "summary",
    "representations",
    "title",
    "name",
    "revenue",
    "expense",
    "board",
    "framework",
    "ventures",
    "endaoment",
    "charity",
    "block",
    "giving",
    "tree",
    "technologies",
    "cvs",
}

ROOTS: dict[str, Path] = {
    "documents": Path.home() / "Documents",
    "gdrive.personal": Path.home() / "My Drive (rheeger@gmail.com)",
    "gdrive.endaoment": Path.home() / "My Drive (robbie@endaoment.org)",
    "gdrive.gtt": Path.home() / "My Drive (robbie@givingtree.tech)",
    "downloads": Path.home() / "Downloads",
}

ROOT_PATH_TO_LABEL = {path.expanduser().resolve(): label for label, path in ROOTS.items()}

INCLUDED_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".txt",
    ".md",
    ".rtf",
    ".xml",
    ".html",
    ".htm",
    ".pages",
    ".csv",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".key",
    ".json",
    ".eml",
    ".msg",
    ".ics",
    ".vcf",
}
PACKAGE_EXTENSIONS = {".pages", ".key"}
TEXT_EXTENSIONS = {".txt", ".md", ".rtf", ".json", ".csv", ".xml", ".html", ".htm"}
TEXTUTIL_EXTENSIONS = {".doc", ".docx", ".pages", ".rtf"}
XML_SPREADSHEET_EXTENSIONS = {".xlsx"}
XML_PRESENTATION_EXTENSIONS = {".pptx"}
METADATA_ONLY_EXTENSIONS = {".key", ".ppt", ".xls", ".msg"}
SKIP_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "site-packages",
    ".pytest_cache",
    ".mypy_cache",
    ".next",
    ".cache",
    "dist",
    "build",
    "DerivedData",
    ".Trash",
}
SKIP_DIR_SUFFIXES = {
    ".app",
    ".photoslibrary",
    ".xcarchive",
    ".bundle",
    ".framework",
    ".rtfd",
    ".numbers",
    ".keynote",
}
REPO_MARKERS = {
    ".git",
    "package.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "bun.lockb",
    "Cargo.toml",
    "go.mod",
    "pyproject.toml",
    "requirements.txt",
    "Pipfile",
    "composer.json",
    "Gemfile",
}
PATH_TAGS = {
    "work": "work",
    "personal": "personal",
    "health": "health",
    "medical": "medical",
    "legal": "legal",
    "compliance": "compliance",
    "tax": "tax",
    "taxes": "tax",
    "finance": "finance",
    "financial": "finance",
    "investments": "finance",
    "fundraising": "fundraising",
    "board": "board",
    "marketing": "marketing",
    "partnerships": "partnerships",
    "employee": "people-ops",
    "employees": "people-ops",
    "wedding": "wedding",
    "travel": "travel",
    "property": "real-estate",
    "housing": "real-estate",
    "downloads": "downloads",
    "drive": "drive",
}
ORG_PATTERNS = (
    (" uvvu ", "UVVU"),
    (" solawave ", "Solawave"),
    ("endaoment.tech", "Endaoment.Tech"),
    ("endaoment tech", "Endaoment.Tech"),
    ("endaoment", "Endaoment"),
    (" charity block ", "Charity Block"),
    ("giving tree technologies", "Giving Tree Technologies"),
    ("giving tree", "Giving Tree Technologies"),
    ("charity block", "Charity Block"),
    ("shloopy doopy enterprises", "Shloopy Doopy Enterprises"),
    ("cvs pharmacy", "CVS Pharmacy"),
    ("wells fargo", "Wells Fargo"),
    ("state street", "State Street"),
    ("silvergate", "Silvergate"),
    ("framework ventures", "Framework Ventures"),
    ("altruist", "Altruist"),
    ("jewish community federation", "Jewish Community Federation and Endowment Fund"),
    ("sfjcf", "Jewish Community Federation and Endowment Fund"),
)

_STAGE_WORKER_IDENTITY_CACHE: IdentityCache | None = None


def _clean(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _clean_list(values: Iterable[Any]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean(value)
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def _path_text(path: Path) -> str:
    return path.as_posix().replace("_", " ").replace("-", " ")


def _filename_title(path: Path) -> str:
    return _clean(re.sub(r"\s+", " ", re.sub(r"[_-]+", " ", path.stem)))


def _normalized_line(value: str) -> str:
    cleaned = _clean(MARKDOWN_PREFIX_RE.sub("", value).strip(">*_` "))
    return cleaned


def _candidate_lines(text: str, *, limit: int = 20) -> list[str]:
    candidates: list[str] = []
    for line in text.splitlines():
        cleaned = _normalized_line(line)
        if not cleaned:
            continue
        candidates.append(cleaned)
        if len(candidates) >= limit:
            break
    return candidates


def _title_is_bad(candidate: str, *, filename_title: str) -> bool:
    cleaned = _clean(candidate)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    if lowered == filename_title.lower():
        return False
    if ISO_DATE_RE.fullmatch(cleaned.replace("/", "-").replace("_", "-")) or MONTH_NAME_DATE_RE.fullmatch(cleaned):
        return True
    if len(cleaned) > 180:
        return True
    if "|" in cleaned and len(cleaned) > 80:
        return True
    if any(bit in lowered for bit in NOISY_TITLE_BITS):
        return True
    if len(cleaned) > 80:
        letters = [char for char in cleaned if char.isalpha()]
        if letters:
            uppercase_ratio = sum(1 for char in letters if char.isupper()) / len(letters)
            if uppercase_ratio > 0.75:
                return True
    return False


def _derive_title(path: Path, text: str, *, preferred: str = "") -> tuple[str, bool]:
    filename_title = _filename_title(path)
    explicit = _normalized_line(preferred)
    if explicit and not _title_is_bad(explicit, filename_title=filename_title):
        return explicit, False
    for line in _candidate_lines(text):
        if not _title_is_bad(line, filename_title=filename_title):
            return line, False
    return filename_title, True


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        if not child.is_file():
            continue
        try:
            total += child.stat().st_size
        except OSError:
            continue
    return total


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    if path.is_dir():
        for child in sorted(path.rglob("*")):
            digest.update(child.relative_to(path).as_posix().encode("utf-8"))
            if not child.is_file():
                continue
            with child.open("rb") as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
        return digest.hexdigest()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iso_from_timestamp(timestamp: float) -> str:
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    except Exception:
        return ""


def _normalize_extension(path: Path) -> str:
    return path.suffix.lower().lstrip(".")


def _normalize_source_suffix(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def _date_bucket(*values: str) -> str:
    for value in values:
        text = _clean(value)
        if len(text) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", text):
            return text[:10]
    return date.today().isoformat()


def _detect_date(text: str) -> str:
    if match := ISO_DATE_RE.search(text):
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    if match := MONTH_NAME_DATE_RE.search(text):
        try:
            return (
                datetime.strptime(
                    f"{match.group(1)} {match.group(2)} {match.group(3)}",
                    "%B %d %Y",
                )
                .date()
                .isoformat()
            )
        except ValueError:
            pass
    if match := YEAR_MONTH_RE.search(text):
        return f"{match.group(1)}-{match.group(2)}"
    return ""


def _trim_text(text: str, *, limit: int = MAX_EXTRACT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "\n\n[truncated]"


def _first_nonempty_line(text: str) -> str:
    for line in text.splitlines():
        cleaned = _clean(line)
        if cleaned:
            return cleaned
    return ""


def _preview_description(text: str, *, title: str = "") -> str:
    lines = []
    normalized_title = _clean(title).lower()
    for line in text.splitlines():
        cleaned = _clean(line)
        if not cleaned:
            continue
        if normalized_title and cleaned.lower() == normalized_title:
            continue
        lines.append(cleaned)
        if len(" ".join(lines)) >= 280:
            break
    preview = " ".join(lines)
    if len(preview) > 280:
        preview = preview[:277].rstrip() + "..."
    return preview


def _extract_urls(text: str) -> list[str]:
    return _clean_list(match.rstrip(".,);") for match in URL_RE.findall(text))


def _extract_phones(text: str) -> list[str]:
    matches: list[str] = []
    for match in PHONE_RE.findall(text):
        cleaned = _clean(match)
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) not in {10, 11}:
            continue
        matches.append(cleaned)
    return _clean_list(matches)


def _extract_name_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    known_org_needles = {needle.strip() for needle, _ in ORG_PATTERNS}
    for line in _candidate_lines(text, limit=120):
        lowered = line.lower()
        if lowered in known_org_needles:
            continue
        if any(token in lowered for token in ("http", "@", "www.", "|", "://", ":")):
            continue
        if any(char.isdigit() for char in line):
            continue
        words = [word.strip(".,;:()[]{}") for word in line.split()]
        if not (2 <= len(words) <= 4):
            continue
        if any(word.lower() in PERSON_NAME_STOPWORDS for word in words):
            continue
        if UPPERCASE_WORD_RE.fullmatch(" ".join(words)):
            title_case = " ".join(word.capitalize() for word in words)
            if title_case not in candidates:
                candidates.append(title_case)
            continue
        if TITLE_CASE_WORD_RE.fullmatch(" ".join(words)) and line not in candidates:
            candidates.append(line)
    return candidates


def _counterparties_from_filename(path: Path) -> list[str]:
    stem = _filename_title(path)
    leading = stem.split(" - ", 1)[0]
    if leading == stem:
        return []
    candidates = [part.strip() for part in re.split(r"\s*&\s*|\s+and\s+", leading) if part.strip()]
    filtered: list[str] = []
    for candidate in candidates:
        lowered = candidate.lower()
        if len(candidate) < 4:
            continue
        if lowered in {"signed", "redline", "copy"}:
            continue
        filtered.append(candidate)
    return _clean_list(filtered)


def _name_span_allowed(words: list[str]) -> bool:
    if len(words) < 2:
        return False
    if any(word.lower() in PERSON_NAME_STOPWORDS for word in words):
        return False
    for index, word in enumerate(words):
        if len(word) == 1:
            continue
        if word.isupper():
            continue
        if word[0].isupper() and word[1:].replace(".", "").replace("-", "").replace("'", "").isalpha():
            continue
        return False
    return len(words[0]) > 1 and len(words[-1]) > 1


def _scan_text_people_mentions(text: str, identity_cache: IdentityCache) -> list[str]:
    snippet = text[:20000]
    tokens = re.findall(r"[A-Za-z][A-Za-z'.-]*", snippet)
    candidates: set[str] = set()
    for index in range(len(tokens)):
        for size in (2, 3, 4):
            span = tokens[index : index + size]
            if len(span) < size:
                break
            if _name_span_allowed(span):
                candidates.add(" ".join(span))
    links: list[str] = []
    for candidate in sorted(candidates):
        direct = identity_cache.resolve("name", candidate)
        if direct and direct not in links:
            links.append(direct)
    return links


def _stage_file_basename(root_label: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", root_label.lower()).strip("-")
    return cleaned or "documents"


def _init_stage_worker(vault_path: str) -> None:  # pragma: no cover - process pool runtime only
    global _STAGE_WORKER_IDENTITY_CACHE
    _STAGE_WORKER_IDENTITY_CACHE = IdentityCache(vault_path)


def _stage_analyze_candidate(
    candidate: tuple[str, str, str],
) -> tuple[str, dict[str, Any] | None, str | None, str, str]:
    global _STAGE_WORKER_IDENTITY_CACHE
    if _STAGE_WORKER_IDENTITY_CACHE is None:  # pragma: no cover - defensive
        raise RuntimeError("Stage worker identity cache was not initialized")
    root_label, root_path_str, path_str = candidate
    adapter = FileLibrariesAdapter()
    try:
        item = adapter._build_item(
            path=Path(path_str),
            root_label=root_label,
            root_path=Path(root_path_str),
            identity_cache=_STAGE_WORKER_IDENTITY_CACHE,
        )
    except Exception as exc:
        return root_label, None, "read_failed", path_str, str(exc)
    return root_label, item, None, path_str, ""


def _strip_html(value: str) -> str:
    text = re.sub(r"<(script|style)\b.*?</\1>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    return _clean(text)


def _is_probably_repo_tree(dir_path: Path, dirnames: list[str], filenames: list[str]) -> bool:
    markers = set(dirnames) | set(filenames)
    return bool(markers & REPO_MARKERS)


def _iter_plain_rows(text: str, *, delimiter: str) -> list[str]:
    reader = csv.reader(text.splitlines(), delimiter=delimiter)
    rows: list[str] = []
    for row in reader:
        cleaned = [cell.strip() for cell in row if cell and cell.strip()]
        if cleaned:
            rows.append(" | ".join(cleaned))
        if len(rows) >= 200:
            break
    return rows


def _extract_text_file(path: Path) -> tuple[str, str]:
    if _path_size(path) > MAX_TEXT_FILE_BYTES:
        return "", "metadata_only"
    raw = path.read_text(encoding="utf-8", errors="ignore")
    if path.suffix.lower() == ".csv":
        return "\n".join(_iter_plain_rows(raw, delimiter=",")), "csv"
    return _trim_text(raw), "plain"


def _extract_email(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        message = BytesParser(policy=policy.default).parse(handle)
    subject = _clean(message.get("subject", ""))
    authors = _clean_list([name or addr for name, addr in getaddresses(message.get_all("from", []))])
    counterparties = _clean_list(
        [name or addr for name, addr in getaddresses(message.get_all("to", []) + message.get_all("cc", []))]
    )
    emails = _clean_list(
        [
            addr
            for _, addr in getaddresses(
                message.get_all("from", [])
                + message.get_all("to", [])
                + message.get_all("cc", [])
                + message.get_all("bcc", [])
                + message.get_all("reply-to", [])
            )
        ]
    )
    sent_at = ""
    try:
        parsed = parsedate_to_datetime(message.get("date", ""))
    except (TypeError, ValueError, IndexError):
        parsed = None
    if parsed is not None:
        sent_at = parsed.astimezone(timezone.utc).isoformat()
    body = ""
    if message.is_multipart():
        plain_parts: list[str] = []
        html_parts: list[str] = []
        for part in message.walk():
            if part.get_content_disposition() == "attachment":
                continue
            content_type = part.get_content_type()
            try:
                payload = part.get_content()
            except Exception:
                payload = ""
            if not payload:
                continue
            if content_type == "text/plain":
                plain_parts.append(str(payload))
            elif content_type == "text/html":
                html_parts.append(_strip_html(str(payload)))
        body = "\n\n".join(item for item in plain_parts if item.strip()) or "\n\n".join(
            item for item in html_parts if item.strip()
        )
    else:
        try:
            body = str(message.get_content())
        except Exception:
            body = ""
    body = _trim_text(body)
    title, title_from_filename = _derive_title(path, body, preferred=subject)
    return {
        "title": title,
        "description": _preview_description(body, title=title),
        "authors": authors,
        "counterparties": counterparties,
        "emails": emails,
        "websites": _extract_urls(body),
        "document_date": sent_at[:10] if sent_at else "",
        "document_type": "email_export",
        "text": body,
        "text_source": "eml",
        "quality_flags": ["title_from_filename"] if title_from_filename else [],
    }


def _unfold_ical_lines(raw: str) -> list[str]:
    lines: list[str] = []
    for line in raw.splitlines():
        if line.startswith((" ", "\t")) and lines:
            lines[-1] += line[1:]
            continue
        lines.append(line)
    return lines


def _parse_ical_date(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    if value.endswith("Z") and len(value) >= 16:
        try:
            parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            return parsed.isoformat()
        except ValueError:
            return value
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    if len(value) >= 15 and value[:8].isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}T{value[9:11]}:{value[11:13]}:{value[13:15]}"
    return value


def _extract_ics(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    lines = _unfold_ical_lines(raw)
    title = ""
    organizer = ""
    attendees: list[str] = []
    emails: list[str] = []
    start_at = ""
    end_at = ""
    location = ""
    snippets: list[str] = []
    for line in lines:
        if line.startswith("SUMMARY:"):
            title = _clean(line.partition(":")[2])
        elif line.startswith("DTSTART"):
            start_at = _parse_ical_date(line.partition(":")[2])
        elif line.startswith("DTEND"):
            end_at = _parse_ical_date(line.partition(":")[2])
        elif line.startswith("LOCATION:"):
            location = _clean(line.partition(":")[2])
        elif line.startswith("ORGANIZER"):
            value = line.partition(":")[2]
            organizer = _clean(value.replace("mailto:", ""))
            emails.extend(EMAIL_RE.findall(value))
        elif line.startswith("ATTENDEE"):
            value = line.partition(":")[2]
            attendee = _clean(value.replace("mailto:", ""))
            if attendee:
                attendees.append(attendee)
            emails.extend(EMAIL_RE.findall(value))
        snippets.append(line)
    text = _trim_text("\n".join(snippets))
    title, title_from_filename = _derive_title(path, text, preferred=title)
    return {
        "title": title,
        "counterparties": _clean_list([organizer, *attendees]),
        "emails": _clean_list(emails),
        "document_date": start_at[:10] if start_at else "",
        "date_start": start_at,
        "date_end": end_at,
        "location": location,
        "websites": _extract_urls(text),
        "document_type": "calendar_invite",
        "text": text,
        "text_source": "ics",
        "quality_flags": ["title_from_filename"] if title_from_filename else [],
    }


def _extract_vcf(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    lines = _unfold_ical_lines(raw)
    full_name = ""
    org = ""
    emails: list[str] = []
    phones: list[str] = []
    websites: list[str] = []
    for line in lines:
        upper = line.upper()
        if upper.startswith("FN:"):
            full_name = _clean(line.partition(":")[2])
        elif upper.startswith("ORG:"):
            org = _clean(line.partition(":")[2].replace(";", " "))
        elif upper.startswith("EMAIL"):
            emails.extend(EMAIL_RE.findall(line))
        elif upper.startswith("TEL"):
            phones.append(_clean(line.partition(":")[2]))
        elif upper.startswith("URL"):
            websites.extend(_extract_urls(line.partition(":")[2]))
    text = _trim_text(raw)
    title, title_from_filename = _derive_title(path, text, preferred=full_name)
    return {
        "title": title,
        "authors": _clean_list([full_name]),
        "counterparties": _clean_list([org, *phones]),
        "emails": _clean_list(emails),
        "phones": _clean_list(phones),
        "websites": _clean_list(websites),
        "orgs": _clean_list([org]),
        "document_type": "contact_card",
        "text": text,
        "text_source": "vcf",
        "quality_flags": ["title_from_filename"] if title_from_filename else [],
    }


def _extract_pdf(path: Path) -> dict[str, Any]:
    if PdfReader is None:
        return {
            "document_type": "pdf",
            "text_source": "metadata_only",
            "text": "",
        }
    try:
        use_signal_timeout = threading.current_thread() is threading.main_thread()
        previous_handler = None
        if use_signal_timeout:
            previous_handler = signal.getsignal(signal.SIGALRM)

            def _timeout_handler(signum, frame):  # pragma: no cover - runtime safety only
                raise TimeoutError("pdf extraction timed out")

            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.setitimer(signal.ITIMER_REAL, EXTRACTION_TIMEOUT_SECONDS)
        reader = PdfReader(str(path))
        text_parts: list[str] = []
        page_count = len(reader.pages)
        for page in reader.pages[:50]:
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            if text:
                text_parts.append(text)
            if len("\n".join(text_parts)) >= MAX_EXTRACT_CHARS:
                break
        text = _trim_text("\n\n".join(text_parts))
        metadata = reader.metadata or {}
        title, title_from_filename = _derive_title(
            path,
            text,
            preferred=_clean(getattr(metadata, "title", "") or metadata.get("/Title", "")),
        )
        author = _clean(getattr(metadata, "author", "") or metadata.get("/Author", ""))
        document_date = _detect_date(text[:4000]) or _detect_date(_path_text(path))
        return {
            "title": title,
            "authors": _clean_list([author]),
            "counterparties": _clean_list(_extract_name_candidates(text)),
            "description": _preview_description(text, title=title),
            "document_date": document_date,
            "document_type": "pdf",
            "page_count": page_count,
            "text": text,
            "text_source": "pdf",
            "quality_flags": ["title_from_filename"] if title_from_filename else [],
        }
    except Exception:
        return {
            "title": _filename_title(path),
            "document_type": "pdf",
            "text_source": "metadata_only",
            "text": "",
            "quality_flags": ["metadata_only", "pdf_extract_failed"],
        }
    finally:
        if threading.current_thread() is threading.main_thread():
            signal.setitimer(signal.ITIMER_REAL, 0)
            if previous_handler is not None:
                signal.signal(signal.SIGALRM, previous_handler)


def _extract_with_textutil(path: Path) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["textutil", "-convert", "txt", "-stdout", str(path)],
            capture_output=True,
            text=True,
            check=False,
            timeout=EXTRACTION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {
            "document_type": path.suffix.lower().lstrip("."),
            "text_source": "metadata_only",
            "text": "",
            "quality_flags": ["metadata_only", "textutil_timeout"],
        }
    if result.returncode != 0:
        return {
            "document_type": path.suffix.lower().lstrip("."),
            "text_source": "metadata_only",
            "text": "",
            "quality_flags": ["metadata_only", "textutil_failed"],
        }
    text = _trim_text(result.stdout)
    title, title_from_filename = _derive_title(path, text)
    return {
        "title": title,
        "description": _preview_description(text, title=title),
        "document_type": path.suffix.lower().lstrip("."),
        "text": text,
        "text_source": "textutil",
        "counterparties": _clean_list(_counterparties_from_filename(path) + _extract_name_candidates(text)),
        "document_date": _detect_date(text[:4000]) or _detect_date(_path_text(path)),
        "quality_flags": ["title_from_filename"] if title_from_filename else [],
    }


def _extract_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    namespace = {"a": root.tag.partition("}")[0].strip("{")}
    values: list[str] = []
    for item in root.findall("a:si", namespace):
        text = "".join(node.text or "" for node in item.findall(".//a:t", namespace))
        values.append(_clean(text))
    return values


def _extract_xlsx(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as archive:
        shared_strings = _extract_shared_strings(archive)
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        namespace = {"a": workbook.tag.partition("}")[0].strip("{")}
        sheet_names = [sheet.attrib.get("name", "") for sheet in workbook.findall(".//a:sheet", namespace)]
        text_rows: list[str] = []
        for name in sorted(
            item for item in archive.namelist() if item.startswith("xl/worksheets/sheet") and item.endswith(".xml")
        ):
            root = ElementTree.fromstring(archive.read(name))
            ns = {"a": root.tag.partition("}")[0].strip("{")}
            for row in root.findall(".//a:row", ns):
                values: list[str] = []
                for cell in row.findall("a:c", ns):
                    cell_type = cell.attrib.get("t", "")
                    raw = ""
                    if cell_type == "inlineStr":
                        raw = "".join(node.text or "" for node in cell.findall(".//a:t", ns))
                    else:
                        raw = _clean(cell.findtext("a:v", default="", namespaces=ns))
                        if cell_type == "s" and raw.isdigit():
                            index = int(raw)
                            raw = shared_strings[index] if 0 <= index < len(shared_strings) else raw
                    raw = _clean(raw)
                    if raw:
                        values.append(raw)
                if values:
                    text_rows.append(" | ".join(values))
                if len(text_rows) >= 200:
                    break
            if len(text_rows) >= 200:
                break
    text = _trim_text("\n".join(text_rows))
    title, title_from_filename = _derive_title(path, text, preferred=_filename_title(path))
    return {
        "title": title,
        "description": _preview_description(text, title=title),
        "sheet_names": _clean_list(sheet_names[:20]),
        "document_date": _detect_date(text[:4000]) or _detect_date(_path_text(path)),
        "document_type": "spreadsheet",
        "text": text,
        "text_source": "xlsx",
        "quality_flags": ["title_from_filename"] if title_from_filename else [],
    }


def _extract_pptx(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as archive:
        slide_names = sorted(
            item for item in archive.namelist() if item.startswith("ppt/slides/slide") and item.endswith(".xml")
        )
        text_parts: list[str] = []
        for slide_name in slide_names:
            root = ElementTree.fromstring(archive.read(slide_name))
            ns = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}
            slide_text = " ".join(
                _clean(node.text or "") for node in root.findall(".//a:t", ns) if _clean(node.text or "")
            )
            if slide_text:
                text_parts.append(slide_text)
            if len("\n".join(text_parts)) >= MAX_EXTRACT_CHARS:
                break
    text = _trim_text("\n\n".join(text_parts))
    title, title_from_filename = _derive_title(path, text)
    return {
        "title": title,
        "description": _preview_description(text, title=title),
        "document_type": "presentation",
        "page_count": len(slide_names),
        "text": text,
        "text_source": "pptx",
        "quality_flags": ["title_from_filename"] if title_from_filename else [],
    }


def _extract_generic_metadata(path: Path) -> dict[str, Any]:
    return {
        "title": _filename_title(path),
        "document_type": path.suffix.lower().lstrip("."),
        "document_date": _detect_date(_path_text(path)),
        "text": "",
        "text_source": "metadata_only",
        "quality_flags": ["metadata_only", "title_from_filename"],
    }


def _extract_payload(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix in TEXT_EXTENSIONS:
        text, text_source = _extract_text_file(path)
        title, title_from_filename = _derive_title(path, text)
        return {
            "title": title,
            "description": _preview_description(text, title=title),
            "document_date": _detect_date(text[:4000]) or _detect_date(_path_text(path)),
            "document_type": "spreadsheet" if suffix == ".csv" else suffix.lstrip("."),
            "text": text,
            "text_source": text_source,
            "quality_flags": ["title_from_filename"] if title_from_filename else [],
        }
    if suffix == ".eml":
        return _extract_email(path)
    if suffix == ".ics":
        return _extract_ics(path)
    if suffix == ".vcf":
        return _extract_vcf(path)
    if suffix == ".pdf":
        return _extract_pdf(path)
    if suffix in TEXTUTIL_EXTENSIONS:
        return _extract_with_textutil(path)
    if suffix in XML_SPREADSHEET_EXTENSIONS:
        return _extract_xlsx(path)
    if suffix in XML_PRESENTATION_EXTENSIONS:
        return _extract_pptx(path)
    if suffix in METADATA_ONLY_EXTENSIONS:
        return _extract_generic_metadata(path)
    return _extract_generic_metadata(path)


def _infer_tags(root_label: str, relative_path: str) -> list[str]:
    tags = ["documents"]
    if root_label == "downloads":
        tags.append("downloads")
    if root_label.startswith("gdrive"):
        tags.append("drive")
    lowered = f"{root_label} {relative_path}".lower()
    for token, tag in PATH_TAGS.items():
        if token in lowered and tag not in tags:
            tags.append(tag)
    return tags


def _infer_orgs(relative_path: str, title: str, text: str, explicit: list[str] | None = None) -> list[str]:
    haystack = f" {relative_path} {title} {text[:4000]} ".lower()
    orgs = list(explicit or [])
    for needle, label in ORG_PATTERNS:
        if needle in haystack and label not in orgs:
            orgs.append(label)
    return _clean_list(orgs)


def _resolve_people(
    *,
    identity_cache: IdentityCache,
    emails: list[str],
    names: list[str],
) -> list[str]:
    links: list[str] = []
    for email in emails:
        direct = identity_cache.resolve("email", email)
        if direct and direct not in links:
            links.append(direct)
    for name in names:
        direct = identity_cache.resolve("name", name)
        if direct and direct not in links:
            links.append(direct)
    return links


def _render_body(item: dict[str, Any]) -> str:
    lines: list[str] = [
        f"Library root: {item['library_root']}",
        f"Relative path: {item['relative_path']}",
        f"Filename: {item['filename']}",
    ]
    if item.get("title"):
        lines.append(f"Title: {item['title']}")
    if item.get("document_type"):
        lines.append(f"Document type: {item['document_type']}")
    if item.get("document_date"):
        lines.append(f"Document date: {item['document_date']}")
    if item.get("date_start") or item.get("date_end"):
        lines.append(f"Date range: {item.get('date_start', '')} -> {item.get('date_end', '')}")
    if item.get("location"):
        lines.append(f"Location: {item['location']}")
    if item.get("authors"):
        lines.append(f"Authors: {', '.join(item['authors'])}")
    if item.get("counterparties"):
        lines.append(f"Counterparties: {', '.join(item['counterparties'])}")
    if item.get("emails"):
        lines.append(f"Emails: {', '.join(item['emails'])}")
    if item.get("phones"):
        lines.append(f"Phones: {', '.join(item['phones'])}")
    if item.get("websites"):
        lines.append(f"Websites: {', '.join(item['websites'])}")
    if item.get("sheet_names"):
        lines.append(f"Sheets: {', '.join(item['sheet_names'])}")
    if item.get("page_count"):
        lines.append(f"Page count: {item['page_count']}")
    if item.get("people"):
        lines.append(f"Resolved people: {', '.join(item['people'])}")
    if item.get("orgs"):
        lines.append(f"Organizations: {', '.join(item['orgs'])}")
    if item.get("tags"):
        lines.append(f"Tags: {', '.join(item['tags'])}")
    if item.get("extraction_status"):
        lines.append(f"Extraction status: {item['extraction_status']}")
    if item.get("quality_flags"):
        lines.append(f"Quality flags: {', '.join(item['quality_flags'])}")
    if item.get("file_modified_at"):
        lines.append(f"Modified at: {item['file_modified_at']}")
    if item.get("mime_type"):
        lines.append(f"MIME type: {item['mime_type']}")
    if item.get("size_bytes"):
        lines.append(f"Size bytes: {item['size_bytes']}")
    text = _clean(item.get("text", ""))
    if text:
        excerpt = item["text"][:MAX_BODY_CHARS].rstrip()
        lines.append("")
        lines.append("Extracted text:")
        lines.append("")
        lines.append(excerpt)
    return "\n".join(lines).strip()


def _relative_path(path: Path, root_path: Path) -> str:
    return path.relative_to(root_path).as_posix()


class FileLibrariesAdapter(BaseAdapter):
    source_id = "file-libraries"
    enable_person_resolution = False
    preload_existing_uid_index = False

    def _analysis_log(self, message: str, *, verbose: bool) -> None:
        if not verbose:
            return
        timestamp = datetime.now().isoformat(timespec="seconds")
        print(f"[{timestamp}] {self.source_id}: {message}", flush=True)

    def _selected_roots(self, roots: str | list[str] | tuple[str, ...] | None) -> list[tuple[str, Path]]:
        if roots is None:
            labels = list(ROOTS)
        elif isinstance(roots, str):
            labels = [item.strip() for item in roots.split(",") if item.strip()]
        else:
            labels = [str(item).strip() for item in roots if str(item).strip()]
        selected: list[tuple[str, Path]] = []
        for label in labels:
            if label in ROOTS:
                selected.append((label, ROOTS[label].expanduser().resolve()))
                continue
            path = Path(label).expanduser().resolve()
            root_label = ROOT_PATH_TO_LABEL.get(path, f"custom:{path.name.lower()}")
            selected.append((root_label, path))
        return selected

    def _load_existing_hashes(self, vault_path: str) -> dict[str, str]:
        hashes: dict[str, str] = {}
        documents_dir = Path(vault_path) / "Documents"
        if not documents_dir.exists():
            return hashes
        for path in documents_dir.rglob("*.md"):
            rel_path = path.relative_to(vault_path)
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if str(frontmatter.get("type", "")).strip() != "document":
                continue
            source_id = _clean(frontmatter.get("source_id", ""))
            metadata_sha = _clean(frontmatter.get("metadata_sha", ""))
            if source_id and metadata_sha:
                hashes[source_id] = metadata_sha
        return hashes

    def _build_item(
        self,
        *,
        path: Path,
        root_label: str,
        root_path: Path,
        identity_cache: IdentityCache,
    ) -> dict[str, Any]:
        stat = path.stat()
        relative_path = _relative_path(path, root_path)
        payload = _extract_payload(path)
        source_id = f"{root_label}:{relative_path}"
        filename = path.name
        extension = _normalize_extension(path)
        file_created_at = _iso_from_timestamp(stat.st_birthtime if hasattr(stat, "st_birthtime") else stat.st_ctime)
        file_modified_at = _iso_from_timestamp(stat.st_mtime)
        text = _trim_text(str(payload.get("text", "")))
        document_type = _clean(payload.get("document_type", "")) or extension
        authors = _clean_list(payload.get("authors", []))
        extracted_names = (
            _extract_name_candidates(text)
            if document_type in {"pdf", "doc", "docx", "email_export", "contact_card"}
            else []
        )
        counterparties = _clean_list(
            payload.get("counterparties", []) + _counterparties_from_filename(path) + extracted_names
        )
        email_candidates = _clean_list(
            list(payload.get("emails", [])) + EMAIL_RE.findall(text[:12000]) + EMAIL_RE.findall(relative_path)
        )
        phones = _clean_list(list(payload.get("phones", [])) + _extract_phones(text[:12000]))
        websites = _clean_list(list(payload.get("websites", [])) + _extract_urls(text[:12000]))
        people = _resolve_people(
            identity_cache=identity_cache,
            emails=email_candidates,
            names=authors + counterparties + extracted_names,
        )
        for wikilink in _scan_text_people_mentions(text, identity_cache):
            if wikilink not in people:
                people.append(wikilink)
        title = _clean(payload.get("title", "")) or _filename_title(path)
        orgs = _infer_orgs(
            relative_path,
            title,
            text,
            explicit=_clean_list(payload.get("orgs", [])),
        )
        document_date = _clean(payload.get("document_date", "")) or _detect_date(f"{relative_path} {title}")
        date_start = _clean(payload.get("date_start", "")) or document_date
        date_end = _clean(payload.get("date_end", ""))
        tags = _infer_tags(root_label, relative_path)
        source_suffix = _normalize_source_suffix(root_label)
        extraction_status = _clean(payload.get("extraction_status", ""))
        if not extraction_status:
            if text:
                extraction_status = "content_extracted"
            elif any(
                payload.get(key)
                for key in ("emails", "phones", "websites", "location", "date_start", "date_end", "sheet_names")
            ):
                extraction_status = "structured_only"
            else:
                extraction_status = "metadata_only"
        quality_flags = _clean_list(payload.get("quality_flags", []))
        if extraction_status == "metadata_only" and "metadata_only" not in quality_flags:
            quality_flags.append("metadata_only")
        if document_date and not _clean(payload.get("document_date", "")) and "date_from_path" not in quality_flags:
            quality_flags.append("date_from_path")
        item = {
            "kind": "document",
            "source": [FILE_LIBRARY_SOURCE, f"{FILE_LIBRARY_SOURCE}.{source_suffix}"],
            "source_id": source_id,
            "library_root": root_label,
            "relative_path": relative_path,
            "filename": filename,
            "extension": extension,
            "mime_type": mimetypes.guess_type(filename)[0] or "",
            "size_bytes": int(_path_size(path)),
            "content_sha": _sha256_file(path),
            "file_created_at": file_created_at,
            "file_modified_at": file_modified_at,
            "date_start": date_start,
            "date_end": date_end,
            "document_type": document_type,
            "document_date": document_date,
            "title": title,
            "description": _clean(payload.get("description", "")),
            "authors": authors,
            "counterparties": counterparties,
            "emails": email_candidates,
            "phones": phones,
            "websites": websites,
            "location": _clean(payload.get("location", "")),
            "sheet_names": _clean_list(payload.get("sheet_names", [])),
            "page_count": int(payload.get("page_count", 0) or 0),
            "text_source": _clean(payload.get("text_source", "")) or "metadata_only",
            "extracted_text_sha": _sha256_text(text) if text else "",
            "extraction_status": extraction_status,
            "quality_flags": quality_flags,
            "summary": title or filename,
            "created": _date_bucket(document_date, file_modified_at, file_created_at),
            "people": people,
            "orgs": orgs,
            "tags": tags,
            "text": text,
        }
        item["metadata_sha"] = _sha256_text(
            json.dumps(
                {
                    key: value
                    for key, value in item.items()
                    if key not in {"kind", "text", "summary", "created", "source", "source_id"}
                },
                sort_keys=True,
                ensure_ascii=False,
                default=str,
            )
        )[:16]
        item["body"] = _render_body(item)
        return item

    def _inventory_candidates(
        self,
        selected_roots: list[tuple[str, Path]],
        *,
        max_files: int | None = None,
        verbose: bool = False,
    ) -> tuple[list[tuple[str, Path, Path]], Counter, dict[str, dict[str, Any]]]:
        started_at = perf_counter()
        candidates: list[tuple[str, Path, Path]] = []
        skip_counts: Counter[str] = Counter()
        root_stats: dict[str, dict[str, Any]] = {}
        for root_label, root_path in selected_roots:
            root_started_at = perf_counter()
            stats = root_stats.setdefault(root_label, {"candidates": 0, "skip_details": {}})
            if not root_path.exists():
                skip_counts["missing_root"] += 1
                stats["skip_details"]["missing_root"] = 1
                continue
            for dirpath, dirnames, filenames in os.walk(root_path):
                current_dir = Path(dirpath)
                if _is_probably_repo_tree(current_dir, dirnames, filenames):
                    skip_counts["skipped_repo_tree"] += 1
                    stats["skip_details"]["skipped_repo_tree"] = stats["skip_details"].get("skipped_repo_tree", 0) + 1
                    dirnames[:] = []
                    continue
                package_dirs: list[str] = []
                kept_dirnames: list[str] = []
                for dirname in dirnames:
                    dir_path = current_dir / dirname
                    suffix = dir_path.suffix.lower()
                    if suffix in PACKAGE_EXTENSIONS:
                        package_dirs.append(dirname)
                        continue
                    if dirname.startswith(".") or dirname in SKIP_DIR_NAMES or suffix in SKIP_DIR_SUFFIXES:
                        skip_counts["skipped_directory"] += 1
                        stats["skip_details"]["skipped_directory"] = (
                            stats["skip_details"].get("skipped_directory", 0) + 1
                        )
                        continue
                    kept_dirnames.append(dirname)
                dirnames[:] = kept_dirnames
                candidate_paths = [current_dir / name for name in filenames] + [
                    current_dir / name for name in package_dirs
                ]
                for path in candidate_paths:
                    suffix = path.suffix.lower()
                    if suffix not in INCLUDED_EXTENSIONS:
                        skip_counts["unsupported_extension"] += 1
                        stats["skip_details"]["unsupported_extension"] = (
                            stats["skip_details"].get("unsupported_extension", 0) + 1
                        )
                        continue
                    if path.name.startswith("~$"):
                        skip_counts["temp_lock_file"] += 1
                        stats["skip_details"]["temp_lock_file"] = stats["skip_details"].get("temp_lock_file", 0) + 1
                        continue
                    if path.name.startswith("."):
                        skip_counts["hidden_file"] += 1
                        stats["skip_details"]["hidden_file"] = stats["skip_details"].get("hidden_file", 0) + 1
                        continue
                    try:
                        size_bytes = _path_size(path)
                    except OSError:
                        skip_counts["stat_failed"] += 1
                        stats["skip_details"]["stat_failed"] = stats["skip_details"].get("stat_failed", 0) + 1
                        continue
                    if size_bytes > MAX_FILE_BYTES:
                        skip_counts["skipped_too_large"] += 1
                        stats["skip_details"]["skipped_too_large"] = (
                            stats["skip_details"].get("skipped_too_large", 0) + 1
                        )
                        continue
                    candidates.append((root_label, root_path, path))
                    stats["candidates"] += 1
                    if max_files is not None and len(candidates) >= max(0, int(max_files)):
                        break
                if max_files is not None and len(candidates) >= max(0, int(max_files)):
                    break
            self._analysis_log(
                f"inventory root={root_label} candidates={stats['candidates']} elapsed_s={perf_counter() - root_started_at:.2f}",
                verbose=verbose,
            )
            if max_files is not None and len(candidates) >= max(0, int(max_files)):
                break
        self._analysis_log(
            f"inventory complete candidates={len(candidates)} elapsed_s={perf_counter() - started_at:.2f}",
            verbose=verbose,
        )
        return candidates, skip_counts, root_stats

    def stage_documents(
        self,
        vault_path: str,
        stage_dir: str | Path,
        *,
        roots: str | list[str] | tuple[str, ...] | None = None,
        max_files: int | None = None,
        quick_update: bool = True,
        workers: int | None = None,
        progress_every: int = 100,
        verbose: bool = False,
    ) -> dict[str, Any]:
        selected_roots = self._selected_roots(roots)
        stage_path = Path(stage_dir).expanduser().resolve()
        stage_path.mkdir(parents=True, exist_ok=True)
        candidates, inventory_skips, root_stats = self._inventory_candidates(
            selected_roots,
            max_files=max_files,
            verbose=verbose,
        )
        existing_hashes = self._load_existing_hashes(vault_path) if quick_update else {}
        max_workers = max(
            1, int(workers or os.environ.get("HFA_FILE_LIBRARY_STAGE_WORKERS") or min(8, os.cpu_count() or 1))
        )
        started_at = perf_counter()
        processed = 0
        emitted = 0
        analysis_skips: Counter[str] = Counter()
        per_root_emitted: Counter[str] = Counter()
        stage_files: dict[str, Path] = {}
        handles: dict[str, Any] = {}

        failures_path = stage_path / "analysis-failures.jsonl"
        failure_handle = failures_path.open("a", encoding="utf-8")
        serialized_candidates = [(root_label, str(root_path), str(path)) for root_label, root_path, path in candidates]

        executor: ProcessPoolExecutor | None = None
        try:
            if max_workers <= 1:
                results_iter = (_stage_analyze_candidate(candidate) for candidate in serialized_candidates)
            else:
                executor = ProcessPoolExecutor(
                    max_workers=max_workers,
                    initializer=_init_stage_worker,
                    initargs=(str(vault_path),),
                )
                futures = [executor.submit(_stage_analyze_candidate, candidate) for candidate in serialized_candidates]
                results_iter = (future.result() for future in as_completed(futures))

            for root_label, item, skip_reason, raw_path, error_message in results_iter:
                processed += 1
                if skip_reason:
                    analysis_skips[skip_reason] += 1
                    if skip_reason == "read_failed":
                        failure_handle.write(
                            json.dumps(
                                {
                                    "root": root_label,
                                    "path": raw_path,
                                    "reason": skip_reason,
                                    "error": error_message,
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                elif item is not None:
                    if quick_update and existing_hashes.get(item["source_id"]) == item["metadata_sha"]:
                        analysis_skips["skipped_unchanged_documents"] += 1
                    else:
                        emitted += 1
                        per_root_emitted[root_label] += 1
                        output_path = stage_files.setdefault(
                            root_label,
                            stage_path / f"{_stage_file_basename(root_label)}.jsonl",
                        )
                        if root_label not in handles:
                            handles[root_label] = output_path.open("a", encoding="utf-8")
                        handles[root_label].write(json.dumps(item, ensure_ascii=False) + "\n")
                if verbose and processed % max(1, progress_every) == 0:
                    elapsed = max(perf_counter() - started_at, 0.001)
                    rate = processed / elapsed
                    remaining = max(len(candidates) - processed, 0)
                    eta_seconds = remaining / rate if rate > 0 else 0.0
                    self._analysis_log(
                        f"stage progress processed={processed}/{len(candidates)} emitted={emitted} skipped={sum(analysis_skips.values())} "
                        f"rate={rate:.2f}/s eta_s={eta_seconds:.1f}",
                        verbose=verbose,
                    )
        finally:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=False)
            for handle in handles.values():
                handle.close()
            failure_handle.close()

        manifest = {
            "stage_dir": str(stage_path),
            "roots": [label for label, _ in selected_roots],
            "total_candidates": len(candidates),
            "processed_candidates": processed,
            "emitted_documents": emitted,
            "inventory_skip_details": dict(sorted(inventory_skips.items())),
            "analysis_skip_details": dict(sorted(analysis_skips.items())),
            "per_root_emitted": dict(sorted(per_root_emitted.items())),
            "root_stats": root_stats,
            "stage_files": {label: str(path) for label, path in sorted(stage_files.items())},
            "failures_path": str(failures_path),
            "workers": max_workers,
            "elapsed_seconds": round(perf_counter() - started_at, 3),
        }
        (stage_path / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        self._analysis_log(
            f"stage complete processed={processed} emitted={emitted} elapsed_s={manifest['elapsed_seconds']:.2f}",
            verbose=verbose,
        )
        return manifest

    def _iter_staged_batches(
        self,
        stage_dir: str | Path,
        *,
        batch_size: int,
        max_files: int | None = None,
    ) -> Iterable[FetchedBatch]:
        stage_path = Path(stage_dir).expanduser().resolve()
        manifest_path = stage_path / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing stage manifest: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        stage_files = manifest.get("stage_files", {})
        if not isinstance(stage_files, dict):
            raise ValueError("Stage manifest is missing stage_files")
        sequence = 0
        emitted = 0
        batch_items: list[dict[str, Any]] = []
        for _root_label, raw_path in sorted(stage_files.items()):
            stage_file = Path(str(raw_path))
            if not stage_file.exists():
                continue
            with stage_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    if max_files is not None and emitted >= max(0, int(max_files)):
                        break
                    batch_items.append(json.loads(line))
                    emitted += 1
                    if len(batch_items) >= batch_size:
                        yield FetchedBatch(items=list(batch_items), sequence=sequence)
                        sequence += 1
                        batch_items = []
                if max_files is not None and emitted >= max(0, int(max_files)):
                    break
        if batch_items:
            yield FetchedBatch(items=list(batch_items), sequence=sequence)

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for batch in self.fetch_batches(vault_path, cursor, config=config, **kwargs):
            cursor.update(batch.cursor_patch)
            items.extend(batch.items)
        return items

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ) -> Iterable[FetchedBatch]:
        if kwargs.get("stage_dir"):
            requested_batch_size = kwargs.get("batch_size")
            batch_size = max(
                1, int(requested_batch_size or os.environ.get("HFA_FILE_LIBRARY_BATCH_SIZE") or DEFAULT_BATCH_SIZE)
            )
            yield from self._iter_staged_batches(
                kwargs["stage_dir"],
                batch_size=batch_size,
                max_files=kwargs.get("max_files"),
            )
            return
        selected_roots = self._selected_roots(kwargs.get("roots"))
        quick_update = bool(kwargs.get("quick_update", True))
        max_files = kwargs.get("max_files")
        batch_size = max(
            1, int(kwargs.get("batch_size") or os.environ.get("HFA_FILE_LIBRARY_BATCH_SIZE") or DEFAULT_BATCH_SIZE)
        )
        existing_hashes = self._load_existing_hashes(vault_path) if quick_update else {}
        identity_cache = IdentityCache(vault_path)
        batch_items: list[dict[str, Any]] = []
        sequence = 0
        scanned_candidates = 0
        emitted_documents = 0
        skipped_since_yield = Counter()
        total_skips = Counter()

        def _cursor_patch() -> dict[str, Any]:
            return {
                "roots": [label for label, _ in selected_roots],
                "scanned_candidates": scanned_candidates,
                "emitted_documents": emitted_documents,
                "skip_details": dict(sorted(total_skips.items())),
            }

        def _yield_batch() -> FetchedBatch:
            nonlocal sequence
            batch = FetchedBatch(
                items=list(batch_items),
                cursor_patch=_cursor_patch(),
                sequence=sequence,
                skipped_count=sum(skipped_since_yield.values()),
                skip_details=dict(sorted(skipped_since_yield.items())),
            )
            sequence += 1
            return batch

        for root_label, root_path in selected_roots:
            if not root_path.exists():
                total_skips["missing_root"] += 1
                skipped_since_yield["missing_root"] += 1
                continue
            for dirpath, dirnames, filenames in os.walk(root_path):
                current_dir = Path(dirpath)
                if _is_probably_repo_tree(current_dir, dirnames, filenames):
                    total_skips["skipped_repo_tree"] += 1
                    skipped_since_yield["skipped_repo_tree"] += 1
                    dirnames[:] = []
                    continue
                package_dirs: list[str] = []
                kept_dirnames: list[str] = []
                for dirname in dirnames:
                    dir_path = current_dir / dirname
                    suffix = dir_path.suffix.lower()
                    if suffix in PACKAGE_EXTENSIONS:
                        package_dirs.append(dirname)
                        continue
                    if dirname.startswith(".") or dirname in SKIP_DIR_NAMES or suffix in SKIP_DIR_SUFFIXES:
                        total_skips["skipped_directory"] += 1
                        skipped_since_yield["skipped_directory"] += 1
                        continue
                    kept_dirnames.append(dirname)
                dirnames[:] = kept_dirnames
                candidates = [current_dir / name for name in filenames] + [current_dir / name for name in package_dirs]
                for path in candidates:
                    scanned_candidates += 1
                    suffix = path.suffix.lower()
                    if suffix not in INCLUDED_EXTENSIONS:
                        total_skips["unsupported_extension"] += 1
                        skipped_since_yield["unsupported_extension"] += 1
                        continue
                    if path.name.startswith("."):
                        total_skips["hidden_file"] += 1
                        skipped_since_yield["hidden_file"] += 1
                        continue
                    try:
                        size_bytes = _path_size(path)
                    except OSError:
                        total_skips["stat_failed"] += 1
                        skipped_since_yield["stat_failed"] += 1
                        continue
                    if size_bytes > MAX_FILE_BYTES:
                        total_skips["skipped_too_large"] += 1
                        skipped_since_yield["skipped_too_large"] += 1
                        continue
                    try:
                        item = self._build_item(
                            path=path,
                            root_label=root_label,
                            root_path=root_path,
                            identity_cache=identity_cache,
                        )
                    except Exception:
                        total_skips["read_failed"] += 1
                        skipped_since_yield["read_failed"] += 1
                        continue
                    if quick_update and existing_hashes.get(item["source_id"]) == item["metadata_sha"]:
                        total_skips["skipped_unchanged_documents"] += 1
                        skipped_since_yield["skipped_unchanged_documents"] += 1
                        continue
                    batch_items.append(item)
                    emitted_documents += 1
                    if max_files is not None and emitted_documents >= max(0, int(max_files)):
                        if batch_items or skipped_since_yield:
                            yield _yield_batch()
                        return
                    if len(batch_items) >= batch_size:
                        yield _yield_batch()
                        batch_items = []
                        skipped_since_yield = Counter()
        if batch_items or skipped_since_yield:
            yield _yield_batch()

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        if _clean(item.get("kind", "")) != "document":
            raise ValueError(f"Unsupported file library record kind: {_clean(item.get('kind', ''))}")
        card = DocumentCard(
            uid=generate_uid("document", FILE_LIBRARY_SOURCE, _clean(item.get("source_id", ""))),
            type="document",
            source=list(item.get("source", [])) or [FILE_LIBRARY_SOURCE],
            source_id=_clean(item.get("source_id", "")),
            created=_clean(item.get("created", "")) or today,
            updated=today,
            summary=_clean(item.get("summary", "")),
            tags=list(item.get("tags", [])),
            people=list(item.get("people", [])),
            orgs=list(item.get("orgs", [])),
            library_root=_clean(item.get("library_root", "")),
            relative_path=_clean(item.get("relative_path", "")),
            filename=_clean(item.get("filename", "")),
            extension=_clean(item.get("extension", "")),
            mime_type=_clean(item.get("mime_type", "")),
            size_bytes=int(item.get("size_bytes", 0) or 0),
            content_sha=_clean(item.get("content_sha", "")),
            metadata_sha=_clean(item.get("metadata_sha", "")),
            file_created_at=_clean(item.get("file_created_at", "")),
            file_modified_at=_clean(item.get("file_modified_at", "")),
            date_start=_clean(item.get("date_start", "")),
            date_end=_clean(item.get("date_end", "")),
            document_type=_clean(item.get("document_type", "")),
            document_date=_clean(item.get("document_date", "")),
            title=_clean(item.get("title", "")),
            description=_clean(item.get("description", "")),
            authors=list(item.get("authors", [])),
            counterparties=list(item.get("counterparties", [])),
            emails=list(item.get("emails", [])),
            phones=list(item.get("phones", [])),
            websites=list(item.get("websites", [])),
            location=_clean(item.get("location", "")),
            sheet_names=list(item.get("sheet_names", [])),
            page_count=int(item.get("page_count", 0) or 0),
            text_source=_clean(item.get("text_source", "")),
            extracted_text_sha=_clean(item.get("extracted_text_sha", "")),
            extraction_status=_clean(item.get("extraction_status", "")),
            quality_flags=list(item.get("quality_flags", [])),
        )
        provenance = deterministic_provenance(card, FILE_LIBRARY_SOURCE)
        return card, provenance, str(item.get("body", "")).strip()

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
