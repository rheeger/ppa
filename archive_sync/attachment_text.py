"""Extract email-attachment files via the document anydoc / hosted-OCR stack.

Gmail attachment cards are metadata-only (bytes live in Gmail or vault
``Attachments/``). When a file is local — or just downloaded during apply —
this module converts it with ``convert_document_to_markdown`` and writes:

- extracted markdown on the **attachment card body** (retriever does not
  chunk ``email_attachment`` today)
- a ``## Attachments`` section on the **email message card** (already chunked
  as ``message_body``)
- frontmatter ``extraction_status`` / ``text_source`` / ``extracted_text_sha``
  (sha of **source bytes**, used to skip re-OCR)

Do not dump extracted text into YAML.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_sync.adapters.file_libraries import MAX_EXTRACT_CHARS, MAX_FILE_BYTES, _trim_text
from archive_sync.llm_enrichment.document_text_extractor import convert_document_to_markdown
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, write_card

log = logging.getLogger("ppa.attachment_text")

ATTACHMENTS_DIR = "Attachments"
ATTACHMENTS_SECTION_HEADING = "## Attachments"
ATTACHMENTS_SECTION_SENTINEL = "<!-- ppa-attachment-text -->"
_SECTION_RE = re.compile(
    rf"\n*{re.escape(ATTACHMENTS_SECTION_SENTINEL)}\n{re.escape(ATTACHMENTS_SECTION_HEADING)}\n.*\Z",
    re.DOTALL,
)

# Images are scans we still want OCR on when anydoc accepts them.
CONVERTIBLE_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".rtf",
        ".ppt",
        ".pptx",
        ".xls",
        ".xlsx",
        ".csv",
        ".html",
        ".htm",
        ".txt",
        ".md",
        ".png",
        ".jpg",
        ".jpeg",
        ".tif",
        ".tiff",
        ".webp",
        ".gif",
        ".bmp",
    }
)
SKIP_EXTENSIONS = frozenset(
    {
        ".mp3",
        ".m4a",
        ".wav",
        ".aac",
        ".flac",
        ".ogg",
        ".wma",
        ".opus",
        ".mp4",
        ".mov",
        ".avi",
        ".mkv",
        ".webm",
        ".m4v",
        ".wmv",
        ".zip",
        ".tar",
        ".gz",
        ".tgz",
        ".rar",
        ".7z",
        ".bz2",
        ".exe",
        ".dmg",
        ".pkg",
        ".iso",
        ".ics",
        ".vcf",
        ".dat",
    }
)
SKIP_MIME_PREFIXES = ("audio/", "video/")
SKIP_MIME_TYPES = frozenset(
    {
        "application/zip",
        "application/x-tar",
        "application/x-7z-compressed",
        "application/x-rar-compressed",
        "application/gzip",
        "application/x-gzip",
        "application/octet-stream",
        "application/ms-tnef",
        "text/calendar",
        "application/ics",
    }
)
STATUS_EXTRACTED = "content_extracted"
STATUS_TOO_LARGE = "skipped_too_large"
STATUS_NON_DOC = "skipped_non_doc"
STATUS_MISSING = "skipped_missing"
STATUS_LOCKFILE = "skipped_lockfile"
STATUS_FAILED = "failed"
_DONE_SKIP_STATUSES = frozenset({STATUS_TOO_LARGE, STATUS_NON_DOC, STATUS_LOCKFILE})

FetchBytesFn = Callable[[str, str, str], bytes]


@dataclass
class AttachmentExtraction:
    status: str
    text: str = ""
    text_source: str = ""
    extracted_text_sha: str = ""
    filename: str = ""
    uid: str = ""
    reason: str = ""


@dataclass
class AttachmentJob:
    uid: str
    filename: str
    mime_type: str = ""
    size_bytes: int = 0
    message_id: str = ""
    attachment_id: str = ""
    account_email: str = ""
    existing_sha: str = ""
    existing_status: str = ""
    existing_text: str = ""
    existing_text_source: str = ""
    inline_bytes: bytes = field(default_factory=bytes)


def bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_filename(filename: str) -> str:
    name = Path(filename or "").name.strip()
    if not name or name in {".", ".."}:
        return "attachment"
    return name


def vault_attachment_path(vault: Path, uid: str, filename: str) -> Path:
    return Path(vault) / ATTACHMENTS_DIR / uid / safe_filename(filename)


def resolve_local_attachment(vault: Path, uid: str, filename: str) -> Path | None:
    """Return a local cached file if present. Never walks the vault."""

    name = safe_filename(filename)
    if name.startswith("~$"):
        return None
    candidates = [
        vault_attachment_path(vault, uid, name),
        Path(vault) / ATTACHMENTS_DIR / uid / name,
        Path(vault) / ATTACHMENTS_DIR / f"{uid}{Path(name).suffix.lower()}",
    ]
    for path in candidates:
        try:
            if path.is_file() and not path.name.startswith("~$"):
                return path
        except OSError:
            continue
    return None


def cache_attachment_bytes(vault: Path, uid: str, filename: str, data: bytes) -> Path:
    path = vault_attachment_path(vault, uid, filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def is_lockfile(filename: str) -> bool:
    return safe_filename(filename).startswith("~$")


def is_skippable_non_doc(filename: str, mime_type: str) -> bool:
    if is_lockfile(filename):
        return True
    suffix = Path(safe_filename(filename)).suffix.lower()
    mime = (mime_type or "").strip().lower()
    if suffix in SKIP_EXTENSIONS:
        return True
    if any(mime.startswith(prefix) for prefix in SKIP_MIME_PREFIXES):
        return True
    if mime in SKIP_MIME_TYPES and suffix not in CONVERTIBLE_EXTENSIONS:
        return True
    if suffix and suffix not in CONVERTIBLE_EXTENSIONS and not mime.startswith("image/"):
        if mime.startswith("application/") and suffix not in CONVERTIBLE_EXTENSIONS:
            return suffix not in {".pdf"}
        if mime.startswith("text/") and suffix not in CONVERTIBLE_EXTENSIONS:
            return suffix not in {".txt", ".md", ".html", ".htm", ".csv"}
    return False


def strip_attachments_section(body: str) -> str:
    text = str(body or "")
    return _SECTION_RE.sub("", text).rstrip()


def extract_attachments_section(body: str) -> str:
    match = _SECTION_RE.search(str(body or ""))
    if not match:
        return ""
    return match.group(0).strip()


def render_attachments_section(extractions: list[AttachmentExtraction]) -> str:
    blocks: list[str] = []
    for item in extractions:
        if item.status != STATUS_EXTRACTED or not item.text.strip():
            continue
        heading = safe_filename(item.filename) or item.uid or "attachment"
        blocks.append(f"### {heading}\n\n{item.text.strip()}")
    if not blocks:
        return ""
    return f"{ATTACHMENTS_SECTION_SENTINEL}\n{ATTACHMENTS_SECTION_HEADING}\n\n" + "\n\n".join(blocks)


def merge_message_body(raw_body: str, section: str) -> str:
    stripped = strip_attachments_section(raw_body)
    section = (section or "").strip()
    if not section:
        return stripped
    if stripped:
        return f"{stripped}\n\n{section}"
    return section


def preserve_message_attachments_section(incoming_body: str, existing_body: str) -> str:
    """Keep an existing ``## Attachments`` block when incoming body is raw email."""

    incoming_section = extract_attachments_section(incoming_body)
    if incoming_section:
        return merge_message_body(strip_attachments_section(incoming_body), incoming_section)
    existing_section = extract_attachments_section(existing_body)
    if existing_section:
        return merge_message_body(strip_attachments_section(incoming_body), existing_section)
    return incoming_body


def extract_local_file(path: Path) -> AttachmentExtraction:
    """Run the shared document converter. Never calls Firecrawl unless anydoc does."""

    filename = path.name
    if is_lockfile(filename):
        return AttachmentExtraction(status=STATUS_LOCKFILE, filename=filename, reason="lockfile")
    try:
        size = path.stat().st_size
    except OSError:
        return AttachmentExtraction(status=STATUS_MISSING, filename=filename, reason="source_missing")
    if size > MAX_FILE_BYTES:
        return AttachmentExtraction(status=STATUS_TOO_LARGE, filename=filename, reason="too_large")
    try:
        data = path.read_bytes()
    except OSError:
        return AttachmentExtraction(status=STATUS_MISSING, filename=filename, reason="source_missing")
    sha = bytes_sha256(data)
    if is_skippable_non_doc(filename, ""):
        return AttachmentExtraction(
            status=STATUS_NON_DOC, filename=filename, extracted_text_sha=sha, reason="non_doc"
        )
    try:
        text, text_source = convert_document_to_markdown(path)
    except Exception as exc:
        log.warning("attachment convert failed path=%s err=%s", path, exc)
        return AttachmentExtraction(
            status=STATUS_FAILED, filename=filename, extracted_text_sha=sha, reason=str(exc)
        )
    text = _trim_text(str(text or "").strip(), limit=MAX_EXTRACT_CHARS)
    if not text:
        return AttachmentExtraction(
            status=STATUS_FAILED, filename=filename, extracted_text_sha=sha, reason="empty_output"
        )
    return AttachmentExtraction(
        status=STATUS_EXTRACTED,
        text=text,
        text_source=text_source,
        extracted_text_sha=sha,
        filename=filename,
    )


def _reuse_if_unchanged(job: AttachmentJob, sha: str) -> AttachmentExtraction | None:
    if (
        job.existing_sha
        and job.existing_sha == sha
        and job.existing_status == STATUS_EXTRACTED
        and job.existing_text.strip()
    ):
        return AttachmentExtraction(
            status=STATUS_EXTRACTED,
            text=job.existing_text,
            text_source=job.existing_text_source,
            extracted_text_sha=sha,
            filename=job.filename,
            uid=job.uid,
            reason="unchanged",
        )
    if job.existing_sha and job.existing_sha == sha and job.existing_status in _DONE_SKIP_STATUSES:
        return AttachmentExtraction(
            status=job.existing_status,
            extracted_text_sha=sha,
            filename=job.filename,
            uid=job.uid,
            reason="unchanged",
        )
    return None


def extract_job(
    vault: Path,
    job: AttachmentJob,
    *,
    fetch_bytes: FetchBytesFn | None = None,
) -> AttachmentExtraction:
    filename = safe_filename(job.filename)
    if is_lockfile(filename):
        return AttachmentExtraction(
            status=STATUS_LOCKFILE, filename=filename, uid=job.uid, reason="lockfile"
        )
    if is_skippable_non_doc(filename, job.mime_type):
        return AttachmentExtraction(
            status=STATUS_NON_DOC, filename=filename, uid=job.uid, reason="non_doc"
        )
    if job.size_bytes and job.size_bytes > MAX_FILE_BYTES:
        return AttachmentExtraction(
            status=STATUS_TOO_LARGE, filename=filename, uid=job.uid, reason="too_large"
        )

    path = resolve_local_attachment(vault, job.uid, filename)
    if path is None and (job.inline_bytes or fetch_bytes is not None):
        data = job.inline_bytes
        if not data and fetch_bytes is not None and job.message_id and job.attachment_id:
            try:
                data = fetch_bytes(job.message_id, job.attachment_id, job.account_email)
            except Exception as exc:
                log.warning(
                    "attachment fetch failed uid=%s message_id=%s err=%s",
                    job.uid,
                    job.message_id,
                    exc,
                )
                data = b""
        if data:
            if len(data) > MAX_FILE_BYTES:
                return AttachmentExtraction(
                    status=STATUS_TOO_LARGE,
                    filename=filename,
                    uid=job.uid,
                    extracted_text_sha=bytes_sha256(data),
                    reason="too_large",
                )
            reused = _reuse_if_unchanged(job, bytes_sha256(data))
            if reused is not None:
                return reused
            path = cache_attachment_bytes(vault, job.uid, filename, data)

    if path is None:
        return AttachmentExtraction(status=STATUS_MISSING, filename=filename, uid=job.uid, reason="missing")

    try:
        sha = bytes_sha256(path.read_bytes())
    except OSError:
        return AttachmentExtraction(status=STATUS_MISSING, filename=filename, uid=job.uid, reason="source_missing")
    reused = _reuse_if_unchanged(job, sha)
    if reused is not None:
        return reused
    result = extract_local_file(path)
    result.uid = job.uid
    result.filename = filename
    return result


def extract_jobs(
    vault: Path,
    jobs: list[AttachmentJob],
    *,
    fetch_bytes: FetchBytesFn | None = None,
    workers: int | None = None,
) -> list[AttachmentExtraction]:
    if not jobs:
        return []
    from archive_cli.index_config import get_gmail_api_workers

    worker_count = workers if workers is not None else get_gmail_api_workers()
    worker_count = min(max(1, worker_count), max(1, len(jobs)))
    if worker_count <= 1 or len(jobs) <= 1:
        return [extract_job(vault, job, fetch_bytes=fetch_bytes) for job in jobs]
    by_uid: dict[str, AttachmentExtraction] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {pool.submit(extract_job, vault, job, fetch_bytes=fetch_bytes): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                by_uid[job.uid] = future.result()
            except Exception as exc:
                log.warning("attachment job failed uid=%s err=%s", job.uid, exc)
                by_uid[job.uid] = AttachmentExtraction(
                    status=STATUS_FAILED, filename=job.filename, uid=job.uid, reason=str(exc)
                )
    return [by_uid[job.uid] for job in jobs]


def apply_extractions_to_gmail_records(
    message_records: list[dict[str, Any]],
    attachment_records: list[dict[str, Any]],
    extractions: list[AttachmentExtraction],
) -> None:
    """Mutate Gmail fetch records in place: attachment body + message section."""

    by_uid = {item.uid: item for item in extractions if item.uid}
    persist_statuses = {
        STATUS_EXTRACTED,
        STATUS_NON_DOC,
        STATUS_TOO_LARGE,
        STATUS_LOCKFILE,
        STATUS_FAILED,
    }
    for record in attachment_records:
        uid = str(record.get("uid") or "").strip()
        item = by_uid.get(uid)
        if item is None or item.status not in persist_statuses:
            continue
        record["extraction_status"] = item.status
        record["text_source"] = item.text_source
        record["extracted_text_sha"] = item.extracted_text_sha
        if item.status == STATUS_EXTRACTED:
            record["body"] = item.text
    by_message: dict[str, list[AttachmentExtraction]] = {}
    for record in attachment_records:
        uid = str(record.get("uid") or "").strip()
        item = by_uid.get(uid)
        if item is None:
            continue
        mid = str(record.get("message_id") or "").strip()
        by_message.setdefault(mid, []).append(item)
    for message in message_records:
        mid = str(message.get("message_id") or "").strip()
        section = render_attachments_section(by_message.get(mid) or [])
        if not section:
            continue
        message["body"] = merge_message_body(str(message.get("body") or ""), section)


def _job_from_attachment_card(
    fm: dict[str, Any],
    body: str,
    *,
    uid: str,
) -> AttachmentJob:
    return AttachmentJob(
        uid=uid,
        filename=str(fm.get("filename") or "").strip(),
        mime_type=str(fm.get("mime_type") or "").strip(),
        size_bytes=int(fm.get("size_bytes") or 0),
        message_id=str(fm.get("gmail_message_id") or "").strip(),
        attachment_id=str(fm.get("attachment_id") or "").strip(),
        account_email=str(fm.get("account_email") or "").strip(),
        existing_sha=str(fm.get("extracted_text_sha") or "").strip(),
        existing_status=str(fm.get("extraction_status") or "").strip(),
        existing_text=body,
        existing_text_source=str(fm.get("text_source") or "").strip(),
    )


def _write_attachment_extraction(
    vault: Path,
    rel_path: str,
    result: AttachmentExtraction,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    if dry_run:
        return {
            "rel_path": rel_path,
            "status": result.status,
            "dry_run": True,
            "reason": result.reason,
            "bytes_out": len(result.text.encode("utf-8")),
        }
    fm, old_body, existing_prov = read_note(vault, rel_path)
    if str(fm.get("type") or "") != "email_attachment":
        return {"rel_path": rel_path, "status": "skipped", "reason": "not_attachment"}
    field_updates = {
        "extraction_status": result.status,
        "text_source": result.text_source,
        "extracted_text_sha": result.extracted_text_sha,
    }
    merged = {**fm, **{k: v for k, v in field_updates.items() if v}}
    if result.status:
        merged["extraction_status"] = result.status
    card = validate_card_strict(merged)
    new_body = result.text if result.status == STATUS_EXTRACTED else old_body
    incoming: dict[str, ProvenanceEntry] = {}
    today = datetime.now(timezone.utc).date().isoformat()
    for key, value in field_updates.items():
        if not value:
            continue
        incoming[key] = ProvenanceEntry(
            source="attachment_text",
            date=today,
            method="deterministic",
            model=result.text_source or "anydoc",
            input_hash=(result.extracted_text_sha or "")[:16],
        )
    prov = merge_provenance(existing_prov, incoming)
    write_card(vault, rel_path, card, new_body, prov)
    return {
        "rel_path": rel_path,
        "status": result.status,
        "bytes_out": len(new_body.encode("utf-8")),
        "reason": result.reason,
    }


def _append_section_to_message_card(
    vault: Path,
    message_wikilink: str,
    extraction: AttachmentExtraction,
    *,
    dry_run: bool,
    uid_to_rel: dict[str, str],
) -> None:
    if extraction.status != STATUS_EXTRACTED or dry_run:
        return
    slug = message_wikilink.strip().strip("[]")
    if not slug:
        return
    rel = uid_to_rel.get(slug)
    if not rel:
        return
    fm, body, prov = read_note(vault, rel)
    if str(fm.get("type") or "") != "email_message":
        return
    section = render_attachments_section([extraction])
    if not section:
        return
    # Rebuild the whole attachments section from this one file plus any other
    # ### headings already present for different filenames.
    existing = extract_attachments_section(body)
    if existing and f"### {safe_filename(extraction.filename)}" in existing:
        merged_section = existing
        # replace just this file's block
        file_re = re.compile(
            rf"### {re.escape(safe_filename(extraction.filename))}\n\n.*?(?=\n### |\Z)",
            re.DOTALL,
        )
        replacement = f"### {safe_filename(extraction.filename)}\n\n{extraction.text.strip()}\n\n"
        if file_re.search(existing):
            merged_section = file_re.sub(replacement, existing, count=1)
        else:
            merged_section = existing.rstrip() + "\n\n" + replacement
        new_body = merge_message_body(strip_attachments_section(body), merged_section)
    elif existing:
        extra = f"### {safe_filename(extraction.filename)}\n\n{extraction.text.strip()}"
        merged_section = existing.rstrip() + "\n\n" + extra
        new_body = merge_message_body(strip_attachments_section(body), merged_section)
    else:
        new_body = merge_message_body(body, section)
    if new_body == body:
        return
    card = validate_card_strict(fm)
    write_card(vault, rel, card, new_body, prov)


def run_attachment_text_extraction(
    vault: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    fetch_bytes: FetchBytesFn | None = None,
) -> dict[str, Any]:
    """Backfill email_attachment cards that have a local file (or fetch_bytes)."""

    from archive_cli.index_config import get_gmail_api_workers
    from archive_cli.vault_cache import VaultScanCache

    vault = Path(vault).resolve()
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    paths = sorted(scan_cache.rel_paths_by_type().get("email_attachment") or [])
    uid_to_rel = scan_cache.uid_to_rel_path()
    jobs: list[tuple[str, AttachmentJob]] = []
    for rel_path in paths:
        fm = scan_cache.frontmatter_for_rel_path(rel_path) or {}
        uid = str(fm.get("uid") or Path(rel_path).stem).strip()
        filename = str(fm.get("filename") or "").strip()
        local = resolve_local_attachment(vault, uid, filename)
        status = str(fm.get("extraction_status") or "").strip()
        sha = str(fm.get("extracted_text_sha") or "").strip()
        if local is None and fetch_bytes is None:
            continue
        if local is not None:
            try:
                current = bytes_sha256(local.read_bytes())
            except OSError:
                continue
            if sha and sha == current and status in {STATUS_EXTRACTED, *_DONE_SKIP_STATUSES}:
                continue
        body = ""
        if status == STATUS_EXTRACTED:
            try:
                _fm, body, _prov = read_note(vault, rel_path)
            except OSError:
                body = ""
        jobs.append((rel_path, _job_from_attachment_card(fm, body, uid=uid)))
        if limit is not None and len(jobs) >= limit:
            break

    workers = min(get_gmail_api_workers(), max(1, len(jobs)))
    results: list[dict[str, Any]] = []
    extracted = [extract_job(vault, job, fetch_bytes=fetch_bytes) for _, job in jobs] if workers <= 1 else []
    if workers > 1 and jobs:
        extracted = extract_jobs(vault, [job for _, job in jobs], fetch_bytes=fetch_bytes, workers=workers)

    for (rel_path, job), result in zip(jobs, extracted, strict=True):
        out = _write_attachment_extraction(vault, rel_path, result, dry_run=dry_run)
        out["uid"] = job.uid
        results.append(out)
        if result.status == STATUS_EXTRACTED and not dry_run:
            fm = scan_cache.frontmatter_for_rel_path(rel_path) or {}
            _append_section_to_message_card(
                vault,
                str(fm.get("message") or ""),
                result,
                dry_run=dry_run,
                uid_to_rel=uid_to_rel,
            )

    ok = sum(1 for out in results if out.get("status") == STATUS_EXTRACTED)
    skipped = sum(
        1
        for out in results
        if str(out.get("status") or "").startswith("skipped") or out.get("status") in _DONE_SKIP_STATUSES
    )
    errors = sum(1 for out in results if out.get("status") == STATUS_FAILED)
    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "total_attachment_cards": len(paths),
        "processed": len(results),
        "ok": ok,
        "skipped": skipped,
        "errors": errors,
        "results": results,
    }
