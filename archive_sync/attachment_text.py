"""Extract email-attachment files via the shared document extract library.

Extracted markdown lives **only** on the attachment card body. Email message
cards get a filename + wikilink list — never an OCR/markdown dump into
``message_body``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_sync.document_extract import (
    DONE_SKIP_STATUSES,
    MAX_FILE_BYTES,
    TINY_IMAGE_BYTES,
    STATUS_EXTRACTED,
    STATUS_FAILED,
    STATUS_LOCKFILE,
    STATUS_MISSING,
    STATUS_NEEDS_OCR,
    STATUS_NON_DOC,
    STATUS_SUPPRESSED,
    STATUS_TINY_IMAGE,
    STATUS_TOO_LARGE,
    ExtractResult,
    bytes_sha256,
    extract_from_bytes,
    extract_from_path,
    is_lockfile,
    is_skippable_non_doc,
    is_suppressed_classification,
    is_tiny_image,
    safe_filename,
)
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, write_card

log = logging.getLogger("ppa.attachment_text")

ATTACHMENTS_DIR = "Attachments"
ATTACHMENTS_SECTION_HEADING = "## Attachments"
ATTACHMENTS_LIST_SENTINEL = "<!-- ppa-attachment-list -->"
# Legacy OCR dump written by 535679b — strip on sight, never re-emit.
ATTACHMENTS_SECTION_SENTINEL = "<!-- ppa-attachment-text -->"
_OCR_DUMP_RE = re.compile(
    rf"\n*{re.escape(ATTACHMENTS_SECTION_SENTINEL)}\n{re.escape(ATTACHMENTS_SECTION_HEADING)}\n.*\Z",
    re.DOTALL,
)
_LIST_RE = re.compile(
    rf"\n*{re.escape(ATTACHMENTS_LIST_SENTINEL)}\n{re.escape(ATTACHMENTS_SECTION_HEADING)}\n.*\Z",
    re.DOTALL,
)

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

    @classmethod
    def from_result(cls, result: ExtractResult, *, uid: str = "") -> AttachmentExtraction:
        return cls(
            status=result.status,
            text=result.text,
            text_source=result.text_source,
            extracted_text_sha=result.extracted_text_sha,
            filename=result.filename,
            uid=uid,
            reason=result.reason,
        )


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
    is_inline: bool = False
    skip_hosted: bool = False
    skip_extract: bool = False


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


def strip_ocr_dump_section(body: str) -> str:
    """Remove a legacy OCR dump from an email message body."""

    return _OCR_DUMP_RE.sub("", str(body or "")).rstrip()


def strip_attachments_section(body: str) -> str:
    text = strip_ocr_dump_section(body)
    return _LIST_RE.sub("", text).rstrip()


def extract_attachments_section(body: str) -> str:
    text = strip_ocr_dump_section(body)
    match = _LIST_RE.search(text)
    if not match:
        return ""
    return match.group(0).strip()


def render_attachment_list(items: list[tuple[str, str]]) -> str:
    """Filename + wikilink list only — never extracted markdown."""

    lines: list[str] = []
    seen: set[str] = set()
    for uid, filename in items:
        slug = str(uid or "").strip().strip("[]")
        name = safe_filename(filename)
        if not slug or slug in seen:
            continue
        seen.add(slug)
        lines.append(f"- [[{slug}]] {name}")
    if not lines:
        return ""
    return f"{ATTACHMENTS_LIST_SENTINEL}\n{ATTACHMENTS_SECTION_HEADING}\n\n" + "\n".join(lines)


def render_attachments_section(extractions: list[AttachmentExtraction]) -> str:
    """Compatibility wrapper: list only, no OCR text."""

    return render_attachment_list([(item.uid, item.filename) for item in extractions])


def merge_message_body(raw_body: str, section: str) -> str:
    stripped = strip_attachments_section(raw_body)
    section = (section or "").strip()
    if not section:
        return stripped
    if stripped:
        return f"{stripped}\n\n{section}"
    return section


def preserve_message_attachments_section(incoming_body: str, existing_body: str) -> str:
    """Keep a filename list; drop any legacy OCR dump."""

    incoming = strip_ocr_dump_section(incoming_body)
    existing = strip_ocr_dump_section(existing_body)
    incoming_section = extract_attachments_section(incoming)
    if incoming_section:
        return merge_message_body(strip_attachments_section(incoming), incoming_section)
    existing_section = extract_attachments_section(existing)
    if existing_section:
        return merge_message_body(strip_attachments_section(incoming), existing_section)
    return incoming


def extract_local_file(path: Path, *, skip_hosted: bool = False) -> AttachmentExtraction:
    return AttachmentExtraction.from_result(
        extract_from_path(path, filename=path.name, skip_hosted=skip_hosted)
    )


def extract_job(
    vault: Path,
    job: AttachmentJob,
    *,
    fetch_bytes: FetchBytesFn | None = None,
) -> AttachmentExtraction:
    filename = safe_filename(job.filename)
    if job.skip_extract:
        return AttachmentExtraction(
            status=STATUS_SUPPRESSED, filename=filename, uid=job.uid, reason="suppressed"
        )
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
    skip_hosted = job.skip_hosted or (
        job.is_inline and is_tiny_image(filename, job.size_bytes, job.mime_type)
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
            result = extract_from_bytes(
                data,
                filename=filename,
                mime_type=job.mime_type,
                is_inline=job.is_inline,
                skip_hosted=skip_hosted,
                existing_sha=job.existing_sha,
                existing_status=job.existing_status,
                existing_text=job.existing_text,
                existing_text_source=job.existing_text_source,
            )
            if result.status == STATUS_EXTRACTED and result.reason != "unchanged":
                cache_attachment_bytes(vault, job.uid, filename, data)
            out = AttachmentExtraction.from_result(result, uid=job.uid)
            out.filename = filename
            return out

    if path is None:
        return AttachmentExtraction(status=STATUS_MISSING, filename=filename, uid=job.uid, reason="missing")

    result = extract_from_path(
        path,
        filename=filename,
        mime_type=job.mime_type,
        is_inline=job.is_inline,
        skip_hosted=skip_hosted,
        existing_sha=job.existing_sha,
        existing_status=job.existing_status,
        existing_text=job.existing_text,
        existing_text_source=job.existing_text_source,
    )
    out = AttachmentExtraction.from_result(result, uid=job.uid)
    out.filename = filename
    return out


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
    """Mutate Gmail fetch records: attachment body + message filename list.

    Never copies extracted markdown onto the email message card.
    """

    by_uid = {item.uid: item for item in extractions if item.uid}
    persist_statuses = {
        STATUS_EXTRACTED,
        STATUS_NON_DOC,
        STATUS_TOO_LARGE,
        STATUS_LOCKFILE,
        STATUS_FAILED,
        STATUS_TINY_IMAGE,
        STATUS_SUPPRESSED,
        STATUS_NEEDS_OCR,
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
    by_message: dict[str, list[tuple[str, str]]] = {}
    for record in attachment_records:
        uid = str(record.get("uid") or "").strip()
        filename = str(record.get("filename") or "").strip()
        mid = str(record.get("message_id") or "").strip()
        if uid:
            by_message.setdefault(mid, []).append((uid, filename))
    for message in message_records:
        mid = str(message.get("message_id") or "").strip()
        section = render_attachment_list(by_message.get(mid) or [])
        if not section:
            continue
        message["body"] = merge_message_body(strip_ocr_dump_section(str(message.get("body") or "")), section)


def _job_from_attachment_card(
    fm: dict[str, Any],
    body: str,
    *,
    uid: str,
    skip_extract: bool = False,
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
        is_inline=bool(fm.get("is_inline", False)),
        skip_extract=skip_extract,
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
            "text_source": result.text_source,
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
        "text_source": result.text_source,
        "bytes_out": len(new_body.encode("utf-8")),
        "reason": result.reason,
    }


def _write_message_attachment_list(
    vault: Path,
    message_wikilink: str,
    attachment_uid: str,
    filename: str,
    *,
    dry_run: bool,
    uid_to_rel: dict[str, str],
) -> None:
    if dry_run:
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
    existing_pairs: list[tuple[str, str]] = []
    for link in fm.get("attachments") or []:
        att_uid = str(link).strip().strip("[]")
        if att_uid:
            existing_pairs.append((att_uid, ""))
    existing_pairs.append((attachment_uid, filename))
    # Prefer filenames already listed in the current (non-OCR) section.
    listed = extract_attachments_section(body)
    if listed:
        for match in re.finditer(r"- \[\[([^\]]+)\]\](?:\s+(.+))?", listed):
            existing_pairs.append((match.group(1), (match.group(2) or "").strip()))
    by_uid: dict[str, str] = {}
    for uid, name in existing_pairs:
        if uid and (name or uid not in by_uid):
            by_uid[uid] = name or by_uid.get(uid, "")
    if attachment_uid:
        by_uid[attachment_uid] = filename or by_uid.get(attachment_uid, "")
    section = render_attachment_list(list(by_uid.items()))
    new_body = merge_message_body(strip_attachments_section(body), section)
    if new_body == body:
        return
    card = validate_card_strict(fm)
    write_card(vault, rel, card, new_body, prov)


def _yield_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    local_ok = 0
    hosted_ok = 0
    skipped = 0
    failed = 0
    needs_ocr = 0
    for out in results:
        status = str(out.get("status") or "")
        source = str(out.get("text_source") or "")
        if status == STATUS_EXTRACTED and source == "anydoc_hosted":
            hosted_ok += 1
        elif status == STATUS_EXTRACTED:
            local_ok += 1
        elif status == STATUS_NEEDS_OCR:
            needs_ocr += 1
        elif status == STATUS_FAILED:
            failed += 1
        elif status.startswith("skipped") or status in DONE_SKIP_STATUSES or status == STATUS_MISSING:
            skipped += 1
        else:
            skipped += 1
    return {
        "local_ok": local_ok,
        "hosted_ok": hosted_ok,
        "skipped": skipped,
        "failed": failed,
        "needs_ocr": needs_ocr,
    }


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
    log.info("extract-attachment-text start vault=%s dry_run=%s", vault, dry_run)
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    paths = sorted(scan_cache.rel_paths_by_type().get("email_attachment") or [])
    uid_to_rel = scan_cache.uid_to_rel_path()
    thread_class: dict[str, str] = {}
    for rel in scan_cache.rel_paths_by_type().get("email_thread") or []:
        tfm = scan_cache.frontmatter_for_rel_path(rel) or {}
        thread_class[str(tfm.get("uid") or Path(rel).stem)] = str(
            tfm.get("triage_classification") or ""
        )

    jobs: list[tuple[str, AttachmentJob]] = []
    skipped_missing = 0
    for rel_path in paths:
        fm = scan_cache.frontmatter_for_rel_path(rel_path) or {}
        uid = str(fm.get("uid") or Path(rel_path).stem).strip()
        filename = str(fm.get("filename") or "").strip()
        mime = str(fm.get("mime_type") or "").strip()
        size_bytes = int(fm.get("size_bytes") or 0)
        is_inline = bool(fm.get("is_inline", False))
        if is_skippable_non_doc(filename, mime):
            continue
        if is_tiny_image(filename, size_bytes, mime) and (is_inline or size_bytes <= TINY_IMAGE_BYTES):
            continue
        local = resolve_local_attachment(vault, uid, filename)
        # Hosted OCR is credit-based: do not fetch raster logos/inline images.
        if local is None and fetch_bytes is not None:
            suffix = Path(safe_filename(filename)).suffix.lower()
            if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".heic", ".svg"}:
                continue
        status = str(fm.get("extraction_status") or "").strip()
        sha = str(fm.get("extracted_text_sha") or "").strip()
        if local is None and fetch_bytes is None:
            skipped_missing += 1
            continue
        if local is not None:
            try:
                current = bytes_sha256(local.read_bytes())
            except OSError:
                skipped_missing += 1
                continue
            if sha and sha == current and status in {STATUS_EXTRACTED, *DONE_SKIP_STATUSES}:
                continue
        thread_slug = str(fm.get("thread") or "").strip().strip("[]")
        skip_extract = is_suppressed_classification(thread_class.get(thread_slug, ""))
        body = ""
        if status == STATUS_EXTRACTED:
            try:
                _fm, body, _prov = read_note(vault, rel_path)
            except OSError:
                body = ""
        jobs.append((rel_path, _job_from_attachment_card(fm, body, uid=uid, skip_extract=skip_extract)))
        if limit is not None and len(jobs) >= limit:
            break

    workers = min(get_gmail_api_workers(), max(1, len(jobs)))
    log.info(
        "extract-attachment-text eligible=%s/%s workers=%s",
        len(jobs),
        len(paths),
        workers,
    )
    extracted = [extract_job(vault, job, fetch_bytes=fetch_bytes) for _, job in jobs] if workers <= 1 else []
    if workers > 1 and jobs:
        extracted = extract_jobs(vault, [job for _, job in jobs], fetch_bytes=fetch_bytes, workers=workers)

    results: list[dict[str, Any]] = []
    for (rel_path, job), result in zip(jobs, extracted, strict=True):
        out = _write_attachment_extraction(vault, rel_path, result, dry_run=dry_run)
        out["uid"] = job.uid
        results.append(out)
        if not dry_run:
            fm = scan_cache.frontmatter_for_rel_path(rel_path) or {}
            _write_message_attachment_list(
                vault,
                str(fm.get("message") or ""),
                job.uid,
                job.filename,
                dry_run=dry_run,
                uid_to_rel=uid_to_rel,
            )

    counts = _yield_counts(results)
    ok = counts["local_ok"] + counts["hosted_ok"]
    errors = counts["failed"]
    log.info(
        "extract-attachment-text done processed=%s local_ok=%s hosted_ok=%s needs_ocr=%s skipped=%s failed=%s",
        len(results),
        counts["local_ok"],
        counts["hosted_ok"],
        counts["needs_ocr"],
        counts["skipped"],
        counts["failed"],
    )
    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "total_attachment_cards": len(paths),
        "processed": len(results),
        "ok": ok,
        "errors": errors,
        **counts,
        "skipped_missing": skipped_missing,
        "results": results,
    }
