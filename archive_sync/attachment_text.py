"""Extract email-attachment files via the shared document extract library.

Extracted markdown lives **only** on the attachment card body. Email message
cards get a filename + wikilink list — never an OCR/markdown dump into
``message_body``.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_sync.adapters.gmail_http_errors import (
    GmailDailyQuotaExceeded,
    GmailPermissionDenied,
    classify_gmail_error,
)
from archive_sync.extract_cache import get_extract_cache, seed_from_scan_cache
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
    is_extractable,
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
STATUS_FETCH_DENIED = "fetch_denied"
STATUS_FETCHED = "fetched"
STATUS_ALREADY_CACHED = "already_cached"
RETRYABLE_FETCH_STATUSES = frozenset({STATUS_MISSING, STATUS_FETCH_DENIED})
RASTER_FETCH_SKIP = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".heic", ".svg"}


def _fmt_elapsed(seconds: float) -> str:
    total = int(round(max(0.0, seconds)))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def _log_progress(prefix: str, i: int, n: int, t0: float, extra: str = "") -> None:
    elapsed = time.monotonic() - t0
    rate = i / elapsed if elapsed > 0 else 0.0
    remain = (n - i) / rate if rate > 0 else 0.0
    pct = (100.0 * i / n) if n else 100.0
    log.info(
        "%s %s/%s (%.1f%%) elapsed=%s eta_remaining=%s rate_per_s=%.2f %s",
        prefix,
        i,
        n,
        pct,
        _fmt_elapsed(elapsed),
        _fmt_elapsed(remain),
        rate,
        extra,
    )

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
    fetch_only: bool = False,
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
            except GmailDailyQuotaExceeded:
                raise
            except GmailPermissionDenied as exc:
                log.warning(
                    "attachment fetch denied uid=%s account=%s reason=%s",
                    job.uid,
                    job.account_email,
                    exc.reason,
                )
                return AttachmentExtraction(
                    status=STATUS_FETCH_DENIED,
                    filename=filename,
                    uid=job.uid,
                    reason=f"forbidden:{exc.reason}",
                )
            except Exception as exc:
                kind = classify_gmail_error(str(exc))
                if kind == "daily_quota":
                    raise GmailDailyQuotaExceeded(str(exc)) from exc
                if kind == "permission":
                    log.warning(
                        "attachment fetch denied uid=%s account=%s reason=forbidden",
                        job.uid,
                        job.account_email,
                    )
                    return AttachmentExtraction(
                        status=STATUS_FETCH_DENIED,
                        filename=filename,
                        uid=job.uid,
                        reason="forbidden",
                    )
                log.warning(
                    "attachment fetch failed uid=%s message_id=%s kind=%s err=%s",
                    job.uid,
                    job.message_id,
                    kind,
                    exc,
                )
                data = b""
        if data:
            cache_attachment_bytes(vault, job.uid, filename, data)
            if fetch_only:
                return AttachmentExtraction(
                    status=STATUS_FETCHED,
                    filename=filename,
                    uid=job.uid,
                    reason="fetched",
                )
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
            out = AttachmentExtraction.from_result(result, uid=job.uid)
            out.filename = filename
            return out

    if path is None:
        return AttachmentExtraction(status=STATUS_MISSING, filename=filename, uid=job.uid, reason="missing")
    if fetch_only:
        return AttachmentExtraction(
            status=STATUS_ALREADY_CACHED,
            filename=filename,
            uid=job.uid,
            reason="already_cached",
        )

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
    fetch_only: bool = False,
) -> list[AttachmentExtraction]:
    if not jobs:
        return []
    from archive_cli.index_config import get_gmail_api_workers

    worker_count = workers if workers is not None else get_gmail_api_workers()
    worker_count = min(max(1, worker_count), max(1, len(jobs)))
    halt_lock = threading.Lock()
    halted = {"daily_quota": False}

    def _run(job: AttachmentJob) -> AttachmentExtraction:
        if halted["daily_quota"]:
            return AttachmentExtraction(
                status=STATUS_MISSING, filename=job.filename, uid=job.uid, reason="gmail_daily_quota"
            )
        try:
            return extract_job(vault, job, fetch_bytes=fetch_bytes, fetch_only=fetch_only)
        except GmailDailyQuotaExceeded as exc:
            with halt_lock:
                halted["daily_quota"] = True
            log.error("gmail daily quota exceeded; stopping attachment fetches for this run")
            return AttachmentExtraction(
                status=STATUS_MISSING, filename=job.filename, uid=job.uid, reason=str(exc.reason)
            )

    t0 = time.monotonic()
    prefix = "extract-attachment-text fetch" if fetch_only else "extract-attachment-text job"
    if worker_count <= 1 or len(jobs) <= 1:
        results = []
        for idx, job in enumerate(jobs, start=1):
            item = _run(job)
            if idx == 1 or idx % 25 == 0 or idx == len(jobs):
                _log_progress(prefix, idx, len(jobs), t0, extra=f"uid={job.uid} status={item.status}")
            results.append(item)
        return results
    by_uid: dict[str, AttachmentExtraction] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {pool.submit(_run, job): job for job in jobs}
        done = 0
        for future in as_completed(futures):
            job = futures[future]
            try:
                by_uid[job.uid] = future.result()
            except Exception as exc:
                log.warning("attachment job failed uid=%s err=%s", job.uid, exc)
                by_uid[job.uid] = AttachmentExtraction(
                    status=STATUS_FAILED, filename=job.filename, uid=job.uid, reason=str(exc)
                )
            done += 1
            if done == 1 or done % 25 == 0 or done == len(jobs):
                _log_progress(
                    prefix,
                    done,
                    len(jobs),
                    t0,
                    extra=f"uid={job.uid} status={by_uid[job.uid].status}",
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
    if result.status in RETRYABLE_FETCH_STATUSES and not result.text:
        # Leave the card retryable — do not stamp skipped_missing forever.
        return {
            "rel_path": rel_path,
            "status": result.status,
            "reason": result.reason,
            "written": False,
        }
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
    hash_reuse = 0
    for out in results:
        status = str(out.get("status") or "")
        source = str(out.get("text_source") or "")
        if str(out.get("reason") or "") == "hash_reuse":
            hash_reuse += 1
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
        "hash_reuse": hash_reuse,
    }


def _eligible_attachment_jobs(
    vault: Path,
    scan_cache: Any,
    *,
    fetch_bytes: FetchBytesFn | None,
    fetch_only: bool,
    limit: int | None,
) -> tuple[list[tuple[str, AttachmentJob]], dict[str, int]]:
    """Select document-like attachment cards. Resume = skip bytes already on disk."""

    paths = sorted(scan_cache.rel_paths_by_type().get("email_attachment") or [])
    thread_class: dict[str, str] = {}
    if not fetch_only:
        for rel in scan_cache.rel_paths_by_type().get("email_thread") or []:
            tfm = scan_cache.frontmatter_for_rel_path(rel) or {}
            thread_class[str(tfm.get("uid") or Path(rel).stem)] = str(
                tfm.get("triage_classification") or ""
            )

    jobs: list[tuple[str, AttachmentJob]] = []
    skipped_missing = 0
    skipped_unsupported = 0
    skipped_existing = 0
    skipped_raster = 0
    for rel_path in paths:
        fm = scan_cache.frontmatter_for_rel_path(rel_path) or {}
        uid = str(fm.get("uid") or Path(rel_path).stem).strip()
        filename = str(fm.get("filename") or "").strip()
        mime = str(fm.get("mime_type") or "").strip()
        size_bytes = int(fm.get("size_bytes") or 0)
        is_inline = bool(fm.get("is_inline", False))
        if not filename or filename.startswith("ANGjd") or not is_extractable(filename, mime):
            skipped_unsupported += 1
            continue
        if is_skippable_non_doc(filename, mime):
            continue
        if is_lockfile(filename):
            continue
        if size_bytes and size_bytes > MAX_FILE_BYTES:
            continue
        if is_tiny_image(filename, size_bytes, mime) and (is_inline or size_bytes <= TINY_IMAGE_BYTES):
            continue
        local = resolve_local_attachment(vault, uid, filename)
        suffix = Path(safe_filename(filename)).suffix.lower()
        if local is None and (fetch_bytes is not None or fetch_only) and suffix in RASTER_FETCH_SKIP:
            skipped_raster += 1
            continue
        status = str(fm.get("extraction_status") or "").strip()
        sha = str(fm.get("extracted_text_sha") or "").strip()
        if local is not None and fetch_only:
            skipped_existing += 1
            continue
        if local is None and fetch_bytes is None:
            skipped_missing += 1
            continue
        if local is not None and not fetch_only:
            try:
                current = bytes_sha256(local.read_bytes())
            except OSError:
                skipped_missing += 1
                continue
            if sha and sha == current and status in {STATUS_EXTRACTED, *DONE_SKIP_STATUSES}:
                continue
        thread_slug = str(fm.get("thread") or "").strip().strip("[]")
        skip_extract = (not fetch_only) and is_suppressed_classification(
            thread_class.get(thread_slug, "")
        )
        body = ""
        if not fetch_only and status == STATUS_EXTRACTED:
            try:
                _fm, body, _prov = read_note(vault, rel_path)
            except OSError:
                body = ""
        jobs.append((rel_path, _job_from_attachment_card(fm, body, uid=uid, skip_extract=skip_extract)))
        if limit is not None and len(jobs) >= limit:
            break
    return jobs, {
        "total_attachment_cards": len(paths),
        "skipped_missing": skipped_missing,
        "skipped_unsupported": skipped_unsupported,
        "skipped_existing": skipped_existing,
        "skipped_raster": skipped_raster,
    }


def run_attachment_fetch(
    vault: Path,
    *,
    fetch_bytes: FetchBytesFn,
    limit: int | None = None,
) -> dict[str, Any]:
    """Download document-like attachments. Bytes only — no extract, no hosted OCR."""

    from archive_cli.index_config import get_gmail_api_workers
    from archive_cli.vault_cache import VaultScanCache

    vault = Path(vault).resolve()
    log.info("extract-attachment-text fetch-only start vault=%s", vault)
    t0 = time.monotonic()
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    jobs, skip_counts = _eligible_attachment_jobs(
        vault, scan_cache, fetch_bytes=fetch_bytes, fetch_only=True, limit=limit
    )
    workers = min(get_gmail_api_workers(), max(1, len(jobs)))
    log.info(
        "extract-attachment-text fetch-only eligible=%s/%s skipped_existing=%s "
        "skipped_unsupported=%s skipped_raster=%s workers=%s",
        len(jobs),
        skip_counts["total_attachment_cards"],
        skip_counts["skipped_existing"],
        skip_counts["skipped_unsupported"],
        skip_counts["skipped_raster"],
        workers,
    )
    results = extract_jobs(
        vault,
        [job for _rel, job in jobs],
        fetch_bytes=fetch_bytes,
        workers=workers,
        fetch_only=True,
    )
    downloaded = 0
    already_cached = 0
    denied = 0
    failed = 0
    missing = 0
    bytes_written = 0
    for (_rel, job), item in zip(jobs, results, strict=True):
        if item.status == STATUS_FETCHED:
            downloaded += 1
            cached = resolve_local_attachment(vault, job.uid, job.filename)
            if cached is not None:
                try:
                    bytes_written += cached.stat().st_size
                except OSError:
                    pass
        elif item.status == STATUS_ALREADY_CACHED:
            already_cached += 1
        elif item.status == STATUS_FETCH_DENIED:
            denied += 1
        elif item.status == STATUS_FAILED:
            failed += 1
        else:
            missing += 1
    log.info(
        "extract-attachment-text fetch-only done elapsed=%s downloaded=%s already_cached=%s "
        "denied=%s failed=%s missing=%s bytes_written=%s",
        _fmt_elapsed(time.monotonic() - t0),
        downloaded,
        already_cached + skip_counts["skipped_existing"],
        denied,
        failed,
        missing,
        bytes_written,
    )
    return {
        "vault": str(vault),
        "fetch_only": True,
        "dry_run": False,
        "total_attachment_cards": skip_counts["total_attachment_cards"],
        "eligible": len(jobs),
        "downloaded": downloaded,
        "already_cached": already_cached + skip_counts["skipped_existing"],
        "denied": denied,
        "failed": failed,
        "missing": missing,
        "bytes_written": bytes_written,
        "skipped_unsupported": skip_counts["skipped_unsupported"],
        "skipped_raster": skip_counts["skipped_raster"],
        "workers": workers,
        "results": [
            {
                "uid": item.uid,
                "status": item.status,
                "reason": item.reason,
                "filename": item.filename,
            }
            for item in results
        ],
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
    uid_to_rel = scan_cache.uid_to_rel_path()
    seeded = seed_from_scan_cache(scan_cache)
    jobs, skip_counts = _eligible_attachment_jobs(
        vault, scan_cache, fetch_bytes=fetch_bytes, fetch_only=False, limit=limit
    )
    skipped_missing = skip_counts["skipped_missing"]
    skipped_unsupported = skip_counts["skipped_unsupported"]

    workers = min(get_gmail_api_workers(), max(1, len(jobs)))
    log.info(
        "extract-attachment-text eligible=%s/%s skipped_unsupported=%s cache_seeded=%s workers=%s",
        len(jobs),
        skip_counts["total_attachment_cards"],
        skipped_unsupported,
        seeded,
        workers,
    )
    write_lock = threading.Lock()
    halt_lock = threading.Lock()
    halted = {"daily_quota": False}
    results_by_uid: dict[str, dict[str, Any]] = {}
    done = {"n": 0}

    def _extract_and_write(rel_path: str, job: AttachmentJob) -> dict[str, Any]:
        if halted["daily_quota"]:
            result = AttachmentExtraction(
                status=STATUS_MISSING, filename=job.filename, uid=job.uid, reason="gmail_daily_quota"
            )
            with write_lock:
                out = _write_attachment_extraction(vault, rel_path, result, dry_run=dry_run)
                out["uid"] = job.uid
                out.setdefault("reason", result.reason)
                return out
        try:
            result = extract_job(vault, job, fetch_bytes=fetch_bytes)
        except GmailDailyQuotaExceeded as exc:
            with halt_lock:
                halted["daily_quota"] = True
            log.error("gmail daily quota exceeded; stopping attachment fetches for this run")
            result = AttachmentExtraction(
                status=STATUS_MISSING, filename=job.filename, uid=job.uid, reason=str(exc.reason)
            )
        with write_lock:
            out = _write_attachment_extraction(vault, rel_path, result, dry_run=dry_run)
            out["uid"] = job.uid
            out.setdefault("reason", result.reason)
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
            done["n"] += 1
            n = done["n"]
            if n == 1 or n % 25 == 0 or n == len(jobs):
                log.info(
                    "extract-attachment-text write %s/%s status=%s uid=%s",
                    n,
                    len(jobs),
                    result.status,
                    job.uid,
                )
        return out

    if not jobs:
        results: list[dict[str, Any]] = []
    elif workers <= 1:
        results = [_extract_and_write(rel_path, job) for rel_path, job in jobs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_extract_and_write, rel_path, job): job for rel_path, job in jobs}
            for future in as_completed(futures):
                job = futures[future]
                try:
                    results_by_uid[job.uid] = future.result()
                except Exception as exc:
                    log.warning("attachment extract+write failed uid=%s err=%s", job.uid, exc)
                    results_by_uid[job.uid] = {
                        "uid": job.uid,
                        "status": STATUS_FAILED,
                        "reason": str(exc),
                    }
        results = [results_by_uid[job.uid] for _, job in jobs]

    counts = _yield_counts(results)
    ok = counts["local_ok"] + counts["hosted_ok"]
    errors = counts["failed"]
    cache_stats = get_extract_cache().stats()
    log.info(
        "extract-attachment-text done processed=%s local_ok=%s hosted_ok=%s hash_reuse=%s "
        "needs_ocr=%s skipped=%s failed=%s cache_hits=%s",
        len(results),
        counts["local_ok"],
        counts["hosted_ok"],
        counts["hash_reuse"],
        counts["needs_ocr"],
        counts["skipped"],
        counts["failed"],
        cache_stats["hits"],
    )
    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "total_attachment_cards": skip_counts["total_attachment_cards"],
        "processed": len(results),
        "ok": ok,
        "errors": errors,
        **counts,
        "skipped_missing": skipped_missing,
        "skipped_unsupported": skipped_unsupported,
        "cache": cache_stats,
        "results": results,
    }
