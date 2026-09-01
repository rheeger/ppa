"""Re-extract document body text when ingestion left garbled/plain binary text."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_sync.adapters.file_libraries import resolve_library_root
from archive_sync.document_extract import (
    ANYDOC_EXTENSIONS,
    CONVERTIBLE_EXTENSIONS,
    DONE_TEXT_SOURCES,
    MAX_FILE_BYTES,
    STATUS_EXTRACTED,
    bytes_sha256,
    convert_document_to_markdown,
    extract_from_path,
    is_lockfile,
)
from archive_sync.extract_cache import get_extract_cache, seed_from_scan_cache
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, write_card

log = logging.getLogger("ppa.document_text_extractor")

_RICH_EXTENSIONS = frozenset(
    {
        "rtf",
        "doc",
        "docx",
        "xlsx",
        "xls",
        "pptx",
        "html",
        "htm",
    }
)


def needs_markitdown_extraction(card_fm: dict[str, Any]) -> bool:
    """Whether this card should be run through the shared extract stack."""

    ts = str(card_fm.get("text_source") or "").strip().lower()
    if ts in DONE_TEXT_SOURCES:
        return False
    ext = str(card_fm.get("extension") or "").strip().lower().lstrip(".")
    if ts == "plain" and ext in {"txt", "md", "json"}:
        return False
    suffix = f".{ext}" if ext else ""
    if suffix in CONVERTIBLE_EXTENSIONS or suffix in ANYDOC_EXTENSIONS:
        return True
    if ts == "plain" and ext in _RICH_EXTENSIONS:
        return True
    flags = card_fm.get("quality_flags") or []
    if isinstance(flags, list) and "metadata_only" in {str(x) for x in flags}:
        return True
    return False


def resolve_source_file(library_root: str, relative_path: str) -> Path | None:
    """Return absolute path to the indexed library file, or None if paths are unusable.

    ``library_root`` may be a ROOTS / CUSTOM_ROOTS label (``documents``,
    ``gdrive.personal``, ``custom:requested record``, …) or an existing directory.
    Office lock files (``~$…``) are skipped.
    """

    rel = (relative_path or "").strip()
    if not rel or is_lockfile(Path(rel).name):
        return None
    root = resolve_library_root(library_root)
    if root is None:
        return None
    p = root / rel
    try:
        p = p.resolve()
    except OSError:
        return None
    return p if p.is_file() else None


def extract_markdown_text(source_path: Path) -> str:
    """Convert a file to markdown/plain text (anydoc local-first, then fallbacks)."""

    text, _source = convert_document_to_markdown(source_path)
    return text


def reextract_one_card(
    vault: Path,
    rel_path: str,
    *,
    dry_run: bool,
    model: str = "markitdown",
) -> dict[str, Any]:
    """Re-read source file, replace card body, set text_source and extracted_text_sha."""

    vault = Path(vault).resolve()
    fm, _old_body, existing_prov = read_note(vault, rel_path)
    if str(fm.get("type") or "") != "document":
        return {"rel_path": rel_path, "status": "skipped", "reason": "not_document"}

    if not needs_markitdown_extraction(fm):
        return {"rel_path": rel_path, "status": "skipped", "reason": "not_eligible"}

    src = resolve_source_file(str(fm.get("library_root") or ""), str(fm.get("relative_path") or ""))
    if src is None:
        return {"rel_path": rel_path, "status": "skipped", "reason": "source_missing"}

    try:
        if src.stat().st_size > MAX_FILE_BYTES:
            return {"rel_path": rel_path, "status": "skipped", "reason": "too_large"}
    except OSError:
        return {"rel_path": rel_path, "status": "skipped", "reason": "source_missing"}

    result = extract_from_path(
        src,
        filename=src.name,
        existing_sha=str(fm.get("extracted_text_sha") or "").strip(),
        existing_status=str(fm.get("extraction_status") or "").strip(),
        existing_text=_old_body if str(fm.get("text_source") or "") in DONE_TEXT_SOURCES else "",
        existing_text_source=str(fm.get("text_source") or "").strip(),
    )
    if result.status != STATUS_EXTRACTED or not result.text:
        return {
            "rel_path": rel_path,
            "status": "skipped" if result.status.startswith("skipped") or result.status == "needs_ocr" else "error",
            "reason": result.reason or result.status,
            "text_source": result.text_source,
        }

    new_body = result.text
    text_source = result.text_source
    if dry_run:
        return {
            "rel_path": rel_path,
            "status": "ok",
            "dry_run": True,
            "bytes_out": len(new_body.encode("utf-8")),
            "source": str(src),
            "text_source": text_source,
            "reason": result.reason,
        }

    field_updates: dict[str, Any] = {
        "text_source": text_source,
        "extracted_text_sha": result.extracted_text_sha or bytes_sha256(src.read_bytes()),
        "extraction_status": STATUS_EXTRACTED,
    }
    merged = {**fm, **field_updates}
    card = validate_card_strict(merged)
    incoming: dict[str, ProvenanceEntry] = {}
    for key in field_updates:
        incoming[key] = ProvenanceEntry(
            source="document_text_extractor",
            date=datetime.now(timezone.utc).date().isoformat(),
            method="deterministic",
            model=text_source if model == "markitdown" else model,
            input_hash=str(field_updates["extracted_text_sha"])[:16],
        )
    prov = merge_provenance(existing_prov, incoming)
    write_card(vault, rel_path, card, new_body, prov)
    sha = str(field_updates.get("extracted_text_sha") or fm.get("content_sha") or "").strip()
    uid = str(fm.get("uid") or "").strip()
    if sha and uid:
        try:
            from archive_sync.file_identity import register_ingested_file

            register_ingested_file(vault, uid=uid, rel_path=rel_path, sha256=sha)
        except Exception as exc:
            log.warning("file-identity extract link skipped uid=%s err=%s", uid, exc)
    return {
        "rel_path": rel_path,
        "status": "ok",
        "bytes_out": len(new_body.encode("utf-8")),
        "text_source": text_source,
        "reason": result.reason,
    }


def run_document_text_extraction(
    vault: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    """Scan vault for document cards needing re-extraction; process each."""

    from archive_cli.index_config import get_rebuild_workers
    from archive_cli.vault_cache import VaultScanCache

    vault = Path(vault).resolve()
    log.info("extract-document-text start vault=%s dry_run=%s", vault, dry_run)
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    seeded = seed_from_scan_cache(scan_cache)
    paths = sorted(scan_cache.rel_paths_by_type().get("document") or [])
    eligible: list[str] = []
    for rel_path in paths:
        fm = scan_cache.frontmatter_for_rel_path(rel_path)
        if not needs_markitdown_extraction(fm):
            continue
        eligible.append(rel_path)
        if limit is not None and len(eligible) >= limit:
            break

    workers = min(get_rebuild_workers(), max(1, len(eligible)))
    log.info(
        "extract-document-text eligible=%s/%s cache_seeded=%s workers=%s",
        len(eligible),
        len(paths),
        seeded,
        workers,
    )
    results: list[dict[str, Any]] = []
    if workers <= 1 or len(eligible) <= 1:
        results = [reextract_one_card(vault, rel_path, dry_run=dry_run) for rel_path in eligible]
    else:
        by_path: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(reextract_one_card, vault, rel_path, dry_run=dry_run): rel_path for rel_path in eligible
            }
            for future in as_completed(futures):
                by_path[futures[future]] = future.result()
        results = [by_path[rel_path] for rel_path in eligible]

    ok = sum(1 for out in results if out.get("status") == "ok")
    skipped = sum(1 for out in results if out.get("status") == "skipped")
    errors = sum(1 for out in results if out.get("status") not in {"ok", "skipped"})
    local_ok = sum(1 for out in results if out.get("status") == "ok" and out.get("text_source") != "anydoc_hosted")
    hosted_ok = sum(1 for out in results if out.get("status") == "ok" and out.get("text_source") == "anydoc_hosted")
    hash_reuse = sum(1 for out in results if out.get("reason") == "hash_reuse")
    cache_stats = get_extract_cache().stats()
    log.info(
        "extract-document-text done processed=%s local_ok=%s hosted_ok=%s hash_reuse=%s skipped=%s failed=%s cache_hits=%s",
        len(results),
        local_ok,
        hosted_ok,
        hash_reuse,
        skipped,
        errors,
        cache_stats["hits"],
    )

    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "total_document_cards": len(paths),
        "processed": len(results),
        "ok": ok,
        "skipped": skipped,
        "errors": errors,
        "local_ok": local_ok,
        "hosted_ok": hosted_ok,
        "hash_reuse": hash_reuse,
        "cache": cache_stats,
        "results": results,
    }
