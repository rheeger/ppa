"""Re-extract document body text with markitdown when ingestion left garbled/plain binary text."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hfa.provenance import ProvenanceEntry, merge_provenance
from hfa.schema import validate_card_strict
from hfa.vault import read_note, write_card

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
    """Whether this card should be run through markitdown (idempotent for already-markitdown text)."""

    ts = str(card_fm.get("text_source") or "").strip().lower()
    if ts == "markitdown":
        return False
    ext = str(card_fm.get("extension") or "").strip().lower().lstrip(".")
    if ts == "plain" and ext in _RICH_EXTENSIONS:
        return True
    flags = card_fm.get("quality_flags") or []
    if isinstance(flags, list) and "metadata_only" in {str(x) for x in flags}:
        return True
    return False


def resolve_source_file(library_root: str, relative_path: str) -> Path | None:
    """Return absolute path to the indexed library file, or None if paths are unusable."""

    root = (library_root or "").strip()
    rel = (relative_path or "").strip()
    if not root or not rel:
        return None
    p = Path(root).expanduser() / rel
    try:
        p = p.resolve()
    except OSError:
        return None
    return p if p.is_file() else None


def extract_markdown_text(source_path: Path) -> str:
    """Convert a file to markdown/plain text using markitdown."""

    from markitdown import MarkItDown

    md = MarkItDown()
    result = md.convert(str(source_path))
    text = getattr(result, "text_content", None) or getattr(result, "text", None) or ""
    return str(text).strip()


def _body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def reextract_one_card(
    vault: Path,
    rel_path: str,
    *,
    dry_run: bool,
    model: str = "markitdown",
) -> dict[str, Any]:
    """Re-read source file, replace card body, set text_source and extracted_text_sha. Returns a status dict."""

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
        new_body = extract_markdown_text(src)
    except Exception as exc:
        log.warning("markitdown failed rel_path=%s src=%s err=%s", rel_path, src, exc)
        return {"rel_path": rel_path, "status": "error", "reason": str(exc)}

    if not new_body:
        return {"rel_path": rel_path, "status": "skipped", "reason": "empty_output"}

    if dry_run:
        return {
            "rel_path": rel_path,
            "status": "ok",
            "dry_run": True,
            "bytes_out": len(new_body.encode("utf-8")),
            "source": str(src),
        }

    field_updates: dict[str, Any] = {
        "text_source": "markitdown",
        "extracted_text_sha": _body_sha256(new_body),
    }
    merged = {**fm, **field_updates}
    card = validate_card_strict(merged)
    incoming: dict[str, ProvenanceEntry] = {}
    for key in field_updates:
        incoming[key] = ProvenanceEntry(
            source="document_text_extractor",
            date=datetime.now(timezone.utc).date().isoformat(),
            method="deterministic",
            model=model,
            input_hash=field_updates["extracted_text_sha"][:16],
        )
    prov = merge_provenance(existing_prov, incoming)
    write_card(vault, rel_path, card, new_body, prov)
    return {"rel_path": rel_path, "status": "ok", "bytes_out": len(new_body.encode("utf-8"))}


def run_document_text_extraction(
    vault: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    """Scan vault for document cards needing re-extraction; process each."""

    from archive_mcp.vault_cache import VaultScanCache

    vault = Path(vault).resolve()
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    paths = sorted(scan_cache.rel_paths_by_type().get("document") or [])
    results: list[dict[str, Any]] = []
    ok = 0
    skipped = 0
    errors = 0
    n = 0
    for rel_path in paths:
        fm = scan_cache.frontmatter_for_rel_path(rel_path)
        if not needs_markitdown_extraction(fm):
            continue
        if limit is not None and n >= limit:
            break
        n += 1
        out = reextract_one_card(vault, rel_path, dry_run=dry_run)
        results.append(out)
        st = out.get("status")
        if st == "ok":
            ok += 1
        elif st == "skipped":
            skipped += 1
        else:
            errors += 1

    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "total_document_cards": len(paths),
        "processed": len(results),
        "ok": ok,
        "skipped": skipped,
        "errors": errors,
        "results": results,
    }
