"""Junk email-attachment cards: inline logos, signatures, tiny rasters, ANGjd tokens.

Same criteria as the seed purge (``logs/ppa-seed-purge-junk-attachments``).
Gmail apply uses ``should_emit_email_attachment`` so cards are never written.
Maintain runs ``run_junk_attachment_purge`` for anything that slipped through.
"""

from __future__ import annotations

import logging
import re
import shutil
import time
from collections import Counter
from pathlib import Path
from typing import Any

from archive_sync.document_extract import (
    DONE_TEXT_SOURCES,
    STATUS_EXTRACTED,
    TINY_IMAGE_BYTES,
    TINY_IMAGE_EXTENSIONS,
    is_tiny_image,
    safe_filename,
)
from archive_sync.file_identity import FileIdentityIndex, uid_from_wikilink, wikilink_uid

log = logging.getLogger("ppa.junk_attachments")

IMAGE_MIMES = frozenset(
    {
        "image/png",
        "image/jpeg",
        "image/jpg",
        "image/gif",
        "image/webp",
        "image/bmp",
        "image/x-ms-bmp",
    }
)
DOC_SUFFIXES = frozenset(
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
    }
)
IMAGE001_RE = re.compile(r"^image\d+\.(png|jpe?g|gif|webp|bmp)$", re.I)
JUNK_WORD_RE = re.compile(
    r"(?:^|[\s_\-./])(logo|signature|outlook|cid)(?:$|[\s_\-./])",
    re.I,
)
SIG_RE = re.compile(r"(?:^|[\s_\-./])sig(?:$|[\s_\-./]|\.(?:png|jpe?g|gif|webp|bmp)$)", re.I)
LARGE_IMAGE_BYTES = 100 * 1024
ATTACHMENT_UID_PREFIX = "hfa-email-attachment-"
ATTACHMENTS_DIR = "Attachments"


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


def is_image_attachment(filename: str, mime: str) -> bool:
    mime = (mime or "").strip().lower()
    suffix = Path(safe_filename(filename)).suffix.lower()
    if mime in IMAGE_MIMES or mime.startswith("image/"):
        if suffix in {".tif", ".tiff", ".pdf"}:
            return False
        return True
    return suffix in TINY_IMAGE_EXTENSIONS


def junk_filename_reason(filename: str) -> str:
    raw = (filename or "").strip()
    if not raw:
        return "nameless"
    if raw.startswith("ANGjd"):
        return "angjd"
    lower = raw.lower()
    if lower.startswith("cid:") or "cid:" in lower:
        return "cid"
    name = Path(safe_filename(raw)).name
    if IMAGE001_RE.match(name):
        return "image001"
    if JUNK_WORD_RE.search(name) or JUNK_WORD_RE.search(raw):
        if re.search(r"logo", name, re.I):
            return "logo"
        if re.search(r"signature", name, re.I):
            return "signature"
        if re.search(r"outlook", name, re.I):
            return "outlook"
        return "cid"
    if SIG_RE.search(name):
        return "sig"
    return ""


def classify_email_attachment(fm: dict[str, Any], body: str = "") -> tuple[str, str]:
    """Return ``(action, reason)`` where action is ``delete`` or ``keep``.

    Conservative: when in doubt keep. Real document suffixes and successful
    extracts stay. Inline/tiny/named junk rasters and Gmail token names go.
    """

    filename = str(fm.get("filename") or "").strip()
    mime = str(fm.get("mime_type") or "").strip().lower()
    try:
        size = int(fm.get("size_bytes") or 0)
    except (TypeError, ValueError):
        size = 0
    is_inline = bool(fm.get("is_inline", False))
    status = str(fm.get("extraction_status") or "").strip()
    text_source = str(fm.get("text_source") or "").strip()
    suffix = Path(safe_filename(filename)).suffix.lower() if filename else ""

    real_extract = (
        status == STATUS_EXTRACTED and text_source in DONE_TEXT_SOURCES and bool((body or "").strip())
    )
    if real_extract:
        return "keep", "keep_extracted"

    if suffix in DOC_SUFFIXES and filename and not filename.startswith("ANGjd"):
        return "keep", "keep_document"

    fname_junk = junk_filename_reason(filename)
    img = is_image_attachment(filename, mime)

    if fname_junk in {"angjd", "nameless"}:
        return "delete", fname_junk
    if fname_junk == "cid":
        return "delete", "cid"

    if img:
        if is_inline:
            return "delete", "inline_image"
        if 0 < size <= TINY_IMAGE_BYTES or is_tiny_image(filename, size, mime):
            return "delete", "tiny_image"
        if fname_junk in {"logo", "signature", "sig", "outlook"}:
            return "delete", fname_junk
        if fname_junk == "image001" and size <= LARGE_IMAGE_BYTES:
            return "delete", "image001"
        if size > LARGE_IMAGE_BYTES and not is_inline:
            return "keep", "keep_large_image"
        return "keep", "keep_midsize_image"

    if fname_junk:
        return "keep", "keep_nonimage_junkname"
    return "keep", "keep_other"


def should_emit_email_attachment(
    *,
    filename: str,
    mime_type: str = "",
    size_bytes: int = 0,
    is_inline: bool = False,
) -> bool:
    """True when Gmail apply should write an email_attachment card."""

    action, _reason = classify_email_attachment(
        {
            "filename": filename,
            "mime_type": mime_type,
            "size_bytes": size_bytes,
            "is_inline": is_inline,
        }
    )
    return action == "keep"


def _safe_under_vault(vault: Path, rel: str) -> Path | None:
    rel = str(rel or "").strip()
    if not rel:
        return None
    root = vault.resolve()
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        log.warning("junk-attachment path-escape skipped rel_path=%s", rel)
        return None
    return candidate


def _delete_sidecar_bytes(vault: Path, uid: str) -> int:
    removed = 0
    att_dir = _safe_under_vault(vault, f"{ATTACHMENTS_DIR}/{uid}")
    if att_dir is not None and att_dir.is_dir():
        shutil.rmtree(att_dir)
        removed += 1
    if att_dir is not None:
        parent = att_dir.parent
        if parent.is_dir():
            for leftover in parent.glob(f"{uid}.*"):
                if leftover.is_file():
                    leftover.unlink()
                    removed += 1
    return removed


def _unlink_from_message(
    vault: Path,
    message_rel: str,
    purged_uids: set[str],
    *,
    dry_run: bool,
) -> bool:
    from archive_sync.attachment_text import (
        extract_attachments_section,
        merge_message_body,
        render_attachment_list,
        strip_attachments_section,
    )
    from archive_vault.schema import validate_card_strict
    from archive_vault.vault import read_note, write_card

    fm, body, prov = read_note(vault, message_rel)
    if str(fm.get("type") or "") != "email_message":
        return False
    existing = [str(link).strip() for link in (fm.get("attachments") or []) if str(link).strip()]
    kept_links = [link for link in existing if uid_from_wikilink(link) not in purged_uids]
    listed: list[tuple[str, str]] = []
    section = extract_attachments_section(body)
    if section:
        for match in re.finditer(r"- \[\[([^\]]+)\]\](?:\s+(.+))?", section):
            uid = match.group(1)
            if uid not in purged_uids:
                listed.append((uid, (match.group(2) or "").strip()))
    by_uid: dict[str, str] = {}
    for link in kept_links:
        uid = uid_from_wikilink(link)
        if uid:
            by_uid[uid] = ""
    for uid, name in listed:
        if uid:
            by_uid[uid] = name or by_uid.get(uid, "")
    new_links = [wikilink_uid(uid) for uid in by_uid]
    new_section = render_attachment_list(list(by_uid.items()))
    new_body = merge_message_body(strip_attachments_section(body), new_section)
    if new_links == existing and new_body == body:
        return False
    if dry_run:
        return True
    merged = {**fm, "attachments": new_links, "has_attachments": bool(new_links)}
    write_card(vault, message_rel, validate_card_strict(merged), new_body, prov)
    return True


def _purge_index(store: Any, uids: list[str]) -> int:
    if store is None or not uids:
        return 0
    index = getattr(store, "index", None)
    if index is None or not hasattr(index, "_connect"):
        return 0
    schema = str(getattr(index, "schema", "ppa"))
    try:
        from archive_cli.corpus_hygiene.state_store import purge_card_uids

        with index._connect() as conn:
            n = purge_card_uids(conn, schema, uids)
        return n
    except Exception as exc:
        log.warning("junk-attachment index purge skipped err=%s", exc)
        return 0


def run_junk_attachment_purge(
    vault: Path,
    *,
    dry_run: bool = False,
    store: Any | None = None,
    uid_allowlist: set[str] | None = None,
    progress_every: int = 5000,
) -> dict[str, Any]:
    """Delete slipped-through junk email_attachment cards. Cache walk, no os.walk."""

    from archive_cli.corpus_hygiene.apply import delete_vault_markdown
    from archive_cli.vault_cache import VaultScanCache

    vault = Path(vault).resolve()
    log.info("junk-attachment purge start vault=%s dry_run=%s", vault, dry_run)
    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    paths = list(scan_cache.rel_paths_by_type().get("email_attachment") or [])
    allow = {str(uid).strip() for uid in (uid_allowlist or set()) if str(uid).strip()}
    reasons: Counter[str] = Counter()
    deletes: list[dict[str, Any]] = []
    t0 = time.monotonic()
    for i, rel in enumerate(paths, start=1):
        fm = scan_cache.frontmatter_for_rel_path(rel) or {}
        uid = str(fm.get("uid") or Path(rel).stem).strip()
        if allow and uid not in allow:
            continue
        if uid and not uid.startswith(ATTACHMENT_UID_PREFIX):
            reasons["keep_wrong_prefix"] += 1
            continue
        body = ""
        status = str(fm.get("extraction_status") or "").strip()
        if status == STATUS_EXTRACTED:
            try:
                body = scan_cache.body_for_rel_path(rel) or ""
            except Exception:
                body = ""
        action, reason = classify_email_attachment({**fm, "uid": uid}, body)
        reasons[reason] += 1
        if action == "delete":
            deletes.append(
                {
                    "uid": uid,
                    "rel_path": rel,
                    "reason": reason,
                    "message": str(fm.get("message") or "").strip(),
                    "filename": str(fm.get("filename") or ""),
                }
            )
        if i == 1 or i % progress_every == 0 or i == len(paths):
            _log_progress("junk-attachment scan", i, len(paths), t0, extra=f"delete={len(deletes)}")

    purged_uids = [str(item["uid"]) for item in deletes if item.get("uid")]
    rels = [str(item["rel_path"]) for item in deletes if item.get("rel_path")]
    files_deleted = 0
    sidecars = 0
    parents_updated = 0
    dirty: list[str] = []
    if deletes and not dry_run:
        files_deleted = delete_vault_markdown(vault, rels, progress_every=progress_every)
        for item in deletes:
            sidecars += _delete_sidecar_bytes(vault, str(item["uid"]))
        parent_uids: list[str] = []
        for item in deletes:
            parent = uid_from_wikilink(str(item.get("message") or ""))
            if parent:
                parent_uids.append(parent)
        parent_set = set(parent_uids)
        if parent_set:
            rows = scan_cache.frontmatter_rows_for_uids(parent_set)
            purged = set(purged_uids)
            for row in rows:
                rel = str(row.get("rel_path") or "")
                uid = str(row.get("uid") or "")
                if not rel or not uid:
                    continue
                if _unlink_from_message(vault, rel, purged, dry_run=False):
                    parents_updated += 1
                    dirty.append(uid)
        _purge_index(store, purged_uids)
        try:
            FileIdentityIndex().drop_uids(purged_uids)
        except Exception as exc:
            log.warning("junk-attachment identity drop skipped err=%s", exc)
    elif deletes and dry_run:
        dirty.extend(uid_from_wikilink(str(item.get("message") or "")) for item in deletes)
        dirty = [uid for uid in dirty if uid]

    log.info(
        "junk-attachment purge done scanned=%s delete=%s files=%s sidecars=%s parents=%s dry_run=%s reasons=%s",
        len(paths),
        len(deletes),
        files_deleted,
        sidecars,
        parents_updated,
        dry_run,
        dict(reasons),
    )
    return {
        "vault": str(vault),
        "dry_run": dry_run,
        "cards_scanned": len(paths),
        "purged": len(deletes),
        "files_deleted": files_deleted,
        "sidecars_deleted": sidecars,
        "parents_updated": parents_updated,
        "reason_counts": dict(reasons),
        "purged_uids": purged_uids,
        "dirty_uids": sorted(set(dirty)),
    }
