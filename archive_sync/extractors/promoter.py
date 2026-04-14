"""Promote staged derived cards from staging directory to vault."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from archive_cli.features import card_activity_at
from archive_sync.extractors.runner import derive_output_rel_path
from archive_vault.vault import read_note_file

log = logging.getLogger("ppa.extractor.promoter")


@dataclass
class PromotionResult:
    moved: int = 0
    skipped: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)
    moved_by_type: dict[str, int] = field(default_factory=dict)


def _vault_rel_path_for_card(card_type: str, uid: str, activity_at: str) -> str:
    """Vault-relative path aligned with extraction runner output layout."""
    hint = (activity_at or "")[:10]
    return derive_output_rel_path(card_type, uid, hint)


def promote_staging(
    vault_path: str,
    staging_dir: str,
    dry_run: bool = False,
) -> PromotionResult:
    """Move staged cards from staging_dir into vault_path (copy-verify-delete)."""
    result = PromotionResult()
    vroot = Path(vault_path)
    sroot = Path(staging_dir)
    if not sroot.is_dir():
        result.error_details.append(f"staging dir not found: {staging_dir}")
        result.errors += 1
        return result

    for src in sroot.rglob("*.md"):
        if src.name.startswith("_"):
            continue
        try:
            rec = read_note_file(src)
        except OSError as exc:
            result.errors += 1
            result.error_details.append(f"{src}: read failed: {exc}")
            continue
        fm = rec.frontmatter
        ct = str(fm.get("type") or "").strip()
        uid = str(fm.get("uid") or "").strip()
        if not ct or not uid:
            result.errors += 1
            result.error_details.append(f"{src}: missing type or uid")
            continue
        try:
            rel = _vault_rel_path_for_card(ct, uid, card_activity_at(fm))
        except Exception as exc:
            result.errors += 1
            result.error_details.append(f"{src}: path error: {exc}")
            continue

        dest = vroot / rel
        try:
            src_bytes = src.read_bytes()
        except OSError as exc:
            result.errors += 1
            result.error_details.append(f"{src}: read bytes: {exc}")
            continue

        if dest.is_file():
            try:
                dest_bytes = dest.read_bytes()
            except OSError as exc:
                result.errors += 1
                result.error_details.append(f"{dest}: read failed: {exc}")
                continue
            if dest_bytes == src_bytes:
                result.skipped += 1
                continue

        if dry_run:
            result.moved += 1
            result.moved_by_type[ct] = result.moved_by_type.get(ct, 0) + 1
            continue

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        except OSError as exc:
            result.errors += 1
            result.error_details.append(f"{src} -> {dest}: copy failed: {exc}")
            continue

        if dest.stat().st_size != len(src_bytes):
            result.errors += 1
            result.error_details.append(f"{dest}: size mismatch after copy")
            try:
                dest.unlink(missing_ok=True)
            except OSError:
                pass
            continue

        try:
            src.unlink()
        except OSError as exc:
            result.errors += 1
            result.error_details.append(f"{src}: unlink after copy failed: {exc}")
            continue

        result.moved += 1
        result.moved_by_type[ct] = result.moved_by_type.get(ct, 0) + 1

    return result
