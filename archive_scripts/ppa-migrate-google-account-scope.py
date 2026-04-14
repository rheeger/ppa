#!/usr/bin/env python3
"""Migrate existing Google-derived notes to account-scoped canonical identities."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive_sync.adapters.calendar_events import CalendarEventsAdapter, _event_identity, _event_uid
from archive_sync.adapters.gmail_messages import (
    GmailMessagesAdapter,
    _attachment_identity,
    _attachment_uid,
    _message_identity,
    _message_uid,
    _thread_identity,
    _thread_uid,
)
from archive_vault.schema import validate_card_permissive
from archive_vault.vault import iter_note_paths, read_note_file, write_card

TARGET_ROOTS = {"Email", "EmailThreads", "EmailAttachments", "Calendar"}
WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(\|[^\]]*)?\]\]")


@dataclass(slots=True)
class MigrationEntry:
    rel_path: Path
    frontmatter: dict[str, Any]
    body: str
    provenance: dict[str, Any]
    new_uid: str
    new_source_id: str
    new_rel_path: Path


def _normalize_account_email(account_email: str) -> str:
    return account_email.strip().lower()


def _rewrite_wikilinks(content: str, uid_mapping: dict[str, str]) -> str:
    def _replace(match: re.Match[str]) -> str:
        target = match.group(1).strip()
        suffix = match.group(2) or ""
        return f"[[{uid_mapping.get(target, target)}{suffix}]]"

    return WIKILINK_RE.sub(_replace, content)


def _rewrite_link_value(value: Any, uid_mapping: dict[str, str]) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[[") and stripped.endswith("]]"):
            return _rewrite_wikilinks(value, uid_mapping)
        return value
    if isinstance(value, list):
        return [_rewrite_link_value(item, uid_mapping) for item in value]
    return value


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f".tmp_{path.stem}_", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _build_migration_entry(vault: Path, note, *, account_email: str) -> MigrationEntry | None:
    top_level = note.rel_path.parts[0] if note.rel_path.parts else ""
    if top_level not in TARGET_ROOTS:
        return None
    frontmatter = dict(note.frontmatter)
    if _normalize_account_email(str(frontmatter.get("account_email", ""))) != account_email:
        return None

    card_type = str(frontmatter.get("type", "")).strip()
    gmail_adapter = GmailMessagesAdapter()
    calendar_adapter = CalendarEventsAdapter()

    if card_type == "email_thread":
        gmail_thread_id = str(frontmatter.get("gmail_thread_id", "")).strip()
        new_uid = _thread_uid(account_email, gmail_thread_id)
        new_source_id = _thread_identity(account_email, gmail_thread_id)
    elif card_type == "email_message":
        gmail_message_id = str(frontmatter.get("gmail_message_id", "")).strip()
        new_uid = _message_uid(account_email, gmail_message_id)
        new_source_id = _message_identity(account_email, gmail_message_id)
    elif card_type == "email_attachment":
        gmail_message_id = str(frontmatter.get("gmail_message_id", "")).strip()
        attachment_id = str(frontmatter.get("attachment_id", "")).strip()
        new_uid = _attachment_uid(account_email, gmail_message_id, attachment_id)
        new_source_id = _attachment_identity(account_email, gmail_message_id, attachment_id)
    elif card_type == "calendar_event":
        calendar_id = str(frontmatter.get("calendar_id", "")).strip()
        event_id = str(frontmatter.get("event_id", "")).strip()
        new_uid = _event_uid(account_email, calendar_id, event_id)
        new_source_id = _event_identity(account_email, calendar_id, event_id)
    else:
        return None

    if card_type.startswith("email_"):
        updated = {**frontmatter, "uid": new_uid, "source_id": new_source_id}
        card = validate_card_permissive(updated)
        new_rel_path = Path(gmail_adapter._card_rel_path(vault, card))
    else:
        updated = {**frontmatter, "uid": new_uid, "source_id": new_source_id}
        card = validate_card_permissive(updated)
        new_rel_path = Path(calendar_adapter._card_rel_path(vault, card))

    return MigrationEntry(
        rel_path=note.rel_path,
        frontmatter=frontmatter,
        body=note.body,
        provenance=dict(note.provenance),
        new_uid=new_uid,
        new_source_id=new_source_id,
        new_rel_path=new_rel_path,
    )


def _iter_google_notes(vault: Path):
    for root_name in sorted(TARGET_ROOTS):
        root = vault / root_name
        if not root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [name for name in dirnames if not name.startswith(".")]
            for file_name in sorted(filenames):
                if file_name.startswith(".") or not file_name.endswith(".md"):
                    continue
                yield read_note_file(Path(dirpath) / file_name, vault_root=vault)


def _candidate_link_paths(vault: Path) -> list[Path]:
    pattern = r"hfa-(?:email-thread|email-message|email-attachment|calendar-event)-"
    try:
        proc = subprocess.run(
            ["rg", "-l", pattern, str(vault)],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        proc = None
    if proc is None or proc.returncode not in (0, 1):
        return [vault / rel_path for rel_path in iter_note_paths(vault)]
    if proc.returncode == 1 or not proc.stdout.strip():
        return []
    return [Path(line.strip()) for line in proc.stdout.splitlines() if line.strip()]


def _backup_entries(vault: Path, entries: list[MigrationEntry], backup_dir: Path) -> None:
    for entry in entries:
        source = vault / entry.rel_path
        target = backup_dir / entry.rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    sync_state = vault / "_meta" / "sync-state.json"
    if sync_state.exists():
        target = backup_dir / "_meta" / "sync-state.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(sync_state, target)


def _migrate_entries(
    vault: Path,
    entries: list[MigrationEntry],
    *,
    uid_mapping: dict[str, str],
    dry_run: bool,
) -> dict[str, int]:
    report = {
        "notes_migrated": 0,
        "paths_renamed": 0,
        "wikilinks_rewritten": 0,
    }
    if dry_run:
        report["notes_migrated"] = len(entries)
        report["paths_renamed"] = sum(1 for entry in entries if entry.rel_path != entry.new_rel_path)
        return report

    print(json.dumps({"phase": "write-migrated-notes", "entries": len(entries)}, sort_keys=True), flush=True)
    new_paths = {entry.new_rel_path for entry in entries}
    old_paths_to_remove: list[Path] = []
    for entry in entries:
        updated_frontmatter = dict(entry.frontmatter)
        updated_frontmatter["uid"] = entry.new_uid
        updated_frontmatter["source_id"] = entry.new_source_id
        for field_name, value in list(updated_frontmatter.items()):
            updated_frontmatter[field_name] = _rewrite_link_value(value, uid_mapping)
        card = validate_card_permissive(updated_frontmatter)
        write_card(vault, str(entry.new_rel_path), card, body=entry.body, provenance=entry.provenance)
        report["notes_migrated"] += 1
        if entry.rel_path != entry.new_rel_path:
            old_paths_to_remove.append(entry.rel_path)
            report["paths_renamed"] += 1

    for rel_path in old_paths_to_remove:
        if rel_path in new_paths:
            continue
        old_path = vault / rel_path
        if old_path.exists():
            old_path.unlink()

    print(json.dumps({"phase": "rewrite-wikilinks", "uid_mappings": len(uid_mapping)}, sort_keys=True), flush=True)
    for path in _candidate_link_paths(vault):
        if not path.exists():
            continue
        original = path.read_text(encoding="utf-8")
        rewritten = _rewrite_wikilinks(original, uid_mapping)
        if rewritten == original:
            continue
        _write_text_atomic(path, rewritten)
        report["wikilinks_rewritten"] += 1
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate Google-derived notes to account-scoped identities")
    parser.add_argument("--vault", required=True)
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--backup-dir", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    vault = Path(args.vault).expanduser().resolve()
    account_email = _normalize_account_email(args.account_email)
    entries: list[MigrationEntry] = []
    uid_mapping: dict[str, str] = {}
    rel_path_mapping: dict[str, str] = {}

    for note in _iter_google_notes(vault):
        entry = _build_migration_entry(vault, note, account_email=account_email)
        if entry is None:
            continue
        old_uid = str(entry.frontmatter.get("uid", "")).strip()
        if old_uid and old_uid != entry.new_uid:
            uid_mapping[old_uid] = entry.new_uid
        rel_path_key = str(entry.new_rel_path)
        existing_rel_path = rel_path_mapping.get(rel_path_key)
        if existing_rel_path is not None and existing_rel_path != str(entry.rel_path):
            raise RuntimeError(f"Multiple notes would migrate to {rel_path_key}")
        rel_path_mapping[rel_path_key] = str(entry.rel_path)
        entries.append(entry)

    print(
        json.dumps(
            {"phase": "collected-google-notes", "entries": len(entries), "uid_mappings": len(uid_mapping)},
            sort_keys=True,
        ),
        flush=True,
    )

    report = {
        "vault": str(vault),
        "account_email": account_email,
        "notes_found": len(entries),
        "uids_changed": len(uid_mapping),
    }
    if args.backup_dir and entries and not args.dry_run:
        backup_dir = Path(args.backup_dir).expanduser().resolve()
        _backup_entries(vault, entries, backup_dir)
        report["backup_dir"] = str(backup_dir)

    report.update(_migrate_entries(vault, entries, uid_mapping=uid_mapping, dry_run=args.dry_run))
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
