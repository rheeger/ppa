#!/usr/bin/env python3
"""Single-writer Gmail import from extracted spool files."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from archive_sync.adapters.gmail_correspondents import load_own_aliases
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter
from archive_vault.identity import IdentityCache
from archive_vault.vault import write_card


def _read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default
    return payload if isinstance(payload, dict) else default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _collect_thread_files(spool_dir: Path) -> list[Path]:
    candidates = sorted(spool_dir.glob("**/threads/*.json"))
    deduped: dict[str, Path] = {}
    for path in candidates:
        deduped.setdefault(path.stem, path)
    return [deduped[key] for key in sorted(deduped)]


def _collect_manifest_files(spool_dir: Path) -> list[Path]:
    return sorted(spool_dir.glob("**/_meta/manifest.json"))


def _assert_manifest_consistency(spool_dir: Path, *, account_email: str) -> None:
    manifests = _collect_manifest_files(spool_dir)
    if not manifests:
        return
    expected_account = account_email.strip().lower()
    for manifest_path in manifests:
        payload = _read_json(manifest_path, {})
        manifest_account = str(payload.get("account_email", "")).strip().lower()
        if not manifest_account:
            raise RuntimeError(f"Spool manifest missing account_email: {manifest_path}")
        if manifest_account != expected_account:
            raise RuntimeError(
                f"Spool manifest account mismatch in {manifest_path}: expected {expected_account}, found {manifest_account}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-writer Gmail import from spool files")
    parser.add_argument("--vault", required=True)
    parser.add_argument("--spool-dir", required=True)
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--max-threads", type=int, default=0, help="0 means no limit")
    parser.add_argument("--checkpoint-every-threads", type=int, default=25)
    args = parser.parse_args()

    vault = Path(args.vault).expanduser().resolve()
    spool_dir = Path(args.spool_dir).expanduser().resolve()
    meta_dir = spool_dir / "_meta"
    state_path = meta_dir / "import-state.json"
    adapter = GmailMessagesAdapter()
    account_email = args.account_email.strip().lower()
    _assert_manifest_consistency(spool_dir, account_email=account_email)
    own_emails = {account_email}
    own_emails.update(load_own_aliases(str(vault)))
    identity_cache = IdentityCache(vault)

    thread_files = _collect_thread_files(spool_dir)
    state = _read_json(
        state_path,
        {
            "next_index": 0,
            "processed_threads": 0,
            "processed_messages": 0,
            "processed_attachments": 0,
            "created_thread_cards": 0,
            "created_message_cards": 0,
            "created_attachment_cards": 0,
            "complete": False,
        },
    )
    if state.get("complete"):
        print(json.dumps({"status": "already_complete", **state}, indent=2))
        return 0

    next_index = int(state.get("next_index", 0) or 0)
    processed_threads = int(state.get("processed_threads", 0) or 0)
    processed_messages = int(state.get("processed_messages", 0) or 0)
    processed_attachments = int(state.get("processed_attachments", 0) or 0)
    created = Counter(
        {
            "email_thread": int(state.get("created_thread_cards", 0) or 0),
            "email_message": int(state.get("created_message_cards", 0) or 0),
            "email_attachment": int(state.get("created_attachment_cards", 0) or 0),
        }
    )
    threads_since_checkpoint = 0

    print(
        json.dumps(
            {
                "mode": "gmail-spool-import",
                "next_index": next_index,
                "thread_file_count": len(thread_files),
                "processed_threads": processed_threads,
            },
            indent=2,
        ),
        flush=True,
    )

    def checkpoint(*, complete: bool = False) -> None:
        _write_json(
            state_path,
            {
                "next_index": next_index,
                "processed_threads": processed_threads,
                "processed_messages": processed_messages,
                "processed_attachments": processed_attachments,
                "created_thread_cards": created["email_thread"],
                "created_message_cards": created["email_message"],
                "created_attachment_cards": created["email_attachment"],
                "complete": complete,
            },
        )

    while next_index < len(thread_files):
        if args.max_threads and processed_threads >= args.max_threads:
            checkpoint()
            print(
                json.dumps({"status": "paused", "reason": "max_threads", "next_index": next_index}, indent=2),
                flush=True,
            )
            return 0

        thread_data = json.loads(thread_files[next_index].read_text(encoding="utf-8"))
        thread_record, message_records, attachment_records = adapter._thread_records(
            thread_data,
            account_email=account_email,
            own_emails=own_emails,
            identity_cache=identity_cache,
            attachment_workers=1,
        )
        batch_items = [thread_record, *message_records, *attachment_records]

        for record in batch_items:
            card, provenance, body = adapter.to_card(record)
            rel_path = Path(adapter._card_rel_path(vault, card))
            abs_path = vault / rel_path
            if abs_path.exists():
                adapter.merge_card(vault, rel_path, card, body, provenance)
                continue
            write_card(vault, rel_path, card, body=body, provenance=provenance)
            created[card.type] += 1

        processed_threads += 1
        processed_messages += len(message_records)
        processed_attachments += len(attachment_records)
        next_index += 1
        threads_since_checkpoint += 1

        if threads_since_checkpoint >= args.checkpoint_every_threads:
            checkpoint()
            print(
                json.dumps(
                    {
                        "progress": {
                            "next_index": next_index,
                            "processed_threads": processed_threads,
                            "processed_messages": processed_messages,
                            "processed_attachments": processed_attachments,
                            "created_thread_cards": created["email_thread"],
                            "created_message_cards": created["email_message"],
                            "created_attachment_cards": created["email_attachment"],
                        }
                    },
                    indent=2,
                ),
                flush=True,
            )
            threads_since_checkpoint = 0

    checkpoint(complete=True)
    print(
        json.dumps(
            {
                "status": "complete",
                "processed_threads": processed_threads,
                "processed_messages": processed_messages,
                "processed_attachments": processed_attachments,
                "created_thread_cards": created["email_thread"],
                "created_message_cards": created["email_message"],
                "created_attachment_cards": created["email_attachment"],
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
