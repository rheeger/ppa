#!/usr/bin/env python3
"""Incremental Gmail messages import with frequent checkpoints."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from archive_auth import build_google_cli_token_manager
from archive_sync.adapters.gmail_correspondents import load_own_aliases
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter
from archive_vault.identity import IdentityCache
from archive_vault.sync_state import load_sync_state, update_cursor
from archive_vault.vault import write_card


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental Gmail messages import")
    parser.add_argument("--vault", required=True)
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--max-threads", type=int, default=25)
    parser.add_argument("--max-messages", type=int, default=250)
    parser.add_argument("--max-attachments", type=int, default=250)
    parser.add_argument("--checkpoint-every-threads", type=int, default=5)
    args = parser.parse_args()

    vault = Path(args.vault).expanduser().resolve()
    adapter = GmailMessagesAdapter()
    account_email = args.account_email.strip().lower()
    token_manager = build_google_cli_token_manager(account_email=account_email, services=["gmail"])
    if token_manager is None:
        raise RuntimeError(f"Could not build Gmail token manager for {account_email}")
    token_manager.ensure_env()
    own_emails = {account_email}
    own_emails.update(load_own_aliases(str(vault)))
    identity_cache = IdentityCache(vault)
    created = Counter()
    created["email_thread"] = (
        len(list((vault / "EmailThreads").rglob("*.md"))) if (vault / "EmailThreads").exists() else 0
    )
    created["email_message"] = len(list((vault / "Email").rglob("*.md"))) if (vault / "Email").exists() else 0
    created["email_attachment"] = (
        len(list((vault / "EmailAttachments").rglob("*.md"))) if (vault / "EmailAttachments").exists() else 0
    )
    pass_created = Counter()

    cursor_key = adapter.get_cursor_key(account_email=account_email)
    state = load_sync_state(vault).get(cursor_key, {})
    if not isinstance(state, dict):
        state = {}
    page_token = state.get("page_token")
    page_thread_ids = [str(item).strip() for item in (state.get("page_thread_ids") or []) if str(item).strip()]
    page_next_token = state.get("page_next_token")
    page_index = int(state.get("page_index", 0) or 0)
    processed_threads = int(state.get("processed_threads", 0) or 0)
    processed_messages = int(state.get("processed_messages", 0) or 0)
    processed_attachments = int(state.get("processed_attachments", 0) or 0)
    threads_since_checkpoint = 0

    print(
        json.dumps(
            {
                "mode": "gmail-stream-resume",
                "resume_page_token_present": bool(page_token),
                "existing_email_threads": created["email_thread"],
                "existing_email_messages": created["email_message"],
                "existing_email_attachments": created["email_attachment"],
                "processed_threads_checkpoint": processed_threads,
            },
            indent=2,
        ),
        flush=True,
    )

    def checkpoint() -> None:
        update_cursor(
            vault,
            cursor_key,
            {
                "page_token": page_token,
                "page_thread_ids": page_thread_ids,
                "page_index": page_index,
                "page_next_token": page_next_token,
                "processed_threads": processed_threads,
                "processed_messages": processed_messages,
                "processed_attachments": processed_attachments,
                "created_thread_cards": created["email_thread"],
                "created_message_cards": created["email_message"],
                "created_attachment_cards": created["email_attachment"],
            },
        )

    while True:
        if page_thread_ids and page_index >= len(page_thread_ids):
            if not page_next_token:
                break
            page_thread_ids = []
            page_index = 0
            page_token = page_next_token
            page_next_token = None
            continue

        if not page_thread_ids:
            params = {"userId": "me", "maxResults": args.page_size}
            if page_token:
                params["pageToken"] = page_token
            list_data = adapter._gws_with_retry(["gmail", "users", "threads", "list", "--params", json.dumps(params)])
            page_thread_ids = [
                str(thread.get("id", "")).strip() for thread in list_data.get("threads", []) or [] if thread.get("id")
            ]
            page_next_token = list_data.get("nextPageToken")
            page_index = 0
            if not page_thread_ids:
                if not page_next_token:
                    break
                page_token = page_next_token
                page_next_token = None
                continue

        while page_index < len(page_thread_ids):
            thread_id = page_thread_ids[page_index]
            thread_data = adapter._gws_with_retry(
                [
                    "gmail",
                    "users",
                    "threads",
                    "get",
                    "--params",
                    json.dumps({"userId": "me", "id": thread_id, "format": "full"}),
                ]
            )
            thread_record, message_records, attachment_records = adapter._thread_records(
                thread_data,
                account_email=account_email,
                own_emails=own_emails,
                identity_cache=identity_cache,
                attachment_workers=1,
            )
            thread_record, message_records, attachment_records = adapter._apply_attachment_cap(
                thread_record=thread_record,
                message_records=message_records,
                attachment_records=attachment_records,
                emitted_attachments=created["email_attachment"],
                max_attachments=args.max_attachments,
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
                pass_created[card.type] += 1

            processed_threads += 1
            processed_messages += len(message_records)
            processed_attachments += len(attachment_records)
            threads_since_checkpoint += 1
            page_index += 1

            if threads_since_checkpoint >= args.checkpoint_every_threads:
                checkpoint()
                print(
                    json.dumps(
                        {
                            "progress": {
                                "processed_threads": processed_threads,
                                "processed_messages": processed_messages,
                                "processed_attachments": processed_attachments,
                                "created_thread_cards": created["email_thread"],
                                "created_message_cards": created["email_message"],
                                "created_attachment_cards": created["email_attachment"],
                                "next_page_token_present": bool(page_token),
                            }
                        },
                        indent=2,
                    ),
                    flush=True,
                )
                threads_since_checkpoint = 0

            if (
                pass_created["email_thread"] >= args.max_threads
                or pass_created["email_message"] >= args.max_messages
                or pass_created["email_attachment"] >= args.max_attachments
            ):
                checkpoint()
                print("=== PASS LIMIT REACHED ===", flush=True)
                print(
                    json.dumps(
                        {
                            "pass_created_thread_cards": pass_created["email_thread"],
                            "pass_created_message_cards": pass_created["email_message"],
                            "pass_created_attachment_cards": pass_created["email_attachment"],
                            "processed_threads": processed_threads,
                            "processed_messages": processed_messages,
                            "processed_attachments": processed_attachments,
                            "created_thread_cards": created["email_thread"],
                            "created_message_cards": created["email_message"],
                            "created_attachment_cards": created["email_attachment"],
                            "page_token_present": bool(page_token),
                        },
                        indent=2,
                    ),
                    flush=True,
                )
                return 0

    checkpoint()
    print("=== GMAIL STREAM COMPLETE ===", flush=True)
    print(
        json.dumps(
            {
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
