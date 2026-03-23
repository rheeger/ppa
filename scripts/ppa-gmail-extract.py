#!/usr/bin/env python3
"""Parallel read-only Gmail thread extraction into spool files."""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from archive_sync.adapters.gmail_messages import CALENDAR_MIME_TYPES, GmailMessagesAdapter, _iter_parts
from ppa_google_auth import build_google_cli_token_manager


def _encode_body_data(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8").rstrip("=")


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


def _inline_calendar_attachment_bodies(adapter: GmailMessagesAdapter, thread_data: dict) -> int:
    fetched = 0
    for message in thread_data.get("messages", []) or []:
        payload = message.get("payload", {}) or {}
        message_id = str(message.get("id", "")).strip()
        if not message_id:
            continue
        for part in _iter_parts(payload):
            mime_type = str(part.get("mimeType", "")).lower()
            filename = str(part.get("filename", "")).lower()
            body_info = part.get("body") or {}
            attachment_id = str(body_info.get("attachmentId", "")).strip()
            if body_info.get("data"):
                continue
            is_calendar_part = mime_type in CALENDAR_MIME_TYPES or filename.endswith(".ics")
            if not is_calendar_part or not attachment_id:
                continue
            body_text = adapter._fetch_attachment_body(message_id, attachment_id)
            if not body_text:
                continue
            body_info["data"] = _encode_body_data(body_text)
            body_info.pop("attachmentId", None)
            fetched += 1
    return fetched


def main() -> int:
    parser = argparse.ArgumentParser(description="Parallel Gmail extraction into spool files")
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--query", default="")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--thread-workers", type=int, default=8)
    parser.add_argument("--max-threads", type=int, default=0, help="0 means no limit")
    args = parser.parse_args()

    account_email = args.account_email.strip().lower()
    output_dir = Path(args.output_dir).expanduser().resolve()
    threads_dir = output_dir / "threads"
    meta_dir = output_dir / "_meta"
    threads_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    state_path = meta_dir / "extract-state.json"
    manifest_path = meta_dir / "manifest.json"
    adapter = GmailMessagesAdapter()
    token_manager = build_google_cli_token_manager(account_email=account_email, services=["gmail"])
    if token_manager is None:
        raise RuntimeError(f"Could not build Gmail token manager for {account_email}")
    token_manager.ensure_env()
    adapter._token_manager = token_manager
    adapter._token_manager_key = ("gmail", account_email)

    state = _read_json(
        state_path,
        {
            "account_email": account_email,
            "query": args.query,
            "page_token": None,
            "extracted_threads": 0,
            "calendar_attachment_fetches": 0,
            "complete": False,
        },
    )
    if state.get("complete"):
        print(json.dumps({"status": "already_complete", **state}, indent=2))
        return 0

    _write_json(
        manifest_path,
        {
            "account_email": account_email,
            "query": args.query,
            "page_size": args.page_size,
            "thread_workers": args.thread_workers,
            "started_at": state.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )

    page_token = state.get("page_token")
    extracted_threads = int(state.get("extracted_threads", 0) or 0)
    calendar_attachment_fetches = int(state.get("calendar_attachment_fetches", 0) or 0)

    while True:
        if args.max_threads and extracted_threads >= args.max_threads:
            break
        params = {"userId": "me", "maxResults": max(1, min(args.page_size, 500))}
        if page_token:
            params["pageToken"] = page_token
        if args.query.strip():
            params["q"] = args.query.strip()
        list_data = adapter._gws_with_retry(["gmail", "users", "threads", "list", "--params", json.dumps(params)])
        thread_ids = [str(thread.get("id", "")).strip() for thread in list_data.get("threads", []) or [] if thread.get("id")]
        page_token = list_data.get("nextPageToken")
        if not thread_ids:
            break

        if args.max_threads:
            remaining = max(0, args.max_threads - extracted_threads)
            thread_ids = thread_ids[:remaining]

        def _fetch(thread_id: str) -> tuple[str, dict, int]:
            thread_data = adapter._gws_with_retry(
                ["gmail", "users", "threads", "get", "--params", json.dumps({"userId": "me", "id": thread_id, "format": "full"})]
            )
            attachment_fetch_count = _inline_calendar_attachment_bodies(adapter, thread_data)
            return thread_id, thread_data, attachment_fetch_count

        worker_count = max(1, min(args.thread_workers, len(thread_ids)))
        if worker_count == 1:
            results = [_fetch(thread_id) for thread_id in thread_ids]
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                results = list(executor.map(_fetch, thread_ids))

        for thread_id, thread_data, attachment_fetch_count in results:
            target = threads_dir / f"{thread_id}.json"
            target.write_text(json.dumps(thread_data, separators=(",", ":")), encoding="utf-8")
            extracted_threads += 1
            calendar_attachment_fetches += attachment_fetch_count

        state.update(
            {
                "page_token": page_token,
                "extracted_threads": extracted_threads,
                "calendar_attachment_fetches": calendar_attachment_fetches,
                "complete": not bool(page_token),
            }
        )
        _write_json(state_path, state)
        print(
            json.dumps(
                {
                    "progress": {
                        "extracted_threads": extracted_threads,
                        "calendar_attachment_fetches": calendar_attachment_fetches,
                        "page_token_present": bool(page_token),
                    }
                },
                indent=2,
            ),
            flush=True,
        )

        if not page_token:
            break

    state["complete"] = not bool(page_token)
    _write_json(state_path, state)
    print(json.dumps({"status": "complete" if state["complete"] else "paused", **state}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
