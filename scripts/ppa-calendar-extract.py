#!/usr/bin/env python3
"""Read-only Calendar extraction into spool files."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from archive_sync.adapters.calendar_events import CalendarEventsAdapter
from arnoldlib.google_cli_auth import (CALENDAR_READONLY_SCOPES,
                                       build_google_cli_token_manager)


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


def _event_filename(event_id: str) -> str:
    return urllib.parse.quote(event_id, safe="") + ".json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Calendar extraction into spool files")
    parser.add_argument("--account-email", default="rheeger@gmail.com")
    parser.add_argument("--calendar-id", default="primary")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--query", default="")
    parser.add_argument("--time-min", default="")
    parser.add_argument("--time-max", default="")
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--max-events", type=int, default=0, help="0 means no limit")
    args = parser.parse_args()

    account_email = args.account_email.strip().lower()
    output_dir = Path(args.output_dir).expanduser().resolve()
    events_dir = output_dir / "events"
    meta_dir = output_dir / "_meta"
    events_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    state_path = meta_dir / "extract-state.json"
    manifest_path = meta_dir / "manifest.json"
    adapter = CalendarEventsAdapter()
    token_manager = build_google_cli_token_manager(
        account_email=account_email,
        scopes=CALENDAR_READONLY_SCOPES,
    )
    if token_manager is not None:
        token_manager.ensure_env()
        adapter._token_manager = token_manager
        adapter._token_manager_key = ("calendar", account_email)

    state = _read_json(
        state_path,
        {
            "account_email": account_email,
            "calendar_id": args.calendar_id,
            "query": args.query,
            "time_min": args.time_min,
            "time_max": args.time_max,
            "page_token": None,
            "extracted_events": 0,
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
            "calendar_id": args.calendar_id,
            "query": args.query,
            "time_min": args.time_min,
            "time_max": args.time_max,
            "page_size": args.page_size,
            "started_at": state.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )

    page_token = state.get("page_token")
    extracted_events = int(state.get("extracted_events", 0) or 0)

    while True:
        if args.max_events and extracted_events >= args.max_events:
            break
        params = {
            "calendarId": args.calendar_id,
            "maxResults": max(1, min(args.page_size, 2500)),
            "singleEvents": True,
            "orderBy": "startTime",
        }
        if page_token:
            params["pageToken"] = page_token
        if args.query.strip():
            params["q"] = args.query.strip()
        if args.time_min.strip():
            params["timeMin"] = args.time_min.strip()
        if args.time_max.strip():
            params["timeMax"] = args.time_max.strip()

        response = adapter._list_events(params, account_email=account_email)
        events = response.get("items", []) or []
        page_token = response.get("nextPageToken")
        if not events:
            break

        if args.max_events:
            remaining = max(0, args.max_events - extracted_events)
            events = events[:remaining]

        for event in events:
            event_id = str(event.get("id", "")).strip()
            if not event_id:
                continue
            (events_dir / _event_filename(event_id)).write_text(json.dumps(event, separators=(",", ":")), encoding="utf-8")
            extracted_events += 1

        state.update(
            {
                "page_token": page_token,
                "extracted_events": extracted_events,
                "complete": not bool(page_token),
            }
        )
        _write_json(state_path, state)
        print(
            json.dumps(
                {
                    "progress": {
                        "extracted_events": extracted_events,
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
