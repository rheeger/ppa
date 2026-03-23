#!/usr/bin/env python3
"""Chunked Apple Messages backfill runner for PPA."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from archive_sync.adapters.imessage import IMessageAdapter, MessageSnapshot
from hfa.sync_state import load_sync_state


def _cursor_key(source_label: str) -> str:
    normalized = source_label.strip().lower()
    return f"imessage:{normalized}" if normalized else "imessage"


def _rowid(cursor: dict[str, object]) -> int:
    try:
        return int(cursor.get("last_completed_message_rowid", 0) or 0)
    except (TypeError, ValueError):
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full Apple Messages PPA backfill in resumable chunks")
    parser.add_argument("--vault", default=os.environ.get("PPA_PATH", str(Path.home() / "Archive" / "production" / "hf-archives")))
    parser.add_argument("--snapshot-dir", required=True)
    parser.add_argument("--source-label", default="local-messages")
    parser.add_argument("--batch-size", type=int, default=10000, help="Max source messages per ingest chunk")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent row-processing workers per chunk")
    parser.add_argument("--max-batches", type=int, default=0, help="Optional safety cap on batch count; 0 means no cap")
    args = parser.parse_args()

    snapshot = MessageSnapshot(args.snapshot_dir)
    try:
        target_rowid = snapshot.max_message_rowid()
        snapshot_id = snapshot.snapshot_id()
    finally:
        snapshot.close()

    adapter = IMessageAdapter()
    key = _cursor_key(args.source_label)
    batch = 0

    while True:
        state = load_sync_state(args.vault)
        cursor = state.get(key, {}) if isinstance(state, dict) else {}
        if not isinstance(cursor, dict):
            cursor = {}
        before = _rowid(cursor)
        if before >= target_rowid:
            print(f"ppa-imessage-import-all: complete rowid={before} target={target_rowid} batches={batch}")
            return 0
        if args.max_batches and batch >= args.max_batches:
            print(f"ppa-imessage-import-all: stopped rowid={before} target={target_rowid} batches={batch}")
            return 0

        batch += 1
        result = adapter.ingest(
            args.vault,
            snapshot_dir=args.snapshot_dir,
            source_label=args.source_label,
            max_messages=max(1, int(args.batch_size)),
            workers=args.workers,
        )

        state = load_sync_state(args.vault)
        cursor = state.get(key, {}) if isinstance(state, dict) else {}
        if not isinstance(cursor, dict):
            cursor = {}
        after = _rowid(cursor)
        print(
            f"batch={batch} before={before} after={after} target={target_rowid} "
            f"created={result.created} merged={result.merged} errors={len(result.errors)} snapshot_id={snapshot_id}"
        )

        if result.errors:
            return 1
        if after <= before:
            print("ppa-imessage-import-all: cursor did not advance; aborting to avoid infinite loop", file=sys.stderr)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
