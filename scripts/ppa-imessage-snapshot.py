#!/usr/bin/env python3
"""Create a readonly Apple Messages snapshot bundle for PPA imports."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from archive_sync.adapters.imessage import MessageSnapshot


def _scan_attachment_manifest(attachments_root: Path) -> dict[str, object]:
    entries: list[dict[str, object]] = []
    total_size_bytes = 0
    if attachments_root.exists():
        for path in sorted(attachments_root.rglob("*")):
            if not path.is_file():
                continue
            relative_path = path.relative_to(attachments_root).as_posix()
            stat = path.stat()
            total_size_bytes += stat.st_size
            entries.append(
                {
                    "original_path": str(path),
                    "exported_path": relative_path,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            )
    return {
        "attachments_root": str(attachments_root),
        "attachment_file_count": len(entries),
        "attachment_store_size_bytes": total_size_bytes,
        "entries": entries,
    }


def build_snapshot(messages_dir: Path, output_dir: Path, *, source_label: str, top_chat_limit: int) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    chat_db = messages_dir / "chat.db"
    if not chat_db.exists():
        raise FileNotFoundError(f"chat.db not found in {messages_dir}")

    copied_files: list[str] = []
    for name in ("chat.db", "chat.db-wal", "chat.db-shm"):
        source_path = messages_dir / name
        if not source_path.exists():
            continue
        shutil.copy2(source_path, output_dir / name)
        copied_files.append(name)

    attachments_manifest = _scan_attachment_manifest(messages_dir / "Attachments")
    (output_dir / "attachments-manifest.json").write_text(
        json.dumps(attachments_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    snapshot_meta = {
        "source_label": source_label,
        "source_messages_dir": str(messages_dir),
        "exported_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "copied_files": copied_files,
    }
    (output_dir / "snapshot-meta.json").write_text(
        json.dumps(snapshot_meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    snapshot = MessageSnapshot(output_dir)
    try:
        inspection = snapshot.inspection_report(top_chat_limit=top_chat_limit)
    finally:
        snapshot.close()
    inspection.update(
        {
            "source_messages_dir": str(messages_dir),
            "attachment_file_count": attachments_manifest["attachment_file_count"],
            "attachment_store_size_bytes": attachments_manifest["attachment_store_size_bytes"],
        }
    )
    (output_dir / "inspection.json").write_text(
        json.dumps(inspection, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return inspection


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a PPA Apple Messages snapshot bundle")
    parser.add_argument("--messages-dir", default=str(Path.home() / "Library" / "Messages"))
    parser.add_argument("--output-dir", required=True, help="Destination directory for the snapshot bundle")
    parser.add_argument("--source-label", default="local-messages", help="Stable source label used by PPA sync-state")
    parser.add_argument("--top-chat-limit", type=int, default=20, help="How many top chats to include in inspection.json")
    args = parser.parse_args()

    inspection = build_snapshot(
        Path(args.messages_dir).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        source_label=args.source_label,
        top_chat_limit=max(1, int(args.top_chat_limit)),
    )
    print(
        "ppa-imessage-snapshot:"
        f" messages={inspection['message_count']}"
        f" chats={inspection['chat_count']}"
        f" attachments={inspection['attachment_count']}"
        f" latest_message_rowid={inspection['latest_message_rowid']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
