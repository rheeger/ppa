#!/usr/bin/env python3
"""Copy planted identity-repair fixtures and pinned Sam cards into a slice vault."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

PINNED_REL_PATHS = (
    "People/sam-panken.md",
    "IMessageThreads/2026-03/hfa-imessage-thread-45b3a963c99a.md",
    "IMessageThreads/2015-07/hfa-imessage-thread-bb764c10b57b.md",
    "IMessage/2026-03/hfa-imessage-message-f87b5dda20c1.md",
)

PINNED_UIDS = (
    "hfa-person-54fc3b19aeda",
    "hfa-imessage-thread-45b3a963c99a",
    "hfa-imessage-thread-bb764c10b57b",
    "hfa-imessage-message-f87b5dda20c1",
)


def _copy_file(src: Path, dest: Path) -> bool:
    if not src.is_file():
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slice", required=True, type=Path)
    parser.add_argument("--source-vault", required=True, type=Path)
    parser.add_argument("--inventory", type=Path, default=None)
    args = parser.parse_args()

    slice_root = args.slice.resolve()
    source_vault = args.source_vault.resolve()
    fixture_dir = Path(__file__).resolve().parent

    copied_fixtures: list[str] = []
    for path in sorted(fixture_dir.glob("*.md")):
        dest = slice_root / "_fixtures" / "identity-repair" / path.name
        if _copy_file(path, dest):
            copied_fixtures.append(str(dest.relative_to(slice_root)))

    copied_pins: list[str] = []
    missing_pins: list[str] = []
    for rel in PINNED_REL_PATHS:
        dest = slice_root / rel
        if dest.is_file() or _copy_file(source_vault / rel, dest):
            copied_pins.append(rel)
        else:
            missing_pins.append(rel)

    inventory = {
        "slice": str(slice_root),
        "pinned_uids": list(PINNED_UIDS),
        "pinned_rel_paths": copied_pins,
        "missing_pins": missing_pins,
        "planted_fixtures": copied_fixtures,
        "sam_person_present": (slice_root / "People" / "sam-panken.md").is_file(),
    }
    if args.inventory:
        args.inventory.parent.mkdir(parents=True, exist_ok=True)
        args.inventory.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(inventory, indent=2))
    return 1 if missing_pins else 0


if __name__ == "__main__":
    raise SystemExit(main())
