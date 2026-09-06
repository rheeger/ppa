#!/usr/bin/env python3
"""Emit archive_crate/materializer_registry.json from the live Python card registry.

Run from repo root after changing archive_cli/card_registry.py or projections/base.py:

  python archive_scripts/export_materializer_registry.py
"""

from __future__ import annotations

from pathlib import Path

from archive_cli.card_registry import dump_registry_json, materializer_registry_payload

CHECKED_IN_REGISTRY = Path(__file__).resolve().parents[1] / "archive_crate" / "materializer_registry.json"


def main() -> None:
    CHECKED_IN_REGISTRY.write_text(dump_registry_json(materializer_registry_payload()), encoding="utf-8")
    print(f"Wrote {CHECKED_IN_REGISTRY}")


if __name__ == "__main__":
    main()
