"""Fail if a join-key normalizer is reintroduced outside archive_vault.canon."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PATTERN = re.compile(r"def _normalize_(phone|email|handle|account_email|slug|contact_handle)\b")
ALLOWED_PREFIXES = ("archive_vault/canon/",)


def test_no_stray_join_key_normalizer_definitions() -> None:
    hits: list[str] = []
    for folder in ("archive_vault", "archive_cli", "archive_sync", "archive_scripts"):
        root = ROOT / folder
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            rel = path.relative_to(ROOT).as_posix()
            if rel.startswith(ALLOWED_PREFIXES):
                continue
            text = path.read_text(encoding="utf-8")
            for match in PATTERN.finditer(text):
                # Wrappers are allowed when they immediately call archive_vault.canon.
                start = max(0, match.start() - 80)
                window = text[match.start() : match.start() + 400]
                if "archive_vault.canon" in window or "canon_" in text[start : match.start() + 400]:
                    continue
                hits.append(f"{rel}:{match.group(0)}")
    assert hits == [], "stray join-key normalizers:\n" + "\n".join(hits)
