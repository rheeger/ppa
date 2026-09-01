"""SHA-256 of source file bytes — one helper for extract cache and identity.

Gmail apply, file-library ingest, and maintain all key off this digest.
"""

from __future__ import annotations

import hashlib
import re

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_sha256(value: str) -> bool:
    return bool(_SHA256_RE.fullmatch((value or "").strip().lower()))
