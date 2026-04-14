"""Hash helpers."""

from __future__ import annotations

import hashlib


def file_hash(path: str, block_size: int = 65536) -> str:
    """Return a SHA-256 content hash for a file."""

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(block_size), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"
