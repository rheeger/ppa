"""Identity map helpers and batch cache."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

IDENTIFIER_PREFIX_ALIASES = {
    "emails": "email",
    "phones": "phone",
}


def identity_map_path(vault_path: str | Path) -> Path:
    """Return the on-disk identity map path."""

    return Path(vault_path) / "_meta" / "identity-map.json"


def _normalize_identifier(prefix: str, value: str) -> str:
    prefix = IDENTIFIER_PREFIX_ALIASES.get(prefix, prefix)
    raw = value.strip()
    if not raw:
        return ""
    if prefix in {"email", "github", "linkedin", "twitter"}:
        return raw.lower()
    if prefix == "name":
        return " ".join(raw.lower().split())
    if prefix == "phone":
        digits = re.sub(r"\D", "", raw)
        if not digits:
            return ""
        if raw.startswith("+"):
            return f"+{digits}"
        if len(digits) == 11 and digits.startswith("1"):
            return f"+{digits}"
        if len(digits) == 10:
            return f"+1{digits}"
        return digits
    return raw


def _iter_identifier_pairs(identifiers: dict[str, str | list[str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for prefix, value in identifiers.items():
        normalized_prefix = IDENTIFIER_PREFIX_ALIASES.get(prefix, prefix)
        values = value if isinstance(value, list) else [value]
        for item in values:
            if not isinstance(item, str):
                continue
            normalized = _normalize_identifier(normalized_prefix, item)
            if normalized:
                pairs.append((normalized_prefix, normalized))
    return pairs


def load_identity_map(vault_path: str | Path) -> dict[str, str]:
    """Load the identity map, skipping internal metadata keys."""

    path = identity_map_path(vault_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items() if not str(key).startswith("_")}


def save_identity_map(vault_path: str | Path, entries: dict[str, str]) -> None:
    """Atomically persist the identity map to disk."""

    path = identity_map_path(vault_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"_comment": "Alias -> canonical person wikilink", **dict(sorted(entries.items()))}
    fd, tmp_path = tempfile.mkstemp(prefix="identity-map-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def upsert_identity_map(
    vault_path: str | Path,
    wikilink: str,
    identifiers: dict[str, str | list[str]],
) -> None:
    """Add or update all identity aliases for a person."""

    entries = load_identity_map(vault_path)
    for prefix, value in _iter_identifier_pairs(identifiers):
        entries[f"{prefix}:{value}"] = wikilink
    save_identity_map(vault_path, entries)


def resolve_email(vault_path: str | Path, email: str) -> str | None:
    """Resolve an email alias to a canonical wikilink."""

    return resolve_any(vault_path, "email", email)


def resolve_any(vault_path: str | Path, prefix: str, value: str) -> str | None:
    """Resolve any normalized identity alias."""

    prefix = IDENTIFIER_PREFIX_ALIASES.get(prefix, prefix)
    normalized = _normalize_identifier(prefix, value)
    if not normalized:
        return None
    return load_identity_map(vault_path).get(f"{prefix}:{normalized}")


class IdentityCache:
    """In-memory identity map for batch operations."""

    def __init__(self, vault_path: str | Path):
        self.vault_path = Path(vault_path)
        self.entries = load_identity_map(self.vault_path)

    def resolve(self, prefix: str, value: str) -> str | None:
        prefix = IDENTIFIER_PREFIX_ALIASES.get(prefix, prefix)
        normalized = _normalize_identifier(prefix, value)
        if not normalized:
            return None
        return self.entries.get(f"{prefix}:{normalized}")

    def upsert(self, wikilink: str, identifiers: dict[str, str | list[str]]) -> None:
        for prefix, value in _iter_identifier_pairs(identifiers):
            self.entries[f"{prefix}:{value}"] = wikilink

    def flush(self) -> None:
        save_identity_map(self.vault_path, self.entries)
