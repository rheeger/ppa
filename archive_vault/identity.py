"""Identity map helpers, redirects, and batch cache."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from archive_vault.change_journal import OPERATION_CREATE, OPERATION_UPDATE, ChangeJournal
from archive_vault.paths import normalize_vault_rel

IDENTIFIER_PREFIX_ALIASES = {
    "emails": "email",
    "phones": "phone",
}

IDENTITY_MAP_REL = "_meta/identity-map.json"
IDENTITY_MAP_UID = "hfa-identity-canonical"
REDIRECT_UID_PREFIX = "redirect:"
REDIRECT_WIKILINK_PREFIX = "redirect-wikilink:"
UID_ALIAS_PREFIX = "uid:"


def identity_map_path(vault_path: str | Path) -> Path:
    """Return the on-disk identity map path."""

    return Path(vault_path) / IDENTITY_MAP_REL


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
    """Persist the identity map through the P02 change journal."""

    vault = Path(vault_path)
    rel = str(normalize_vault_rel(IDENTITY_MAP_REL))
    payload = {"_comment": "Alias -> canonical person wikilink", **dict(sorted(entries.items()))}
    content = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    operation = OPERATION_UPDATE if (vault / rel).is_file() else OPERATION_CREATE
    with ChangeJournal(vault) as journal:
        journal.apply_mutation(
            uid=IDENTITY_MAP_UID,
            rel_path=rel,
            operation=operation,
            content=content,
            source="identity",
        )


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
    return canonicalize_wikilink(load_identity_map(vault_path), f"{prefix}:{normalized}", lookup=True)


def canonicalize_wikilink(entries: dict[str, str], key_or_wikilink: str, *, lookup: bool = False) -> str | None:
    """Follow recorded redirect-wikilink hops. Does not guess owners."""

    current = entries.get(key_or_wikilink) if lookup else key_or_wikilink
    if not current:
        return None
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        nxt = entries.get(f"{REDIRECT_WIKILINK_PREFIX}{current}")
        if not nxt:
            return current
        current = nxt
    return current


def redirect_target_uid(vault_path: str | Path, uid: str) -> str | None:
    entries = load_identity_map(vault_path)
    target = entries.get(f"{REDIRECT_UID_PREFIX}{uid}")
    return str(target) if target else None


def resolve_uid_wikilink(vault_path: str | Path, uid: str) -> str | None:
    entries = load_identity_map(vault_path)
    return canonicalize_wikilink(entries, f"{UID_ALIAS_PREFIX}{uid}", lookup=True)


def apply_alias_moves(entries: dict[str, str], moves: list[dict[str, str]]) -> dict[str, str]:
    updated = dict(entries)
    for move in moves:
        key = str(move.get("key") or "")
        dest = str(move.get("to") or "")
        if key and dest:
            updated[key] = dest
    return updated


def revert_alias_moves(entries: dict[str, str], moves: list[dict[str, str]]) -> dict[str, str]:
    updated = dict(entries)
    for move in moves:
        key = str(move.get("key") or "")
        src = str(move.get("from") or "")
        dest = str(move.get("to") or "")
        if not key:
            continue
        if updated.get(key) == dest:
            if src:
                updated[key] = src
            else:
                updated.pop(key, None)
    return updated


def person_alias_pairs(wikilink: str, card: dict[str, Any]) -> list[tuple[str, str]]:
    identifiers: dict[str, str | list[str]] = {
        "name": str(card.get("summary") or ""),
        "emails": list(card.get("emails") or []),
        "phones": list(card.get("phones") or []),
        "github": str(card.get("github") or ""),
        "linkedin": str(card.get("linkedin") or ""),
        "twitter": str(card.get("twitter") or ""),
        "instagram": str(card.get("instagram") or ""),
        "telegram": str(card.get("telegram") or ""),
        "discord": str(card.get("discord") or ""),
    }
    pairs = _iter_identifier_pairs(identifiers)
    uid = str(card.get("uid") or "")
    if uid:
        pairs.append(("uid", uid))
    return pairs


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
        return canonicalize_wikilink(self.entries, f"{prefix}:{normalized}", lookup=True)

    def upsert(self, wikilink: str, identifiers: dict[str, str | list[str]]) -> None:
        for prefix, value in _iter_identifier_pairs(identifiers):
            self.entries[f"{prefix}:{value}"] = wikilink

    def flush(self) -> None:
        save_identity_map(self.vault_path, self.entries)
