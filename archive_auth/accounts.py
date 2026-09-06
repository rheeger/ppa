"""Account registry for PPA-managed Google accounts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

ACCOUNTS = {
    "arnold": {
        "token_env": "GOOGLE_OAUTH_REFRESH_TOKEN_ARNOLD",
        "token_op_ref": "op://Arnold-Passkey-Gate/GOOGLE_OAUTH_REFRESH_TOKEN_ARNOLD/credential",
        "email": "arnold@shloopydoopy.com",
        "read_only": False,
    },
    "rheeger": {
        "token_env": None,
        "token_op_ref": "op://Arnold-Passkey-Gate/GOOGLE_OAUTH_REFRESH_TOKEN_RHEEGER/credential",
        "email": "rheeger@gmail.com",
        "read_only": False,
    },
    "endaoment": {
        "token_env": None,
        "token_op_ref": "op://Arnold-Passkey-Gate/GOOGLE_OAUTH_REFRESH_TOKEN_ENDAOMENT/credential",
        "email": "robbie@endaoment.org",
        "read_only": False,
        "scope_profile": "standard",
    },
    "givingtree": {
        "token_env": None,
        "token_op_ref": "op://Arnold-Passkey-Gate/GOOGLE_OAUTH_REFRESH_TOKEN_GIVINGTREE/credential",
        "email": "robbie@givingtree.tech",
        "read_only": False,
        "scope_profile": "standard",
    },
}

INTERNAL_DOMAINS = {"endaoment.org", "givingtree.tech", "shloopydoopy.com"}

_ACCOUNT_REGISTRY: dict[str, dict[str, Any]] | None = None


def get_account_registry() -> dict[str, dict[str, Any]]:
    """Configured account registry. Defaults to the hardcoded ``ACCOUNTS`` map."""

    return dict(_ACCOUNT_REGISTRY if _ACCOUNT_REGISTRY is not None else ACCOUNTS)


def configure_account_registry(registry: Mapping[str, Mapping[str, Any]] | None) -> None:
    """Replace the process registry. ``None`` restores the hardcoded defaults."""

    global _ACCOUNT_REGISTRY
    if registry is None:
        _ACCOUNT_REGISTRY = None
        return
    _ACCOUNT_REGISTRY = {str(key): dict(value) for key, value in registry.items()}


def is_internal_recipient(address: str) -> bool:
    """True if recipient is internal (coworker/self)."""
    addr = address.strip().lower()
    managed_emails = {str(account.get("email", "")).lower() for account in get_account_registry().values()}
    if addr in managed_emails:
        return True
    domain = addr.rsplit("@", 1)[-1] if "@" in addr else ""
    return domain in INTERNAL_DOMAINS
