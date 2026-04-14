"""Account registry for PPA-managed Google accounts."""

from __future__ import annotations

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


def is_internal_recipient(address: str) -> bool:
    """True if recipient is internal (coworker/self)."""
    addr = address.strip().lower()
    managed_emails = {a["email"].lower() for a in ACCOUNTS.values()}
    if addr in managed_emails:
        return True
    domain = addr.rsplit("@", 1)[-1] if "@" in addr else ""
    return domain in INTERNAL_DOMAINS
