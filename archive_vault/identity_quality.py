"""Person-name and shared-mailbox guards for identity writes.

Invitation platforms put event titles and host names in From:. Those strings
must not become person summaries or aliases, and the mailbox must not become
a person email.
"""

from __future__ import annotations

import re

# Registrable domains. Subdomains match (accounts.paperlesspost.com).
SHARED_MAILBOX_DOMAINS: frozenset[str] = frozenset(
    {
        "paperlesspost.com",
        "evite.com",
        "punchbowl.com",
        "greenvelope.com",
        "smilebox.com",
        "pingg.com",
        "eventbrite.com",
        "minted.com",
    }
)

NON_PERSON_NAME_TOKENS: frozenset[str] = frozenset(
    {
        "advisors",
        "alliance",
        "american",
        "and",
        "animal",
        "air",
        "buy",
        "capital",
        "club",
        "community",
        "company",
        "cooking",
        "daily",
        "express",
        "facebook",
        "foundation",
        "from",
        "fund",
        "geographic",
        "group",
        "hospital",
        "hotels",
        "information",
        "institute",
        "lines",
        "linkedin",
        "mail",
        "management",
        "making",
        "national",
        "news",
        "on",
        "or",
        "partners",
        "porter",
        "resident",
        "residents",
        "running",
        "team",
        "the",
        "university",
        "via",
        "moves",
    }
)

_TOKEN_CLEAN = re.compile(r"[^A-Za-z'-]")


def email_domain(email: str) -> str:
    return str(email or "").partition("@")[2].strip().lower()


def shared_mailbox_domain(email: str) -> str | None:
    """Return the matching shared-mailbox domain, or None."""

    domain = email_domain(email)
    if not domain:
        return None
    for shared in SHARED_MAILBOX_DOMAINS:
        if domain == shared or domain.endswith("." + shared):
            return shared
    return None


def is_shared_mailbox_email(email: str) -> bool:
    return shared_mailbox_domain(email) is not None


def looks_like_person_name(name: str) -> bool:
    """True when a display name is plausible as one human, not an event title."""

    cleaned = " ".join(str(name or "").strip().split())
    if not cleaned or any(char in cleaned for char in "@/&,"):
        return False
    if any(char.isdigit() for char in cleaned):
        return False
    if cleaned.isupper():
        return False
    tokens = [_TOKEN_CLEAN.sub("", token) for token in cleaned.split()]
    tokens = [token for token in tokens if token]
    if len(tokens) < 2 or len(tokens) > 4:
        return False
    lowered = {token.lower() for token in tokens}
    if lowered & NON_PERSON_NAME_TOKENS:
        return False

    def _token_ok(token: str, *, edge: bool) -> bool:
        core = token.replace("-", "").replace("'", "")
        if not core.isalpha():
            return False
        if edge:
            return len(core) >= 2
        return len(core) >= 1

    return _token_ok(tokens[0], edge=True) and _token_ok(tokens[-1], edge=True) and all(
        _token_ok(token, edge=False) for token in tokens[1:-1]
    )


def looks_like_event_title(name: str) -> bool:
    """True for invitation From-lines like 'Candy and Annie' or '40 years of Candy'."""

    cleaned = " ".join(str(name or "").strip().split())
    if not cleaned or "@" in cleaned:
        return False
    if "&" in cleaned:
        return True
    if any(char.isdigit() for char in cleaned):
        return True
    tokens = {token.strip(".,;:()[]").lower() for token in cleaned.split() if token.strip(".,;:()[]")}
    return bool(tokens & {"and", "or"})


def should_strip_invitation_alias(
    alias: str,
    *,
    card_uid: str,
    emails: list[str],
    person_summaries: dict[str, list[str]],
) -> bool:
    """Strip event titles, and names stolen onto a shared-mailbox person card."""

    if looks_like_event_title(alias):
        return True
    owners = [other for other in person_summaries.get(alias.lower(), []) if other and other != card_uid]
    if owners and any(is_shared_mailbox_email(email) for email in emails):
        return True
    return False


def alias_is_trustworthy(alias: str, *, incoming_emails: list[str] | None = None) -> bool:
    """False when an incoming name should not be stored as a person alias."""

    if incoming_emails and any(is_shared_mailbox_email(item) for item in incoming_emails):
        return False
    return looks_like_person_name(alias)
