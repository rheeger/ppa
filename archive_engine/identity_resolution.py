"""Read-only authorized candidate resolution. Does not write cards or merges."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from archive_vault.canon import phone as canon_phone
from archive_vault.canon.email import canonical as canon_email
from archive_vault.identity_resolver import is_same_person

ResolutionStatus = Literal["unique", "ambiguous", "unresolved"]


@dataclass(frozen=True, slots=True)
class IdentityResolution:
    status: ResolutionStatus
    candidate_uids: tuple[str, ...]
    identifier_kinds: tuple[str, ...] = ()
    policy_version: str = "p31.1"


def resolve_candidates(uids: Iterable[str], *, kinds: Iterable[str] = ()) -> IdentityResolution:
    unique = tuple(dict.fromkeys(str(uid).strip() for uid in uids if str(uid).strip()))
    if not unique:
        return IdentityResolution(status="unresolved", candidate_uids=(), identifier_kinds=tuple(kinds))
    if len(unique) == 1:
        return IdentityResolution(status="unique", candidate_uids=unique, identifier_kinds=tuple(kinds))
    return IdentityResolution(status="ambiguous", candidate_uids=tuple(sorted(unique)), identifier_kinds=tuple(kinds))


def _is_stub(frontmatter: dict) -> bool:
    first = str(frontmatter.get("first_name") or "").strip()
    last = str(frontmatter.get("last_name") or "").strip()
    return not first and not last


def given_names_compatible(left: dict, right: dict, nicknames: dict | None = None) -> bool:
    """True only with compatible given/full/nickname evidence, not surname-token overlap."""

    same, _conf, reasons = is_same_person(left, right, nicknames or {})
    if any(item in reasons for item in ("exact_name", "nickname_name")):
        return True
    left_first = str(left.get("first_name") or "").strip().lower()
    right_first = str(right.get("first_name") or "").strip().lower()
    if left_first and right_first and left_first == right_first:
        return same or "fuzzy_name" in reasons
    return False


def auto_merge_eligible(left: dict, right: dict, nicknames: dict | None = None) -> tuple[bool, str]:
    """Return (eligible, reason). Locked predicates from the PR31 plan."""

    left_emails = {canon_email(x) for x in left.get("emails") or []} - {""}
    right_emails = {canon_email(x) for x in right.get("emails") or []} - {""}
    same_email = bool(left_emails & right_emails)
    left_phones = {canon_phone.canonical(str(x)) for x in left.get("phones") or []} - {""}
    right_phones = {canon_phone.canonical(str(x)) for x in right.get("phones") or []} - {""}
    same_phone = bool(left_phones & right_phones)
    if same_email:
        if _is_stub(left) or _is_stub(right):
            return True, "exact_email_stub"
        return False, "shared_email_named"
    if same_phone and given_names_compatible(left, right, nicknames):
        return True, "exact_phone_compatible_name"
    if same_phone:
        return False, "phone_incompatible_name"
    return False, "ambiguous"
