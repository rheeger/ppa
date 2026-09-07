from __future__ import annotations

from archive_engine.identity_resolution import auto_merge_eligible, given_names_compatible, resolve_candidates
from archive_vault.canon import phone as canon_phone


def test_household_namesakes_are_not_compatible() -> None:
    alice = {"first_name": "Alice", "last_name": "Smith"}
    bob = {"first_name": "Bob", "last_name": "Smith"}
    assert given_names_compatible(alice, bob) is False
    ok, reason = auto_merge_eligible(
        {**alice, "phones": ["+15551230000"]},
        {**bob, "phones": ["+15551230000"]},
    )
    assert ok is False
    assert reason == "phone_incompatible_name"


def test_named_email_share_is_ambiguous() -> None:
    ok, reason = auto_merge_eligible(
        {"first_name": "Alex", "last_name": "Rivera", "emails": ["a@example.com"]},
        {"first_name": "Alex", "last_name": "Rivera", "emails": ["a@example.com"]},
    )
    assert ok is False
    assert reason == "shared_email_named"


def test_stub_email_may_auto_merge() -> None:
    ok, reason = auto_merge_eligible(
        {"first_name": "Alex", "last_name": "Rivera", "emails": ["a@example.com"]},
        {"first_name": "", "last_name": "", "emails": ["a@example.com"]},
    )
    assert ok is True
    assert reason == "exact_email_stub"


def test_opaque_phones_do_not_collide() -> None:
    assert canon_phone.canonical("alice42") == ""
    assert canon_phone.canonical("bob42") == ""
    assert resolve_candidates(["u1", "u1"]).status == "unique"
    assert resolve_candidates(["u1", "u2"]).status == "ambiguous"
    assert resolve_candidates([]).status == "unresolved"
