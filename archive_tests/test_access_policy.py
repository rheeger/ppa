"""P05-B access policy units. Isolated env only; no warehouse or seed vault."""

from __future__ import annotations

from archive_cli.query_embed_cache import query_embed_cache_key
from archive_engine.access import (
    DOMAIN_TAXONOMY_VERSION,
    card_permitted,
    classify_domain,
    is_restricted,
    is_unrestricted,
    normalize_tool_profile,
    policy_identity,
    resolve_access_context,
    source_allowed,
    tool_permitted,
)
from archive_engine.contracts import AccessContext


def _ctx(**kwargs) -> AccessContext:
    payload = {
        "archive_id": "aid-1",
        "principal": "alice",
        "profile": "read-only",
        "allowed_tools": (),
        "allowed_sources": (),
        "allowed_domains": (),
        "egress_policy_revision": "p05b-unspecified",
        "deny": False,
        "deny_reason": "",
    }
    payload.update(kwargs)
    return AccessContext.from_payload(payload)


def test_unrestricted_trusted_local_permits_unknown() -> None:
    access = _ctx(principal="local-operator", profile="trusted-local")
    assert is_unrestricted(access)
    assert card_permitted(access, {"sources": [], "type": "person"})
    assert card_permitted(access, None)


def test_restricted_denies_unknown_provenance() -> None:
    access = _ctx(allowed_sources=("gmail",))
    assert is_restricted(access)
    assert not card_permitted(access, {"sources": [], "type": "person"})
    assert not card_permitted(access, None)


def test_mixed_source_derived_denies_if_any_required_denied() -> None:
    access = _ctx(allowed_sources=("gmail",))
    assert card_permitted(access, {"sources": ["gmail"], "type": "email_message"})
    assert not card_permitted(
        access,
        {"sources": ["gmail", "medical"], "type": "purchase", "required_sources": ["gmail", "medical"]},
    )


def test_incomplete_lineage_denied_in_restricted_context() -> None:
    access = _ctx(allowed_sources=("gmail",))
    assert not card_permitted(
        access,
        {"sources": ["gmail"], "type": "purchase", "lineage_complete": False},
    )


def test_domain_classifier_is_versioned_and_conservative() -> None:
    assert DOMAIN_TAXONOMY_VERSION == "p05b-domain-v1"
    assert classify_domain(card_type="medical_record", sources=["ehr"]) == "medical"
    assert classify_domain(card_type="email_message", sources=["gmail"]) == "communication"
    assert classify_domain(card_type="", sources=[]) == "unknown"
    access = _ctx(allowed_domains=("communication",))
    assert card_permitted(access, {"sources": ["gmail"], "type": "email_message"})
    assert not card_permitted(access, {"sources": ["mystery"], "type": ""})


def test_source_allow_matches_account_qualified_prefix() -> None:
    assert source_allowed(["gmail"], "gmail:personal")
    assert source_allowed(["gmail:personal"], "gmail:personal")
    assert not source_allowed(["gmail"], "medical")


def test_explicit_deny_fails_closed() -> None:
    access = _ctx(deny=True, deny_reason="policy deny")
    assert not card_permitted(access, {"sources": ["gmail"], "type": "email_message"})
    assert not tool_permitted(access, "archive_search")


def test_policy_identity_changes_with_allow_list() -> None:
    a = _ctx(allowed_sources=("gmail",))
    b = _ctx(allowed_sources=("gmail", "medical"))
    assert policy_identity(a) != policy_identity(b)
    key_a = query_embed_cache_key("hello", model="m", version=1, provider="p", dimension=8, policy_identity=policy_identity(a))
    key_b = query_embed_cache_key("hello", model="m", version=1, provider="p", dimension=8, policy_identity=policy_identity(b))
    assert key_a != key_b


def test_unknown_profile_fails_closed(monkeypatch) -> None:
    profile, err = normalize_tool_profile("typo", present=True)
    assert profile == ""
    assert err is not None and err.startswith("Invalid PPA_MCP_TOOL_PROFILE=")
    access = resolve_access_context("aid-1", environ={"PPA_MCP_TOOL_PROFILE": "typo"})
    assert access.deny
    assert not tool_permitted(access, "archive_search")


def test_future_claims_inherit_derived_deny_comment() -> None:
    from archive_engine import access as access_mod

    assert "Future claims use this same derived-deny rule" in (access_mod.card_permitted.__doc__ or "")
