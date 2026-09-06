"""P09-B: saved scopes are filters and cannot widen AccessContext."""

from __future__ import annotations

import pytest

from archive_engine.contracts import AccessContext
from archive_engine.errors import ConfigError
from archive_engine.scopes import (
    RequestFilters,
    SavedScope,
    empty_scope_result,
    resolve_effective_scope,
)


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


def _access(**overrides: object) -> AccessContext:
    payload = {
        "archive_id": "archive-test",
        "principal": "local-operator",
        "profile": "trusted-local",
        "allowed_tools": (),
        "allowed_sources": (),
        "allowed_domains": (),
        "deny": False,
        "deny_reason": "",
    }
    payload.update(overrides)
    return AccessContext(**payload)  # type: ignore[arg-type]


def _scope(**overrides: object) -> SavedScope:
    payload = {
        "name": "family-2024",
        "description": "family year",
        "card_types": ("email_message",),
        "sources": ("gmail",),
        "people": ("ada",),
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "retrieval_profile": "historical",
    }
    payload.update(overrides)
    return SavedScope(**payload)  # type: ignore[arg-type]


def test_request_replaces_one_dimension_keeps_others() -> None:
    effective = resolve_effective_scope(
        access=_access(),
        scope=_scope(),
        request=RequestFilters(sources=("slack",)),
    )
    assert effective.empty is False
    assert effective.sources == ("slack",)
    assert effective.card_types == ("email_message",)
    assert effective.people == ("ada",)
    assert effective.start_date == "2024-01-01"
    assert effective.origins["sources"] == "request"
    assert effective.origins["card_types"] == "scope"


def test_scope_cannot_widen_access_sources() -> None:
    effective = resolve_effective_scope(
        access=_access(allowed_sources=("gmail",)),
        scope=_scope(sources=("gmail", "slack")),
    )
    assert effective.empty is False
    assert effective.sources == ("gmail",)
    assert "slack" not in effective.sources
    assert effective.origins["sources"] == "access_intersect"


def test_empty_intersection_is_empty_scope_not_unconstrained() -> None:
    effective = resolve_effective_scope(
        access=_access(allowed_sources=("gmail",)),
        scope=_scope(sources=("slack",)),
    )
    assert effective.empty is True
    assert effective.reason.startswith("empty_intersection")
    payload = empty_scope_result(reason=effective.reason, effective=effective)
    assert payload["empty_scope"] is True
    assert payload["rows"] == []
    assert payload["hits"] == []
    kwargs = effective.query_kwargs()
    assert kwargs["source_filter"] == "__empty_scope__"
    assert kwargs["type_filter"] == "__empty_scope__"


def test_access_deny_is_empty_scope() -> None:
    effective = resolve_effective_scope(
        access=_access(deny=True, deny_reason="profile_denied"),
        scope=_scope(),
        request=RequestFilters(sources=("gmail",)),
    )
    assert effective.empty is True
    assert effective.reason == "profile_denied"
    assert effective.sources == ()
    assert effective.card_types == ()


def test_unrestricted_access_still_narrows_to_scope() -> None:
    effective = resolve_effective_scope(access=_access(), scope=_scope())
    assert effective.empty is False
    assert effective.sources == ("gmail",)
    assert effective.card_types == ("email_message",)
    assert effective.people == ("ada",)


def test_inferred_dimension_is_not_applied_silently() -> None:
    effective = resolve_effective_scope(
        access=_access(),
        scope=_scope(),
        request=RequestFilters(sources=("slack",), inferred_dimensions=("sources",)),
    )
    assert effective.sources == ("gmail",)
    assert effective.origins["sources"] == "scope"
    assert "sources" in effective.inferred


def test_domain_intersection_can_empty() -> None:
    effective = resolve_effective_scope(
        access=_access(allowed_domains=("medical",)),
        scope=_scope(card_types=("email_message",)),
    )
    assert effective.empty is True
    assert effective.reason == "empty_intersection:domains"


def test_invalid_retrieval_profile_rejected() -> None:
    with pytest.raises(ConfigError, match="retrieval_profile"):
        SavedScope.from_payload({"name": "bad", "retrieval_profile": "everything"})
