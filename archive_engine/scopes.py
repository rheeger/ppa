"""Policy-safe saved scopes (P09-B).

Saved scopes are reusable query filters for this instance — not tenancy or
project ACLs. Explicit request filters replace the same preset dimension.
The resolved AccessContext is always intersected and never widened. An empty
permitted intersection is an empty-scope result, never “search everything.”
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from archive_engine.access import classify_domain, normalize_label, normalize_labels, source_allowed
from archive_engine.contracts import AccessContext
from archive_engine.errors import ConfigError

SCOPE_SCHEMA_VERSION = 1
RETRIEVAL_PROFILES = frozenset({"historical", "current_ops"})
FILTER_DIMENSIONS = ("card_types", "sources", "account_scopes", "people", "orgs", "start_date", "end_date")


@dataclass(frozen=True)
class SavedScope:
    name: str
    description: str = ""
    version: int = SCOPE_SCHEMA_VERSION
    card_types: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    account_scopes: tuple[str, ...] = ()
    people: tuple[str, ...] = ()
    orgs: tuple[str, ...] = ()
    start_date: str = ""
    end_date: str = ""
    retrieval_profile: Literal["historical", "current_ops"] = "historical"

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "card_types": list(self.card_types),
            "sources": list(self.sources),
            "account_scopes": list(self.account_scopes),
            "people": list(self.people),
            "orgs": list(self.orgs),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "retrieval_profile": self.retrieval_profile,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> SavedScope:
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ConfigError("saved scope name is required")
        profile = str(payload.get("retrieval_profile") or "historical").strip() or "historical"
        if profile not in RETRIEVAL_PROFILES:
            raise ConfigError(f"invalid retrieval_profile {profile!r}")
        try:
            version = int(payload.get("version") or SCOPE_SCHEMA_VERSION)
        except (TypeError, ValueError) as exc:
            raise ConfigError("saved scope version must be an integer") from exc
        return cls(
            name=name,
            description=str(payload.get("description") or ""),
            version=version,
            card_types=_labels(payload.get("card_types") or payload.get("allowed_card_types")),
            sources=_labels(payload.get("sources") or payload.get("allowed_sources")),
            account_scopes=_labels(payload.get("account_scopes")),
            people=_labels(payload.get("people") or payload.get("person_refs")),
            orgs=_labels(payload.get("orgs") or payload.get("org_refs")),
            start_date=str(payload.get("start_date") or payload.get("time_start") or "").strip(),
            end_date=str(payload.get("end_date") or payload.get("time_end") or "").strip(),
            retrieval_profile=profile,  # type: ignore[arg-type]
        )


@dataclass(frozen=True)
class RequestFilters:
    card_types: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    account_scopes: tuple[str, ...] = ()
    people: tuple[str, ...] = ()
    orgs: tuple[str, ...] = ()
    start_date: str = ""
    end_date: str = ""
    inferred_dimensions: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> RequestFilters:
        data = dict(payload or {})
        return cls(
            card_types=_labels(data.get("card_types") or data.get("type_filter")),
            sources=_labels(data.get("sources") or data.get("source_filter")),
            account_scopes=_labels(data.get("account_scopes")),
            people=_labels(data.get("people") or data.get("people_filter")),
            orgs=_labels(data.get("orgs") or data.get("org_filter")),
            start_date=str(data.get("start_date") or "").strip(),
            end_date=str(data.get("end_date") or "").strip(),
            inferred_dimensions=_labels(data.get("inferred_dimensions")),
        )


@dataclass(frozen=True)
class EffectiveScope:
    empty: bool
    reason: str
    card_types: tuple[str, ...]
    sources: tuple[str, ...]
    account_scopes: tuple[str, ...]
    people: tuple[str, ...]
    orgs: tuple[str, ...]
    start_date: str
    end_date: str
    retrieval_profile: str
    origins: dict[str, str] = field(default_factory=dict)
    inferred: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "empty_scope": self.empty,
            "reason": self.reason,
            "card_types": list(self.card_types),
            "sources": list(self.sources),
            "account_scopes": list(self.account_scopes),
            "people": list(self.people),
            "orgs": list(self.orgs),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "retrieval_profile": self.retrieval_profile,
            "origins": dict(self.origins),
            "inferred": list(self.inferred),
        }

    def query_kwargs(self) -> dict[str, str]:
        if self.empty:
            return {
                "type_filter": "__empty_scope__",
                "source_filter": "__empty_scope__",
                "people_filter": "__empty_scope__",
                "org_filter": "__empty_scope__",
                "start_date": "9999-12-31",
                "end_date": "9999-12-31",
            }
        return {
            "type_filter": ",".join(self.card_types),
            "source_filter": ",".join(self.sources),
            "people_filter": ",".join(self.people),
            "org_filter": ",".join(self.orgs),
            "start_date": self.start_date,
            "end_date": self.end_date,
        }


def _labels(raw: object) -> tuple[str, ...]:
    if raw is None or raw == "":
        return ()
    if isinstance(raw, str):
        parts = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
        return normalize_labels(parts)
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return normalize_labels(str(item) for item in raw)
    return normalize_labels((str(raw),))


def _intersect_labels(left: Sequence[str], right: Sequence[str]) -> tuple[str, ...]:
    if not left:
        return tuple(right)
    if not right:
        return tuple(left)
    allowed = list(right)
    out: list[str] = []
    for item in left:
        if source_allowed(allowed, item) or normalize_label(item) in {normalize_label(x) for x in allowed}:
            if item not in out:
                out.append(item)
    return tuple(out)


def _intersect_sources(scope_or_request: Sequence[str], access_sources: Sequence[str]) -> tuple[str, ...]:
    if not access_sources:
        return tuple(scope_or_request)
    if not scope_or_request:
        return tuple(access_sources)
    out = [item for item in scope_or_request if source_allowed(access_sources, item)]
    return tuple(out)


def _types_allowed_by_domains(card_types: Sequence[str], allowed_domains: Sequence[str]) -> tuple[str, ...]:
    if not allowed_domains or not card_types:
        return tuple(card_types)
    allowed = set(normalize_labels(allowed_domains))
    kept: list[str] = []
    for card_type in card_types:
        domain = classify_domain(card_type=card_type)
        if domain in allowed or domain in {"general", "unknown"}:
            kept.append(card_type)
    return tuple(kept)


def parse_saved_scopes(payloads: Sequence[Mapping[str, object]] | None) -> tuple[SavedScope, ...]:
    return tuple(SavedScope.from_payload(item) for item in payloads or ())


def scope_by_name(scopes: Sequence[SavedScope], name: str) -> SavedScope | None:
    needle = normalize_label(name)
    for scope in scopes:
        if normalize_label(scope.name) == needle:
            return scope
    return None


def resolve_effective_scope(
    *,
    access: AccessContext,
    scope: SavedScope | None = None,
    request: RequestFilters | None = None,
) -> EffectiveScope:
    """Intersect saved scope, request filters, and AccessContext.

    Request filters replace the same preset dimension. Access is an upper bound.
    """

    req = request or RequestFilters()
    origins: dict[str, str] = {}
    inferred = tuple(dim for dim in req.inferred_dimensions if dim in FILTER_DIMENSIONS)

    def _dimension(
        name: str, scope_value: tuple[str, ...] | str, request_value: tuple[str, ...] | str
    ) -> tuple[str, ...] | str:
        if name in inferred:
            origins[name] = "inferred_ignored"
            if isinstance(scope_value, str):
                if scope_value:
                    origins[name] = "scope"
                return scope_value
            if scope_value:
                origins[name] = "scope"
            return scope_value
        if isinstance(request_value, str):
            if request_value.strip():
                origins[name] = "request"
                return request_value.strip()
            if isinstance(scope_value, str) and scope_value:
                origins[name] = "scope"
            return scope_value
        if request_value:
            origins[name] = "request"
            return request_value
        if scope_value:
            origins[name] = "scope"
        return scope_value

    card_types = _dimension("card_types", scope.card_types if scope else (), req.card_types)
    sources = _dimension("sources", scope.sources if scope else (), req.sources)
    account_scopes = _dimension("account_scopes", scope.account_scopes if scope else (), req.account_scopes)
    people = _dimension("people", scope.people if scope else (), req.people)
    orgs = _dimension("orgs", scope.orgs if scope else (), req.orgs)
    start_date = str(_dimension("start_date", scope.start_date if scope else "", req.start_date))
    end_date = str(_dimension("end_date", scope.end_date if scope else "", req.end_date))
    assert isinstance(card_types, tuple)
    assert isinstance(sources, tuple)
    assert isinstance(account_scopes, tuple)
    assert isinstance(people, tuple)
    assert isinstance(orgs, tuple)

    if access.deny:
        return EffectiveScope(
            empty=True,
            reason=access.deny_reason or "access_denied",
            card_types=(),
            sources=(),
            account_scopes=(),
            people=(),
            orgs=(),
            start_date="",
            end_date="",
            retrieval_profile=scope.retrieval_profile if scope else "historical",
            origins={"access": "deny"},
            inferred=inferred,
        )

    if access.allowed_sources:
        intersected = _intersect_sources(sources, access.allowed_sources)
        if sources and not intersected:
            return EffectiveScope(
                empty=True,
                reason="empty_intersection:sources",
                card_types=card_types,
                sources=(),
                account_scopes=account_scopes,
                people=people,
                orgs=orgs,
                start_date=start_date,
                end_date=end_date,
                retrieval_profile=scope.retrieval_profile if scope else "historical",
                origins={**origins, "sources": "access_intersect"},
                inferred=inferred,
            )
        if not sources:
            sources = tuple(access.allowed_sources)
            origins["sources"] = "access"
        else:
            sources = intersected
            origins["sources"] = "access_intersect"

    if access.allowed_domains and card_types:
        kept = _types_allowed_by_domains(card_types, access.allowed_domains)
        if card_types and not kept:
            return EffectiveScope(
                empty=True,
                reason="empty_intersection:domains",
                card_types=(),
                sources=sources,
                account_scopes=account_scopes,
                people=people,
                orgs=orgs,
                start_date=start_date,
                end_date=end_date,
                retrieval_profile=scope.retrieval_profile if scope else "historical",
                origins={**origins, "card_types": "access_intersect"},
                inferred=inferred,
            )
        card_types = kept
        origins["card_types"] = "access_intersect"

    return EffectiveScope(
        empty=False,
        reason="",
        card_types=card_types,
        sources=sources,
        account_scopes=account_scopes,
        people=people,
        orgs=orgs,
        start_date=start_date,
        end_date=end_date,
        retrieval_profile=scope.retrieval_profile if scope else "historical",
        origins=origins,
        inferred=inferred,
    )


def empty_scope_result(*, reason: str, effective: EffectiveScope | None = None) -> dict[str, Any]:
    """Honest empty-scope payload. Never a fallback to unscoped search."""

    payload = {
        "ok": True,
        "empty_scope": True,
        "reason": reason,
        "rows": [],
        "hits": [],
    }
    if effective is not None:
        payload["effective_scope"] = effective.to_payload()
    return payload
