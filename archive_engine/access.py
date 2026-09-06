"""Access policy for every retrieval surface (P05-B).

One ``AccessContext`` is resolved at the entry point and passed through
search, hybrid/vector, graph, person, timeline, evidence, and raw reads.
Filtering happens before top-k, counts, and graph expansion.

Mixed-source derived records deny if any required source is denied. Future
claims inherit the same rule — do not implement a claim layer here.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from archive_engine.contracts import AccessContext, ArchiveIdentity

# Versioned conservative classifier. Unknown type/source stays unknown.
DOMAIN_TAXONOMY_VERSION = "p05b-domain-v1"

MEDICAL_TYPES = frozenset({"medical_record", "vaccination", "health_metric"})
FINANCE_TYPES = frozenset(
    {
        "finance",
        "purchase",
        "meal_order",
        "grocery_order",
        "ride",
        "flight",
        "subscription",
        "invoice",
        "receipt",
        "bank_transaction",
        "tax_document",
    }
)
COMMUNICATION_TYPES = frozenset(
    {
        "email_message",
        "email_thread",
        "email_attachment",
        "imessage_message",
        "imessage_thread",
        "imessage_attachment",
        "beeper_message",
        "beeper_thread",
        "beeper_attachment",
        "sms",
    }
)
CALENDAR_TYPES = frozenset({"calendar_event", "meeting_transcript"})
IDENTITY_TYPES = frozenset({"person", "organization", "place"})
CODE_TYPES = frozenset({"git_repository", "git_commit", "git_thread", "git_message"})
MEDIA_TYPES = frozenset({"media_asset", "document"})

MEDICAL_SOURCE_MARKERS = ("medical", "health", "hospital", "clinic", "ehr")
FINANCE_SOURCE_MARKERS = ("bank", "finance", "stripe", "plaid", "tax", "payroll")

VALID_TOOL_PROFILES = frozenset({"full", "read-only", "remote-read", "admin-only"})
PROFILE_LABELS = ", ".join(sorted(VALID_TOOL_PROFILES))

# Empty allow-list means unrestricted at this layer (trusted-local). None = all tools.
TOOL_PROFILES: dict[str, frozenset[str] | None] = {
    "full": None,
    "read-only": frozenset(
        {
            "archive_search",
            "archive_read",
            "archive_query",
            "archive_analytics",
            "archive_graph",
            "archive_person",
            "archive_timeline",
            "archive_evidence",
            "archive_temporal_neighbors",
            "archive_knowledge",
            "archive_stats",
            "archive_vector_search",
            "archive_hybrid_search",
            "archive_search_json",
            "archive_hybrid_search_json",
            "archive_read_many",
            "archive_status_json",
            "archive_retrieval_explain_json",
        }
    ),
    "remote-read": frozenset(
        {
            "archive_search",
            "archive_query",
            "archive_analytics",
            "archive_timeline",
            "archive_evidence",
            "archive_stats",
            "archive_search_json",
        }
    ),
    "admin-only": frozenset(
        {
            "archive_validate",
            "archive_duplicates",
            "archive_duplicate_uids",
            "archive_rebuild_indexes",
            "archive_bootstrap_postgres",
            "archive_index_status",
            "archive_projection_inventory",
            "archive_projection_status",
            "archive_projection_explain",
            "archive_retrieval_explain",
            "archive_embedding_status",
            "archive_embedding_backlog",
            "archive_embed_estimate",
            "archive_embed_pending",
            "archive_seed_link_surface",
            "archive_seed_link_enqueue",
            "archive_seed_link_backfill",
            "archive_seed_link_refresh",
            "archive_seed_link_worker",
            "archive_seed_link_promote",
            "archive_seed_link_report",
            "archive_link_candidates",
            "archive_link_candidate",
            "archive_review_link_candidate",
            "archive_link_quality_gate",
            "archive_status_json",
        }
    ),
}

TRUSTED_LOCAL_PRINCIPAL = "local-operator"
TRUSTED_LOCAL_PROFILE = "trusted-local"
DEFAULT_EGRESS_POLICY_REVISION = "p05b-unspecified"

RETRIEVAL_TOOLS = frozenset(
    {
        "archive_search",
        "archive_query",
        "archive_analytics",
        "archive_graph",
        "archive_person",
        "archive_timeline",
        "archive_evidence",
        "archive_temporal_neighbors",
        "archive_vector_search",
        "archive_hybrid_search",
        "archive_read",
        "archive_read_many",
    }
)


def normalize_label(value: str) -> str:
    return " ".join((value or "").strip().casefold().split())


def normalize_labels(values: Iterable[str] | None) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or ():
        label = normalize_label(str(raw))
        if not label or label in seen:
            continue
        seen.add(label)
        out.append(label)
    return tuple(out)


def _csv_labels(raw: str) -> tuple[str, ...]:
    if not raw.strip():
        return ()
    return normalize_labels(part for part in raw.split(",") if part.strip())


def classify_domain(*, card_type: str = "", sources: Sequence[str] = ()) -> str:
    """Deterministic domain from type/source metadata. Unknown stays unknown."""

    kind = normalize_label(card_type).replace("-", "_")
    if kind in MEDICAL_TYPES:
        return "medical"
    if kind in FINANCE_TYPES:
        return "finance"
    if kind in COMMUNICATION_TYPES:
        return "communication"
    if kind in CALENDAR_TYPES:
        return "calendar"
    if kind in IDENTITY_TYPES:
        return "identity"
    if kind in CODE_TYPES:
        return "code"
    if kind in MEDIA_TYPES:
        return "media"
    joined = " ".join(normalize_label(src) for src in sources)
    if any(marker in joined for marker in MEDICAL_SOURCE_MARKERS):
        return "medical"
    if any(marker in joined for marker in FINANCE_SOURCE_MARKERS):
        return "finance"
    if kind:
        return "general"
    return "unknown"


def card_domains(*, card_type: str = "", sources: Sequence[str] = (), domains: Sequence[str] = ()) -> tuple[str, ...]:
    explicit = normalize_labels(domains)
    if explicit:
        return explicit
    return (classify_domain(card_type=card_type, sources=sources),)


def source_allowed(allowed: Sequence[str], source: str) -> bool:
    """Exact normalized match, or account-qualified prefix (gmail:foo vs gmail)."""

    needle = normalize_label(source)
    if not needle:
        return False
    for item in allowed:
        allow = normalize_label(item)
        if not allow:
            continue
        if needle == allow or needle.startswith(f"{allow}:") or allow.startswith(f"{needle}:"):
            return True
    return False


def is_restricted(access: AccessContext) -> bool:
    if access.deny:
        return True
    return bool(access.allowed_sources or access.allowed_domains)


def is_unrestricted(access: AccessContext) -> bool:
    return not is_restricted(access)


def required_sources_for(record: Mapping[str, Any]) -> tuple[str, ...]:
    required = record.get("required_sources")
    if isinstance(required, (list, tuple)) and required:
        return normalize_labels(str(item) for item in required)
    sources = record.get("sources")
    if isinstance(sources, str):
        return normalize_labels((sources,))
    if isinstance(sources, (list, tuple)):
        return normalize_labels(str(item) for item in sources)
    source = record.get("source")
    if isinstance(source, str):
        return normalize_labels((source,))
    if isinstance(source, (list, tuple)):
        return normalize_labels(str(item) for item in source)
    return ()


def lineage_complete_for(record: Mapping[str, Any]) -> bool | None:
    if "lineage_complete" not in record:
        return None
    raw = record.get("lineage_complete")
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def card_permitted(access: AccessContext, record: Mapping[str, Any] | None) -> bool:
    """Permit a card/row. Missing lineage in a restricted context is deny.

    Future claims use this same derived-deny rule: a claim whose required
    evidence includes a denied source is denied.
    """

    if access.deny:
        return False
    if not is_restricted(access):
        return True
    if record is None:
        return False
    required = required_sources_for(record)
    if not required:
        return False
    if access.allowed_sources:
        for source in required:
            if not source_allowed(access.allowed_sources, source):
                return False
    if lineage_complete_for(record) is False:
        return False
    if access.allowed_domains:
        domains = card_domains(
            card_type=str(record.get("type") or record.get("card_type") or ""),
            sources=required,
            domains=tuple(str(item) for item in (record.get("domains") or ())),
        )
        if not domains or any(domain == "unknown" for domain in domains):
            return False
        allowed = set(normalize_labels(access.allowed_domains))
        for domain in domains:
            if domain not in allowed:
                return False
    return True


def tool_permitted(access: AccessContext, tool_name: str) -> bool:
    if access.deny:
        return False
    if not access.allowed_tools:
        return True
    return tool_name in access.allowed_tools


def policy_identity(access: AccessContext) -> str:
    """Stable cache/result scope. Changing lists or revision changes the id."""

    material = "\0".join(
        (
            access.archive_id,
            access.principal,
            access.profile,
            ",".join(access.allowed_tools),
            ",".join(access.allowed_sources),
            ",".join(access.allowed_domains),
            access.egress_policy_revision,
            "1" if access.deny else "0",
            DOMAIN_TAXONOMY_VERSION,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def access_request_fields(access: AccessContext) -> dict[str, Any]:
    """Native request fields. Unrestricted omits restriction so P01 ranking is unchanged."""

    fields: dict[str, Any] = {
        "access_deny": bool(access.deny),
        "access_restricted": is_restricted(access),
        "access_policy_identity": policy_identity(access),
    }
    if is_restricted(access):
        fields["access_sources"] = list(access.allowed_sources)
        fields["access_domains"] = list(access.allowed_domains)
    return fields


def filter_records(access: AccessContext, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if card_permitted(access, row)]


def not_found_payload(path_or_uid: str = "", *, include_rel: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {"found": False, "content": ""}
    if path_or_uid.endswith(".md") or include_rel:
        payload["rel_path"] = ""
    return payload


def normalize_tool_profile(raw: str | None, *, present: bool) -> tuple[str, str | None]:
    """Return ``(normalized_profile, invalid_error)``.

    Unset env uses documented ``full``. Empty or unknown values fail closed.
    """

    if not present:
        return "full", None
    profile = (raw or "").strip().lower()
    if not profile or profile not in TOOL_PROFILES:
        shown = (raw or "").strip()
        if len(shown) > 64:
            shown = shown[:64] + "..."
        label = shown if shown else "(empty)"
        return "", f"Invalid PPA_MCP_TOOL_PROFILE={label!r}. Valid profiles: {PROFILE_LABELS}"
    return profile, None


def tools_for_profile(profile: str) -> tuple[str, ...]:
    allowed = TOOL_PROFILES.get(profile)
    if allowed is None:
        return ()
    return tuple(sorted(allowed))


def env_flag(name: str, environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return str(env.get(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def resolve_access_context(
    archive_id: str,
    *,
    environ: Mapping[str, str] | None = None,
    identity: ArchiveIdentity | None = None,
) -> AccessContext:
    """Resolve one explicit context from process env. Never widen a typo to full."""

    env = os.environ if environ is None else environ
    aid = (identity.archive_id if identity is not None else archive_id).strip()
    present = "PPA_MCP_TOOL_PROFILE" in env
    profile, invalid = normalize_tool_profile(env.get("PPA_MCP_TOOL_PROFILE"), present=present)
    explicit_profile = (env.get("PPA_ACCESS_PROFILE") or "").strip()
    if explicit_profile:
        profile = explicit_profile
    principal = (env.get("PPA_ACCESS_PRINCIPAL") or "").strip() or TRUSTED_LOCAL_PRINCIPAL
    if not explicit_profile and profile == "full" and not present:
        profile = TRUSTED_LOCAL_PROFILE
    elif not explicit_profile and profile == "full":
        profile = "full"
    allowed_tools = tools_for_profile("full" if profile in {TRUSTED_LOCAL_PROFILE, "full"} else profile)
    extra_tools = _csv_labels(env.get("PPA_ACCESS_ALLOWED_TOOLS", ""))
    if extra_tools:
        allowed_tools = extra_tools
    allowed_sources = _csv_labels(env.get("PPA_ACCESS_ALLOWED_SOURCES", ""))
    allowed_domains = _csv_labels(env.get("PPA_ACCESS_ALLOWED_DOMAINS", ""))
    egress = (env.get("PPA_ACCESS_EGRESS_POLICY_REVISION") or "").strip() or DEFAULT_EGRESS_POLICY_REVISION
    deny = env_flag("PPA_ACCESS_DENY", env)
    deny_reason = ""
    if invalid:
        deny = True
        deny_reason = invalid
        profile = profile or "invalid"
        allowed_tools = ()
        allowed_sources = ()
        allowed_domains = ()
    return AccessContext(
        archive_id=aid or "unspecified",
        principal=principal,
        profile=profile or "invalid",
        allowed_tools=allowed_tools,
        allowed_sources=allowed_sources,
        allowed_domains=allowed_domains,
        egress_policy_revision=egress,
        deny=deny,
        deny_reason=deny_reason,
    )


def record_from_frontmatter(frontmatter: Mapping[str, Any] | None, *, card_type: str = "") -> dict[str, Any]:
    fm = dict(frontmatter or {})
    sources = fm.get("source") or fm.get("sources") or []
    if isinstance(sources, str):
        sources = [sources]
    return {
        "type": str(fm.get("type") or card_type or ""),
        "sources": list(sources),
        "required_sources": list(fm.get("required_sources") or sources),
        "domains": list(fm.get("domains") or []),
        "lineage_complete": fm.get("lineage_complete"),
    }
