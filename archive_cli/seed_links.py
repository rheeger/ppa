"""Seed link enrichment policies, candidate generation, and scoring."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from threading import Lock
from typing import Any

from archive_vault.provenance import ProvenanceEntry, compute_input_hash
from archive_vault.schema import validate_card_permissive, validate_card_strict
from archive_vault.vault import extract_wikilinks, read_note, write_card

from .features import card_activity_at, external_ids_by_provider
from .index_config import (get_default_embedding_model,
                           get_default_embedding_version)
from .vault_cache import VaultScanCache

try:
    from archive_vault.llm_provider import (GROUNDING_INSTRUCTION,
                                            get_provider_chain)
except Exception:  # pragma: no cover
    GROUNDING_INSTRUCTION = "Use only the provided data. Do not invent facts."

    def get_provider_chain(_vault_path: str | Path) -> list[Any]:
        return []


SEED_LINKER_VERSION = 1
# Phase 6.5 (2026-04-20): bumped from 5 -> 6 in tandem with register_linker
# wiring the new modules (finance_reconcile, trip_cluster, meeting_artifact).
# Cache-invalidation semantics: rows in link_decisions with policy_version<6
# are stale; seed-link-run re-evaluates them on next sweep. No migration.
SEED_LINK_POLICY_VERSION = 6
DEFAULT_SEED_LINK_WORKERS = 8
DEFAULT_SEED_LINK_CLAIM_BATCH_SIZE = 32
DEFAULT_PROMOTION_CLAIM_BATCH_SIZE = 64
DEFAULT_SCORE_BANDS = (
    "0.00-0.24",
    "0.25-0.44",
    "0.45-0.59",
    "0.60-0.79",
    "0.80-0.91",
    "0.92-1.00",
)

MODULE_IDENTITY = "identityLinker"
MODULE_COMMUNICATION = "communicationLinker"
MODULE_CALENDAR = "calendarLinker"
MODULE_MEDIA = "mediaLinker"
MODULE_ORPHAN = "orphanRepairLinker"
MODULE_GRAPH = "graphConsistencyLinker"
MODULE_SEMANTIC = "semanticLinker"

DEFAULT_SEMANTIC_K = 20
DEFAULT_SEMANTIC_THRESHOLD = 0.7
DEFAULT_SEMANTIC_OVERFETCH_RATIO = 3
DEFAULT_SEMANTIC_CHUNK_FANOUT = 10

# Phase 6 Tier 4: triage classifications (from card_classifications projection or
# email_thread.triage_classification frontmatter) that disqualify a card from being
# either a source or target of a semantic link. The default skip set mirrors
# archive_sync/llm_enrichment/triage.SKIP_CLASSIFICATIONS plus the equivalent legacy
# values that the older classify pipeline writes ('marketing', 'automated', 'noise',
# 'personal' from the v1 classifier vs 'marketing', 'automated_notification',
# 'noise', 'person_to_person' from the v2 triage prompt).
DEFAULT_SEMANTIC_SKIP_CLASSIFICATIONS = frozenset(
    {
        "marketing",
        "automated",
        "automated_notification",
        "noise",
        "personal",
        "person_to_person",
    }
)

# Phase 6 Tier 4 / Step 24: card-type allowlist for the semantic linker. Calibrated
# 2026-04-19 against the 1pct sweep (qualitative report at
# _artifacts/_semantic-linker-calibration/qualitative-1pct-20260419.md): without an
# allowlist, 99.5% of surfaced candidates are noise (filename-similar attachments,
# Apple-Health aggregate clusters, github review notifications, near-duplicate
# photos). The allowlist names the types where cross-domain semantic bridges are
# plausibly useful. Every type NOT in this set is excluded as both source and target.
DEFAULT_SEMANTIC_ALLOWED_TYPES = frozenset(
    {
        "calendar_event",
        "meeting_transcript",
        "document",
        "place",
        "organization",
        "person",
        "knowledge",
        "observation",
        "accommodation",
        "flight",
        "car_rental",
        "ride",
        "finance",
        "purchase",
        "subscription",
        "payroll",
        "event_ticket",
        "medical_record",
        "vaccination",
        "meal_order",
        "grocery_order",
        "shipment",
        "email_thread",  # threads OK; messages excluded (already linked deterministically)
    }
)

# Same-type semantic links: only useful for a small set of card types where a
# secondary instance can be a meaningfully-related cluster (multiple meetings on
# the same project, multiple records about the same diagnosis). For the rest,
# same-type pairs are nearly always template / aggregate noise.
DEFAULT_SEMANTIC_ALLOW_SAME_TYPE = frozenset(
    {
        "calendar_event",
        "meeting_transcript",
        "document",
        "place",
        "organization",
        "knowledge",
        "observation",
    }
)

# Summary patterns that flag a card as template/aggregate noise rather than
# semantic content. Cards whose summary matches are excluded from semantic linking
# regardless of type. (The major offenders observed in the 1pct sweep.)
DEFAULT_SEMANTIC_NOISE_SUMMARY_RE = re.compile(
    r"^("
    r"HK\w+Identifier|"           # Apple Health aggregate metric classes
    r"IMG[_-]?\d|"                 # iPhone photos
    r"MOV[_-]?\d|"                 # iPhone videos
    r"DSC[_-]?\d|"                 # Sony / generic camera blobs
    r"image\d+\.|"                 # embedded inline email images
    r"~WRD\d|"                     # Word temp attachments
    r"giphy"                       # giphy stickers
    r")",
    re.IGNORECASE,
)

REVIEW_ACTION_APPROVE = "approve"
REVIEW_ACTION_REJECT = "reject"
REVIEW_ACTION_OVERRIDE_APPROVE = "override_approve"
REVIEW_ACTION_OVERRIDE_REJECT = "override_reject"
REVIEW_ACTIONS = frozenset(
    {
        REVIEW_ACTION_APPROVE,
        REVIEW_ACTION_REJECT,
        REVIEW_ACTION_OVERRIDE_APPROVE,
        REVIEW_ACTION_OVERRIDE_REJECT,
    }
)

DECISION_DISCARD = "discard"
DECISION_REVIEW = "review"
DECISION_AUTO_PROMOTE = "auto_promote"
DECISION_CANONICAL_SAFE = "canonical_safe"

DECISION_REASON_EXACT_IDENTIFIER = "exact_identifier"
DECISION_REASON_EXACT_REVERSE_LINK = "exact_reverse_link"
DECISION_REASON_EXACT_PARTICIPANT = "exact_participant"
DECISION_REASON_CALENDAR_HINT = "calendar_hint"
DECISION_REASON_HIGH_RISK = "high_risk"
DECISION_REASON_BORDERLINE = "borderline"
DECISION_REASON_LOW_CONFIDENCE = "low_confidence"
DECISION_REASON_REVIEW_OVERRIDE = "review_override"
DECISION_REASON_NO_TARGET = "no_target"

STATUS_PENDING_QC = "pending_qc"
STATUS_NEEDS_REVIEW = "needs_review"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"
STATUS_PROMOTED = "promoted"

PROMOTION_TARGET_DERIVED_EDGE = "derived_edge"
PROMOTION_TARGET_CANONICAL_FIELD = "canonical_field"

PROMOTION_STATUS_QUEUED = "queued"
PROMOTION_STATUS_APPLIED = "applied"
PROMOTION_STATUS_BLOCKED = "blocked"
PROMOTION_STATUS_ROLLED_BACK = "rolled_back"

SURFACE_CANDIDATE_DERIVED = "candidate_derived"
SURFACE_DERIVED_ONLY = "derived_only"
SURFACE_CANONICAL_SAFE = "canonical_safe"

LINK_TYPE_MESSAGE_IN_THREAD = "message_in_thread"
LINK_TYPE_THREAD_HAS_MESSAGE = "thread_has_message"
LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT = "message_has_calendar_event"
LINK_TYPE_THREAD_HAS_CALENDAR_EVENT = "thread_has_calendar_event"
LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT = "transcript_has_calendar_event"
LINK_TYPE_EVENT_HAS_MESSAGE = "event_has_message"
LINK_TYPE_EVENT_HAS_THREAD = "event_has_thread"
LINK_TYPE_EVENT_HAS_TRANSCRIPT = "event_has_transcript"
LINK_TYPE_MESSAGE_HAS_ATTACHMENT = "message_has_attachment"
LINK_TYPE_THREAD_HAS_ATTACHMENT = "thread_has_attachment"
LINK_TYPE_THREAD_HAS_PERSON = "thread_has_person"
LINK_TYPE_MESSAGE_MENTIONS_PERSON = "message_mentions_person"
LINK_TYPE_EVENT_HAS_PERSON = "event_has_person"
LINK_TYPE_MEDIA_HAS_PERSON = "media_has_person"
LINK_TYPE_MEDIA_HAS_EVENT = "media_has_event"
LINK_TYPE_POSSIBLE_SAME_PERSON = "possible_same_person"
LINK_TYPE_ORPHAN_REPAIR_EXACT = "orphan_repair_exact"
LINK_TYPE_ORPHAN_REPAIR_FUZZY = "orphan_repair_fuzzy"
LINK_TYPE_SEMANTICALLY_RELATED = "semantically_related"

# Phase 6.5 new link types.
LINK_TYPE_FINANCE_RECONCILES = "finance_reconciles"
LINK_TYPE_PART_OF_TRIP = "part_of_trip"

# Mutable set so Phase 6.5's register_linker can extend it at module-load time.
# Membership checks (`x in PROPOSED_LINK_TYPES`) continue to work.
PROPOSED_LINK_TYPES: set[str] = {
    LINK_TYPE_MESSAGE_IN_THREAD,
    LINK_TYPE_THREAD_HAS_MESSAGE,
    LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT,
    LINK_TYPE_THREAD_HAS_CALENDAR_EVENT,
    LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,
    LINK_TYPE_EVENT_HAS_MESSAGE,
    LINK_TYPE_EVENT_HAS_THREAD,
    LINK_TYPE_EVENT_HAS_TRANSCRIPT,
    LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
    LINK_TYPE_THREAD_HAS_ATTACHMENT,
    LINK_TYPE_THREAD_HAS_PERSON,
    LINK_TYPE_MESSAGE_MENTIONS_PERSON,
    LINK_TYPE_EVENT_HAS_PERSON,
    LINK_TYPE_MEDIA_HAS_PERSON,
    LINK_TYPE_MEDIA_HAS_EVENT,
    LINK_TYPE_POSSIBLE_SAME_PERSON,
    LINK_TYPE_ORPHAN_REPAIR_EXACT,
    LINK_TYPE_ORPHAN_REPAIR_FUZZY,
    LINK_TYPE_SEMANTICALLY_RELATED,
}

CARD_TYPE_MODULES = {
    "person": (MODULE_IDENTITY, MODULE_ORPHAN),
    "finance": (),
    "medical_record": (),
    "vaccination": (),
    "email_thread": (MODULE_COMMUNICATION, MODULE_CALENDAR, MODULE_GRAPH, MODULE_ORPHAN),
    "email_message": (MODULE_COMMUNICATION, MODULE_CALENDAR, MODULE_GRAPH, MODULE_ORPHAN),
    "email_attachment": (MODULE_COMMUNICATION, MODULE_GRAPH),
    "imessage_thread": (MODULE_COMMUNICATION, MODULE_GRAPH, MODULE_ORPHAN),
    "imessage_message": (MODULE_COMMUNICATION, MODULE_GRAPH, MODULE_ORPHAN),
    "imessage_attachment": (MODULE_COMMUNICATION, MODULE_GRAPH),
    "beeper_thread": (MODULE_COMMUNICATION, MODULE_GRAPH),
    "beeper_message": (MODULE_COMMUNICATION, MODULE_GRAPH),
    "beeper_attachment": (MODULE_COMMUNICATION, MODULE_GRAPH),
    "calendar_event": (MODULE_CALENDAR, MODULE_GRAPH, MODULE_ORPHAN),
    "meeting_transcript": (MODULE_CALENDAR, MODULE_GRAPH, MODULE_ORPHAN),
    "media_asset": (MODULE_MEDIA, MODULE_ORPHAN),
    "document": (),
    "git_repository": (MODULE_GRAPH, MODULE_ORPHAN),
    "git_commit": (MODULE_GRAPH,),
    "git_thread": (MODULE_GRAPH, MODULE_ORPHAN),
    "git_message": (MODULE_GRAPH, MODULE_ORPHAN),
    "meal_order": (),
    "grocery_order": (),
    "ride": (),
    "flight": (),
    "accommodation": (),
    "car_rental": (),
    "purchase": (),
    "shipment": (),
    "subscription": (),
    "event_ticket": (),
    "payroll": (),
    "place": (),
    "organization": (),
    "knowledge": (),
    "observation": (),
}

# Phase 6 Tier 3 retirement (2026-04-19): MODULE_SEMANTIC is kept in source for
# reference but not wired into CARD_TYPE_MODULES. See
# archive_docs/runbooks/phase6-retirement-rationale.md for the honest account of
# why the semantic kNN linker was retired in favor of the Phase 6.5 structural
# cross-derived-card linkers (MODULE_FINANCE_RECONCILE, MODULE_TRIP_CLUSTER, etc.).
# Mutable so Phase 6.5's register_linker can extend it based on
# LinkerSpec.requires_llm_judge. The three new Phase 6.5 modules are
# deterministic (requires_llm_judge=False) so they do NOT end up here.
LLM_REVIEW_MODULES: set[str] = {
    MODULE_IDENTITY, MODULE_CALENDAR, MODULE_MEDIA, MODULE_ORPHAN,
}
HIGH_PRIORITY_CARD_TYPES = frozenset(
    {
        "person",
        "email_thread",
        "email_message",
        "imessage_thread",
        "imessage_message",
        "calendar_event",
        "meeting_transcript",
    }
)
LOW_PRIORITY_CARD_TYPES = frozenset({"finance"})
WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")


@dataclass(frozen=True, slots=True)
class LinkSurfacePolicy:
    link_type: str
    module_name: str
    surface: str
    promotion_target: str
    canonical_field_name: str = ""
    canonical_value_mode: str = "slug"
    auto_review_floor: float = 0.45
    auto_promote_floor: float = 0.80
    canonical_floor: float = 0.92
    description: str = ""


@dataclass(slots=True)
class LinkEvidence:
    evidence_type: str
    evidence_source: str
    feature_name: str
    feature_value: str
    feature_weight: float
    raw_payload_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SeedCardSketch:
    uid: str
    rel_path: str
    slug: str
    card_type: str
    summary: str
    frontmatter: dict[str, Any]
    body: str
    content_hash: str
    activity_at: str
    wikilinks: list[str]
    emails: set[str] = field(default_factory=set)
    phones: set[str] = field(default_factory=set)
    handles: set[str] = field(default_factory=set)
    aliases: set[str] = field(default_factory=set)
    participant_emails: set[str] = field(default_factory=set)
    participant_handles: set[str] = field(default_factory=set)
    external_ids: dict[str, set[str]] = field(default_factory=dict)
    event_hints: set[str] = field(default_factory=set)
    locations: set[str] = field(default_factory=set)
    person_labels: set[str] = field(default_factory=set)


@dataclass(slots=True)
class SeedLinkCatalog:
    cards_by_uid: dict[str, SeedCardSketch]
    cards_by_exact_slug: dict[str, SeedCardSketch]
    cards_by_slug: dict[str, SeedCardSketch]
    cards_by_type: dict[str, list[SeedCardSketch]]
    person_by_email: dict[str, list[SeedCardSketch]]
    person_by_phone: dict[str, list[SeedCardSketch]]
    person_by_handle: dict[str, list[SeedCardSketch]]
    person_by_alias: dict[str, list[SeedCardSketch]]
    email_threads_by_thread_id: dict[str, list[SeedCardSketch]]
    email_messages_by_thread_id: dict[str, list[SeedCardSketch]]
    email_messages_by_message_id: dict[str, list[SeedCardSketch]]
    email_attachments_by_message_id: dict[str, list[SeedCardSketch]]
    email_attachments_by_thread_id: dict[str, list[SeedCardSketch]]
    imessage_threads_by_chat_id: dict[str, list[SeedCardSketch]]
    imessage_messages_by_chat_id: dict[str, list[SeedCardSketch]]
    calendar_events_by_event_id: dict[str, list[SeedCardSketch]]
    calendar_events_by_ical_uid: dict[str, list[SeedCardSketch]]
    media_by_day: dict[str, list[SeedCardSketch]]
    events_by_day: dict[str, list[SeedCardSketch]]
    path_buckets: dict[str, list[SeedCardSketch]]


@dataclass(slots=True)
class SeedLinkCandidate:
    module_name: str
    source_card_uid: str
    source_rel_path: str
    target_card_uid: str
    target_rel_path: str
    target_kind: str
    proposed_link_type: str
    candidate_group: str
    input_hash: str
    evidence_hash: str
    features: dict[str, Any]
    evidences: list[LinkEvidence]
    surface: str
    promotion_target: str
    canonical_field_name: str = ""
    canonical_value_mode: str = "slug"


@dataclass(slots=True)
class SeedLinkDecision:
    deterministic_score: float
    lexical_score: float
    graph_score: float
    embedding_score: float
    llm_score: float
    risk_penalty: float
    final_confidence: float
    decision: str
    decision_reason: str
    auto_approved_floor: float
    review_floor: float
    discard_floor: float
    policy_version: int
    llm_model: str = ""
    llm_output_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SeedLinkRunSummary:
    jobs_enqueued: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    candidates: int = 0
    needs_review: int = 0
    auto_promoted: int = 0
    canonical_safe: int = 0
    llm_judged: int = 0
    canonical_applied: int = 0
    derived_promotions_applied: int = 0
    module_metrics: dict[str, dict[str, float]] = field(default_factory=dict)


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_slug(value: str) -> str:
    return _clean_text(value).replace(" ", "-").lower()


def _normalize_email(value: str) -> str:
    return _clean_text(value).lower()


def _normalize_phone(value: str) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        return digits[1:]
    return digits


def _normalize_handle(value: str) -> str:
    return _clean_text(value).removeprefix("@").strip("/").lower()


def _normalize_alias(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s@.+-]", " ", _clean_text(value).lower())).strip()


def _normalize_location(value: str) -> str:
    return _clean_text(value).lower()


def _path_bucket(rel_path: str) -> str:
    parts = Path(rel_path).parts
    if len(parts) >= 2 and re.match(r"\d{4}-\d{2}", parts[1]):
        return f"{parts[0]}/{parts[1]}"
    if parts:
        return parts[0]
    return "unknown"


def _score_band(value: float) -> str:
    clipped = max(0.0, min(float(value), 1.0))
    for band in DEFAULT_SCORE_BANDS:
        start_text, end_text = band.split("-", 1)
        start = float(start_text)
        end = float(end_text)
        if start <= clipped <= end + 1e-9:
            return band
    return DEFAULT_SCORE_BANDS[-1]


def _slug_from_ref(value: str) -> str:
    cleaned = _clean_text(value)
    if cleaned.startswith("[[") and cleaned.endswith("]]"):
        return cleaned[2:-2].split("|", 1)[0].strip()
    if cleaned.endswith(".md"):
        return Path(cleaned).stem
    return cleaned


def _reference_slug_set(frontmatter: dict[str, Any], body: str) -> set[str]:
    refs: set[str] = set()
    for field_name, value in frontmatter.items():
        if field_name == "summary":
            continue
        for item in _iter_string_values(value):
            slug = _slug_from_ref(item)
            if slug:
                refs.add(_normalize_slug(slug))
    for slug in extract_wikilinks(body):
        if slug:
            refs.add(_normalize_slug(slug))
    return refs


def _orphan_reference_slugs(sketch: SeedCardSketch, catalog: SeedLinkCatalog) -> list[tuple[str, str]]:
    known_exact = set(catalog.cards_by_exact_slug)
    refs: list[tuple[str, str]] = []
    for field_name, value in sketch.frontmatter.items():
        for item in _iter_string_values(value):
            if item.startswith("[[") and item.endswith("]]"):
                raw_slug = _slug_from_ref(item)
                if raw_slug and raw_slug not in known_exact:
                    refs.append((field_name, raw_slug))
    for slug in extract_wikilinks(sketch.body):
        if slug and slug not in known_exact:
            refs.append(("body", slug))
    return refs


def _has_orphan_references(sketch: SeedCardSketch, catalog: SeedLinkCatalog) -> bool:
    return bool(_orphan_reference_slugs(sketch, catalog))


def _module_should_enqueue(sketch: SeedCardSketch, catalog: SeedLinkCatalog, module_name: str, *, force: bool) -> bool:
    if force:
        return True
    if module_name == MODULE_ORPHAN:
        return _has_orphan_references(sketch, catalog)
    if module_name == MODULE_GRAPH:
        return False
    if module_name == MODULE_MEDIA:
        return bool(sketch.person_labels or sketch.locations or sketch.event_hints)
    if module_name == MODULE_IDENTITY:
        return sketch.card_type == "person" and bool(sketch.emails or sketch.phones or sketch.handles or sketch.aliases)
    if module_name == MODULE_CALENDAR:
        if sketch.card_type == "calendar_event":
            return bool(sketch.emails or sketch.event_hints)
        if sketch.card_type == "meeting_transcript":
            return bool(
                sketch.event_hints
                or sketch.participant_emails
                or _clean_text(sketch.frontmatter.get("conference_url", ""))
                or _clean_text(sketch.frontmatter.get("title", ""))
            )
        return bool(sketch.event_hints)
    if module_name == MODULE_COMMUNICATION:
        frontmatter = sketch.frontmatter
        if sketch.card_type == "email_message":
            return bool(
                (_clean_text(frontmatter.get("gmail_thread_id", "")) and not _clean_text(frontmatter.get("thread", "")))
                or (sketch.participant_emails and not _iter_string_values(frontmatter.get("people", [])))
                or (sketch.event_hints and not _iter_string_values(frontmatter.get("calendar_events", [])))
                or (
                    bool(frontmatter.get("has_attachments"))
                    and not _iter_string_values(frontmatter.get("attachments", []))
                )
            )
        if sketch.card_type == "email_thread":
            return bool(
                (
                    _clean_text(frontmatter.get("gmail_thread_id", ""))
                    and not _iter_string_values(frontmatter.get("messages", []))
                )
                or (sketch.participant_emails and not _iter_string_values(frontmatter.get("people", [])))
                or (sketch.event_hints and not _iter_string_values(frontmatter.get("calendar_events", [])))
            )
        if sketch.card_type == "email_attachment":
            return False
        if sketch.card_type == "imessage_message":
            return bool(
                (
                    _clean_text(frontmatter.get("imessage_chat_id", ""))
                    and not _clean_text(frontmatter.get("thread", ""))
                )
                or (sketch.participant_handles and not _iter_string_values(frontmatter.get("people", [])))
            )
        if sketch.card_type == "imessage_thread":
            return bool(
                (
                    _clean_text(frontmatter.get("imessage_chat_id", ""))
                    and not _iter_string_values(frontmatter.get("messages", []))
                )
                or (sketch.participant_handles and not _iter_string_values(frontmatter.get("people", [])))
            )
        if sketch.card_type == "imessage_attachment":
            return False
    return True


def _has_frontmatter_orphan_references(frontmatter: dict[str, Any], known_exact_slugs: set[str]) -> bool:
    for value in frontmatter.values():
        for item in _iter_string_values(value):
            if item.startswith("[[") and item.endswith("]]"):
                raw_slug = _slug_from_ref(item)
                if raw_slug and raw_slug not in known_exact_slugs:
                    return True
    return False


def _module_should_enqueue_fast(
    sketch: SeedCardSketch,
    module_name: str,
    *,
    force: bool,
    known_exact_slugs: set[str],
) -> bool:
    if force:
        return True
    if module_name == MODULE_ORPHAN:
        return _has_frontmatter_orphan_references(sketch.frontmatter, known_exact_slugs)
    if module_name == MODULE_GRAPH:
        return False
    if module_name == MODULE_MEDIA:
        return bool(sketch.person_labels or sketch.locations or sketch.event_hints)
    if module_name == MODULE_IDENTITY:
        return sketch.card_type == "person" and bool(sketch.emails or sketch.phones or sketch.handles or sketch.aliases)
    if module_name == MODULE_CALENDAR:
        if sketch.card_type == "calendar_event":
            return bool(sketch.emails or sketch.event_hints)
        if sketch.card_type == "meeting_transcript":
            return bool(
                sketch.event_hints
                or sketch.participant_emails
                or _clean_text(sketch.frontmatter.get("conference_url", ""))
                or _clean_text(sketch.frontmatter.get("title", ""))
            )
        return bool(sketch.event_hints)
    if module_name == MODULE_COMMUNICATION:
        frontmatter = sketch.frontmatter
        if sketch.card_type == "email_message":
            return bool(
                (_clean_text(frontmatter.get("gmail_thread_id", "")) and not _clean_text(frontmatter.get("thread", "")))
                or (sketch.participant_emails and not _iter_string_values(frontmatter.get("people", [])))
                or (sketch.event_hints and not _iter_string_values(frontmatter.get("calendar_events", [])))
                or (
                    bool(frontmatter.get("has_attachments"))
                    and not _iter_string_values(frontmatter.get("attachments", []))
                )
            )
        if sketch.card_type == "email_thread":
            return bool(
                (
                    _clean_text(frontmatter.get("gmail_thread_id", ""))
                    and not _iter_string_values(frontmatter.get("messages", []))
                )
                or (sketch.participant_emails and not _iter_string_values(frontmatter.get("people", [])))
                or (sketch.event_hints and not _iter_string_values(frontmatter.get("calendar_events", [])))
            )
        if sketch.card_type == "email_attachment":
            return False
        if sketch.card_type == "imessage_message":
            return bool(
                (
                    _clean_text(frontmatter.get("imessage_chat_id", ""))
                    and not _clean_text(frontmatter.get("thread", ""))
                )
                or (sketch.participant_handles and not _iter_string_values(frontmatter.get("people", [])))
            )
        if sketch.card_type == "imessage_thread":
            return bool(
                (
                    _clean_text(frontmatter.get("imessage_chat_id", ""))
                    and not _iter_string_values(frontmatter.get("messages", []))
                )
                or (sketch.participant_handles and not _iter_string_values(frontmatter.get("people", [])))
            )
        if sketch.card_type == "imessage_attachment":
            return False
    return True


def _iter_string_values(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if _clean_text(value) else []
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            if isinstance(item, str) and _clean_text(item):
                values.append(item)
        return values
    return []


def _external_ids(frontmatter: dict[str, Any]) -> dict[str, set[str]]:
    return {
        provider: {item for item in values if _clean_text(item)}
        for provider, values in external_ids_by_provider(frontmatter).items()
    }


def _frontmatter_content_hash(rel_path: str, frontmatter: dict[str, Any]) -> str:
    return _json_hash({"rel_path": rel_path, "frontmatter": frontmatter})


def _sketch_from_frontmatter(
    *,
    rel_path: str,
    frontmatter: dict[str, Any],
    body: str = "",
    content_hash: str = "",
) -> SeedCardSketch:
    card = validate_card_permissive(frontmatter)
    uid = str(card.uid)
    slug = Path(rel_path).stem
    aliases = {_normalize_alias(card.summary)}
    for field_name in ("summary", "name", "first_name", "last_name"):
        value = _clean_text(frontmatter.get(field_name, ""))
        if value:
            aliases.add(_normalize_alias(value))
    first_name = _clean_text(frontmatter.get("first_name", ""))
    last_name = _clean_text(frontmatter.get("last_name", ""))
    if first_name or last_name:
        aliases.add(_normalize_alias(" ".join(part for part in (first_name, last_name) if part)))
    for alias in _iter_string_values(frontmatter.get("aliases", [])):
        aliases.add(_normalize_alias(alias))
    emails = {
        _normalize_email(item)
        for field_name in (
            "emails",
            "to_emails",
            "cc_emails",
            "bcc_emails",
            "reply_to_emails",
            "attendee_emails",
            "participant_emails",
        )
        for item in _iter_string_values(frontmatter.get(field_name, []))
    }
    for field_name in ("account_email", "from_email", "organizer_email"):
        value = _normalize_email(frontmatter.get(field_name, ""))
        if value:
            emails.add(value)
    phones = {
        _normalize_phone(item) for item in _iter_string_values(frontmatter.get("phones", [])) if _normalize_phone(item)
    }
    handles = set()
    for field_name in ("linkedin", "github", "twitter", "instagram", "telegram", "discord", "sender_handle"):
        value = _normalize_handle(frontmatter.get(field_name, ""))
        if value:
            handles.add(value)
    participant_handles = {
        _normalize_handle(item)
        for field_name in ("participant_handles",)
        for item in _iter_string_values(frontmatter.get(field_name, []))
        if _normalize_handle(item)
    }
    event_hints = {
        _clean_text(item)
        for field_name in ("calendar_events", "invite_ical_uids", "invite_event_id_hints")
        for item in _iter_string_values(frontmatter.get(field_name, []))
        if _clean_text(item)
    }
    for field_name in ("invite_ical_uid", "invite_event_id_hint", "event_id", "ical_uid", "event_id_hint"):
        value = _clean_text(frontmatter.get(field_name, ""))
        if value:
            event_hints.add(value)
    person_labels = {
        _normalize_alias(item)
        for field_name in ("person_labels", "people")
        for item in _iter_string_values(frontmatter.get(field_name, []))
        if _normalize_alias(item)
    }
    locations = {
        _normalize_location(item)
        for field_name in ("location", "place_name", "place_city", "place_state", "place_country")
        for item in _iter_string_values(frontmatter.get(field_name, ""))
        if _normalize_location(item)
    }
    participant_emails = {
        _normalize_email(item)
        for item in (
            _iter_string_values(frontmatter.get("participants", []))
            + _iter_string_values(frontmatter.get("participant_emails", []))
            + _iter_string_values(frontmatter.get("attendee_emails", []))
            + _iter_string_values(frontmatter.get("to_emails", []))
            + _iter_string_values(frontmatter.get("cc_emails", []))
            + _iter_string_values(frontmatter.get("bcc_emails", []))
        )
        if _normalize_email(item)
    }
    return SeedCardSketch(
        uid=uid,
        rel_path=rel_path,
        slug=slug,
        card_type=card.type,
        summary=_clean_text(card.summary),
        frontmatter=frontmatter,
        body=body,
        content_hash=content_hash or _frontmatter_content_hash(rel_path, frontmatter),
        activity_at=_activity_at(frontmatter),
        wikilinks=[_slug_from_ref(item) for item in extract_wikilinks(body)],
        emails=emails,
        phones=phones,
        handles=handles,
        aliases={alias for alias in aliases if alias},
        participant_emails=participant_emails,
        participant_handles=participant_handles,
        external_ids=_external_ids(frontmatter),
        event_hints=event_hints,
        locations=locations,
        person_labels=person_labels,
    )


def _activity_at(frontmatter: dict[str, Any]) -> str:
    return card_activity_at(frontmatter)


def _day_key(value: str) -> str:
    cleaned = _clean_text(value)
    return cleaned[:10] if len(cleaned) >= 10 else ""


def _json_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _feature_excerpt(sketch: SeedCardSketch) -> str:
    frontmatter = sketch.frontmatter
    parts = [
        f"type={sketch.card_type}",
        f"summary={sketch.summary}",
        f"slug={sketch.slug}",
    ]
    for field_name in (
        "subject",
        "title",
        "thread_summary",
        "description",
        "snippet",
        "location",
        "from_email",
        "account_email",
        "organizer_email",
    ):
        value = _clean_text(frontmatter.get(field_name, ""))
        if value:
            parts.append(f"{field_name}={value}")
    if sketch.body:
        parts.append(f"body={_clean_text(sketch.body)[:280]}")
    return "\n".join(parts[:12])


def build_seed_link_catalog(
    vault_path: str | Path,
    *,
    cache: VaultScanCache | None = None,
) -> SeedLinkCatalog:
    vault = Path(vault_path)
    cards_by_uid: dict[str, SeedCardSketch] = {}
    cards_by_exact_slug: dict[str, SeedCardSketch] = {}
    cards_by_slug: dict[str, SeedCardSketch] = {}
    cards_by_type: dict[str, list[SeedCardSketch]] = {}
    person_by_email: dict[str, list[SeedCardSketch]] = {}
    person_by_phone: dict[str, list[SeedCardSketch]] = {}
    person_by_handle: dict[str, list[SeedCardSketch]] = {}
    person_by_alias: dict[str, list[SeedCardSketch]] = {}
    email_threads_by_thread_id: dict[str, list[SeedCardSketch]] = {}
    email_messages_by_thread_id: dict[str, list[SeedCardSketch]] = {}
    email_messages_by_message_id: dict[str, list[SeedCardSketch]] = {}
    email_attachments_by_message_id: dict[str, list[SeedCardSketch]] = {}
    email_attachments_by_thread_id: dict[str, list[SeedCardSketch]] = {}
    imessage_threads_by_chat_id: dict[str, list[SeedCardSketch]] = {}
    imessage_messages_by_chat_id: dict[str, list[SeedCardSketch]] = {}
    calendar_events_by_event_id: dict[str, list[SeedCardSketch]] = {}
    calendar_events_by_ical_uid: dict[str, list[SeedCardSketch]] = {}
    media_by_day: dict[str, list[SeedCardSketch]] = {}
    events_by_day: dict[str, list[SeedCardSketch]] = {}
    path_buckets: dict[str, list[SeedCardSketch]] = {}

    if cache is None:
        cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)

    def _ingest(sketch: SeedCardSketch, frontmatter: dict[str, Any]) -> None:
        uid = sketch.uid
        slug = sketch.slug
        cards_by_uid[uid] = sketch
        cards_by_exact_slug[slug] = sketch
        cards_by_slug[_normalize_slug(slug)] = sketch
        cards_by_type.setdefault(sketch.card_type, []).append(sketch)
        path_buckets.setdefault(_path_bucket(sketch.rel_path), []).append(sketch)
        if sketch.card_type == "person":
            for email in sketch.emails:
                person_by_email.setdefault(email, []).append(sketch)
            for phone in sketch.phones:
                person_by_phone.setdefault(phone, []).append(sketch)
            for handle in sketch.handles:
                person_by_handle.setdefault(handle, []).append(sketch)
            for alias in sketch.aliases:
                person_by_alias.setdefault(alias, []).append(sketch)
        if sketch.card_type == "email_thread":
            for thread_id in sketch.external_ids.get("gmail", set()):
                if thread_id == _clean_text(frontmatter.get("gmail_thread_id", "")):
                    email_threads_by_thread_id.setdefault(thread_id, []).append(sketch)
        if sketch.card_type == "email_message":
            thread_id = _clean_text(frontmatter.get("gmail_thread_id", ""))
            if thread_id:
                email_messages_by_thread_id.setdefault(thread_id, []).append(sketch)
            message_id = _clean_text(frontmatter.get("gmail_message_id", ""))
            if message_id:
                email_messages_by_message_id.setdefault(message_id, []).append(sketch)
        if sketch.card_type == "email_attachment":
            message_id = _clean_text(frontmatter.get("gmail_message_id", ""))
            if message_id:
                email_attachments_by_message_id.setdefault(message_id, []).append(sketch)
            thread_id = _clean_text(frontmatter.get("gmail_thread_id", ""))
            if thread_id:
                email_attachments_by_thread_id.setdefault(thread_id, []).append(sketch)
        if sketch.card_type == "imessage_thread":
            chat_id = _clean_text(frontmatter.get("imessage_chat_id", ""))
            if chat_id:
                imessage_threads_by_chat_id.setdefault(chat_id, []).append(sketch)
        if sketch.card_type == "imessage_message":
            chat_id = _clean_text(frontmatter.get("imessage_chat_id", ""))
            if chat_id:
                imessage_messages_by_chat_id.setdefault(chat_id, []).append(sketch)
        if sketch.card_type == "calendar_event":
            event_id = _clean_text(frontmatter.get("event_id", ""))
            if event_id:
                calendar_events_by_event_id.setdefault(event_id, []).append(sketch)
            ical_uid = _clean_text(frontmatter.get("ical_uid", ""))
            if ical_uid:
                calendar_events_by_ical_uid.setdefault(ical_uid, []).append(sketch)
            day = _day_key(sketch.activity_at or _clean_text(frontmatter.get("start_at", "")))
            if day:
                events_by_day.setdefault(day, []).append(sketch)
        if sketch.card_type == "media_asset":
            day = _day_key(_clean_text(frontmatter.get("captured_at", "")))
            if day:
                media_by_day.setdefault(day, []).append(sketch)

    for rel_path, fm in cache.all_frontmatters():
        body = cache.body_for_rel_path(rel_path)
        ch = cache.raw_content_sha256_for_rel_path(rel_path)
        sketch = _sketch_from_frontmatter(
            rel_path=rel_path,
            frontmatter=fm,
            body=body,
            content_hash=ch,
        )
        _ingest(sketch, fm)

    catalog = SeedLinkCatalog(
        cards_by_uid=cards_by_uid,
        cards_by_exact_slug=cards_by_exact_slug,
        cards_by_slug=cards_by_slug,
        cards_by_type=cards_by_type,
        person_by_email=person_by_email,
        person_by_phone=person_by_phone,
        person_by_handle=person_by_handle,
        person_by_alias=person_by_alias,
        email_threads_by_thread_id=email_threads_by_thread_id,
        email_messages_by_thread_id=email_messages_by_thread_id,
        email_messages_by_message_id=email_messages_by_message_id,
        email_attachments_by_message_id=email_attachments_by_message_id,
        email_attachments_by_thread_id=email_attachments_by_thread_id,
        imessage_threads_by_chat_id=imessage_threads_by_chat_id,
        imessage_messages_by_chat_id=imessage_messages_by_chat_id,
        calendar_events_by_event_id=calendar_events_by_event_id,
        calendar_events_by_ical_uid=calendar_events_by_ical_uid,
        media_by_day=media_by_day,
        events_by_day=events_by_day,
        path_buckets=path_buckets,
    )
    # Phase 6.5: invoke every registered linker's post_build_hook. This is how
    # finance_reconcile, trip_cluster, meeting_artifact and any future new
    # linker populates its private indexes (attached via linker_framework.set_private_index).
    from archive_cli import linker_framework as _lf
    _lf.run_post_build_hooks(catalog)
    return catalog


def get_link_surface_policies() -> list[LinkSurfacePolicy]:
    return [
        LinkSurfacePolicy(
            link_type=LINK_TYPE_MESSAGE_IN_THREAD,
            module_name=MODULE_COMMUNICATION,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="thread",
            canonical_value_mode="slug",
            auto_promote_floor=0.82,
            canonical_floor=0.93,
            description="Exact message-to-thread repair from shared thread identifiers.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_THREAD_HAS_MESSAGE,
            module_name=MODULE_GRAPH,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="messages",
            canonical_value_mode="slug",
            auto_promote_floor=0.78,
            canonical_floor=0.84,
            description="Exact reverse message membership inferred from canonical thread or message IDs.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT,
            module_name=MODULE_CALENDAR,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="calendar_events",
            canonical_value_mode="slug",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Message-to-event link from exact invite and event identifiers.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_THREAD_HAS_CALENDAR_EVENT,
            module_name=MODULE_CALENDAR,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="calendar_events",
            canonical_value_mode="slug",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Thread-to-event link from exact invite hints and reverse event references.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,
            module_name=MODULE_CALENDAR,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="calendar_events",
            canonical_value_mode="slug",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Transcript-to-event link from exact Otter and calendar identifiers.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_EVENT_HAS_MESSAGE,
            module_name=MODULE_GRAPH,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="source_messages",
            canonical_value_mode="slug",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Event reverse-link repair from message invite evidence.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_EVENT_HAS_THREAD,
            module_name=MODULE_GRAPH,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="source_threads",
            canonical_value_mode="slug",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Event reverse-link repair from thread invite evidence.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_EVENT_HAS_TRANSCRIPT,
            module_name=MODULE_GRAPH,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="meeting_transcripts",
            canonical_value_mode="slug",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Event reverse-link repair from transcript meeting evidence.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
            module_name=MODULE_COMMUNICATION,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="attachments",
            canonical_value_mode="slug",
            auto_promote_floor=0.82,
            canonical_floor=0.93,
            description="Message attachment repair from exact attachment parent IDs.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_THREAD_HAS_ATTACHMENT,
            module_name=MODULE_GRAPH,
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_promote_floor=0.78,
            canonical_floor=0.92,
            description="Thread attachment edge derived from exact parent thread linkage.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_THREAD_HAS_PERSON,
            module_name=MODULE_COMMUNICATION,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="people",
            canonical_value_mode="summary",
            auto_promote_floor=0.82,
            canonical_floor=0.93,
            description="Thread-to-person link from exact participant identifiers.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_MESSAGE_MENTIONS_PERSON,
            module_name=MODULE_COMMUNICATION,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="people",
            canonical_value_mode="summary",
            auto_promote_floor=0.82,
            canonical_floor=0.93,
            description="Message-to-person link from exact sender or participant identifiers.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_EVENT_HAS_PERSON,
            module_name=MODULE_CALENDAR,
            surface=SURFACE_CANONICAL_SAFE,
            promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
            canonical_field_name="people",
            canonical_value_mode="summary",
            auto_promote_floor=0.85,
            canonical_floor=0.95,
            description="Event-to-person link from exact organizer or attendee identifiers.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_MEDIA_HAS_PERSON,
            module_name=MODULE_MEDIA,
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_promote_floor=0.88,
            canonical_floor=1.0,
            description="Media-to-person suggestion from exact labels and clustered context.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_MEDIA_HAS_EVENT,
            module_name=MODULE_MEDIA,
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_promote_floor=0.88,
            canonical_floor=1.0,
            description="Media-to-event suggestion from exact date, place, and participant overlap.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_POSSIBLE_SAME_PERSON,
            module_name=MODULE_IDENTITY,
            surface=SURFACE_CANDIDATE_DERIVED,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_promote_floor=0.97,
            canonical_floor=0.97,
            description="Possible same-person cluster candidate. Review-first for canonical merges.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_ORPHAN_REPAIR_EXACT,
            module_name=MODULE_ORPHAN,
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_promote_floor=0.84,
            canonical_floor=0.94,
            description="Exact orphan repair candidate from normalized slug or reverse references.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_ORPHAN_REPAIR_FUZZY,
            module_name=MODULE_ORPHAN,
            surface=SURFACE_CANDIDATE_DERIVED,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_promote_floor=0.98,
            canonical_floor=0.99,
            description="Fuzzy orphan repair candidate. Review-only by default.",
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_SEMANTICALLY_RELATED,
            module_name=MODULE_SEMANTIC,
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            # Calibrated 2026-04-19 (multiple sweeps; see archive_docs/runbooks/
            # phase6-semantic-linker.md and _artifacts/_semantic-linker-calibration/).
            # The single floor below operates on the post-gate distribution from the
            # two-tier formula in evaluate_seed_link_candidate (strict for same-type,
            # lenient for cross-type). 0.50 captures both regimes — same-type pairs
            # land at 0.85+ and cross-type bridges typically at 0.50-0.76.
            auto_review_floor=0.40,
            auto_promote_floor=0.50,
            canonical_floor=1.0,
            description="Semantically-related card pair discovered via embedding kNN.",
        ),
        # Phase 6.5 new link types.
        LinkSurfacePolicy(
            link_type=LINK_TYPE_FINANCE_RECONCILES,
            module_name="financeReconcileLinker",
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_review_floor=0.40,
            auto_promote_floor=0.80,
            canonical_floor=1.0,
            description=(
                "Finance↔derived-transaction reconcile via tier ladder: "
                "source_email + >=2 tight signals (0.98 auto) > "
                "amount+date+merchant (0.90 auto) > "
                "shared-thread (0.78 -> 0.73 review) > "
                "source_email + 1 tight signal (0.72 review) > "
                "amount+date only (0.55 -> 0.40 review). "
                "Bare source_email wikilinks with no tight-bound "
                "corroboration are rejected per "
                "archive_docs/runbooks/linker-quality-gates.md."
            ),
        ),
        LinkSurfacePolicy(
            link_type=LINK_TYPE_PART_OF_TRIP,
            module_name="tripClusterLinker",
            surface=SURFACE_DERIVED_ONLY,
            promotion_target=PROMOTION_TARGET_DERIVED_EDGE,
            auto_review_floor=0.60,
            auto_promote_floor=0.80,
            canonical_floor=1.0,
            description=(
                "Trip-cluster edge: accommodation ↔ flight/car_rental via "
                "exact normalized city match + date-overlap window. "
                "Substring/lenient city matches drop to review-only per "
                "archive_docs/runbooks/linker-quality-gates.md."
            ),
        ),
    ]


LINK_SURFACE_BY_TYPE = {policy.link_type: policy for policy in get_link_surface_policies()}


def get_modules_for_card_type(card_type: str) -> tuple[str, ...]:
    return CARD_TYPE_MODULES.get(card_type, ())


def get_seed_scope_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for card_type, modules in sorted(CARD_TYPE_MODULES.items()):
        priority = 1 if card_type in HIGH_PRIORITY_CARD_TYPES else 3 if card_type in LOW_PRIORITY_CARD_TYPES else 2
        rows.append(
            {
                "card_type": card_type,
                "priority": priority,
                "modules": list(modules),
            }
        )
    return rows


def get_surface_policy_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for policy in get_link_surface_policies():
        rows.append(
            {
                "link_type": policy.link_type,
                "module_name": policy.module_name,
                "surface": policy.surface,
                "promotion_target": policy.promotion_target,
                "canonical_field_name": policy.canonical_field_name,
                "canonical_value_mode": policy.canonical_value_mode,
                "review_floor": policy.auto_review_floor,
                "auto_promote_floor": policy.auto_promote_floor,
                "canonical_floor": policy.canonical_floor,
                "description": policy.description,
            }
        )
    return rows


def _make_evidence(
    evidence_type: str,
    evidence_source: str,
    feature_name: str,
    feature_value: Any,
    feature_weight: float,
    **payload: Any,
) -> LinkEvidence:
    return LinkEvidence(
        evidence_type=evidence_type,
        evidence_source=evidence_source,
        feature_name=feature_name,
        feature_value=str(feature_value),
        feature_weight=float(feature_weight),
        raw_payload_json=payload,
    )


def _candidate_input_hash(source: SeedCardSketch, target: SeedCardSketch, features: dict[str, Any]) -> str:
    return _json_hash(
        {
            "source_uid": source.uid,
            "source_hash": source.content_hash,
            "target_uid": target.uid,
            "target_hash": target.content_hash,
            "features": features,
            "linker_version": SEED_LINKER_VERSION,
        }
    )


def _candidate_evidence_hash(evidences: list[LinkEvidence]) -> str:
    payload = [
        {
            "evidence_type": item.evidence_type,
            "evidence_source": item.evidence_source,
            "feature_name": item.feature_name,
            "feature_value": item.feature_value,
            "feature_weight": item.feature_weight,
            "raw_payload_json": item.raw_payload_json,
        }
        for item in evidences
    ]
    return _json_hash(payload)


def _target_reference_value(target: SeedCardSketch, mode: str) -> str:
    if mode == "summary":
        return target.summary
    return target.slug


def _has_reference(sketch: SeedCardSketch, candidate_value: str) -> bool:
    if not candidate_value:
        return False
    normalized = _normalize_slug(candidate_value)
    refs = _reference_slug_set(sketch.frontmatter, sketch.body)
    if normalized in refs:
        return True
    people_values = {_normalize_alias(item) for item in _iter_string_values(sketch.frontmatter.get("people", []))}
    if _normalize_alias(candidate_value) in people_values:
        return True
    existing = sketch.frontmatter
    for field_name in ("thread", "message"):
        value = _slug_from_ref(existing.get(field_name, ""))
        if value and _normalize_slug(value) == normalized:
            return True
    return False


def _append_candidate(
    results: list[SeedLinkCandidate],
    *,
    module_name: str,
    source: SeedCardSketch,
    target: SeedCardSketch,
    proposed_link_type: str,
    candidate_group: str,
    features: dict[str, Any],
    evidences: list[LinkEvidence],
) -> None:
    policy = LINK_SURFACE_BY_TYPE[proposed_link_type]
    input_hash = _candidate_input_hash(source, target, features)
    evidence_hash = _candidate_evidence_hash(evidences)
    results.append(
        SeedLinkCandidate(
            module_name=module_name,
            source_card_uid=source.uid,
            source_rel_path=source.rel_path,
            target_card_uid=target.uid,
            target_rel_path=target.rel_path,
            target_kind="card",
            proposed_link_type=proposed_link_type,
            candidate_group=candidate_group,
            input_hash=input_hash,
            evidence_hash=evidence_hash,
            features=features,
            evidences=evidences,
            surface=policy.surface,
            promotion_target=policy.promotion_target,
            canonical_field_name=policy.canonical_field_name,
            canonical_value_mode=policy.canonical_value_mode,
        )
    )


def _person_matches_for_identifiers(
    catalog: SeedLinkCatalog,
    *,
    emails: set[str] | None = None,
    phones: set[str] | None = None,
    handles: set[str] | None = None,
    aliases: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    matches: dict[str, dict[str, Any]] = {}
    for email in emails or set():
        for sketch in catalog.person_by_email.get(email, []):
            matches.setdefault(sketch.uid, {"deterministic_hits": set(), "target": sketch})["deterministic_hits"].add(
                "exact_email"
            )
    for phone in phones or set():
        for sketch in catalog.person_by_phone.get(phone, []):
            matches.setdefault(sketch.uid, {"deterministic_hits": set(), "target": sketch})["deterministic_hits"].add(
                "exact_phone"
            )
    for handle in handles or set():
        for sketch in catalog.person_by_handle.get(handle, []):
            matches.setdefault(sketch.uid, {"deterministic_hits": set(), "target": sketch})["deterministic_hits"].add(
                "exact_handle"
            )
    for alias in aliases or set():
        for sketch in catalog.person_by_alias.get(alias, []):
            matches.setdefault(sketch.uid, {"deterministic_hits": set(), "target": sketch})["deterministic_hits"].add(
                "exact_alias"
            )
    return matches


def _name_similarity(left: str, right: str) -> float:
    left_tokens = set(_normalize_alias(left).split())
    right_tokens = set(_normalize_alias(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return intersection / union if union else 0.0


def _shared_people_names(left: SeedCardSketch, right: SeedCardSketch) -> int:
    left_people = {_normalize_alias(item) for item in _iter_string_values(left.frontmatter.get("people", []))}
    right_people = {_normalize_alias(item) for item in _iter_string_values(right.frontmatter.get("people", []))}
    return len(left_people & right_people)


# _generate_identity_candidates moved to archive_cli/linker_modules/identity.py (Phase 6.5 Step 18).


def _generate_person_link_candidates(
    catalog: SeedLinkCatalog,
    *,
    source: SeedCardSketch,
    emails: set[str],
    handles: set[str],
    link_type: str,
    candidate_group: str,
) -> list[SeedLinkCandidate]:
    results: list[SeedLinkCandidate] = []
    matches = _person_matches_for_identifiers(catalog, emails=emails, handles=handles)
    for target_uid, payload in matches.items():
        target = payload["target"]
        target_value = _target_reference_value(target, "summary")
        if _has_reference(source, target_value):
            continue
        deterministic_hits = sorted(payload["deterministic_hits"])
        features = {
            "deterministic_hits": deterministic_hits,
            "participant_overlap": len(emails | handles),
            "ambiguous_target_count": len(matches),
            "path_bucket_match": int(_path_bucket(source.rel_path) == _path_bucket(target.rel_path)),
        }
        evidences = [
            _make_evidence("identifier_match", "frontmatter", hit, 1, 1.0, source_uid=source.uid, target_uid=target.uid)
            for hit in deterministic_hits
        ]
        _append_candidate(
            results,
            module_name=MODULE_COMMUNICATION if link_type != LINK_TYPE_EVENT_HAS_PERSON else MODULE_CALENDAR,
            source=source,
            target=target,
            proposed_link_type=link_type,
            candidate_group=candidate_group,
            features=features,
            evidences=evidences,
        )
    return results


def _message_thread_features(
    message: SeedCardSketch, thread: SeedCardSketch
) -> tuple[dict[str, Any], list[LinkEvidence]]:
    message_thread_id = _clean_text(message.frontmatter.get("gmail_thread_id", "")) or _clean_text(
        message.frontmatter.get("imessage_chat_id", "")
    )
    thread_thread_id = _clean_text(thread.frontmatter.get("gmail_thread_id", "")) or _clean_text(
        thread.frontmatter.get("imessage_chat_id", "")
    )
    reverse_list = {
        _normalize_slug(_slug_from_ref(item)) for item in _iter_string_values(thread.frontmatter.get("messages", []))
    }
    features = {
        "exact_thread_id": int(bool(message_thread_id and message_thread_id == thread_thread_id)),
        "message_thread_field_present": int(bool(_clean_text(message.frontmatter.get("thread", "")))),
        "reverse_messages_present": int(_normalize_slug(message.slug) in reverse_list),
        "subject_similarity": round(
            _name_similarity(
                _clean_text(message.frontmatter.get("subject", "")), _clean_text(thread.frontmatter.get("subject", ""))
            ),
            4,
        ),
    }
    evidences = []
    if features["exact_thread_id"]:
        evidences.append(
            _make_evidence(
                "exact_thread_id",
                "frontmatter",
                "thread_id",
                message_thread_id,
                1.0,
                source_uid=message.uid,
                target_uid=thread.uid,
            )
        )
    if features["reverse_messages_present"]:
        evidences.append(
            _make_evidence(
                "reverse_reference",
                "frontmatter",
                "thread_messages_contains_message",
                message.slug,
                0.9,
                source_uid=thread.uid,
                target_uid=message.uid,
            )
        )
    if features["subject_similarity"]:
        evidences.append(
            _make_evidence(
                "lexical_overlap",
                "frontmatter",
                "subject_similarity",
                features["subject_similarity"],
                0.25,
                source=message.frontmatter.get("subject", ""),
                target=thread.frontmatter.get("subject", ""),
            )
        )
    return features, evidences


# _generate_communication_candidates moved to archive_cli/linker_modules/communication.py (Phase 6.5 Step 18).


def _event_matches_for_source(catalog: SeedLinkCatalog, source: SeedCardSketch) -> list[SeedCardSketch]:
    matches: dict[str, SeedCardSketch] = {}
    for hint in source.event_hints:
        for event in catalog.calendar_events_by_event_id.get(hint, []):
            matches[event.uid] = event
        for event in catalog.calendar_events_by_ical_uid.get(hint, []):
            matches[event.uid] = event
    day = _day_key(
        source.activity_at
        or _clean_text(source.frontmatter.get("invite_start_at", ""))
        or _clean_text(source.frontmatter.get("start_at", ""))
    )
    if day and source.card_type in {"email_message", "email_thread", "media_asset", "meeting_transcript"}:
        for event in catalog.events_by_day.get(day, []):
            title_similarity = _name_similarity(
                _clean_text(source.frontmatter.get("invite_title", ""))
                or _clean_text(source.frontmatter.get("subject", ""))
                or _clean_text(source.frontmatter.get("title", ""))
                or source.summary,
                _clean_text(event.frontmatter.get("title", "")) or event.summary,
            )
            location_match = bool(source.locations & event.locations) if source.locations and event.locations else False
            if title_similarity >= 0.6 or location_match:
                matches[event.uid] = event
    return list(matches.values())


# _generate_calendar_candidates moved to archive_cli/linker_modules/calendar.py (Phase 6.5 Step 18).


# _generate_media_candidates moved to archive_cli/linker_modules/media.py (Phase 6.5 Step 18).


# _generate_orphan_candidates moved to archive_cli/linker_modules/orphan.py (Phase 6.5 Step 18).


# _generate_graph_consistency_candidates moved to archive_cli/linker_modules/graph.py (Phase 6.5 Step 18).


def _edge_exists(conn: Any, schema: str, source_uid: str, target_uid: str) -> bool:
    row = conn.execute(
        f"""
        SELECT 1 FROM {schema}.edges
        WHERE (source_uid = %s AND target_uid = %s)
           OR (source_uid = %s AND target_uid = %s)
        LIMIT 1
        """,
        (source_uid, target_uid, target_uid, source_uid),
    ).fetchone()
    return row is not None


def _candidate_exists(conn: Any, schema: str, source_uid: str, target_uid: str) -> bool:
    row = conn.execute(
        f"""
        SELECT 1 FROM {schema}.link_candidates
        WHERE module_name = %s
          AND ((source_card_uid = %s AND target_card_uid = %s)
               OR (source_card_uid = %s AND target_card_uid = %s))
        LIMIT 1
        """,
        (MODULE_SEMANTIC, source_uid, target_uid, target_uid, source_uid),
    ).fetchone()
    return row is not None


def _semantic_skip_classifications() -> frozenset[str]:
    """Read PPA_SEMANTIC_SKIP_CLASSIFICATIONS env override or fall back to default."""
    raw = os.environ.get("PPA_SEMANTIC_SKIP_CLASSIFICATIONS", "").strip()
    if not raw:
        return DEFAULT_SEMANTIC_SKIP_CLASSIFICATIONS
    return frozenset(item.strip().lower() for item in raw.split(",") if item.strip())


def _semantic_allowed_types() -> frozenset[str]:
    """Type allowlist (env-overridable via PPA_SEMANTIC_ALLOWED_TYPES)."""
    raw = os.environ.get("PPA_SEMANTIC_ALLOWED_TYPES", "").strip()
    if not raw:
        return DEFAULT_SEMANTIC_ALLOWED_TYPES
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def _semantic_allow_same_type() -> frozenset[str]:
    """Same-type allowlist (env-overridable via PPA_SEMANTIC_ALLOW_SAME_TYPE)."""
    raw = os.environ.get("PPA_SEMANTIC_ALLOW_SAME_TYPE", "").strip()
    if not raw:
        return DEFAULT_SEMANTIC_ALLOW_SAME_TYPE
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def is_semantic_noise_summary(summary: str) -> bool:
    """Return True if the summary matches a template / aggregate noise pattern."""
    if not summary:
        return False
    return bool(DEFAULT_SEMANTIC_NOISE_SUMMARY_RE.match(summary.strip()))


def is_semantic_eligible(card_type: str, summary: str) -> bool:
    """Card-level gate: in allowed-type set AND summary isn't a noise template."""
    if card_type not in _semantic_allowed_types():
        return False
    if is_semantic_noise_summary(summary):
        return False
    return True


def _is_classification_skipped(conn: Any, schema: str, card_uid: str) -> bool:
    """Return True iff this card's triage classification is in the skip set.

    Cards without a card_classifications row (e.g. non-email cards, or email cards
    not yet triaged) are NOT skipped — safe-default-include preserves recall.
    """
    skip_set = _semantic_skip_classifications()
    if not skip_set:
        return False
    try:
        row = conn.execute(
            f"SELECT classification FROM {schema}.card_classifications WHERE card_uid = %s",
            (card_uid,),
        ).fetchone()
    except Exception:
        # card_classifications table absent (older schema) — never skip.
        return False
    if row is None:
        return False
    classification = str(row["classification"] if isinstance(row, dict) else row[0]).strip().lower()
    return classification in skip_set


def _generate_semantic_candidates(
    conn: Any,
    schema: str,
    catalog: SeedLinkCatalog,
    source_card_uid: str,
    *,
    k: int = DEFAULT_SEMANTIC_K,
    threshold: float = DEFAULT_SEMANTIC_THRESHOLD,
    embedding_model: str | None = None,
    embedding_version: int | None = None,
) -> list[SeedLinkCandidate]:
    """Top-K semantically similar cards via chunk-level kNN (IVFFlat), grouped to card-level."""
    model = embedding_model or get_default_embedding_model()
    version = int(embedding_version if embedding_version is not None else get_default_embedding_version())
    source = catalog.cards_by_uid.get(source_card_uid)
    if source is None:
        return []

    # Phase 6 Tier 4 / Step 24: type + summary-template gate (cheap, no DB).
    if not is_semantic_eligible(source.card_type, source.summary):
        return []

    # Phase 6 Tier 4: skip the source entirely if it's a classified-junk email
    # (marketing / automated / noise / personal). This avoids ~78% of email candidate
    # generation and the LLM-judge cost that would follow.
    if _is_classification_skipped(conn, schema, source_card_uid):
        return []

    same_type_allowed = _semantic_allow_same_type()

    src_row = conn.execute(
        f"""
        SELECT AVG(e.embedding)::vector AS v
        FROM {schema}.chunks c
        JOIN {schema}.embeddings e ON e.chunk_key = c.chunk_key
        WHERE c.card_uid = %s
          AND e.embedding_model = %s
          AND e.embedding_version = %s
        """,
        (source_card_uid, model, version),
    ).fetchone()
    if src_row is None or src_row["v"] is None:
        return []
    source_vec = src_row["v"]

    overfetch_chunks = k * DEFAULT_SEMANTIC_OVERFETCH_RATIO * DEFAULT_SEMANTIC_CHUNK_FANOUT
    final_card_limit = k * DEFAULT_SEMANTIC_OVERFETCH_RATIO
    # Single-pool kNN: the Phase 6 calibration sweep (2026-04-19, 102 sample sources) showed
    # an explicit cross-type pool added zero candidates above threshold — text-embedding-3-small
    # simply doesn't place cross-type cards within cosine 0.5 of each other on this corpus.
    rows = conn.execute(
        f"""
        WITH nearest_chunks AS (
            SELECT
                c.card_uid,
                e.embedding <=> %s::vector AS dist
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE c.card_uid != %s
              AND e.embedding_model = %s
              AND e.embedding_version = %s
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s
        ),
        per_card AS (
            SELECT card_uid, MIN(dist) AS distance
            FROM nearest_chunks
            GROUP BY card_uid
        )
        SELECT
            pc.card_uid AS target_uid,
            cards.rel_path AS target_rel_path,
            cards.type AS target_type,
            1 - pc.distance AS similarity
        FROM per_card pc
        JOIN {schema}.cards cards ON cards.uid = pc.card_uid
        WHERE 1 - pc.distance >= %s
        ORDER BY pc.distance
        LIMIT %s
        """,
        (
            source_vec,
            source_card_uid,
            model,
            version,
            source_vec,
            overfetch_chunks,
            threshold,
            final_card_limit,
        ),
    ).fetchall()

    sem_policy = LINK_SURFACE_BY_TYPE[LINK_TYPE_SEMANTICALLY_RELATED]
    candidates: list[SeedLinkCandidate] = []
    for row in rows:
        if len(candidates) >= k:
            break
        target_uid = str(row["target_uid"])
        similarity = float(row["similarity"])
        if similarity < threshold:
            continue
        if target_uid == source_card_uid:
            continue
        if _edge_exists(conn, schema, source_card_uid, target_uid):
            continue
        if _candidate_exists(conn, schema, source_card_uid, target_uid):
            continue
        # Phase 6 Tier 4: skip targets that are classified-junk emails. The kNN may
        # surface them based on cosine alone; the LLM-judge cost is wasted on them.
        if _is_classification_skipped(conn, schema, target_uid):
            continue
        # Phase 6 Tier 4 / Step 24: type + summary-template gate for target.
        target_type = str(row["target_type"])
        target_summary = catalog.cards_by_uid.get(target_uid)
        target_summary_str = target_summary.summary if target_summary is not None else ""
        if not is_semantic_eligible(target_type, target_summary_str):
            continue
        # Same-type pair restriction: only allowed for a small set of types.
        if source.card_type == target_type and source.card_type not in same_type_allowed:
            continue
        if catalog.cards_by_uid.get(target_uid) is None:
            continue

        features: dict[str, Any] = {
            "embedding_similarity": round(similarity, 6),
            "deterministic_hits": [],
            "ambiguous_target_count": 0,
        }
        evidences = [
            LinkEvidence(
                evidence_type="embedding_similarity",
                evidence_source="pgvector_knn",
                feature_name="cosine_similarity",
                feature_value=f"{similarity:.6f}",
                feature_weight=similarity,
                raw_payload_json={
                    "k": k,
                    "threshold": threshold,
                    "source_uid": source_card_uid,
                    "target_uid": target_uid,
                    "embedding_model": model,
                    "embedding_version": version,
                },
            ),
        ]
        input_hash = compute_input_hash(
            {
                "source_uid": source_card_uid,
                "target_uid": target_uid,
                "module": MODULE_SEMANTIC,
                "linker_version": SEED_LINKER_VERSION,
            }
        )
        evidence_hash = compute_input_hash({"features": features})
        candidates.append(
            SeedLinkCandidate(
                module_name=MODULE_SEMANTIC,
                source_card_uid=source_card_uid,
                source_rel_path=source.rel_path,
                target_card_uid=target_uid,
                target_rel_path=str(row["target_rel_path"]),
                target_kind="card",
                proposed_link_type=LINK_TYPE_SEMANTICALLY_RELATED,
                candidate_group="",
                input_hash=input_hash,
                evidence_hash=evidence_hash,
                features=features,
                evidences=evidences,
                surface=sem_policy.surface,
                promotion_target=sem_policy.promotion_target,
                canonical_field_name=sem_policy.canonical_field_name,
                canonical_value_mode=sem_policy.canonical_value_mode,
            )
        )
    return candidates


def generate_seed_link_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch, module_name: str
) -> list[SeedLinkCandidate]:
    from archive_cli import linker_framework as _lf
    return _lf.generate_via_spec(catalog, source, module_name)


def _llm_prompt(candidate: SeedLinkCandidate, source: SeedCardSketch, target: SeedCardSketch) -> str:
    return (
        f"{GROUNDING_INSTRUCTION}\n\n"
        "You are judging whether a proposed archive relationship should exist. "
        "Use only the provided data. Return strict JSON with keys "
        '`{"link":"YES|NO|UNSURE","confidence":0.0-1.0,"reason":"...",'
        '"needs_review":true|false}`.\n\n'
        f"module={candidate.module_name}\n"
        f"proposed_link_type={candidate.proposed_link_type}\n"
        f"source:\n{_feature_excerpt(source)}\n\n"
        f"target:\n{_feature_excerpt(target)}\n\n"
        f"features={json.dumps(candidate.features, sort_keys=True)}\n"
    )


def _parse_llm_json(response: str) -> dict[str, Any] | None:
    cleaned = response.strip()
    if not cleaned:
        return None
    if cleaned.startswith("{") and cleaned.endswith("}"):
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            return payload
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        return None
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def llm_judge_candidate(
    vault_path: str | Path, candidate: SeedLinkCandidate, source: SeedCardSketch, target: SeedCardSketch
) -> tuple[float, str, dict[str, Any]]:
    if candidate.module_name not in LLM_REVIEW_MODULES:
        return 1.0, "", {}
    if (
        candidate.features.get("exact_thread_id")
        or candidate.features.get("exact_parent_message")
        or candidate.features.get("exact_event_id")
    ):
        return 1.0, "", {}
    if candidate.features.get("deterministic_hits"):
        return 1.0, "", {}
    prompt = _llm_prompt(candidate, source, target)
    for provider in get_provider_chain(vault_path):
        response = provider.complete(prompt, max_tokens=128)
        if not response:
            continue
        payload = _parse_llm_json(response)
        if not payload:
            continue
        verdict = str(payload.get("link", "")).strip().upper()
        confidence = payload.get("confidence", 0.0)
        try:
            score = float(confidence)
        except (TypeError, ValueError):
            score = 0.0
        score = max(0.0, min(score, 1.0))
        if verdict == "YES":
            return score or 0.75, str(getattr(provider, "model", "")), payload
        if verdict == "UNSURE":
            return min(score or 0.5, 0.65), str(getattr(provider, "model", "")), payload
        if verdict == "NO":
            return 0.0, str(getattr(provider, "model", "")), payload
    return 0.0, "", {}


def _component_scores(candidate: SeedLinkCandidate) -> tuple[float, float, float, float, float]:
    features = candidate.features
    module_name = candidate.module_name
    from archive_cli import linker_framework as _lf
    _spec = _lf.ALL_LINKERS.get(module_name)
    if _spec is not None:
        return _spec.scoring_fn(features)
    return (0.0, 0.0, 0.0, 0.0, 0.2)


def evaluate_seed_link_candidate(
    vault_path: str | Path,
    catalog: SeedLinkCatalog,
    candidate: SeedLinkCandidate,
) -> SeedLinkDecision:
    source = catalog.cards_by_uid[candidate.source_card_uid]
    target = catalog.cards_by_uid[candidate.target_card_uid]
    policy = LINK_SURFACE_BY_TYPE[candidate.proposed_link_type]
    deterministic_score, lexical_score, graph_score, embedding_score, risk_penalty = _component_scores(candidate)
    llm_score, llm_model, llm_output = llm_judge_candidate(vault_path, candidate, source, target)
    review_floor = policy.auto_review_floor
    auto_floor = policy.auto_promote_floor
    canonical_floor = policy.canonical_floor
    # Phase 6.5: deterministic-mode modules (meeting_artifact, trip_cluster,
    # finance_reconcile) bypass the legacy weighted formula — their det_score
    # encodes the full signal and the weighted formula would cap them at 0.45.
    from archive_cli import linker_framework as _lf
    _spec = _lf.ALL_LINKERS.get(candidate.module_name)
    _deterministic_mode = _spec is not None and _spec.scoring_mode == "deterministic"
    if _deterministic_mode:
        final_confidence = round(max(0.0, min(1.0, deterministic_score - risk_penalty)), 6)
    elif candidate.module_name == MODULE_SEMANTIC:
        # Semantic candidates: no deterministic / lexical / graph signal by design.
        # Calibrated 2026-04-19 (1020-source then 1pct=1914-source sweeps, reports under
        # _artifacts/_semantic-linker-calibration/). Two-tier gate based on whether
        # source and target are the same card type:
        #
        # SAME-TYPE pairs (eg. calendar↔calendar) are template-prone and tend to be
        # near-duplicates — strict gate: verdict==YES, llm>=0.90, emb>=0.85 means both
        # signals must agree strongly.
        #
        # CROSS-TYPE pairs (eg. flight↔email_thread, accommodation↔calendar_event,
        # vaccination↔medical_record) are exactly the cross-domain bridges Phase 6 was
        # designed to find. They're rare in the corpus and the LLM is naturally less
        # confident on them (cross-type wording differs more), so a strict same-type
        # gate eliminates 100% of them. Lenient gate: verdict==YES, llm>=0.70,
        # emb>=0.55. The 1pct sweep showed this surfaces ~27 high-quality cross-type
        # bridges out of 167 candidates with zero "junk-mail bridge" leakage thanks
        # to the type-allowlist + classification filter in _generate_semantic_candidates.
        llm_verdict = str(llm_output.get("link", "")).strip().upper() if llm_output else ""
        is_cross_type = source.card_type != target.card_type
        if is_cross_type:
            min_llm, min_emb = 0.70, 0.55
        else:
            min_llm, min_emb = 0.90, 0.85
        if llm_verdict != "YES" or llm_score < min_llm or embedding_score < min_emb:
            final_confidence = 0.0
        else:
            final_confidence = round(
                max(0.0, min(1.0, (llm_score * embedding_score) - risk_penalty)),
                6,
            )
    else:
        final_confidence = round(
            max(
                0.0,
                min(
                    1.0,
                    (0.45 * deterministic_score)
                    + (0.12 * lexical_score)
                    + (0.13 * graph_score)
                    + (0.18 * llm_score)
                    + (0.12 * embedding_score)
                    - risk_penalty,
                ),
            ),
            6,
        )
    if candidate.surface == SURFACE_CANONICAL_SAFE and deterministic_score >= 1.0 and risk_penalty < 0.2:
        final_confidence = round(max(final_confidence, canonical_floor), 6)
    decision_reason = DECISION_REASON_LOW_CONFIDENCE
    decision = DECISION_DISCARD

    if candidate.proposed_link_type == LINK_TYPE_POSSIBLE_SAME_PERSON and deterministic_score < 1.0:
        auto_floor = max(auto_floor, 0.99)
        canonical_floor = max(canonical_floor, 0.99)
    if candidate.module_name == MODULE_MEDIA and deterministic_score < 1.0:
        auto_floor = max(auto_floor, 0.90)
        canonical_floor = 1.0
    if risk_penalty >= 0.25:
        decision_reason = DECISION_REASON_HIGH_RISK
    elif deterministic_score >= 1.0 and candidate.proposed_link_type in {
        LINK_TYPE_MESSAGE_IN_THREAD,
        LINK_TYPE_THREAD_HAS_MESSAGE,
        LINK_TYPE_MESSAGE_HAS_ATTACHMENT,
        LINK_TYPE_EVENT_HAS_MESSAGE,
        LINK_TYPE_EVENT_HAS_THREAD,
    }:
        decision_reason = DECISION_REASON_EXACT_REVERSE_LINK
    elif deterministic_score >= 1.0 and candidate.proposed_link_type in {
        LINK_TYPE_THREAD_HAS_PERSON,
        LINK_TYPE_MESSAGE_MENTIONS_PERSON,
        LINK_TYPE_EVENT_HAS_PERSON,
    }:
        decision_reason = DECISION_REASON_EXACT_PARTICIPANT
    elif deterministic_score >= 1.0:
        decision_reason = DECISION_REASON_EXACT_IDENTIFIER
    elif candidate.module_name == MODULE_CALENDAR and (deterministic_score >= 0.8 or lexical_score >= 0.6):
        decision_reason = DECISION_REASON_CALENDAR_HINT

    if (
        candidate.surface == SURFACE_CANONICAL_SAFE
        and deterministic_score >= 1.0
        and final_confidence >= canonical_floor
        and risk_penalty < 0.2
    ):
        decision = DECISION_CANONICAL_SAFE
    elif final_confidence >= auto_floor and risk_penalty < 0.25:
        decision = DECISION_AUTO_PROMOTE
    elif final_confidence >= review_floor:
        decision = DECISION_REVIEW
        if decision_reason == DECISION_REASON_LOW_CONFIDENCE:
            decision_reason = DECISION_REASON_BORDERLINE
    else:
        decision = DECISION_DISCARD
    return SeedLinkDecision(
        deterministic_score=deterministic_score,
        lexical_score=lexical_score,
        graph_score=graph_score,
        embedding_score=embedding_score,
        llm_score=llm_score,
        risk_penalty=risk_penalty,
        final_confidence=final_confidence,
        decision=decision,
        decision_reason=decision_reason,
        auto_approved_floor=auto_floor,
        review_floor=review_floor,
        discard_floor=0.45,
        policy_version=SEED_LINK_POLICY_VERSION,
        llm_model=llm_model,
        llm_output_json=llm_output,
    )


def candidate_to_metric_row(
    module_name: str, link_type: str, final_confidence: float, decision: str, action: str = ""
) -> dict[str, Any]:
    row = {
        "module_name": module_name,
        "link_type": link_type,
        "score_band": _score_band(final_confidence),
        "candidate_count": 1,
        "approved_count": 1 if decision in {DECISION_AUTO_PROMOTE, DECISION_CANONICAL_SAFE} else 0,
        "rejected_count": 1
        if decision == DECISION_DISCARD or action in {REVIEW_ACTION_REJECT, REVIEW_ACTION_OVERRIDE_REJECT}
        else 0,
        "override_count": 1 if action in {REVIEW_ACTION_OVERRIDE_APPROVE, REVIEW_ACTION_OVERRIDE_REJECT} else 0,
        "auto_promoted_count": 1 if decision == DECISION_AUTO_PROMOTE else 0,
        "sampled_auto_promoted_count": 1 if action and decision == DECISION_AUTO_PROMOTE else 0,
        "sample_precision": 1.0
        if action in {REVIEW_ACTION_APPROVE, REVIEW_ACTION_OVERRIDE_APPROVE} and decision == DECISION_AUTO_PROMOTE
        else 0.0,
    }
    return row


def quality_gate_thresholds() -> dict[str, Any]:
    return {
        "required_scan_coverage": 1.0,
        "required_orphan_review_coverage": 1.0,
        "max_duplicate_uids": 0,
        "max_high_priority_review_backlog": 50,
        "required_high_risk_precision": 0.95,
    }


def _safe_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _merge_module_metric(
    target: dict[str, dict[str, float]], module_name: str, *, elapsed_seconds: float, jobs: int = 0, candidates: int = 0
) -> None:
    bucket = target.setdefault(module_name, {"elapsed_seconds": 0.0, "jobs": 0.0, "candidates": 0.0})
    bucket["elapsed_seconds"] += float(elapsed_seconds)
    bucket["jobs"] += float(jobs)
    bucket["candidates"] += float(candidates)


def _canonical_provenance(module_name: str, input_hash: str) -> ProvenanceEntry:
    return ProvenanceEntry(
        source=f"archive-linker:{module_name}",
        date=date.today().isoformat(),
        method="deterministic",
        enrichment_version=SEED_LINKER_VERSION,
        input_hash=input_hash,
    )


def _canonical_value_for_candidate(candidate: SeedLinkCandidate, target: SeedCardSketch) -> str:
    return _target_reference_value(target, candidate.canonical_value_mode)


def _upsert_meta(conn, schema: str, values: dict[str, str]) -> None:
    for key, value in values.items():
        conn.execute(
            f"""
            INSERT INTO {schema}.meta(key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (key, value),
        )


def _count_orphaned_links(
    vault_path: str | Path,
    *,
    cache: VaultScanCache | None = None,
) -> int:
    vault = Path(vault_path)
    if cache is None:
        cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    known = cache.all_stems()
    total = 0
    for rel_path, wikilinks in cache.all_wikilinks():
        fm = cache.frontmatter_for_rel_path(rel_path)
        for slug in wikilinks:
            if slug not in known:
                total += 1
        for value in fm.values():
            for item in _iter_string_values(value):
                if item.startswith("[[") and item.endswith("]]") and _slug_from_ref(item) not in known:
                    total += 1
    return total


def enqueue_seed_link_jobs(
    index: Any,
    *,
    modules: list[str] | None = None,
    job_type: str = "seed_backfill",
    source_uids: set[str] | None = None,
    reset_existing: bool = False,
    commit_every: int = 1000,
    cache: VaultScanCache | None = None,
) -> dict[str, int]:
    vault = Path(index.vault)
    selected = set(modules or [])
    scoped_uids = set(source_uids or set())
    force_selected = bool(selected)
    if cache is None:
        cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    known_exact_slugs = cache.all_stems()
    prepared = 0
    inserted = 0
    commit_every = max(int(commit_every), 1)
    with index._connect() as conn:
        pending_since_commit = 0

        for rel_path_str, frontmatter in cache.all_frontmatters():
            sketch = _sketch_from_frontmatter(
                rel_path=rel_path_str,
                frontmatter=frontmatter,
                body="",
                content_hash=_frontmatter_content_hash(rel_path_str, frontmatter),
            )
            if scoped_uids and sketch.uid not in scoped_uids:
                continue
            for module_name in get_modules_for_card_type(sketch.card_type):
                if selected and module_name not in selected:
                    continue
                if not _module_should_enqueue_fast(
                    sketch, module_name, force=force_selected, known_exact_slugs=known_exact_slugs
                ):
                    continue
                prepared += 1
                shard_key = f"{module_name}:{_path_bucket(sketch.rel_path)}"
                priority = 100 if sketch.card_type in HIGH_PRIORITY_CARD_TYPES else 25
                row = (
                    job_type,
                    module_name,
                    sketch.uid,
                    sketch.rel_path,
                    shard_key,
                    priority,
                    sketch.content_hash,
                    SEED_LINKER_VERSION,
                )
                if reset_existing:
                    result = conn.execute(
                        f"""
                        INSERT INTO {index.schema}.link_jobs(
                            job_type, module_name, source_card_uid, source_rel_path, shard_key, priority, input_hash, linker_version
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_type, module_name, source_card_uid, input_hash, linker_version)
                        DO UPDATE SET
                            shard_key = EXCLUDED.shard_key,
                            priority = EXCLUDED.priority,
                            status = 'pending',
                            claimed_by = '',
                            claimed_at = NULL,
                            completed_at = NULL,
                            last_error = ''
                        RETURNING job_id
                        """,
                        row,
                    ).fetchone()
                else:
                    result = conn.execute(
                        f"""
                        INSERT INTO {index.schema}.link_jobs(
                            job_type, module_name, source_card_uid, source_rel_path, shard_key, priority, input_hash, linker_version
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_type, module_name, source_card_uid, input_hash, linker_version)
                        DO NOTHING
                        RETURNING job_id
                        """,
                        row,
                    ).fetchone()
                if result is not None:
                    inserted += 1
                pending_since_commit += 1
                if pending_since_commit >= commit_every:
                    conn.commit()
                    pending_since_commit = 0
        conn.commit()
    return {
        "prepared": prepared,
        "enqueued": inserted,
        "existing": max(prepared - inserted, 0),
    }


def _claim_next_jobs(
    conn: Any, index: Any, worker_name: str, limit: int, modules: list[str] | None = None
) -> list[dict[str, Any]]:
    clauses = ["status = 'pending'"]
    params: list[Any] = []
    if modules:
        clauses.append("module_name = ANY(%s)")
        params.append(modules)
    params.extend([limit, worker_name])
    rows = conn.execute(
        f"""
        WITH next_jobs AS (
            SELECT job_id
            FROM {index.schema}.link_jobs
            WHERE {" AND ".join(clauses)}
            ORDER BY priority DESC, job_id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
        )
        UPDATE {index.schema}.link_jobs jobs
        SET status = 'claimed',
            attempt_count = jobs.attempt_count + 1,
            claimed_by = %s,
            claimed_at = NOW()
        FROM next_jobs
        WHERE jobs.job_id = next_jobs.job_id
        RETURNING jobs.job_id, jobs.module_name, jobs.source_card_uid, jobs.source_rel_path, jobs.input_hash
        """,
        params,
    ).fetchall()
    conn.commit()
    return [dict(row) for row in rows]


def _complete_job(conn: Any, index: Any, job_id: int, *, commit: bool = True) -> None:
    conn.execute(
        f"""
        UPDATE {index.schema}.link_jobs
        SET status = 'completed',
            completed_at = NOW(),
            last_error = ''
        WHERE job_id = %s
        """,
        (job_id,),
    )
    if commit:
        conn.commit()


def _fail_job(conn: Any, index: Any, job_id: int, error_text: str, *, commit: bool = True) -> None:
    conn.execute(
        f"""
        UPDATE {index.schema}.link_jobs
        SET status = 'failed',
            last_error = %s
        WHERE job_id = %s
        """,
        (error_text[:500], job_id),
    )
    if commit:
        conn.commit()


def _persist_candidate(
    conn: Any, index: Any, job_id: int, candidate: SeedLinkCandidate, decision: SeedLinkDecision, *, commit: bool = True
) -> tuple[int, str]:
    row = conn.execute(
        f"""
            INSERT INTO {index.schema}.link_candidates(
                job_id, module_name, linker_version, source_card_uid, source_rel_path, target_card_uid,
                target_rel_path, target_kind, proposed_link_type, candidate_group, input_hash, evidence_hash, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (module_name, linker_version, source_card_uid, target_card_uid, proposed_link_type, input_hash)
            DO UPDATE SET
                job_id = EXCLUDED.job_id,
                source_rel_path = EXCLUDED.source_rel_path,
                target_rel_path = EXCLUDED.target_rel_path,
                target_kind = EXCLUDED.target_kind,
                candidate_group = EXCLUDED.candidate_group,
                evidence_hash = EXCLUDED.evidence_hash,
                status = EXCLUDED.status,
                created_at = NOW()
            RETURNING candidate_id
            """,
        (
            job_id,
            candidate.module_name,
            SEED_LINKER_VERSION,
            candidate.source_card_uid,
            candidate.source_rel_path,
            candidate.target_card_uid,
            candidate.target_rel_path,
            candidate.target_kind,
            candidate.proposed_link_type,
            candidate.candidate_group,
            candidate.input_hash,
            candidate.evidence_hash,
            STATUS_NEEDS_REVIEW
            if decision.decision == DECISION_REVIEW
            else STATUS_APPROVED
            if decision.decision in {DECISION_AUTO_PROMOTE, DECISION_CANONICAL_SAFE}
            else STATUS_REJECTED,
        ),
    ).fetchone()
    assert row is not None
    candidate_id = int(row["candidate_id"])
    conn.execute(f"DELETE FROM {index.schema}.link_evidence WHERE candidate_id = %s", (candidate_id,))
    conn.execute(f"DELETE FROM {index.schema}.link_decisions WHERE candidate_id = %s", (candidate_id,))
    conn.execute(f"DELETE FROM {index.schema}.promotion_queue WHERE candidate_id = %s", (candidate_id,))
    for evidence in candidate.evidences:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.link_evidence(
                candidate_id, evidence_type, evidence_source, feature_name, feature_value, feature_weight, raw_payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                candidate_id,
                evidence.evidence_type,
                evidence.evidence_source,
                evidence.feature_name,
                evidence.feature_value,
                evidence.feature_weight,
                _safe_json(evidence.raw_payload_json),
            ),
        )
    conn.execute(
        f"""
        INSERT INTO {index.schema}.link_decisions(
            candidate_id, deterministic_score, lexical_score, graph_score, llm_score, risk_penalty,
            embedding_score,
            final_confidence, decision, decision_reason, auto_approved_floor, review_floor, discard_floor,
            policy_version, llm_model, llm_output_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            candidate_id,
            decision.deterministic_score,
            decision.lexical_score,
            decision.graph_score,
            decision.llm_score,
            decision.risk_penalty,
            decision.embedding_score,
            decision.final_confidence,
            decision.decision,
            decision.decision_reason,
            decision.auto_approved_floor,
            decision.review_floor,
            decision.discard_floor,
            decision.policy_version,
            decision.llm_model,
            _safe_json(decision.llm_output_json),
        ),
    )
    if decision.decision in {DECISION_AUTO_PROMOTE, DECISION_CANONICAL_SAFE}:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.promotion_queue(candidate_id, promotion_target, target_field_name, promotion_status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (candidate_id, promotion_target, target_field_name)
            DO UPDATE SET promotion_status = EXCLUDED.promotion_status, blocked_reason = ''
            """,
            (
                candidate_id,
                candidate.promotion_target,
                candidate.canonical_field_name,
                PROMOTION_STATUS_QUEUED,
            ),
        )
    if commit:
        conn.commit()
    status = (
        STATUS_NEEDS_REVIEW
        if decision.decision == DECISION_REVIEW
        else STATUS_APPROVED
        if decision.decision in {DECISION_AUTO_PROMOTE, DECISION_CANONICAL_SAFE}
        else STATUS_REJECTED
    )
    return candidate_id, status


def _dedupe_candidates(candidates: list[SeedLinkCandidate]) -> list[SeedLinkCandidate]:
    unique: dict[tuple[str, str, str, str], SeedLinkCandidate] = {}
    for candidate in candidates:
        key = (
            candidate.source_card_uid,
            candidate.target_card_uid,
            candidate.proposed_link_type,
            candidate.module_name,
        )
        unique[key] = candidate
    return list(unique.values())


def _apply_canonical_promotion(index: Any, candidate_row: dict[str, Any], catalog: SeedLinkCatalog) -> tuple[bool, str]:
    candidate = SeedLinkCandidate(
        module_name=str(candidate_row["module_name"]),
        source_card_uid=str(candidate_row["source_card_uid"]),
        source_rel_path=str(candidate_row["source_rel_path"]),
        target_card_uid=str(candidate_row["target_card_uid"]),
        target_rel_path=str(candidate_row["target_rel_path"]),
        target_kind=str(candidate_row["target_kind"]),
        proposed_link_type=str(candidate_row["proposed_link_type"]),
        candidate_group=str(candidate_row.get("candidate_group", "")),
        input_hash=str(candidate_row["input_hash"]),
        evidence_hash=str(candidate_row["evidence_hash"]),
        features={},
        evidences=[],
        surface=str(candidate_row["surface"]),
        promotion_target=PROMOTION_TARGET_CANONICAL_FIELD,
        canonical_field_name=str(candidate_row["canonical_field_name"] or ""),
        canonical_value_mode=str(candidate_row["canonical_value_mode"] or "slug"),
    )
    if not candidate.canonical_field_name:
        return False, "missing canonical field"
    source = catalog.cards_by_uid.get(candidate.source_card_uid)
    target = catalog.cards_by_uid.get(candidate.target_card_uid)
    if source is None or target is None:
        return False, "missing source or target"
    vault = Path(index.vault)
    frontmatter, body, provenance = read_note(vault, candidate.source_rel_path)
    card = validate_card_permissive(frontmatter)
    card_data = card.model_dump(mode="python")
    field_name = candidate.canonical_field_name
    value = _canonical_value_for_candidate(candidate, target)
    current = card_data.get(field_name)
    if isinstance(current, list):
        normalized_existing = {
            (_normalize_alias(item) if field_name == "people" else _normalize_slug(_slug_from_ref(str(item))))
            for item in current
        }
        normalized_value = _normalize_alias(value) if field_name == "people" else _normalize_slug(_slug_from_ref(value))
        if normalized_value in normalized_existing:
            return False, "already present"
        card_data[field_name] = [*current, value]
    else:
        current_text = _clean_text(str(current or ""))
        if current_text and _normalize_slug(_slug_from_ref(current_text)) != _normalize_slug(_slug_from_ref(value)):
            return False, "field already set to different value"
        if current_text == _clean_text(value):
            return False, "already present"
        card_data[field_name] = value
    updated_card = validate_card_strict(card_data)
    updated_provenance = dict(provenance)
    if field_name not in {"people", "orgs"}:
        updated_provenance[field_name] = _canonical_provenance(candidate.module_name, candidate.input_hash)
    write_card(vault, candidate.source_rel_path, updated_card, body=body, provenance=updated_provenance)
    return True, ""


def _mark_canonical_dirty(index: Any, conn: Any | None = None) -> None:
    if conn is None:
        with index._connect() as managed:
            _upsert_meta(managed, index.schema, {"seed_link_canonical_dirty": "1"})
            managed.commit()
        return
    _upsert_meta(conn, index.schema, {"seed_link_canonical_dirty": "1"})


def _canonical_dirty(index: Any) -> bool:
    with index._connect() as conn:
        row = conn.execute(f"SELECT value FROM {index.schema}.meta WHERE key = 'seed_link_canonical_dirty'").fetchone()
    return bool(row is not None and str(row["value"]).strip() == "1")


def _clear_canonical_dirty(index: Any) -> None:
    with index._connect() as conn:
        conn.execute(f"DELETE FROM {index.schema}.meta WHERE key = 'seed_link_canonical_dirty'")
        conn.commit()


def _claim_next_promotions(conn: Any, index: Any, worker_name: str, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH next_promotions AS (
            SELECT promotion_id
            FROM {index.schema}.promotion_queue
            WHERE promotion_status = 'queued'
            ORDER BY promotion_id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
        )
        UPDATE {index.schema}.promotion_queue pq
        SET promotion_status = 'claimed',
            claimed_by = %s,
            claimed_at = NOW(),
            attempt_count = pq.attempt_count + 1
        FROM next_promotions
        WHERE pq.promotion_id = next_promotions.promotion_id
        RETURNING pq.promotion_id, pq.candidate_id, pq.promotion_target, pq.target_field_name
        """,
        (limit, worker_name),
    ).fetchall()
    conn.commit()
    return [dict(row) for row in rows]


def _promotion_candidate_record(conn: Any, index: Any, promotion_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        f"""
        SELECT pq.promotion_id, pq.candidate_id, pq.promotion_target, pq.target_field_name,
               lc.module_name, lc.source_card_uid, lc.source_rel_path, lc.target_card_uid, lc.target_rel_path,
               lc.target_kind, lc.proposed_link_type, lc.candidate_group, lc.input_hash, lc.evidence_hash,
               ld.decision,
               CASE
                 WHEN lc.proposed_link_type = 'message_in_thread' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'thread_has_message' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'message_has_calendar_event' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'thread_has_calendar_event' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'transcript_has_calendar_event' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'event_has_message' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'event_has_thread' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'event_has_transcript' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'message_has_attachment' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'thread_has_person' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'message_mentions_person' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'event_has_person' THEN 'canonical_safe'
                 WHEN lc.proposed_link_type = 'media_has_person' THEN 'derived_only'
                 WHEN lc.proposed_link_type = 'media_has_event' THEN 'derived_only'
                 WHEN lc.proposed_link_type = 'possible_same_person' THEN 'candidate_derived'
                 WHEN lc.proposed_link_type = 'orphan_repair_exact' THEN 'derived_only'
                 WHEN lc.proposed_link_type = 'orphan_repair_fuzzy' THEN 'candidate_derived'
                 ELSE 'derived_only'
               END AS surface,
               CASE
                 WHEN lc.proposed_link_type = 'message_in_thread' THEN 'thread'
                 WHEN lc.proposed_link_type = 'thread_has_message' THEN 'messages'
                 WHEN lc.proposed_link_type = 'message_has_calendar_event' THEN 'calendar_events'
                 WHEN lc.proposed_link_type = 'thread_has_calendar_event' THEN 'calendar_events'
                 WHEN lc.proposed_link_type = 'transcript_has_calendar_event' THEN 'calendar_events'
                 WHEN lc.proposed_link_type = 'event_has_message' THEN 'source_messages'
                 WHEN lc.proposed_link_type = 'event_has_thread' THEN 'source_threads'
                 WHEN lc.proposed_link_type = 'event_has_transcript' THEN 'meeting_transcripts'
                 WHEN lc.proposed_link_type = 'message_has_attachment' THEN 'attachments'
                 WHEN lc.proposed_link_type = 'thread_has_person' THEN 'people'
                 WHEN lc.proposed_link_type = 'message_mentions_person' THEN 'people'
                 WHEN lc.proposed_link_type = 'event_has_person' THEN 'people'
                 ELSE ''
               END AS canonical_field_name,
               CASE
                 WHEN lc.proposed_link_type IN ('thread_has_person', 'message_mentions_person', 'event_has_person') THEN 'summary'
                 ELSE 'slug'
               END AS canonical_value_mode
        FROM {index.schema}.promotion_queue pq
        JOIN {index.schema}.link_candidates lc ON lc.candidate_id = pq.candidate_id
        JOIN {index.schema}.link_decisions ld ON ld.candidate_id = pq.candidate_id
        WHERE pq.promotion_id = %s
        """,
        (promotion_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _complete_promotion(
    conn: Any, index: Any, promotion_id: int, candidate_id: int, *, blocked_reason: str = "", applied: bool
) -> None:
    conn.execute(
        f"""
        UPDATE {index.schema}.promotion_queue
        SET promotion_status = %s,
            applied_at = CASE WHEN %s = 'applied' THEN NOW() ELSE applied_at END,
            blocked_reason = %s
        WHERE promotion_id = %s
        """,
        (
            PROMOTION_STATUS_APPLIED if applied else PROMOTION_STATUS_BLOCKED,
            PROMOTION_STATUS_APPLIED if applied else PROMOTION_STATUS_BLOCKED,
            blocked_reason,
            promotion_id,
        ),
    )
    conn.execute(
        f"UPDATE {index.schema}.link_candidates SET status = %s WHERE candidate_id = %s",
        (STATUS_PROMOTED if applied else STATUS_NEEDS_REVIEW, candidate_id),
    )


def apply_pending_link_promotions(index: Any, catalog: SeedLinkCatalog, *, limit: int = 0) -> dict[str, int]:
    return run_seed_link_promotion_workers(index, max_workers=1, limit=limit)


def refresh_link_review_metrics(index: Any) -> None:
    with index._connect() as conn:
        rows = conn.execute(
            f"""
            SELECT lc.candidate_id, lc.module_name, lc.proposed_link_type, ld.final_confidence, ld.decision,
                   last_action.action
            FROM {index.schema}.link_candidates lc
            JOIN {index.schema}.link_decisions ld ON ld.candidate_id = lc.candidate_id
            LEFT JOIN (
                SELECT DISTINCT ON (candidate_id) candidate_id, action
                FROM {index.schema}.review_actions
                ORDER BY candidate_id, created_at DESC
            ) AS last_action ON last_action.candidate_id = lc.candidate_id
            """
        ).fetchall()
        today = date.today().isoformat()
        conn.execute(f"DELETE FROM {index.schema}.link_review_metrics WHERE metric_date = %s", (today,))
        aggregates: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            metric = candidate_to_metric_row(
                module_name=str(row["module_name"]),
                link_type=str(row["proposed_link_type"]),
                final_confidence=float(row["final_confidence"]),
                decision=str(row["decision"]),
                action=str(row["action"] or ""),
            )
            key = (metric["module_name"], metric["link_type"], metric["score_band"])
            bucket = aggregates.setdefault(
                key,
                {
                    "candidate_count": 0,
                    "approved_count": 0,
                    "rejected_count": 0,
                    "override_count": 0,
                    "auto_promoted_count": 0,
                    "sampled_auto_promoted_count": 0,
                    "sample_precision_sum": 0.0,
                },
            )
            bucket["candidate_count"] += metric["candidate_count"]
            bucket["approved_count"] += metric["approved_count"]
            bucket["rejected_count"] += metric["rejected_count"]
            bucket["override_count"] += metric["override_count"]
            bucket["auto_promoted_count"] += metric["auto_promoted_count"]
            bucket["sampled_auto_promoted_count"] += metric["sampled_auto_promoted_count"]
            bucket["sample_precision_sum"] += metric["sample_precision"]
        for (module_name, link_type, score_band), values in aggregates.items():
            sample_precision = (
                values["sample_precision_sum"] / values["sampled_auto_promoted_count"]
                if values["sampled_auto_promoted_count"]
                else 0.0
            )
            conn.execute(
                f"""
                INSERT INTO {index.schema}.link_review_metrics(
                    metric_date, module_name, link_type, score_band, candidate_count, approved_count, rejected_count,
                    override_count, auto_promoted_count, sampled_auto_promoted_count, sample_precision
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    today,
                    module_name,
                    link_type,
                    score_band,
                    values["candidate_count"],
                    values["approved_count"],
                    values["rejected_count"],
                    values["override_count"],
                    values["auto_promoted_count"],
                    values["sampled_auto_promoted_count"],
                    sample_precision,
                ),
            )
        conn.commit()


def refresh_link_dead_ends(index: Any) -> None:
    with index._connect() as conn:
        rows = conn.execute(
            f"""
            WITH graph_edges AS (
                SELECT source_uid, target_uid
                FROM {index.schema}.edges
                WHERE target_kind = 'card' AND target_uid <> ''
                UNION ALL
                SELECT lc.source_card_uid AS source_uid, lc.target_card_uid AS target_uid
                FROM {index.schema}.link_candidates lc
                JOIN {index.schema}.promotion_queue pq
                  ON pq.candidate_id = lc.candidate_id
                 AND pq.promotion_target = 'derived_edge'
                 AND pq.promotion_status = 'applied'
                WHERE lc.target_kind = 'card' AND lc.target_card_uid <> ''
            ),
            degree_counts AS (
                SELECT uid, COUNT(*) AS degree
                FROM (
                    SELECT source_uid AS uid FROM graph_edges
                    UNION ALL
                    SELECT target_uid AS uid FROM graph_edges
                ) AS degree_rows
                GROUP BY uid
            )
            SELECT c.uid, c.rel_path, c.type, COALESCE(d.degree, 0) AS degree
            FROM {index.schema}.cards c
            LEFT JOIN degree_counts d ON d.uid = c.uid
            """
        ).fetchall()
        conn.execute(f"DELETE FROM {index.schema}.link_dead_ends")
        for row in rows:
            degree = int(row["degree"])
            if degree > 1:
                continue
            conn.execute(
                f"""
                INSERT INTO {index.schema}.link_dead_ends(card_uid, rel_path, card_type, degree, reason)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    str(row["uid"]),
                    str(row["rel_path"]),
                    str(row["type"]),
                    degree,
                    "degree<=1 after canonical and approved derived edges",
                ),
            )
        conn.commit()


def run_seed_link_enqueue(
    index: Any,
    *,
    modules: list[str] | None = None,
    job_type: str = "seed_backfill",
    source_uids: set[str] | None = None,
    reset_existing: bool = False,
    cache: VaultScanCache | None = None,
) -> dict[str, int]:
    index.ensure_ready()
    result = enqueue_seed_link_jobs(
        index,
        modules=modules,
        job_type=job_type,
        source_uids=source_uids,
        reset_existing=reset_existing,
        cache=cache,
    )
    with index._connect() as conn:
        _upsert_meta(
            conn,
            index.schema,
            {
                "seed_link_last_enqueue_at": date.today().isoformat(),
                "seed_link_last_job_type": job_type,
                "seed_link_jobs_prepared": str(result["prepared"]),
                "seed_link_jobs_enqueued": str(result["enqueued"]),
            },
        )
        conn.commit()
    return result


def run_seed_link_workers(
    index: Any,
    *,
    modules: list[str] | None = None,
    limit: int = 0,
    max_workers: int = 0,
    include_llm: bool = True,
    worker_name_prefix: str = "seed-link-worker",
    cache: VaultScanCache | None = None,
) -> dict[str, Any]:
    index.ensure_ready()
    catalog = build_seed_link_catalog(index.vault, cache=cache)
    summary = SeedLinkRunSummary()
    reserve_lock = Lock()
    max_to_process = max(int(limit or 0), 0)
    workers = max(1, int(max_workers or DEFAULT_SEED_LINK_WORKERS))
    processed_jobs = 0
    claim_batch_size = DEFAULT_SEED_LINK_CLAIM_BATCH_SIZE

    def reserve_job_slots(requested: int) -> int:
        nonlocal processed_jobs
        with reserve_lock:
            if max_to_process and processed_jobs >= max_to_process:
                return 0
            if max_to_process:
                remaining = max_to_process - processed_jobs
                granted = min(requested, remaining)
            else:
                granted = requested
            processed_jobs += granted
            return granted

    def worker_loop(worker_idx: int) -> SeedLinkRunSummary:
        worker_summary = SeedLinkRunSummary()
        with index._connect() as conn:
            while True:
                requested = reserve_job_slots(claim_batch_size)
                if requested <= 0:
                    break
                jobs = _claim_next_jobs(conn, index, f"{worker_name_prefix}-{worker_idx}", requested, modules=modules)
                if not jobs:
                    break
                for job in jobs:
                    try:
                        job_started = time.perf_counter()
                        source = catalog.cards_by_uid.get(str(job["source_card_uid"]))
                        if source is None:
                            raise ValueError("source card missing from catalog")
                        job_module = str(job["module_name"])
                        if job_module == MODULE_SEMANTIC:
                            raw_candidates = _generate_semantic_candidates(
                                conn, index.schema, catalog, source.uid
                            )
                        else:
                            raw_candidates = generate_seed_link_candidates(catalog, source, job_module)
                        candidates = _dedupe_candidates(raw_candidates)
                        if not include_llm:
                            for candidate in candidates:
                                candidate.features["llm_disabled"] = 1
                        for candidate in candidates:
                            decision = evaluate_seed_link_candidate(index.vault, catalog, candidate)
                            _persist_candidate(conn, index, int(job["job_id"]), candidate, decision, commit=False)
                            worker_summary.candidates += 1
                            if decision.decision == DECISION_REVIEW:
                                worker_summary.needs_review += 1
                            elif decision.decision == DECISION_AUTO_PROMOTE:
                                worker_summary.auto_promoted += 1
                            elif decision.decision == DECISION_CANONICAL_SAFE:
                                worker_summary.canonical_safe += 1
                            if decision.llm_model or decision.llm_output_json:
                                worker_summary.llm_judged += 1
                        _complete_job(conn, index, int(job["job_id"]), commit=False)
                        conn.commit()
                        worker_summary.jobs_completed += 1
                        _merge_module_metric(
                            worker_summary.module_metrics,
                            str(job["module_name"]),
                            elapsed_seconds=time.perf_counter() - job_started,
                            jobs=1,
                            candidates=len(candidates),
                        )
                    except Exception as exc:  # pragma: no cover
                        conn.rollback()
                        _fail_job(conn, index, int(job["job_id"]), str(exc), commit=True)
                        worker_summary.jobs_failed += 1
        return worker_summary

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for worker_result in executor.map(worker_loop, range(workers)):
            summary.jobs_completed += worker_result.jobs_completed
            summary.jobs_failed += worker_result.jobs_failed
            summary.candidates += worker_result.candidates
            summary.needs_review += worker_result.needs_review
            summary.auto_promoted += worker_result.auto_promoted
            summary.canonical_safe += worker_result.canonical_safe
            summary.llm_judged += worker_result.llm_judged
            for module_name, metrics in worker_result.module_metrics.items():
                _merge_module_metric(
                    summary.module_metrics,
                    module_name,
                    elapsed_seconds=float(metrics.get("elapsed_seconds", 0.0)),
                    jobs=int(metrics.get("jobs", 0.0)),
                    candidates=int(metrics.get("candidates", 0.0)),
                )

    with index._connect() as conn:
        _upsert_meta(
            conn,
            index.schema,
            {
                "seed_link_jobs_completed": str(summary.jobs_completed),
                "seed_link_jobs_failed": str(summary.jobs_failed),
                "seed_link_candidates": str(summary.candidates),
                "seed_link_auto_promoted": str(summary.auto_promoted),
            },
        )
        conn.commit()
    return {
        "workers": workers,
        "jobs_completed": summary.jobs_completed,
        "jobs_failed": summary.jobs_failed,
        "candidates": summary.candidates,
        "needs_review": summary.needs_review,
        "auto_promoted": summary.auto_promoted,
        "canonical_safe": summary.canonical_safe,
        "llm_judged": summary.llm_judged,
        "module_metrics": {
            module_name: {
                "elapsed_seconds": round(float(metrics["elapsed_seconds"]), 6),
                "jobs": int(metrics["jobs"]),
                "candidates": int(metrics["candidates"]),
                "jobs_per_second": round(float(metrics["jobs"]) / max(float(metrics["elapsed_seconds"]), 0.001), 3),
                "candidates_per_second": round(
                    float(metrics["candidates"]) / max(float(metrics["elapsed_seconds"]), 0.001), 3
                ),
            }
            for module_name, metrics in sorted(summary.module_metrics.items())
        },
    }


def run_seed_link_promotion_workers(
    index: Any,
    *,
    limit: int = 0,
    max_workers: int = 1,
    worker_name_prefix: str = "seed-link-promoter",
    cache: VaultScanCache | None = None,
) -> dict[str, int]:
    index.ensure_ready()
    catalog = build_seed_link_catalog(index.vault, cache=cache)
    counts = {"derived_edge": 0, "canonical_field": 0, "blocked": 0}
    reserve_lock = Lock()
    max_to_process = max(int(limit or 0), 0)
    processed_promotions = 0

    def reserve_slots(requested: int) -> int:
        nonlocal processed_promotions
        with reserve_lock:
            if max_to_process and processed_promotions >= max_to_process:
                return 0
            if max_to_process:
                remaining = max_to_process - processed_promotions
                granted = min(requested, remaining)
            else:
                granted = requested
            processed_promotions += granted
            return granted

    def promotion_loop(worker_idx: int) -> dict[str, int]:
        worker_counts = {"derived_edge": 0, "canonical_field": 0, "blocked": 0}
        with index._connect() as conn:
            while True:
                requested = reserve_slots(DEFAULT_PROMOTION_CLAIM_BATCH_SIZE)
                if requested <= 0:
                    break
                claims = _claim_next_promotions(conn, index, f"{worker_name_prefix}-{worker_idx}", requested)
                if not claims:
                    break
                for claim in claims:
                    record = _promotion_candidate_record(conn, index, int(claim["promotion_id"]))
                    if record is None:
                        conn.rollback()
                        continue
                    if record["promotion_target"] == PROMOTION_TARGET_DERIVED_EDGE:
                        _complete_promotion(
                            conn, index, int(record["promotion_id"]), int(record["candidate_id"]), applied=True
                        )
                        conn.commit()
                        worker_counts["derived_edge"] += 1
                        continue
                    try:
                        applied, blocked_reason = _apply_canonical_promotion(index, record, catalog)
                    except Exception as exc:  # pragma: no cover
                        applied = False
                        blocked_reason = str(exc)
                    _complete_promotion(
                        conn,
                        index,
                        int(record["promotion_id"]),
                        int(record["candidate_id"]),
                        blocked_reason=blocked_reason,
                        applied=applied,
                    )
                    if applied:
                        _mark_canonical_dirty(index, conn)
                        worker_counts["canonical_field"] += 1
                    else:
                        worker_counts["blocked"] += 1
                    conn.commit()
        return worker_counts

    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        for worker_counts in executor.map(promotion_loop, range(max(1, int(max_workers)))):
            for key in counts:
                counts[key] += int(worker_counts[key])
    with index._connect() as conn:
        _upsert_meta(
            conn,
            index.schema,
            {
                "seed_link_derived_promotions_applied": str(counts["derived_edge"]),
                "seed_link_canonical_applied": str(counts["canonical_field"]),
                "seed_link_promotion_blocked": str(counts["blocked"]),
            },
        )
        conn.commit()
    return counts


def run_seed_link_report(
    index: Any,
    *,
    rebuild_if_dirty: bool = True,
) -> dict[str, Any]:
    index.ensure_ready()
    rebuilt = False
    if rebuild_if_dirty and _canonical_dirty(index):
        index.rebuild()
        _clear_canonical_dirty(index)
        rebuilt = True
    refresh_link_review_metrics(index)
    refresh_link_dead_ends(index)
    gate = compute_link_quality_gate(index)
    gate["rebuilt"] = rebuilt
    return gate


def run_seed_link_backfill(
    index: Any,
    *,
    modules: list[str] | None = None,
    limit: int = 0,
    max_workers: int = 0,
    include_llm: bool = True,
    apply_promotions: bool = True,
    job_type: str = "seed_backfill",
    source_uids: set[str] | None = None,
) -> dict[str, Any]:
    index.ensure_ready()
    seed_cache = VaultScanCache.build_or_load(Path(index.vault), tier=2)
    orphaned_before = _count_orphaned_links(index.vault, cache=seed_cache)
    enqueue_result = run_seed_link_enqueue(
        index,
        modules=modules,
        job_type=job_type,
        source_uids=source_uids,
        reset_existing=False,
        cache=seed_cache,
    )
    worker_result = run_seed_link_workers(
        index,
        modules=modules,
        limit=limit,
        max_workers=max_workers,
        include_llm=include_llm,
        cache=seed_cache,
    )
    promotion_counts = {"derived_edge": 0, "canonical_field": 0, "blocked": 0}
    if apply_promotions:
        promotion_counts = run_seed_link_promotion_workers(index, cache=seed_cache)
    gate = run_seed_link_report(index, rebuild_if_dirty=bool(apply_promotions))
    return {
        "workers": int(worker_result["workers"]),
        "jobs_prepared": int(enqueue_result["prepared"]),
        "jobs_enqueued": int(enqueue_result["enqueued"]),
        "jobs_existing": int(enqueue_result["existing"]),
        "jobs_completed": int(worker_result["jobs_completed"]),
        "jobs_failed": int(worker_result["jobs_failed"]),
        "candidates": int(worker_result["candidates"]),
        "needs_review": int(worker_result["needs_review"]),
        "auto_promoted": int(worker_result["auto_promoted"]),
        "canonical_safe": int(worker_result["canonical_safe"]),
        "llm_judged": int(worker_result["llm_judged"]),
        "derived_promotions_applied": int(promotion_counts["derived_edge"]),
        "canonical_applied": int(promotion_counts["canonical_field"]),
        "orphaned_links_before": orphaned_before,
        "orphaned_links_after": int(gate["orphaned_links_after"]),
        "promotion_blocked": int(promotion_counts["blocked"]),
        "job_type": job_type,
        "module_metrics": worker_result["module_metrics"],
        "rebuilt": bool(gate.get("rebuilt")),
    }


def run_incremental_link_refresh(
    index: Any,
    *,
    source_uids: list[str],
    modules: list[str] | None = None,
    max_workers: int = 0,
    include_llm: bool = True,
    apply_promotions: bool = True,
) -> dict[str, Any]:
    scoped = {item.strip() for item in source_uids if item.strip()}
    return run_seed_link_backfill(
        index,
        modules=modules,
        limit=len(scoped),
        max_workers=max_workers,
        include_llm=include_llm,
        apply_promotions=apply_promotions,
        job_type="incremental",
        source_uids=scoped,
    )


def list_link_candidates(
    index: Any,
    *,
    status: str = "",
    module_name: str = "",
    min_confidence: float = 0.0,
    limit: int = 20,
) -> list[dict[str, Any]]:
    clauses = ["1 = 1"]
    params: list[Any] = []
    if status:
        clauses.append("lc.status = %s")
        params.append(status)
    if module_name:
        clauses.append("lc.module_name = %s")
        params.append(module_name)
    clauses.append("ld.final_confidence >= %s")
    params.append(float(min_confidence))
    params.append(max(int(limit), 1))
    with index._connect() as conn:
        rows = conn.execute(
            f"""
            SELECT lc.candidate_id, lc.module_name, lc.source_rel_path, lc.target_rel_path, lc.proposed_link_type,
                   lc.status, ld.final_confidence, ld.decision, ld.decision_reason,
                   pq.promotion_status
            FROM {index.schema}.link_candidates lc
            JOIN {index.schema}.link_decisions ld ON ld.candidate_id = lc.candidate_id
            LEFT JOIN {index.schema}.promotion_queue pq ON pq.candidate_id = lc.candidate_id
            WHERE {" AND ".join(clauses)}
            ORDER BY ld.final_confidence DESC, lc.candidate_id ASC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_link_candidate_details(index: Any, candidate_id: int) -> dict[str, Any] | None:
    with index._connect() as conn:
        row = conn.execute(
            f"""
            SELECT lc.*, ld.deterministic_score, ld.lexical_score, ld.graph_score, ld.llm_score, ld.risk_penalty,
                   ld.final_confidence, ld.decision, ld.decision_reason, ld.llm_model, ld.llm_output_json,
                   pq.promotion_target, pq.target_field_name, pq.promotion_status, pq.blocked_reason
            FROM {index.schema}.link_candidates lc
            JOIN {index.schema}.link_decisions ld ON ld.candidate_id = lc.candidate_id
            LEFT JOIN {index.schema}.promotion_queue pq ON pq.candidate_id = lc.candidate_id
            WHERE lc.candidate_id = %s
            """,
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        evidence_rows = conn.execute(
            f"""
            SELECT evidence_type, evidence_source, feature_name, feature_value, feature_weight, raw_payload_json
            FROM {index.schema}.link_evidence
            WHERE candidate_id = %s
            ORDER BY evidence_id ASC
            """,
            (candidate_id,),
        ).fetchall()
        review_rows = conn.execute(
            f"""
            SELECT reviewer, action, notes, score_at_review, decision_at_review, created_at
            FROM {index.schema}.review_actions
            WHERE candidate_id = %s
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()
    payload = dict(row)
    payload["evidence"] = [dict(item) for item in evidence_rows]
    payload["reviews"] = [dict(item) for item in review_rows]
    return payload


def review_link_candidate(
    index: Any, *, candidate_id: int, reviewer: str, action: str, notes: str = ""
) -> dict[str, Any]:
    if action not in REVIEW_ACTIONS:
        raise ValueError(f"Unsupported review action: {action}")
    details = get_link_candidate_details(index, candidate_id)
    if details is None:
        raise ValueError(f"Candidate not found: {candidate_id}")
    new_decision = str(details["decision"])
    new_status = STATUS_NEEDS_REVIEW
    if action in {REVIEW_ACTION_APPROVE, REVIEW_ACTION_OVERRIDE_APPROVE}:
        if (
            details.get("promotion_target") == PROMOTION_TARGET_CANONICAL_FIELD
            and float(details.get("deterministic_score", 0.0)) >= 1.0
        ):
            new_decision = DECISION_CANONICAL_SAFE
        else:
            new_decision = DECISION_AUTO_PROMOTE
        new_status = STATUS_APPROVED
    elif action in {REVIEW_ACTION_REJECT, REVIEW_ACTION_OVERRIDE_REJECT}:
        new_decision = DECISION_DISCARD
        new_status = STATUS_REJECTED
    with index._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.review_actions(
                candidate_id, reviewer, action, notes, score_at_review, decision_at_review
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                candidate_id,
                reviewer,
                action,
                notes,
                float(details["final_confidence"]),
                str(details["decision"]),
            ),
        )
        conn.execute(
            f"""
            UPDATE {index.schema}.link_decisions
            SET decision = %s,
                decision_reason = %s
            WHERE candidate_id = %s
            """,
            (
                new_decision,
                DECISION_REASON_REVIEW_OVERRIDE if action.startswith("override") else str(details["decision_reason"]),
                candidate_id,
            ),
        )
        conn.execute(
            f"UPDATE {index.schema}.link_candidates SET status = %s WHERE candidate_id = %s",
            (new_status, candidate_id),
        )
        if new_decision in {DECISION_AUTO_PROMOTE, DECISION_CANONICAL_SAFE}:
            conn.execute(
                f"""
                INSERT INTO {index.schema}.promotion_queue(candidate_id, promotion_target, target_field_name, promotion_status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (candidate_id, promotion_target, target_field_name)
                DO UPDATE SET promotion_status = 'queued', blocked_reason = ''
                """,
                (
                    candidate_id,
                    str(details.get("promotion_target") or PROMOTION_TARGET_DERIVED_EDGE),
                    str(details.get("target_field_name") or ""),
                    PROMOTION_STATUS_QUEUED,
                ),
            )
        else:
            conn.execute(
                f"""
                UPDATE {index.schema}.promotion_queue
                SET promotion_status = 'blocked', blocked_reason = %s
                WHERE candidate_id = %s
                """,
                ("review rejected", candidate_id),
            )
        conn.commit()
    return get_link_candidate_details(index, candidate_id) or {}


def compute_link_quality_gate(index: Any, *, cache: VaultScanCache | None = None) -> dict[str, Any]:
    refresh_link_review_metrics(index)
    refresh_link_dead_ends(index)
    thresholds = quality_gate_thresholds()
    if cache is None:
        cache = VaultScanCache.build_or_load(Path(index.vault), tier=2)
    orphaned_after = _count_orphaned_links(index.vault, cache=cache)
    with index._connect() as conn:
        card_row = conn.execute(f"SELECT COUNT(*) AS count FROM {index.schema}.cards").fetchone()
        reviewable_card_row = conn.execute(
            f"SELECT COUNT(DISTINCT source_card_uid) AS count FROM {index.schema}.link_jobs",
        ).fetchone()
        reviewed_row = conn.execute(
            f"SELECT COUNT(DISTINCT source_card_uid) AS count FROM {index.schema}.link_jobs WHERE status = 'completed'"
        ).fetchone()
        duplicate_row = conn.execute(
            f"SELECT value FROM {index.schema}.meta WHERE key = 'duplicate_uid_count'"
        ).fetchone()
        backlog_rows = conn.execute(
            f"""
            SELECT lc.module_name, COUNT(*) AS count
            FROM {index.schema}.link_candidates lc
            JOIN {index.schema}.cards c ON c.uid = lc.source_card_uid
            WHERE lc.status = 'needs_review'
              AND c.type = ANY(%s)
            GROUP BY lc.module_name
            ORDER BY count DESC, lc.module_name ASC
            """,
            (list(HIGH_PRIORITY_CARD_TYPES),),
        ).fetchall()
        dead_end_row = conn.execute(f"SELECT COUNT(*) AS count FROM {index.schema}.link_dead_ends").fetchone()
        auto_rows = conn.execute(
            f"""
            SELECT metric_date, module_name, sample_precision
            FROM {index.schema}.link_review_metrics
            WHERE metric_date = %s
            ORDER BY module_name ASC
            """,
            (date.today().isoformat(),),
        ).fetchall()
        candidate_rows = conn.execute(
            f"""
            SELECT lc.module_name, lc.proposed_link_type, COUNT(*) AS count
            FROM {index.schema}.link_candidates lc
            GROUP BY lc.module_name, lc.proposed_link_type
            ORDER BY lc.module_name ASC, lc.proposed_link_type ASC
            """
        ).fetchall()
        promoted_rows = conn.execute(
            f"""
            SELECT lc.module_name, lc.proposed_link_type, COUNT(*) AS count
            FROM {index.schema}.link_candidates lc
            JOIN {index.schema}.promotion_queue pq
              ON pq.candidate_id = lc.candidate_id
             AND pq.promotion_status = 'applied'
            GROUP BY lc.module_name, lc.proposed_link_type
            ORDER BY lc.module_name ASC, lc.proposed_link_type ASC
            """
        ).fetchall()
    total_cards = int(card_row["count"]) if card_row is not None else 0
    reviewable_cards = int(reviewable_card_row["count"]) if reviewable_card_row is not None else total_cards
    reviewed_cards = int(reviewed_row["count"]) if reviewed_row is not None else 0
    scan_coverage = round(reviewed_cards / reviewable_cards, 6) if reviewable_cards else 0.0
    duplicate_uid_count = int(str(duplicate_row["value"])) if duplicate_row is not None else 0
    high_priority_backlog = sum(int(row["count"]) for row in backlog_rows)
    high_risk_precision = 1.0
    risk_rows = [
        row for row in auto_rows if str(row["module_name"]) in {MODULE_IDENTITY, MODULE_MEDIA, MODULE_CALENDAR}
    ]
    if risk_rows:
        non_zero = [float(row["sample_precision"]) for row in risk_rows if float(row["sample_precision"]) > 0]
        high_risk_precision = min(non_zero) if non_zero else 1.0
    score_precision_rows = []
    for row in auto_rows:
        payload = dict(row)
        metric_date = payload.get("metric_date")
        if hasattr(metric_date, "isoformat"):
            payload["metric_date"] = metric_date.isoformat()
        score_precision_rows.append(payload)
    return {
        "total_cards_reviewed": reviewed_cards,
        "seed_card_count": total_cards,
        "reviewable_seed_card_count": reviewable_cards,
        "scan_coverage": scan_coverage,
        "required_scan_coverage": thresholds["required_scan_coverage"],
        "orphaned_links_after": orphaned_after,
        "required_orphan_review_coverage": thresholds["required_orphan_review_coverage"],
        "duplicate_uid_count": duplicate_uid_count,
        "max_duplicate_uids": thresholds["max_duplicate_uids"],
        "dead_end_count": int(dead_end_row["count"]) if dead_end_row is not None else 0,
        "high_priority_review_backlog": high_priority_backlog,
        "max_high_priority_review_backlog": thresholds["max_high_priority_review_backlog"],
        "high_risk_precision": round(high_risk_precision, 6),
        "required_high_risk_precision": thresholds["required_high_risk_precision"],
        "candidate_counts": [dict(row) for row in candidate_rows],
        "auto_promoted_counts": [dict(row) for row in promoted_rows],
        "score_precision_by_module": score_precision_rows,
        "passes": (
            scan_coverage >= thresholds["required_scan_coverage"]
            and orphaned_after == 0
            and duplicate_uid_count <= thresholds["max_duplicate_uids"]
            and high_priority_backlog <= thresholds["max_high_priority_review_backlog"]
            and high_risk_precision >= thresholds["required_high_risk_precision"]
        ),
    }


# =============================================================================
# Phase 6.5 linker framework integration.
#
# The framework (archive_cli/linker_framework.py) provides a declarative
# LinkerSpec registry. Every linker self-registers from archive_cli/linker_modules
# when the package is imported below.
# =============================================================================

from archive_cli import linker_framework as _lf  # noqa: E402

_lf._bind_wiring_tables(
    card_type_modules=CARD_TYPE_MODULES,
    llm_review_modules=LLM_REVIEW_MODULES,
    proposed_link_types=PROPOSED_LINK_TYPES,
    link_surface_by_type=LINK_SURFACE_BY_TYPE,
)











# Import linker modules so every LinkerSpec self-registers.
try:
    from archive_cli import \
        linker_modules as _new_linker_modules  # noqa: F401, E402
except Exception as _e:  # pragma: no cover - defensive
    import logging as _logging
    _logging.getLogger("ppa.linkers").warning(
        "Phase 6.5 linker_modules import failed: %s", _e,
    )
