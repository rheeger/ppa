"""Post-rank neighbor context. Matched units stay distinct from expansions.

Consumes P01-D ``ChunkEvidenceRef`` / ``SourceSpan`` citations. Does not copy
P01 burst segmentation — callers pass already-ordered chunk or message units.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from archive_engine.contracts import (
    CHUNK_EVIDENCE_REF_VERSION,
    AccessContext,
    ChunkEvidenceRef,
    MessageEvidenceRef,
    SourceSpan,
)
from archive_engine.errors import SpanRequiredError, StaleContextError

CONTEXT_CONTRACT_VERSION = "p10b.1"
DEFAULT_PRECEDING = 1
DEFAULT_FOLLOWING = 1
DEFAULT_MAX_TOKENS_PER_HIT = 2000
DEFAULT_MAX_TOKENS_TOTAL = 8000
STALE_REASON = "stale-context/refresh-required"
SPAN_UNAVAILABLE_REASON = "span_unavailable"

# Conservative bound matching ``archive_cli.chunk_builders._token_count``:
# whitespace-split words, minimum 1 for nonempty text.
ContextRole = Literal["matched", "context"]


def estimate_tokens(text: str) -> int:
    """Whitespace-split word count. Same bound as the CLI chunk tokenizer."""

    return max(len(text.split()), 1) if text.strip() else 0


def revision_for(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def utf8_span(body: str, excerpt: str | None = None) -> tuple[int, int]:
    encoded = body.encode("utf-8")
    if excerpt:
        needle = excerpt.encode("utf-8")
        start = encoded.find(needle)
        if start >= 0:
            return start, start + len(needle)
    return 0, len(encoded)


def source_span_for(
    body: str,
    *,
    source_uid: str,
    source_revision: str,
    source_message_id: str = "",
    excerpt: str | None = None,
) -> SourceSpan:
    start, end = utf8_span(body, excerpt)
    return SourceSpan(
        source_uid=source_uid,
        source_message_id=source_message_id,
        source_revision=source_revision,
        representation="canonical_body_utf8",
        start_byte=start,
        end_byte=end,
    )


def quote_canonical_span(body: str, span: SourceSpan, *, actual_revision: str) -> str:
    """Quote exact UTF-8 bytes. Fail closed on revision or boundary mismatch."""

    if span.source_revision != actual_revision:
        raise StaleContextError(STALE_REASON)
    encoded = body.encode("utf-8")
    if span.start_byte < 0 or span.end_byte > len(encoded) or span.end_byte < span.start_byte:
        raise StaleContextError(STALE_REASON)
    try:
        quoted = encoded[span.start_byte : span.end_byte].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StaleContextError(STALE_REASON) from exc
    return quoted


def _source_allowed(allowed: Sequence[str], source: str) -> bool:
    needle = " ".join(source.split()).casefold()
    if not needle:
        return False
    for item in allowed:
        allow = " ".join(str(item).split()).casefold()
        if allow and (needle == allow or needle.startswith(f"{allow}:") or allow.startswith(f"{needle}:")):
            return True
    return False


def unit_visible(unit: NeighborUnit, access: AccessContext | None) -> bool:
    """Denied and mixed-source units are absent, not redacted."""

    if not unit.allowed:
        return False
    if access is None:
        return True
    if access.deny:
        return False
    if access.allowed_sources:
        required = unit.required_sources
        if not required:
            return False
        for source in required:
            if not _source_allowed(access.allowed_sources, source):
                return False
    return True


@dataclass(frozen=True)
class NeighborUnit:
    """One ordered expandable unit (adjacent chunk or source message)."""

    uid: str
    text: str
    revision: str
    chunk_id: str = ""
    message_id: str = ""
    sequence: int = 0
    thread_id: str = ""
    boundary_key: str = ""
    allowed: bool = True
    required_sources: tuple[str, ...] = ()
    citation: ChunkEvidenceRef | None = None
    span_unavailable: bool = False


@dataclass(frozen=True)
class ContextUnit:
    """One cited unit. ``role`` is matched (the hit) or context (expansion)."""

    role: ContextRole
    reason: str
    uid: str
    chunk_id: str
    message_id: str
    revision: str
    quoted_text: str
    citation: ChunkEvidenceRef
    token_estimate: int
    stale: bool = False
    stale_reason: str = ""

    def to_payload(self) -> dict[str, Any]:
        span = self.citation.source_spans[0] if self.citation.source_spans else None
        return {
            "role": self.role,
            "reason": self.reason,
            "uid": self.uid,
            "chunk_id": self.chunk_id,
            "message_id": self.message_id,
            "revision": self.revision,
            "quoted_text": self.quoted_text,
            "token_estimate": self.token_estimate,
            "stale": self.stale,
            "stale_reason": self.stale_reason,
            "span_unavailable": self.citation.span_unavailable,
            "start_byte": None if span is None else span.start_byte,
            "end_byte": None if span is None else span.end_byte,
            "citation": self.citation.to_payload(),
        }

    def citation_summary(self) -> dict[str, Any]:
        """Compact citation — UID/span/revision only, never quoted text."""

        span = self.citation.source_spans[0] if self.citation.source_spans else None
        return {
            "uid": self.uid,
            "role": self.role,
            "reason": self.reason,
            "chunk_id": self.chunk_id,
            "message_id": self.message_id,
            "revision": self.revision,
            "start_byte": None if span is None else span.start_byte,
            "end_byte": None if span is None else span.end_byte,
            "span_unavailable": self.citation.span_unavailable,
        }


@dataclass(frozen=True)
class ExpandedHit:
    matched: tuple[ContextUnit, ...]
    context: tuple[ContextUnit, ...]
    stale: bool = False
    stale_reason: str = ""
    truncated: bool = False
    truncation_reason: str = ""
    token_estimate: int = 0
    support_uids: tuple[str, ...] = ()
    excluded_uids: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "contract_version": CONTEXT_CONTRACT_VERSION,
            "matched": [item.to_payload() for item in self.matched],
            "context": [item.to_payload() for item in self.context],
            "stale": self.stale,
            "stale_reason": self.stale_reason,
            "truncated": self.truncated,
            "truncation_reason": self.truncation_reason,
            "token_estimate": self.token_estimate,
            "support_uids": list(self.support_uids),
            "excluded_uids": list(self.excluded_uids),
        }


def citation_for_unit(unit: NeighborUnit, *, archive_id: str) -> ChunkEvidenceRef:
    if unit.citation is not None:
        return unit.citation
    span = source_span_for(
        unit.text,
        source_uid=unit.uid,
        source_revision=unit.revision,
        source_message_id=unit.message_id,
    )
    message_ref = MessageEvidenceRef(
        message_id=unit.message_id or unit.uid,
        source_revision=unit.revision,
        spans=(span,),
    )
    return ChunkEvidenceRef(
        version=CHUNK_EVIDENCE_REF_VERSION,
        archive_id=archive_id,
        card_uid=unit.uid,
        chunk_id=unit.chunk_id or f"ck-{unit.uid}",
        chunk_schema_version="6",
        algorithm_version=CONTEXT_CONTRACT_VERSION,
        evidence_kind="source_reported",
        lineage_complete=True,
        parent_thread=unit.thread_id,
        source_revisions=(unit.revision,),
        source_spans=() if unit.span_unavailable else (span,),
        span_unavailable=unit.span_unavailable,
        message_refs=() if unit.span_unavailable else (message_ref,),
        message_refs_available=not unit.span_unavailable,
    )


def _quote_unit(
    unit: NeighborUnit,
    *,
    archive_id: str,
    canonical: Mapping[str, tuple[str, str]] | None,
    span_required: bool,
) -> tuple[str, ChunkEvidenceRef, bool, str]:
    citation = citation_for_unit(unit, archive_id=archive_id)
    body = unit.text
    revision = unit.revision
    if canonical and unit.uid in canonical:
        body, revision = canonical[unit.uid]
    if citation.span_unavailable or unit.span_unavailable:
        if span_required:
            raise SpanRequiredError(SPAN_UNAVAILABLE_REASON)
        return "", citation, True, SPAN_UNAVAILABLE_REASON
    if not citation.source_spans:
        if span_required:
            raise SpanRequiredError(SPAN_UNAVAILABLE_REASON)
        return "", citation, True, SPAN_UNAVAILABLE_REASON
    try:
        quoted = quote_canonical_span(body, citation.source_spans[0], actual_revision=revision)
    except StaleContextError:
        if span_required:
            raise
        return "", citation, True, STALE_REASON
    return quoted, citation, False, ""


def _same_lane(hit: NeighborUnit, other: NeighborUnit) -> bool:
    thread = hit.thread_id or hit.uid
    other_thread = other.thread_id or other.uid
    if thread != other_thread:
        return False
    if hit.boundary_key and other.boundary_key and hit.boundary_key != other.boundary_key:
        return False
    return True


def _overlap_key(unit: ContextUnit) -> tuple[str, int, int, str]:
    span = unit.citation.source_spans[0] if unit.citation.source_spans else None
    start = -1 if span is None else span.start_byte
    end = -1 if span is None else span.end_byte
    return (unit.uid, start, end, unit.chunk_id)


def expand_neighbors(
    hit: NeighborUnit,
    units: Sequence[NeighborUnit],
    *,
    access: AccessContext | None = None,
    archive_id: str = "archive-p10b",
    preceding: int = DEFAULT_PRECEDING,
    following: int = DEFAULT_FOLLOWING,
    max_tokens_per_hit: int = DEFAULT_MAX_TOKENS_PER_HIT,
    max_tokens_total: int = DEFAULT_MAX_TOKENS_TOTAL,
    canonical: Mapping[str, tuple[str, str]] | None = None,
    span_required: bool = False,
    excluded_uids: Sequence[str] = (),
) -> ExpandedHit:
    """Expand one ranked unit by adjacent same-lane neighbors.

    Token budgets skip additional context units. Citations on accepted units
    are never stripped. Denied / excluded / other-lane neighbors stay absent.
    """

    excluded = {uid for uid in excluded_uids if uid}
    if not unit_visible(hit, access):
        return ExpandedHit(matched=(), context=(), excluded_uids=tuple(sorted(excluded)))

    quoted, citation, stale, stale_reason = _quote_unit(
        hit, archive_id=archive_id, canonical=canonical, span_required=span_required
    )
    matched = ContextUnit(
        role="matched",
        reason="ranked_hit",
        uid=hit.uid,
        chunk_id=hit.chunk_id or citation.chunk_id,
        message_id=hit.message_id,
        revision=hit.revision,
        quoted_text=quoted,
        citation=citation,
        token_estimate=estimate_tokens(quoted),
        stale=stale,
        stale_reason=stale_reason,
    )
    lane = [item for item in units if _same_lane(hit, item)]
    lane.sort(key=lambda item: (item.sequence, item.uid, item.chunk_id, item.message_id))
    try:
        index = next(
            i
            for i, item in enumerate(lane)
            if item.uid == hit.uid and item.chunk_id == hit.chunk_id and item.message_id == hit.message_id
        )
    except StopIteration:
        lane.append(hit)
        lane.sort(key=lambda item: (item.sequence, item.uid, item.chunk_id, item.message_id))
        index = next(i for i, item in enumerate(lane) if item.uid == hit.uid)

    candidates: list[tuple[NeighborUnit, str]] = []
    if preceding > 0:
        for item in reversed(lane[:index][-preceding:]):
            candidates.append((item, "preceding_message" if item.message_id or hit.message_id else "adjacent_chunk"))
    if following > 0:
        for item in lane[index + 1 : index + 1 + following]:
            candidates.append((item, "following_message" if item.message_id or hit.message_id else "adjacent_chunk"))

    seen = {_overlap_key(matched)}
    context: list[ContextUnit] = []
    skipped: list[str] = list(excluded)
    truncated = False
    truncation_reason = ""
    used_tokens = matched.token_estimate
    budget = min(max_tokens_per_hit, max_tokens_total)

    for item, reason in candidates:
        if item.uid in excluded or item.uid == hit.uid and item.chunk_id == hit.chunk_id:
            if item.uid in excluded:
                skipped.append(item.uid)
            continue
        if not unit_visible(item, access):
            skipped.append(item.uid)
            continue
        quoted_ctx, citation_ctx, stale_ctx, stale_ctx_reason = _quote_unit(
            item, archive_id=archive_id, canonical=canonical, span_required=False
        )
        unit = ContextUnit(
            role="context",
            reason=reason,
            uid=item.uid,
            chunk_id=item.chunk_id or citation_ctx.chunk_id,
            message_id=item.message_id,
            revision=item.revision,
            quoted_text=quoted_ctx,
            citation=citation_ctx,
            token_estimate=estimate_tokens(quoted_ctx or item.text),
            stale=stale_ctx,
            stale_reason=stale_ctx_reason,
        )
        key = _overlap_key(unit)
        if key in seen:
            continue
        if used_tokens + unit.token_estimate > budget:
            truncated = True
            truncation_reason = "token_budget"
            continue
        seen.add(key)
        context.append(unit)
        used_tokens += unit.token_estimate

    support = tuple(dict.fromkeys([matched.uid, *[item.uid for item in context]]))
    return ExpandedHit(
        matched=(matched,),
        context=tuple(context),
        stale=stale,
        stale_reason=stale_reason,
        truncated=truncated,
        truncation_reason=truncation_reason,
        token_estimate=used_tokens,
        support_uids=support,
        excluded_uids=tuple(dict.fromkeys(skipped)),
    )


def expand_ranked_hits(
    hits: Sequence[Mapping[str, Any]],
    units: Sequence[NeighborUnit],
    *,
    access: AccessContext | None = None,
    archive_id: str = "archive-p10b",
    preceding: int = DEFAULT_PRECEDING,
    following: int = DEFAULT_FOLLOWING,
    max_tokens_per_hit: int = DEFAULT_MAX_TOKENS_PER_HIT,
    max_tokens_total: int = DEFAULT_MAX_TOKENS_TOTAL,
    canonical: Mapping[str, tuple[str, str]] | None = None,
    span_required: bool = False,
    excluded_uids: Sequence[str] = (),
) -> list[ExpandedHit]:
    """Expand each ranked hit. Total token budget is shared across the page."""

    by_chunk = {item.chunk_id: item for item in units if item.chunk_id}
    by_uid: dict[str, NeighborUnit] = {}
    for item in units:
        by_uid.setdefault(item.uid, item)

    remaining_total = max_tokens_total
    expanded: list[ExpandedHit] = []
    for row in hits:
        uid = str(row.get("uid") or row.get("card_uid") or "").strip()
        chunk_id = str(row.get("chunk_id") or row.get("chunk_key") or "").strip()
        unit = by_chunk.get(chunk_id) if chunk_id else None
        if unit is None:
            unit = by_uid.get(uid)
        if unit is None:
            expanded.append(ExpandedHit(matched=(), context=(), excluded_uids=tuple(excluded_uids)))
            continue
        page = expand_neighbors(
            unit,
            units,
            access=access,
            archive_id=archive_id,
            preceding=preceding,
            following=following,
            max_tokens_per_hit=min(max_tokens_per_hit, remaining_total),
            max_tokens_total=remaining_total,
            canonical=canonical,
            span_required=span_required,
            excluded_uids=excluded_uids,
        )
        remaining_total = max(0, remaining_total - page.token_estimate)
        expanded.append(page)
    return expanded


def neighbor_units_from_cards(
    cards: Sequence[Mapping[str, Any]],
    *,
    archive_id: str = "archive-p10b",
    allowed_uids: Sequence[str] | None = None,
) -> list[NeighborUnit]:
    """Build ordered message units from P04-style card dicts."""

    allow = None if allowed_uids is None else set(allowed_uids)
    units: list[NeighborUnit] = []
    for card in cards:
        uid = str(card.get("uid") or card.get("card_uid") or "").strip()
        if not uid or (allow is not None and uid not in allow):
            continue
        fields = dict(card.get("fields") or {})
        body = str(card.get("body") or "")
        revision = revision_for(body)
        message_id = str(fields.get("gmail_message_id") or uid)
        thread_id = str(fields.get("gmail_thread_id") or fields.get("thread") or "")
        sent = str(fields.get("sent_at") or fields.get("created") or "")
        sources = tuple(str(item) for item in (fields.get("source") or card.get("source") or ()) if str(item).strip())
        citation = citation_for_unit(
            NeighborUnit(
                uid=uid,
                text=body,
                revision=revision,
                chunk_id=f"ck-{uid}",
                message_id=message_id,
                thread_id=thread_id,
            ),
            archive_id=archive_id,
        )
        units.append(
            NeighborUnit(
                uid=uid,
                text=body,
                revision=revision,
                chunk_id=f"ck-{uid}",
                message_id=message_id,
                sequence=_sequence_key(sent, uid),
                thread_id=thread_id,
                boundary_key=thread_id,
                allowed=str(card.get("corpus_state") or "active") != "suppressed",
                required_sources=sources,
                citation=citation,
            )
        )
    units.sort(key=lambda item: (item.thread_id, item.sequence, item.uid))
    return units


def _sequence_key(stamp: str, uid: str) -> int:
    digits = "".join(ch for ch in stamp if ch.isdigit())
    if digits:
        try:
            return int(digits[:14])
        except ValueError:
            pass
    return int(hashlib.sha256(uid.encode("utf-8")).hexdigest()[:8], 16)
