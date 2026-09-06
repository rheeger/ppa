"""P10-B: matched vs context labels, revision-checked spans, lost-in-thread."""

from __future__ import annotations

import pytest

from archive_cli.card_traversal import assert_compact_payload, attach_context_labels, compact_hits
from archive_engine.context import (
    CONTEXT_CONTRACT_VERSION,
    NeighborUnit,
    expand_neighbors,
    neighbor_units_from_cards,
    quote_canonical_span,
    revision_for,
    source_span_for,
)
from archive_engine.contracts import (
    CHUNK_EVIDENCE_REF_VERSION,
    AccessContext,
    ChunkEvidenceRef,
    MessageEvidenceRef,
)
from archive_engine.errors import SpanRequiredError, StaleContextError
from archive_tests.acceptance.corpus import build_cards, build_queries, build_relations

REPLY = "hfa-email-message-p04breply01"
REQUEST = "hfa-email-message-p04breq0001"
STALE = "hfa-email-message-p04bstale01"
THREAD = "thread-p04b-auth"
ARCHIVE = "archive-p10b"


def _access(**kwargs) -> AccessContext:
    return AccessContext(
        archive_id=ARCHIVE,
        principal="local-operator",
        profile="trusted-local",
        **kwargs,
    )


def _auth_units() -> list[NeighborUnit]:
    cards = [card for card in build_cards() if card["uid"] in {REPLY, REQUEST, STALE}]
    return neighbor_units_from_cards(cards, archive_id=ARCHIVE)


def _by_uid(units: list[NeighborUnit]) -> dict[str, NeighborUnit]:
    return {item.uid: item for item in units}


def test_lost_in_thread_labels_request_as_context() -> None:
    units = _auth_units()
    hit = _by_uid(units)[REPLY]
    alone = hit.text
    assert "book the loft for April" not in alone
    expanded = expand_neighbors(hit, units, excluded_uids=(STALE,))
    assert [item.uid for item in expanded.matched] == [REPLY]
    assert expanded.matched[0].reason == "ranked_hit"
    assert [item.uid for item in expanded.context] == [REQUEST]
    assert expanded.context[0].reason == "preceding_message"
    assert expanded.context[0].role == "context"
    assert "Can you book the loft for April 11-14?" in expanded.context[0].quoted_text
    assert "P04B-TOKEN-AUTH-REQUEST" in expanded.context[0].quoted_text
    assert "P04B-TOKEN-AUTH-REPLY" in expanded.matched[0].quoted_text
    assert STALE not in expanded.support_uids
    assert "authorized" not in expanded.to_payload()


def test_authorization_support_set_spans_and_revisions() -> None:
    units = _auth_units()
    query = next(item for item in build_queries() if item["query_id"] == "q-p04b-lex-auth-reply")
    relation = next(item for item in build_relations() if item["case_id"] == "rel-p04b-reply-is-authorization")
    expanded = expand_neighbors(_by_uid(units)[REPLY], units, excluded_uids=query["excluded_uids"])
    support = set(expanded.support_uids)
    assert support == {REPLY, REQUEST}
    assert set(query["expected_uids"]) == support
    assert STALE in relation["negative_evidence_ids"]
    assert STALE in expanded.excluded_uids
    for unit in (*expanded.matched, *expanded.context):
        assert unit.citation.span_unavailable is False
        span = unit.citation.source_spans[0]
        assert span.start_byte >= 0
        assert span.end_byte > span.start_byte
        assert unit.revision.startswith("sha256:")
        assert span.source_revision == unit.revision
        quoted = quote_canonical_span(
            unit.quoted_text if unit.uid == REPLY else _by_uid(units)[unit.uid].text,
            span,
            actual_revision=unit.revision,
        )
        assert quoted == unit.quoted_text
    payload = expanded.to_payload()
    assert "authorized" not in payload
    assert payload["contract_version"] == CONTEXT_CONTRACT_VERSION


def test_stale_offsets_never_quote_mismatched_text() -> None:
    body = "Yes, book it, I will cover it. P04B-TOKEN-AUTH-REPLY"
    revision = revision_for(body)
    span = source_span_for(body, source_uid=REPLY, source_revision=revision, source_message_id="msg-p04b-reply")
    stale_body = "Edited later to 'never mind'. Stale authorization."
    with pytest.raises(StaleContextError, match="stale-context"):
        quote_canonical_span(stale_body, span, actual_revision=revision_for(stale_body))
    citation = ChunkEvidenceRef(
        version=CHUNK_EVIDENCE_REF_VERSION,
        archive_id=ARCHIVE,
        card_uid=REPLY,
        chunk_id="ck-reply",
        chunk_schema_version="6",
        algorithm_version=CONTEXT_CONTRACT_VERSION,
        evidence_kind="source_reported",
        lineage_complete=True,
        source_revisions=(revision,),
        source_spans=(span,),
        span_unavailable=False,
        message_refs=(MessageEvidenceRef(message_id="msg-p04b-reply", source_revision=revision, spans=(span,)),),
        message_refs_available=True,
    )
    hit = NeighborUnit(uid=REPLY, text=body, revision=revision, chunk_id="ck-reply", citation=citation)
    expanded = expand_neighbors(hit, [hit], canonical={REPLY: (stale_body, revision_for(stale_body))})
    assert expanded.stale is True
    assert expanded.stale_reason == "stale-context/refresh-required"
    assert expanded.matched[0].quoted_text == ""
    assert "never mind" not in expanded.matched[0].quoted_text


def test_span_required_fails_when_unavailable() -> None:
    hit = NeighborUnit(
        uid=REPLY,
        text="Yes, book it",
        revision=revision_for("Yes, book it"),
        chunk_id="ck-reply",
        span_unavailable=True,
    )
    with pytest.raises(SpanRequiredError, match="span_unavailable"):
        expand_neighbors(hit, [hit], span_required=True)


def test_denied_and_mixed_source_neighbors_absent() -> None:
    request = NeighborUnit(
        uid=REQUEST,
        text="Can you book the loft?",
        revision=revision_for("Can you book the loft?"),
        chunk_id="ck-req",
        message_id="m-req",
        sequence=1,
        thread_id=THREAD,
        required_sources=("gmail:personal",),
    )
    reply = NeighborUnit(
        uid=REPLY,
        text="Yes, book it",
        revision=revision_for("Yes, book it"),
        chunk_id="ck-reply",
        message_id="m-reply",
        sequence=2,
        thread_id=THREAD,
        required_sources=("gmail:personal",),
    )
    private = NeighborUnit(
        uid="hfa-email-message-private",
        text="private adjacent",
        revision=revision_for("private adjacent"),
        chunk_id="ck-priv",
        message_id="m-priv",
        sequence=3,
        thread_id=THREAD,
        required_sources=("gmail:other",),
    )
    mixed = NeighborUnit(
        uid="hfa-email-message-mixed",
        text="derived mixed",
        revision=revision_for("derived mixed"),
        chunk_id="ck-mix",
        message_id="m-mix",
        sequence=0,
        thread_id=THREAD,
        required_sources=("gmail:personal", "finance:bank"),
    )
    access = _access(allowed_sources=("gmail:personal",))
    expanded = expand_neighbors(
        reply,
        [mixed, request, reply, private],
        access=access,
        preceding=2,
        following=1,
    )
    assert [item.uid for item in expanded.context] == [REQUEST]
    assert private.uid not in expanded.support_uids
    assert mixed.uid not in expanded.support_uids
    assert private.uid in expanded.excluded_uids
    assert mixed.uid in expanded.excluded_uids


def test_overlap_dedupe_and_token_budget_keep_citations() -> None:
    long = "word " * 80
    hit = NeighborUnit(
        uid="hfa-note-hit",
        text="short hit text",
        revision=revision_for("short hit text"),
        chunk_id="ck-0",
        sequence=0,
        thread_id="note-1",
        boundary_key="body",
    )
    overlap = NeighborUnit(
        uid="hfa-note-hit",
        text="short hit text",
        revision=hit.revision,
        chunk_id="ck-0",
        sequence=0,
        thread_id="note-1",
        boundary_key="body",
    )
    nxt = NeighborUnit(
        uid="hfa-note-next",
        text=long,
        revision=revision_for(long),
        chunk_id="ck-1",
        sequence=1,
        thread_id="note-1",
        boundary_key="body",
    )
    expanded = expand_neighbors(
        hit, [hit, overlap, nxt], preceding=0, following=2, max_tokens_per_hit=10, max_tokens_total=10
    )
    assert [item.uid for item in expanded.matched] == ["hfa-note-hit"]
    assert expanded.matched[0].citation.card_uid == "hfa-note-hit"
    assert expanded.matched[0].citation.source_spans
    assert expanded.truncated is True
    assert expanded.truncation_reason == "token_budget"
    assert expanded.context == ()


def test_compact_labels_do_not_leak_bodies() -> None:
    units = _auth_units()
    expanded = expand_neighbors(_by_uid(units)[REPLY], units, excluded_uids=(STALE,))
    rows = [
        {
            "card_uid": REPLY,
            "rel_path": "Email/p04-loft-reply.md",
            "summary": "Yes, book it",
            "type": "email_message",
            "activity_at": "2026-04-08T15:11:00Z",
        }
    ]
    hits = compact_hits(rows)
    labeled = attach_context_labels(hits[0], expanded)
    payload = {"hits": [labeled]}
    assert_compact_payload(payload)
    dumped = str(payload)
    assert "P04B-TOKEN-AUTH-REPLY" not in dumped
    assert labeled["matched_uids"] == [REPLY]
    assert labeled["context_uids"] == [REQUEST]
    assert labeled["citations"][0]["revision"].startswith("sha256:")
