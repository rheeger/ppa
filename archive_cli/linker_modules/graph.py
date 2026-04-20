"""MODULE_GRAPH -- Phase 6.5 Step 18 file-split from seed_links.py.

Graph-consistency repair for missing reverse edges on thread and event hubs from exact IDs.

Behavior is byte-equivalent to the pre-Step-18 dispatch.
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (CARD_TYPE_MODULES,
                                    LINK_TYPE_EVENT_HAS_MESSAGE,
                                    LINK_TYPE_EVENT_HAS_THREAD,
                                    LINK_TYPE_EVENT_HAS_TRANSCRIPT,
                                    LINK_TYPE_THREAD_HAS_ATTACHMENT,
                                    LINK_TYPE_THREAD_HAS_MESSAGE, MODULE_GRAPH,
                                    SeedCardSketch, SeedLinkCandidate,
                                    SeedLinkCatalog, _append_candidate,
                                    _clean_text, _iter_string_values,
                                    _make_evidence, _normalize_slug,
                                    _slug_from_ref, get_link_surface_policies)


def _generate_graph_consistency_candidates(catalog: SeedLinkCatalog, source: SeedCardSketch) -> list[SeedLinkCandidate]:
    results: list[SeedLinkCandidate] = []
    if source.card_type == "email_thread":
        thread_id = _clean_text(source.frontmatter.get("gmail_thread_id", ""))
        existing = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("messages", []))
        }
        for message in catalog.email_messages_by_thread_id.get(thread_id, []):
            if _normalize_slug(message.slug) in existing:
                continue
            evidences = [
                _make_evidence(
                    "graph_closure",
                    "index",
                    "missing_reverse_edge",
                    message.slug,
                    1.0,
                    source_uid=source.uid,
                    target_uid=message.uid,
                )
            ]
            _append_candidate(
                results,
                module_name=MODULE_GRAPH,
                source=source,
                target=message,
                proposed_link_type=LINK_TYPE_THREAD_HAS_MESSAGE,
                candidate_group="graph_closure",
                features={"reverse_edge_missing": 1, "exact_thread_id": 1},
                evidences=evidences,
            )
    if source.card_type == "calendar_event":
        existing_messages = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("source_messages", []))
        }
        existing_threads = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("source_threads", []))
        }
        existing_transcripts = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("meeting_transcripts", []))
        }
        for message in catalog.cards_by_type.get("email_message", []):
            if (
                set(message.event_hints) & source.external_ids.get("calendar", set())
                and _normalize_slug(message.slug) not in existing_messages
            ):
                evidences = [
                    _make_evidence(
                        "graph_closure",
                        "index",
                        "missing_reverse_edge",
                        message.slug,
                        1.0,
                        source_uid=source.uid,
                        target_uid=message.uid,
                    )
                ]
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=source,
                    target=message,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_MESSAGE,
                    candidate_group="graph_closure",
                    features={"reverse_edge_missing": 1, "exact_event_id": 1},
                    evidences=evidences,
                )
        for thread in catalog.cards_by_type.get("email_thread", []):
            if (
                set(thread.event_hints) & source.external_ids.get("calendar", set())
                and _normalize_slug(thread.slug) not in existing_threads
            ):
                evidences = [
                    _make_evidence(
                        "graph_closure",
                        "index",
                        "missing_reverse_edge",
                        thread.slug,
                        1.0,
                        source_uid=source.uid,
                        target_uid=thread.uid,
                    )
                ]
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=source,
                    target=thread,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_THREAD,
                    candidate_group="graph_closure",
                    features={"reverse_edge_missing": 1, "exact_event_id": 1},
                    evidences=evidences,
                )
        for transcript in catalog.cards_by_type.get("meeting_transcript", []):
            if (
                set(transcript.event_hints) & source.external_ids.get("calendar", set())
                and _normalize_slug(transcript.slug) not in existing_transcripts
            ):
                evidences = [
                    _make_evidence(
                        "graph_closure",
                        "index",
                        "missing_reverse_edge",
                        transcript.slug,
                        1.0,
                        source_uid=source.uid,
                        target_uid=transcript.uid,
                    )
                ]
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=source,
                    target=transcript,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_TRANSCRIPT,
                    candidate_group="graph_closure",
                    features={"reverse_edge_missing": 1, "exact_event_id": 1},
                    evidences=evidences,
                )
    return results



def _score_graph_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted from ``_component_scores`` for MODULE_GRAPH."""
    deterministic_score = 0.0
    lexical_score = 0.0
    graph_score = 0.0
    embedding_score = 0.0
    risk_penalty = 0.0
    deterministic_score = min(
        1.0,
        (1.0 if int(features.get("reverse_edge_missing", 0)) else 0.0)
        + (1.0 if int(features.get("exact_thread_id", 0)) else 0.0)
        + (1.0 if int(features.get("exact_event_id", 0)) else 0.0),
    )
    lexical_score = 0.0
    graph_score = 0.9 if int(features.get("reverse_edge_missing", 0)) else 0.65
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        max(0.0, min(embedding_score, 1.0)),
        max(0.0, min(risk_penalty, 0.8)),
    )



def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_GRAPH)


def _source_types() -> tuple[str, ...]:
    return tuple(ct for ct, mods in CARD_TYPE_MODULES.items() if MODULE_GRAPH in mods)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_GRAPH,
    source_card_types=_source_types(),
    emits_link_types=(
        LINK_TYPE_THREAD_HAS_MESSAGE,
        LINK_TYPE_THREAD_HAS_ATTACHMENT,
        LINK_TYPE_EVENT_HAS_MESSAGE,
        LINK_TYPE_EVENT_HAS_THREAD,
        LINK_TYPE_EVENT_HAS_TRANSCRIPT,
    ),
    generator=_generate_graph_consistency_candidates,
    scoring_fn=_score_graph_features,
    scoring_mode="weighted",
    policies=_policies(),
    requires_llm_judge=False,
    lifecycle_state="active",
    phase_owner="phase_2.875",
    post_promotion_action="edges_only",
    description="Graph-consistency repair: missing reverse edges on thread/event hubs from exact IDs.",
))
