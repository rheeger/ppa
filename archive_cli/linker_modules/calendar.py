"""MODULE_CALENDAR -- Phase 6.5 Step 18 file-split from seed_links.py.

Thread/message/transcript to calendar event linkage from invite IDs, iCal UIDs, and title/time heuristics.

Behavior is byte-equivalent to the pre-Step-18 dispatch.
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (CARD_TYPE_MODULES,
                                    LINK_TYPE_EVENT_HAS_MESSAGE,
                                    LINK_TYPE_EVENT_HAS_PERSON,
                                    LINK_TYPE_EVENT_HAS_THREAD,
                                    LINK_TYPE_EVENT_HAS_TRANSCRIPT,
                                    LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT,
                                    LINK_TYPE_THREAD_HAS_CALENDAR_EVENT,
                                    LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,
                                    MODULE_CALENDAR, MODULE_GRAPH,
                                    SeedCardSketch, SeedLinkCandidate,
                                    SeedLinkCatalog, _append_candidate,
                                    _clean_text, _event_matches_for_source,
                                    _generate_person_link_candidates,
                                    _iter_string_values, _make_evidence,
                                    _name_similarity, _normalize_email,
                                    _normalize_slug, _slug_from_ref,
                                    get_link_surface_policies)


def _generate_calendar_candidates(catalog: SeedLinkCatalog, source: SeedCardSketch) -> list[SeedLinkCandidate]:
    results: list[SeedLinkCandidate] = []
    if source.card_type in {"email_message", "email_thread", "meeting_transcript"}:
        existing_event_slugs = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("calendar_events", []))
        }
        for event in _event_matches_for_source(catalog, source):
            title_similarity = _name_similarity(
                _clean_text(source.frontmatter.get("invite_title", ""))
                or _clean_text(source.frontmatter.get("subject", ""))
                or _clean_text(source.frontmatter.get("title", ""))
                or source.summary,
                _clean_text(event.frontmatter.get("title", "")) or event.summary,
            )
            exact_event_id = int(bool(set(source.event_hints) & set(event.external_ids.get("calendar", set()))))
            if source.card_type == "email_message":
                reverse_field = "source_messages"
            elif source.card_type == "email_thread":
                reverse_field = "source_threads"
            else:
                reverse_field = "meeting_transcripts"
            reverse_refs = {
                _normalize_slug(_slug_from_ref(item))
                for item in _iter_string_values(event.frontmatter.get(reverse_field, []))
            }
            features = {
                "exact_event_id": exact_event_id,
                "reverse_reference_present": int(_normalize_slug(source.slug) in reverse_refs),
                "title_similarity": round(title_similarity, 4),
                "participant_overlap": len(source.participant_emails & event.emails),
            }
            evidences = []
            if exact_event_id:
                evidences.append(
                    _make_evidence(
                        "exact_event_hint",
                        "frontmatter",
                        "event_hint",
                        sorted(source.event_hints & event.external_ids.get("calendar", set()))[0],
                        1.0,
                    )
                )
            if features["reverse_reference_present"]:
                evidences.append(
                    _make_evidence(
                        "reverse_reference",
                        "frontmatter",
                        reverse_field,
                        source.slug,
                        0.95,
                        source_uid=event.uid,
                        target_uid=source.uid,
                    )
                )
            if title_similarity:
                evidences.append(
                    _make_evidence(
                        "lexical_overlap",
                        "frontmatter",
                        "title_similarity",
                        title_similarity,
                        0.25,
                        source=source.summary,
                        target=event.summary,
                    )
                )
            if _normalize_slug(event.slug) not in existing_event_slugs:
                _append_candidate(
                    results,
                    module_name=MODULE_CALENDAR,
                    source=source,
                    target=event,
                    proposed_link_type=(
                        LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT
                        if source.card_type == "email_message"
                        else LINK_TYPE_THREAD_HAS_CALENDAR_EVENT
                        if source.card_type == "email_thread"
                        else LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT
                    ),
                    candidate_group="event_association",
                    features=features,
                    evidences=evidences,
                )
            if source.card_type == "email_message" and not features["reverse_reference_present"]:
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=event,
                    target=source,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_MESSAGE,
                    candidate_group="event_association",
                    features=features,
                    evidences=evidences,
                )
            if source.card_type == "email_thread" and not features["reverse_reference_present"]:
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=event,
                    target=source,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_THREAD,
                    candidate_group="event_association",
                    features=features,
                    evidences=evidences,
                )
            if source.card_type == "meeting_transcript" and not features["reverse_reference_present"]:
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=event,
                    target=source,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_TRANSCRIPT,
                    candidate_group="event_association",
                    features=features,
                    evidences=evidences,
                )
    elif source.card_type == "calendar_event":
        reverse_messages = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("source_messages", []))
        }
        reverse_threads = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("source_threads", []))
        }
        reverse_transcripts = {
            _normalize_slug(_slug_from_ref(item))
            for item in _iter_string_values(source.frontmatter.get("meeting_transcripts", []))
        }
        for message in catalog.cards_by_type.get("email_message", []):
            event_hints = set(message.event_hints)
            exact_event_id = int(bool(event_hints & source.external_ids.get("calendar", set())))
            if exact_event_id and _normalize_slug(message.slug) not in reverse_messages:
                evidences = [
                    _make_evidence(
                        "exact_event_hint",
                        "frontmatter",
                        "event_hint",
                        sorted(event_hints & source.external_ids.get("calendar", set()))[0],
                        1.0,
                    )
                ]
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=source,
                    target=message,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_MESSAGE,
                    candidate_group="event_association",
                    features={
                        "exact_event_id": 1,
                        "participant_overlap": len(message.participant_emails & source.emails),
                    },
                    evidences=evidences,
                )
        for thread in catalog.cards_by_type.get("email_thread", []):
            event_hints = set(thread.event_hints)
            exact_event_id = int(bool(event_hints & source.external_ids.get("calendar", set())))
            if exact_event_id and _normalize_slug(thread.slug) not in reverse_threads:
                evidences = [
                    _make_evidence(
                        "exact_event_hint",
                        "frontmatter",
                        "event_hint",
                        sorted(event_hints & source.external_ids.get("calendar", set()))[0],
                        1.0,
                    )
                ]
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=source,
                    target=thread,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_THREAD,
                    candidate_group="event_association",
                    features={
                        "exact_event_id": 1,
                        "participant_overlap": len(thread.participant_emails & source.emails),
                    },
                    evidences=evidences,
                )
        for transcript in catalog.cards_by_type.get("meeting_transcript", []):
            event_hints = set(transcript.event_hints)
            exact_event_id = int(bool(event_hints & source.external_ids.get("calendar", set())))
            if exact_event_id and _normalize_slug(transcript.slug) not in reverse_transcripts:
                evidences = [
                    _make_evidence(
                        "exact_event_hint",
                        "frontmatter",
                        "event_hint",
                        sorted(event_hints & source.external_ids.get("calendar", set()))[0],
                        1.0,
                    )
                ]
                _append_candidate(
                    results,
                    module_name=MODULE_GRAPH,
                    source=source,
                    target=transcript,
                    proposed_link_type=LINK_TYPE_EVENT_HAS_TRANSCRIPT,
                    candidate_group="event_association",
                    features={
                        "exact_event_id": 1,
                        "participant_overlap": len(transcript.participant_emails & source.emails),
                    },
                    evidences=evidences,
                )
        results.extend(
            _generate_person_link_candidates(
                catalog,
                source=source,
                emails=source.participant_emails
                | (
                    {_normalize_email(source.frontmatter.get("organizer_email", ""))}
                    if _normalize_email(source.frontmatter.get("organizer_email", ""))
                    else set()
                ),
                handles=set(),
                link_type=LINK_TYPE_EVENT_HAS_PERSON,
                candidate_group="participant_resolution",
            )
        )
    return results



def _score_calendar_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted from ``_component_scores`` for MODULE_CALENDAR."""
    deterministic_score = 0.0
    lexical_score = 0.0
    graph_score = 0.0
    embedding_score = 0.0
    risk_penalty = 0.0
    deterministic_score = min(
        1.0,
        (1.0 if int(features.get("exact_event_id", 0)) else 0.0)
        + (0.8 if int(features.get("reverse_reference_present", 0)) else 0.0),
    )
    lexical_score = min(
        1.0,
        float(features.get("title_similarity", 0.0)) * 0.7
        + min(int(features.get("participant_overlap", 0)), 3) * 0.08,
    )
    graph_score = (
        0.85
        if int(features.get("reverse_reference_present", 0))
        else min(int(features.get("participant_overlap", 0)), 4) * 0.12
    )
    if deterministic_score < 1.0 and lexical_score < 0.55:
        risk_penalty += 0.15
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        max(0.0, min(embedding_score, 1.0)),
        max(0.0, min(risk_penalty, 0.8)),
    )



def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_CALENDAR)


def _source_types() -> tuple[str, ...]:
    return tuple(ct for ct, mods in CARD_TYPE_MODULES.items() if MODULE_CALENDAR in mods)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_CALENDAR,
    source_card_types=_source_types(),
    emits_link_types=(
        LINK_TYPE_MESSAGE_HAS_CALENDAR_EVENT,
        LINK_TYPE_THREAD_HAS_CALENDAR_EVENT,
        LINK_TYPE_TRANSCRIPT_HAS_CALENDAR_EVENT,
        LINK_TYPE_EVENT_HAS_PERSON,
    ),
    generator=_generate_calendar_candidates,
    scoring_fn=_score_calendar_features,
    scoring_mode="weighted",
    policies=_policies(),
    requires_llm_judge=True,
    lifecycle_state="active",
    phase_owner="phase_2.875",
    post_promotion_action="edges_only",
    description="Thread/message/transcript ↔ calendar_event linkage from invite IDs + ical_uids + title/time heuristics.",
))
