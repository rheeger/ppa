"""MODULE_MEDIA — Phase 6.5 Step 18.1 file-split from seed_links.py.

Links a media_asset card to (a) PersonCards via alias / EXIF labels and
(b) calendar_event hubs via same-day + location overlap. Originally lived in
``archive_cli/seed_links.py`` as ``_generate_media_candidates`` plus a branch
in ``_component_scores``; moved here verbatim so each linker is one file.

Behavior is byte-equivalent to the pre-Step-18 dispatch (gated by the
existing ``test_phase_6_5_wiring.py`` + ``test_phase_6_5_linkers.py``).
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (
    CARD_TYPE_MODULES,
    LINK_TYPE_MEDIA_HAS_EVENT,
    LINK_TYPE_MEDIA_HAS_PERSON,
    MODULE_MEDIA,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _append_candidate,
    _clean_text,
    _day_key,
    _event_matches_for_source,
    _iter_string_values,
    _make_evidence,
    _name_similarity,
    _normalize_alias,
    _person_matches_for_identifiers,
    get_link_surface_policies,
)


def _generate_media_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    if source.card_type != "media_asset":
        return []
    results: list[SeedLinkCandidate] = []
    alias_matches = _person_matches_for_identifiers(catalog, aliases=source.person_labels)
    for target_uid, payload in alias_matches.items():
        target = payload["target"]
        deterministic_hits = sorted(payload["deterministic_hits"])
        features = {
            "deterministic_hits": deterministic_hits,
            "same_day_event_cluster": 0,
            "location_overlap": 0,
            "ambiguous_target_count": len(alias_matches),
        }
        evidences = [
            _make_evidence("label_match", "frontmatter", hit, 1, 0.8, source_uid=source.uid, target_uid=target.uid)
            for hit in deterministic_hits
        ]
        _append_candidate(
            results,
            module_name=MODULE_MEDIA,
            source=source,
            target=target,
            proposed_link_type=LINK_TYPE_MEDIA_HAS_PERSON,
            candidate_group="media_person",
            features=features,
            evidences=evidences,
        )
    for event in _event_matches_for_source(catalog, source):
        location_overlap = int(bool(source.locations & event.locations)) if source.locations and event.locations else 0
        same_day_event_cluster = int(
            _day_key(_clean_text(source.frontmatter.get("captured_at", "")))
            == _day_key(_clean_text(event.frontmatter.get("start_at", "")))
        )
        title_similarity = round(_name_similarity(source.summary, event.summary), 4)
        features = {
            "exact_event_id": int(bool(source.event_hints & event.external_ids.get("calendar", set()))),
            "same_day_event_cluster": same_day_event_cluster,
            "location_overlap": location_overlap,
            "title_similarity": title_similarity,
            "participant_overlap": len(
                source.person_labels
                & {_normalize_alias(item) for item in _iter_string_values(event.frontmatter.get("people", []))}
            ),
        }
        evidences = []
        if same_day_event_cluster:
            evidences.append(
                _make_evidence("time_window", "frontmatter", "same_day_event_cluster", 1, 0.45, target_uid=event.uid)
            )
        if location_overlap:
            evidences.append(
                _make_evidence("location_overlap", "frontmatter", "location_overlap", 1, 0.35, target_uid=event.uid)
            )
        if title_similarity:
            evidences.append(
                _make_evidence(
                    "lexical_overlap", "frontmatter", "title_similarity", title_similarity, 0.2, target_uid=event.uid
                )
            )
        _append_candidate(
            results,
            module_name=MODULE_MEDIA,
            source=source,
            target=event,
            proposed_link_type=LINK_TYPE_MEDIA_HAS_EVENT,
            candidate_group="media_event",
            features=features,
            evidences=evidences,
        )
    return results


def _score_media_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted verbatim from ``_component_scores`` for MODULE_MEDIA."""
    deterministic_hits = len(features.get("deterministic_hits", []))
    deterministic_score = min(1.0, 0.9 if deterministic_hits else 0.0)
    lexical_score = min(
        1.0,
        float(features.get("title_similarity", 0.0)) * 0.45
        + (0.35 if int(features.get("location_overlap", 0)) else 0.0),
    )
    graph_score = min(
        1.0,
        (0.4 if int(features.get("same_day_event_cluster", 0)) else 0.0)
        + min(int(features.get("participant_overlap", 0)), 3) * 0.18,
    )
    risk_penalty = 0.18
    if int(features.get("ambiguous_target_count", 0)) > 1:
        risk_penalty += 0.12
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        0.0,
        max(0.0, min(risk_penalty, 0.8)),
    )


def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_MEDIA)


def _source_types() -> tuple[str, ...]:
    return tuple(ct for ct, mods in CARD_TYPE_MODULES.items() if MODULE_MEDIA in mods)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_MEDIA,
    source_card_types=_source_types(),
    emits_link_types=(LINK_TYPE_MEDIA_HAS_PERSON, LINK_TYPE_MEDIA_HAS_EVENT),
    generator=_generate_media_candidates,
    scoring_fn=_score_media_features,
    scoring_mode="weighted",
    policies=_policies(),
    requires_llm_judge=True,
    lifecycle_state="active",
    phase_owner="phase_2.875",
    post_promotion_action="edges_only",
    description=(
        "Media asset ↔ person / event linkage from EXIF labels + same-day + location overlap."
    ),
))
