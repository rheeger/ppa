"""MODULE_IDENTITY -- Phase 6.5 Step 18 file-split from seed_links.py.

Identity resolution for duplicate person cards using exact identifiers plus name/company similarity.

Behavior is byte-equivalent to the pre-Step-18 dispatch.
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (CARD_TYPE_MODULES,
                                    LINK_TYPE_POSSIBLE_SAME_PERSON,
                                    MODULE_IDENTITY, SeedCardSketch,
                                    SeedLinkCandidate, SeedLinkCatalog,
                                    _append_candidate, _clean_text,
                                    _make_evidence, _name_similarity,
                                    _normalize_alias,
                                    _person_matches_for_identifiers,
                                    _shared_people_names,
                                    get_link_surface_policies)


def _generate_identity_candidates(catalog: SeedLinkCatalog, source: SeedCardSketch) -> list[SeedLinkCandidate]:
    if source.card_type != "person":
        return []
    results: list[SeedLinkCandidate] = []
    if source.uid not in catalog.cards_by_uid:
        return results
    match_map = _person_matches_for_identifiers(
        catalog,
        emails=source.emails,
        phones=source.phones,
        handles=source.handles,
        aliases=source.aliases,
    )
    for target_uid, payload in match_map.items():
        if target_uid == source.uid or source.uid > target_uid:
            continue
        target = payload["target"]
        deterministic_hits = sorted(payload["deterministic_hits"])
        features = {
            "deterministic_hits": deterministic_hits,
            "name_similarity": round(_name_similarity(source.summary, target.summary), 4),
            "shared_company": int(
                _normalize_alias(source.frontmatter.get("company", ""))
                == _normalize_alias(target.frontmatter.get("company", ""))
                and bool(_clean_text(source.frontmatter.get("company", "")))
            ),
            "shared_people_names": _shared_people_names(source, target),
            "ambiguous_target_count": len([uid for uid in match_map if uid != source.uid]),
        }
        evidences = [
            _make_evidence("identifier_match", "frontmatter", hit, 1, 1.0, source_uid=source.uid, target_uid=target.uid)
            for hit in deterministic_hits
        ]
        if features["name_similarity"]:
            evidences.append(
                _make_evidence(
                    "lexical_overlap",
                    "frontmatter",
                    "name_similarity",
                    features["name_similarity"],
                    0.4,
                    source=source.summary,
                    target=target.summary,
                )
            )
        _append_candidate(
            results,
            module_name=MODULE_IDENTITY,
            source=source,
            target=target,
            proposed_link_type=LINK_TYPE_POSSIBLE_SAME_PERSON,
            candidate_group="identity",
            features=features,
            evidences=evidences,
        )
    return results



def _score_identity_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted from ``_component_scores`` for MODULE_IDENTITY."""
    deterministic_score = 0.0
    lexical_score = 0.0
    graph_score = 0.0
    embedding_score = 0.0
    risk_penalty = 0.0
    deterministic_score = min(
        1.0,
        (1.0 if "exact_email" in features.get("deterministic_hits", []) else 0.0)
        + (1.0 if "exact_phone" in features.get("deterministic_hits", []) else 0.0)
        + (0.9 if "exact_handle" in features.get("deterministic_hits", []) else 0.0)
        + (0.6 if "exact_alias" in features.get("deterministic_hits", []) else 0.0),
    )
    lexical_score = min(
        1.0,
        float(features.get("name_similarity", 0.0)) * 0.75
        + (0.25 if int(features.get("shared_company", 0)) else 0.0),
    )
    graph_score = min(1.0, min(int(features.get("shared_people_names", 0)), 3) * 0.2)
    if int(features.get("ambiguous_target_count", 0)) > 1:
        risk_penalty += 0.15
    if deterministic_score == 0 and lexical_score < 0.85:
        risk_penalty += 0.2
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        max(0.0, min(embedding_score, 1.0)),
        max(0.0, min(risk_penalty, 0.8)),
    )



def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_IDENTITY)


def _source_types() -> tuple[str, ...]:
    return tuple(ct for ct, mods in CARD_TYPE_MODULES.items() if MODULE_IDENTITY in mods)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_IDENTITY,
    source_card_types=_source_types(),
    emits_link_types=(LINK_TYPE_POSSIBLE_SAME_PERSON,),
    generator=_generate_identity_candidates,
    scoring_fn=_score_identity_features,
    scoring_mode="weighted",
    policies=_policies(),
    requires_llm_judge=True,
    lifecycle_state="active",
    phase_owner="phase_2.875",
    post_promotion_action="frontmatter_delta",
    description="Identity resolution -- merges duplicate persons via exact contact identifiers + name/company similarity.",
))
