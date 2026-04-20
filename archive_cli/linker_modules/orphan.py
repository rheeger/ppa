"""MODULE_ORPHAN — Phase 6.5 Step 18.2 file-split from seed_links.py.

Repairs orphan wikilinks by mapping ``[[some-slug]]`` references that don't
resolve to a card to the closest existing card via normalized slug match.
Originally lived in ``archive_cli/seed_links.py`` as
``_generate_orphan_candidates`` plus a branch in ``_component_scores``.
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (
    CARD_TYPE_MODULES,
    LINK_TYPE_ORPHAN_REPAIR_EXACT,
    LINK_TYPE_ORPHAN_REPAIR_FUZZY,
    MODULE_ORPHAN,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _append_candidate,
    _make_evidence,
    _normalize_slug,
    _orphan_reference_slugs,
    get_link_surface_policies,
)


def _generate_orphan_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    results: list[SeedLinkCandidate] = []
    candidate_pairs: list[tuple[str, SeedCardSketch]] = []
    for field_name, raw_slug in _orphan_reference_slugs(source, catalog):
        normalized = _normalize_slug(raw_slug)
        target = catalog.cards_by_slug.get(normalized)
        if target is not None:
            candidate_pairs.append((field_name, target))
    seen: set[tuple[str, str]] = set()
    for field_name, target in candidate_pairs:
        key = (field_name, target.uid)
        if key in seen:
            continue
        seen.add(key)
        exact = 1
        features = {
            "exact_slug_match": exact,
            "target_exists": 1,
            "ambiguous_target_count": 1,
        }
        evidences = [
            _make_evidence(
                "orphan_reference",
                field_name,
                "normalized_slug",
                target.slug,
                1.0,
                source_uid=source.uid,
                target_uid=target.uid,
            )
        ]
        _append_candidate(
            results,
            module_name=MODULE_ORPHAN,
            source=source,
            target=target,
            proposed_link_type=LINK_TYPE_ORPHAN_REPAIR_EXACT if exact else LINK_TYPE_ORPHAN_REPAIR_FUZZY,
            candidate_group="orphan_repair",
            features=features,
            evidences=evidences,
        )
    return results


def _score_orphan_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted verbatim from ``_component_scores`` for MODULE_ORPHAN."""
    deterministic_score = (
        1.0 if int(features.get("target_exists", 0)) and int(features.get("exact_slug_match", 0)) else 0.0
    )
    lexical_score = 0.65 if int(features.get("target_exists", 0)) else 0.0
    graph_score = 0.4 if int(features.get("target_exists", 0)) else 0.0
    risk_penalty = 0.0
    if not deterministic_score:
        risk_penalty += 0.25
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        0.0,
        max(0.0, min(risk_penalty, 0.8)),
    )


def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_ORPHAN)


def _source_types() -> tuple[str, ...]:
    return tuple(ct for ct, mods in CARD_TYPE_MODULES.items() if MODULE_ORPHAN in mods)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_ORPHAN,
    source_card_types=_source_types(),
    emits_link_types=(LINK_TYPE_ORPHAN_REPAIR_EXACT, LINK_TYPE_ORPHAN_REPAIR_FUZZY),
    generator=_generate_orphan_candidates,
    scoring_fn=_score_orphan_features,
    scoring_mode="weighted",
    policies=_policies(),
    requires_llm_judge=True,
    lifecycle_state="active",
    phase_owner="phase_2.875",
    post_promotion_action="frontmatter_delta",
    description="Orphan-wikilink repair via normalized-slug or fuzzy match against existing cards.",
))
