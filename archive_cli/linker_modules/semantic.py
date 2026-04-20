"""MODULE_SEMANTIC -- retired Phase 6 semantic linker registry shim.

The real Postgres-aware semantic generator remains in seed_links.py because it
takes a database connection and schema. This module only supplies the retired
framework stub plus semantic scoring.
"""

from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import (LINK_TYPE_SEMANTICALLY_RELATED,
                                    MODULE_SEMANTIC, SeedCardSketch,
                                    SeedLinkCandidate, SeedLinkCatalog,
                                    get_link_surface_policies)


def _semantic_retired_stub(
    _catalog: SeedLinkCatalog, _source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    return []


def _score_semantic_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    """Branch lifted from ``_component_scores`` for MODULE_SEMANTIC."""
    deterministic_score = 0.0
    lexical_score = 0.0
    graph_score = 0.0
    embedding_score = 0.0
    risk_penalty = 0.0
    embedding_score = float(features.get("embedding_similarity", 0.0))
    deterministic_score = 0.0
    lexical_score = 0.0
    graph_score = 0.0
    if embedding_score < 0.7:
        risk_penalty += 0.20
    return (
        max(0.0, min(deterministic_score, 1.0)),
        max(0.0, min(lexical_score, 1.0)),
        max(0.0, min(graph_score, 1.0)),
        max(0.0, min(embedding_score, 1.0)),
        max(0.0, min(risk_penalty, 0.8)),
    )



def _policies() -> tuple:
    return tuple(p for p in get_link_surface_policies() if p.module_name == MODULE_SEMANTIC)


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_SEMANTIC,
    source_card_types=(),
    emits_link_types=(LINK_TYPE_SEMANTICALLY_RELATED,),
    generator=_semantic_retired_stub,
    scoring_fn=_score_semantic_features,
    scoring_mode="semantic",
    policies=_policies(),
    requires_llm_judge=True,
    lifecycle_state="retired",
    phase_owner="phase_6",
    post_promotion_action="edges_only",
    description="Semantic kNN linker. Retired 2026-04-19; see archive_docs/runbooks/phase6-retirement-rationale.md.",
))
