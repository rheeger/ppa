"""Integrated release manifest: child finals, platform, and quality contract."""

from __future__ import annotations

from typing import Any, Mapping

CHILD_FINALS: dict[str, dict[str, str]] = {
    "p01": {
        "plan": "P01",
        "sha": "7bf33a3fec79481a81c75ae91415a60b1b1fc1cb",
        "label": "P01-D search product",
        "origin": "inherited",
    },
    "p02": {
        "plan": "P02",
        "sha": "0d221a726308c52bf3e5f543d7002804b22e8c54",
        "label": "P02-D transactional publication",
        "origin": "inherited",
    },
    "p03": {
        "plan": "P03",
        "sha": "39299aef6f5585401cd35abd06573ff43e50ade7",
        "label": "P03-D processor execution",
        "origin": "inherited",
    },
    "p04": {
        "plan": "P04",
        "sha": "70031d06ed35b7800414bb24b55166453c34ac48",
        "label": "P04-C composed faults",
        "origin": "inherited",
    },
    "p05": {
        "plan": "P05",
        "sha": "e51eb5ab4ca8065efbbec4f0b1787be4d8325ae5",
        "label": "P05-C privacy/egress",
        "origin": "inherited",
    },
    "p06": {
        "plan": "P06",
        "sha": "9e9edd072e5397a8ccab8038b60198715b8f2593",
        "label": "P06-D engine convergence",
        "origin": "inherited",
    },
    "p07": {
        "plan": "P07",
        "sha": "6c20aec5b8157d0dcc3094634ea5607315412ebf",
        "label": "P07-D encrypted restore",
        "origin": "inherited",
    },
    "p08": {
        "plan": "P08",
        "sha": "34377be98638499c8c86c47ccf9fd1244fc18731",
        "label": "P08-D contributor connectors",
        "origin": "inherited",
    },
    "p09": {
        "plan": "P09",
        "sha": "663cb2e5bd9606afd706457f19c8fa6c7f4f117c",
        "label": "P09-D independent instances",
        "origin": "inherited",
    },
    "p10": {
        "plan": "P10",
        "sha": "4885ac7e37e5a7b0745167a609f122ce9e3a828d",
        "label": "P10-D evidence/analytics clients",
        "origin": "inherited",
    },
}

REQUIRED_SUITES = tuple(CHILD_FINALS)
QUALITY_VERDICTS = frozenset({"accept_defaults", "retain_baseline_for_optional_feature", "needs_revision"})
RELATION_CASE_IDS = (
    "rel-p04b-same-trip",
    "rel-p04b-same-charge",
    "rel-p04b-not-the-same-person",
    "rel-p04b-reply-is-authorization",
    "rel-p04b-renewal-is-not-current",
)
FORBIDDEN_INFERENCE_KEYS = frozenset({"authorized", "authorized=true", "currently_subscribed"})

PLATFORM_MATRIX = {
    "macos_arm64_cpython_3_12": {
        "status": "proven",
        "source": "inherited",
        "evidence": "P09-D / P09-C packaged install",
    },
    "linux_x86_64": {
        "status": "unproven",
        "source": "inherited",
        "evidence": "P09 did not prove this target; do not invent support",
    },
}

P01_DEFAULTS = {
    "serving_index_format_version": 2,
    "vector_impl": "ivf_centroids_v2",
    "nprobe": 32,
    "fusion": "rrf_k60",
    "rare_token_weight": 0.0,
    "model_rerank": "disabled",
}


def missing_child_suites(suite_counts: Mapping[str, int]) -> list[str]:
    return [suite for suite in REQUIRED_SUITES if int(suite_counts.get(suite) or 0) <= 0]


def quality_payload(
    *,
    verdict: str,
    reasons: list[str],
    held_out: Mapping[str, Any],
    optional_features: list[Mapping[str, Any]],
    material_regressions: list[str],
) -> dict[str, Any]:
    if verdict not in QUALITY_VERDICTS:
        raise ValueError(f"unknown quality verdict {verdict!r}")
    recommend = verdict != "needs_revision" and not material_regressions
    return {
        "verdict": verdict,
        "reasons": list(reasons),
        "held_out": dict(held_out),
        "optional_features": list(optional_features),
        "material_regressions": list(material_regressions),
        "model_rerank": "disabled",
        "recall_at_10_20_gate": "measure_then_propose",
        "release_recommendation": recommend,
        "release_recommendation_scope": "implementation_complete_for_R0_review" if recommend else "none",
        "production_rollout": False,
    }
