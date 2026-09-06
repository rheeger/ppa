"""P01-D acceptance: search-product checkpoint, scale blocked if unrunnable."""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

from archive_cli.serving_scale import probe_million_vector_scale
from archive_tests.acceptance.environment import imported_engine_identity
from archive_tests.acceptance.registry import Scenario, register

REPO = Path(__file__).resolve().parents[3]
FREEZE = REPO / "archive_tests/acceptance/data/p01b_format_freeze.json"
SCHEMA = REPO / "archive_crate/src/serving_index/schema.rs"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_p01_checkpoint(_runtime: object) -> dict[str, Any]:
    started = time.monotonic()
    schema = SCHEMA.read_text(encoding="utf-8")
    if "pub const SERVING_INDEX_FORMAT_VERSION: u32 = 2;" not in schema:
        raise AssertionError("serving format v2 was reopened")
    if 'pub const VECTOR_IMPL: &str = "ivf_centroids_v2";' not in schema:
        raise AssertionError("vector impl identity missing from schema.rs")

    engine = imported_engine_identity()
    scale = probe_million_vector_scale(root=REPO)
    if scale["executed"]:
        raise AssertionError("million-vector profile was executed without an explicit run gate")
    if scale["production_proven"]:
        raise AssertionError("production_proven must stay false until R0")
    if scale["status"] != "blocked":
        scale = {
            **scale,
            "status": "blocked",
            "reasons": list(scale.get("reasons") or [])
            + ["million@1536 was not executed in P01-D; scale remains unproven and is not waived"],
        }

    return {
        "id": "p01.checkpoint.search_product",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "serving_index_format_version": 2,
        "vector_impl": "ivf_centroids_v2",
        "format_freeze_sha256": _sha256(FREEZE),
        "schema_sha256": _sha256(SCHEMA),
        "imported_engine": engine,
        "scale": scale,
        "production_proven": False,
        "p02_independent": True,
        "p04_labeled_corpus": "unavailable",
        "model_quality_proof": "unavailable",
        "recall_at_10": "not_measured_this_slice",
        "recall_at_20": "not_measured_this_slice",
        "slice_shas": {
            "P01-A": "02da6d14dd3189677f026cc6b4f60519258be95a",
            "P01-B": "a014f8f068f8ce552a21bc8688cb55f6bb501f18",
            "P01-B1": "0acb8badac1f1ca980e453636fb14c0a1fdad5ad",
            "P01-B2": "8e32318351180773f00975b42a0de48f8af7631d",
            "P01-C": "2707b6d11c17da25c8b6cd2f8a01a441768e344e",
        },
    }


register(
    Scenario(
        id="p01.checkpoint.search_product",
        suite="p01",
        product_guarantee="Search-product checkpoint records measured A-C proof and blocks unrunnable scale",
        proof_tier="isolated_unit",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p01-checkpoint.json",),
        run=run_p01_checkpoint,
    )
)
