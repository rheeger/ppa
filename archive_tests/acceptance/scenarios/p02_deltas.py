"""P02-B acceptance: incremental and full publish share one live universe."""

from __future__ import annotations

import time
from typing import Any

from archive_cli.serving_index import get_serving_handle
from archive_engine.publication import diff_universes, publish_snapshot, resolve_live_universe
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_publication_equivalence import (
    _base_state,
    _delta_from_base,
    _mutated_full,
    _publish_full,
    _snapshot,
    _vec,
)


def run_p02_deltas(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    reset_serving_handle()
    full_root = runtime.root / "serving-full"
    delta_root = runtime.serving_index_path
    _publish_full(full_root, _mutated_full(), "gen-full")
    _publish_full(delta_root, _base_state(), "gen-base")
    receipt = publish_snapshot(
        delta_root,
        _delta_from_base(),
        generation_id="gen-delta",
        parent_generation="gen-base",
        mode="delta",
    )
    if receipt.mode != "delta":
        raise AssertionError(f"expected delta publish, got {receipt.mode}")
    dest = delta_root / "generations" / "gen-delta"
    if not (delta_root / "generations" / "gen-base").is_dir():
        raise AssertionError("incremental publish pruned the immutable parent")
    if (dest / "cards.jsonl").read_text(encoding="utf-8").count("hfa-person-gone000001"):
        raise AssertionError("delta segment secretly copied the deleted parent card")
    left = resolve_live_universe(full_root)
    right = resolve_live_universe(delta_root)
    diff = diff_universes(left, right)
    if diff.unexplained:
        raise AssertionError(f"full vs incremental mismatch {diff.mismatches} left={diff.left_only} right={diff.right_only}")
    handle = get_serving_handle(runtime.vault)
    listed = {row.get("card_uid") for row in handle.query(limit=20)}
    if "hfa-person-gone000001" in listed:
        raise AssertionError("deleted UID remains searchable")
    if "hfa-person-new0000001" not in listed:
        raise AssertionError("inserted UID missing from live query")
    hits = handle.vector(list(_vec(3)), limit=5)
    if not hits or hits[0].get("card_uid") != "hfa-email-msg00000001":
        raise AssertionError(f"new vector missed after incremental publish: {hits}")
    graph = handle.graph("Cards/hfa-person-keep000001.md", hops=1)
    if "hfa-person-gone000001" in str(graph):
        raise AssertionError("deleted UID remains in the live graph")
    compact = publish_snapshot(
        delta_root,
        _snapshot(
            cards=list(_mutated_full()["cards"]),  # type: ignore[arg-type]
            chunks=list(_mutated_full()["chunks"]),  # type: ignore[arg-type]
            edges=list(_mutated_full()["edges"]),  # type: ignore[arg-type]
            embeddings=list(_mutated_full()["embeddings"]),  # type: ignore[arg-type]
            name="compact",
        ),
        generation_id="gen-compact",
        mode="compact",
        force_compact=True,
    )
    compact_diff = diff_universes(left, resolve_live_universe(delta_root))
    if compact_diff.unexplained:
        raise AssertionError(f"compaction mismatch {compact_diff.mismatches}")
    handle.close()
    reset_serving_handle()
    return {
        "id": "p02.deltas.full_incremental",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "unexplained_mismatches": diff.unexplained,
        "compaction_mismatches": compact_diff.unexplained,
        "delta_mode": receipt.mode,
        "compact_mode": compact.mode,
        "live_uids": list(left.live_uids),
        "live_chunk_keys": list(left.live_chunk_keys),
        "parent_retained": True,
        "format": 2,
    }


register(
    Scenario(
        id="p02.deltas.full_incremental",
        suite="p02",
        product_guarantee="Insert/update/delete/new-vector/edge-change produce the same live universe through incremental and full publish",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p02-deltas.json",),
        run=run_p02_deltas,
    )
)
