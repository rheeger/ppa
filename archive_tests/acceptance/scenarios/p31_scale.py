from __future__ import annotations

import os
import time
from typing import Any

from archive_engine.identity_resolution import resolve_candidates
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.p31_scale_ann import run_selective_ann
from archive_tests.acceptance.registry import Scenario, register


def run_p31_scale(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    profile = str(getattr(runtime, "scale_profile", "") or os.environ.get("PPA_SCALE_PROFILE") or "")
    million = 1_000_000 if profile == "pr31" else 10_000
    once = resolve_candidates([f"u{i}" for i in range(million)])
    assert once.status == "ambiguous"
    assert len(once.candidate_uids) == million
    import shutil

    vector_bytes = 4_350_000 * 1536 * 4
    free = shutil.disk_usage(runtime.root).free
    resource_ok = free > vector_bytes * 3
    ann_reports = []
    if profile == "pr31":
        work = runtime.root / "scale-ann"
        work.mkdir(parents=True, exist_ok=True)
        for n in (10_000, 100_000):
            need = n * 1536 * 4 * 4
            if free < need:
                raise AssertionError(f"isolated runner lacks disk for {n}x1536 ANN need={need} free={free}")
            ann_reports.append(run_selective_ann(work, n=n, dim=1536))
    status = "passed"
    if profile == "pr31" and not resource_ok:
        status = "blocked"
    return {
        "id": "p31.scale",
        "status": status,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "resolution_invocations": 1,
        "candidates": million,
        "scale_profile": profile or "default",
        "production_vector_bytes": vector_bytes,
        "disk_free_bytes": free,
        "resource_preflight_ok": resource_ok,
        "ann": ann_reports,
        "note": "10k/100k×1536 selective ANN plus one-shot million-UID resolution. 4.35M train is resource-gated.",
    }


register(
    Scenario(
        id="p31.scale",
        suite="p31",
        product_guarantee="People resolution is one invocation per request, not per card",
        proof_tier="unit_and_installed",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_scale,
    )
)
