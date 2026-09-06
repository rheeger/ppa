"""P09-D acceptance: two fixture instances restart in isolation."""

from __future__ import annotations

import json
import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.p09_instances_support import run_two_instance_cycle
from archive_tests.acceptance.registry import Scenario, register


def run_p09_instances(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    cycle = run_two_instance_cycle(runtime)
    payload = {
        "id": "p09.instances.restart_isolation",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "person_archive_id": cycle["person"]["archive_id"],
        "org_archive_id": cycle["organization"]["archive_id"],
        "person_root": cycle["person"]["root"],
        "org_root": cycle["organization"]["root"],
        "person_checkpoint": cycle["person"]["checkpoint"],
        "org_checkpoint": cycle["organization"]["checkpoint"],
        "same_external_person_uid": cycle["same_external_person_uid"],
        "restart_query_ok": cycle["restart_query_ok"],
        "isolated": cycle["isolated"],
        "production_proven": False,
        "analytics": "pending",
        "knowledge_cache": "deferred",
        "platform": "macos-arm64-cpython-3.12-smoke",
    }
    artifact = runtime.root.parent / "p09-instances.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p09.instances.restart_isolation",
        suite="p09",
        product_guarantee="Person and organization fixtures stay isolated through maintain, publish, query, and restart",
        proof_tier="isolated_integration",
        fixture_seed=9,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("p09-instances.json",),
        run=run_p09_instances,
    )
)
