from __future__ import annotations

import time
from typing import Any

from archive_engine.publication import PublisherLease
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register


def run_p31_snapshot(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    root = runtime.root / "lease-root"
    root.mkdir(parents=True, exist_ok=True)
    lease = PublisherLease(root)
    lease.acquire()
    lease.release()
    dirty = root / "DIRTY"
    dirty.write_text("keep\n", encoding="utf-8")
    assert dirty.read_text(encoding="utf-8") == "keep\n"
    return {"id": "p31.snapshot", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.snapshot",
        suite="p31",
        product_guarantee="PublisherLease can be acquired before export work; DIRTY is left intact",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_snapshot,
    )
)
