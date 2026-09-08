from __future__ import annotations

import shutil
import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_thread_projection_recovery import (
    test_aggregate_detects_earliest_only_change,
    test_pending_receipt_drains_through_projection,
    test_retry_after_child_exists_is_idempotent_recount,
)


def run_p31_thread_projection(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    test_aggregate_detects_earliest_only_change()
    for name in ("threads", "drain"):
        path = runtime.root / name
        if path.exists():
            shutil.rmtree(path)
    test_retry_after_child_exists_is_idempotent_recount(runtime.root / "threads")
    test_pending_receipt_drains_through_projection(runtime.root / "drain")
    return {"id": "p31.thread_projection", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.thread_projection",
        suite="p31",
        product_guarantee="Thread rollups recount idempotently after child retry; earliest-time-only changes are detected",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_thread_projection,
    )
)
