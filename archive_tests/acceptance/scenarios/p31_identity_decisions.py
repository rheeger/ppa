from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_identity_repair_decisions import (
    test_alice_bob_smith_household_phone_never_auto_merges,
    test_dry_run_merge_writes_nothing,
)


def run_p31_identity_decisions(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    vault = runtime.root / "identity"
    vault.mkdir(parents=True, exist_ok=True)
    test_alice_bob_smith_household_phone_never_auto_merges(vault / "household")
    test_dry_run_merge_writes_nothing(vault / "dry")
    return {"id": "p31.identity_decisions", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.identity_decisions",
        suite="p31",
        product_guarantee="Household namesakes never auto-merge; dry-run writes nothing; accepted merges use merge_identities",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_identity_decisions,
    )
)
