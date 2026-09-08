"""G0 inventory: F01–F14 are mapped; product slices remain pending."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register

CASES_PATH = Path(__file__).resolve().parents[1] / "data" / "pr31_cases.json"
REQUIRED_FINDINGS = tuple(f"F{index:02d}" for index in range(1, 15))
REQUIRED_FIELDS = ("id", "owner", "slice", "scenario", "assertion", "proof_tier", "status")


def run_p31_inventory(_runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    payload = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    cases = list(payload.get("cases") or [])
    ids = [str(case.get("id") or "") for case in cases]
    missing = [finding for finding in REQUIRED_FINDINGS if finding not in ids]
    if missing:
        raise AssertionError(f"pr31_cases.json missing findings: {missing}")
    incomplete = [
        case.get("id")
        for case in cases
        if any(not str(case.get(field) or "").strip() for field in REQUIRED_FIELDS)
    ]
    if incomplete:
        raise AssertionError(f"pr31_cases.json incomplete rows: {incomplete}")
    closed = [case["id"] for case in cases if str(case.get("status") or "") == "closed"]
    if closed:
        raise AssertionError(f"G0 inventory must not mark findings closed: {closed}")
    pending_slices = sorted(
        {
            part.strip()
            for case in cases
            for part in str(case.get("slice") or "").split(",")
            if part.strip() and part.strip() != "All"
        }
    )
    return {
        "id": "p31.inventory",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "findings": ids,
        "pending_slices": pending_slices,
        "product_repair": False,
        "baseline_current": payload.get("baseline_current"),
        "note": "Inventory maps F01–F14 to scenarios. Product slices are exercised by the rest of suite p31.",
    }


register(
    Scenario(
        id="p31.inventory",
        suite="p31",
        product_guarantee="F01–F14 each map to owner, scenario, assertion, and proof tier before product repair",
        proof_tier="harness_inventory",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=("pr31_cases.json",),
        run=run_p31_inventory,
    )
)
