from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_identity_ambiguity import (
    test_household_namesakes_are_not_compatible,
    test_named_email_share_is_ambiguous,
    test_opaque_phones_do_not_collide,
    test_stub_email_may_auto_merge,
)


def run_p31_people_queries(_runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    test_household_namesakes_are_not_compatible()
    test_named_email_share_is_ambiguous()
    test_stub_email_may_auto_merge()
    test_opaque_phones_do_not_collide()
    return {"id": "p31.people_queries", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.people_queries",
        suite="p31",
        product_guarantee="Ambiguous people stay ambiguous; identifier equality yields candidate sets",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_people_queries,
    )
)
