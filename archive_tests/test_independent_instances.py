"""P09-D: person and organization instances stay isolated through restart."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.commands.setup import FIXTURE_PERSON_UID
from archive_tests.acceptance.environment import provision_isolated_runtime
from archive_tests.acceptance.p09_instances_support import run_two_instance_cycle
from archive_tests.conftest import require_integration_enabled


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


@pytest.mark.integration
def test_two_instances_isolated_through_restart(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    if not require_integration_enabled(pytestconfig):
        pytest.skip("P09-D instance restart requires --require-integration")
    runtime = provision_isolated_runtime(tmp_path / "iso", require_integration=True, suite="p09")
    try:
        runtime.apply()
        payload = run_two_instance_cycle(runtime)
        assert payload["person"]["archive_id"] != payload["organization"]["archive_id"]
        assert payload["person"]["root"] != payload["organization"]["root"]
        assert payload["person"]["entity_type"] == "person"
        assert payload["organization"]["entity_type"] == "organization"
        assert payload["person"]["scopes"] == ["personal-fixture"]
        assert payload["organization"]["scopes"] == ["org-ops"]
        assert payload["same_external_person_uid"] == FIXTURE_PERSON_UID
        assert payload["restart_query_ok"] is True
        assert payload["isolated"] is True
        assert payload["production_proven"] is False
        assert payload["analytics"] == "pending"
        assert "hf-archives-seed" not in payload["person"]["root"]
        assert "rheeger@" not in str(payload)
    finally:
        runtime.restore()
        runtime.cleanup()
