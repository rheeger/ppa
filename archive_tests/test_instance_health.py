"""P09-D: current-instance health, freshness honesty, no inherited seed exception."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli.commands.setup import detect_capabilities, run_setup
from archive_cli.status.aggregate import build_blocked_status, build_production_status
from archive_cli.status.instance_policy import (
    HISTORICAL_EXCEPTION_ID,
    HISTORICAL_SEED_PATH,
    current_instance_policy,
    historical_exception_record,
)
from archive_cli.status.text import format_status_text
from archive_tests.acceptance.environment import provision_isolated_runtime
from archive_tests.acceptance.p09_instances_support import (
    bound_instance,
    instance_env,
    setup_person_instance,
)
from archive_tests.conftest import require_integration_enabled


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


def test_new_instance_does_not_inherit_historical_exception(tmp_path: Path) -> None:
    vault = tmp_path / "fresh-archive"
    vault.mkdir()
    policy = current_instance_policy(vault_path=vault, archive_instance="fixture:new")
    assert policy["historical_exception_applies"] is False
    assert policy["inherits_local_seed_exception"] is False
    assert policy["accepted_local_exception"] is None
    assert policy["historical_exception"]["id"] == HISTORICAL_EXCEPTION_ID
    assert policy["historical_exception"]["does_not_transfer"] is True
    assert policy["historical_exception"]["seed_path"] == HISTORICAL_SEED_PATH
    assert policy["fresh"] is False
    assert policy["fresh_from_manifest"] is False
    assert policy["production_proven"] is False
    assert policy["analytics_cli"] == "pending"
    assert policy["analytics_mcp"] == "pending"


def test_historical_exception_record_is_preserved() -> None:
    record = historical_exception_record()
    assert record["id"] == HISTORICAL_EXCEPTION_ID
    assert record["does_not_set_ready"] is True
    policy = current_instance_policy(vault_path=HISTORICAL_SEED_PATH, archive_instance="ppa@local@seed")
    assert policy["historical_exception_applies"] is True
    assert policy["accepted_local_exception"] == HISTORICAL_EXCEPTION_ID
    assert policy["inherits_local_seed_exception"] is False
    assert policy["production_proven"] is False


def test_manifest_is_never_freshness(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    result = run_setup(
        spec={
            "root": str(root),
            "entity_name": "Ada Example",
            "entity_type": "person",
            "index_schema": "ppa_fixture",
            "fixture": "sample.fixture",
            "embedding_provider": "hash",
        },
        apply=True,
        non_interactive=True,
    )
    assert (root / "ppa.json").is_file()
    caps = detect_capabilities(vault=root)
    assert caps["fresh"] is False
    assert result["fresh"] is False
    assert caps["auth"]["status"] == "pending"
    assert caps["production_proven"] is False


def test_missing_warehouse_and_provider_are_pending(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    caps = detect_capabilities(vault=tmp_path, environ={})
    assert caps["warehouse"]["status"] == "pending"
    assert caps["auth"]["status"] == "pending"
    assert caps["fresh"] is False


def test_blocked_status_cannot_claim_fresh() -> None:
    payload = build_blocked_status(reason="vault_not_found", message="missing")
    assert payload["fresh"] is False
    assert payload["production_proven"] is False
    assert payload["analytics"]["cli"] == "pending"
    assert payload["analytics"]["mcp"] == "pending"
    assert payload["instance_policy"]["historical_exception_applies"] is False
    text = format_status_text(payload)
    assert "fresh: false" in text.lower() or "manifest is not freshness" in text.lower()


def test_stale_source_capability_is_visible() -> None:
    from archive_cli.status.aggregate import _flatten_source_entries

    entries = _flatten_source_entries(
        {
            "sources": [
                {
                    "declaration": {"source_key": "gmail-messages:ada@example.test", "enabled": True},
                    "state": {
                        "source_key": "gmail-messages:ada@example.test",
                        "staleness_state": "stale",
                        "last_error": "down",
                    },
                },
                {
                    "declaration": {"source_key": "calendar-events:ada@example.test", "enabled": True},
                    "state": {
                        "source_key": "calendar-events:ada@example.test",
                        "staleness_state": "never_synced",
                    },
                },
            ]
        }
    )
    by_key = {row["source_key"]: row for row in entries}
    assert by_key["gmail-messages:ada@example.test"]["capability"]["status"] == "stale"
    assert by_key["calendar-events:ada@example.test"]["capability"]["status"] == "pending"


@pytest.mark.integration
def test_new_instance_status_policy_and_noop(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    if not require_integration_enabled(pytestconfig):
        pytest.skip("P09-D instance health requires --require-integration")
    runtime = provision_isolated_runtime(tmp_path / "iso", require_integration=True, suite="p09")
    try:
        runtime.apply()
        vault = runtime.root / "person"
        setup = setup_person_instance(vault, runtime.schema)
        assert setup["fresh"] is False
        env = instance_env(
            runtime,
            vault,
            schema=runtime.schema,
            serving=runtime.serving_index_path,
            embed_cache=runtime.query_embed_cache_path,
        )
        from archive_cli.commands.maintain import run_maintenance

        with bound_instance(vault, env) as (store, config):
            store.bootstrap()
            store.rebuild()
            first = run_maintenance(store=store, logger=__import__("logging").getLogger("ppa.p09d"), dry_run=False)
            noop = run_maintenance(store=store, logger=__import__("logging").getLogger("ppa.p09d"), dry_run=False)
            with store.index._connect() as conn:
                payload = build_production_status(
                    store=store,
                    archive_instance=runtime.schema,
                    conn=conn,
                    schema=runtime.schema,
                    require_production_soak=False,
                    include_index_status=False,
                )
        policy = payload["instance_policy"]
        assert policy["historical_exception_applies"] is False
        assert policy["inherits_local_seed_exception"] is False
        assert payload["fresh"] is False
        assert payload["production_proven"] is False
        assert payload["analytics"]["cli"] == "pending"
        assert payload["v3_readiness"].get("instance_policy", {}).get("accepted_local_exception") is None
        assert payload["capabilities"]["fresh"] is False
        assert first.publication.get("ok") is not False or not first.publication.get("error")
        cheap = bool(noop.nothing_to_do) or "processor_execution (no dirty work)" in noop.skipped_steps
        assert cheap
        assert config.identity.archive_id
        assert json.dumps(payload).count("local_seed_living_corpus") >= 1
        assert "does_not_transfer" in json.dumps(payload)
    finally:
        runtime.restore()
        runtime.cleanup()
