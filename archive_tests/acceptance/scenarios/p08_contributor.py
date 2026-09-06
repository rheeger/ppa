"""P08-D: contributor template through SDK write, quality gate, and remaining-legacy list."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from archive_cli.engine_factory import ContainedCanonicalReader, resolve_archive_identity, trusted_local_access
from archive_engine.service import ArchiveEngineService
from archive_sync.connectors.cli import (
    check_migrated_manifests,
    check_package,
    load_manifest,
    reject_incompatible_manifest,
    remaining_legacy_adapters,
)
from archive_sync.connectors.runtime import ContainedVaultWriter
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.acceptance.scenarios.baseline import ScenarioAssertionError

TEMPLATE = Path(__file__).resolve().parents[3] / "archive_docs" / "examples" / "connector-template"


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def run_p08_contributor(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    vault = runtime.root / "p08d-contributor"
    init_vault(vault, owned_root=runtime.root)
    identity = resolve_archive_identity(vault, schema_binding="warehouse:ppa+index_schema_v9")
    access = trusted_local_access(identity.archive_id)
    result = check_package(TEMPLATE, vault=vault, identity=identity, access=access, run_id="p08d-accept")
    if result["status"] != "compatible":
        raise ScenarioAssertionError(f"template package failed: {result!r}")
    if not result["distinct_accounts_same_provider"]:
        raise ScenarioAssertionError("template did not keep two accounts distinct")
    writer = ContainedVaultWriter(vault)
    bad = load_manifest(TEMPLATE)
    bad["sdk_version"] = "99"
    rejected = reject_incompatible_manifest(bad, identity=identity, access=access, writer=writer)
    if not rejected["rejected"] or writer.write_attempts != 0:
        raise ScenarioAssertionError("incompatible manifest reached the writer")
    uid = result["uids"][0]
    rel = next(path.relative_to(vault).as_posix() for path in (vault / "Email").rglob("*.md") if uid in path.name)
    service = ArchiveEngineService(
        identity=identity,
        access=access,
        lookup=_UidLookup({uid: rel}),
        reader=ContainedCanonicalReader(vault),
    )
    read = service.read_exact(uid)
    if not read.found or uid not in read.content:
        raise ScenarioAssertionError(f"engine exact-read missed {uid}")
    listing = remaining_legacy_adapters()
    migrated = check_migrated_manifests()
    if migrated != {"calendar-events": "compatible", "gmail-messages": "compatible"}:
        raise ScenarioAssertionError(f"migrated manifests failed: {migrated!r}")
    return {
        "id": "p08.contributor_template",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "sample_card_uid": uid,
        "uids": result["uids"],
        "replay_created_count": result["replay_created_count"],
        "quality": result["quality"],
        "incompatible_rejected": rejected["rejected"],
        "engine_read_found": read.found,
        "legacy": listing,
        "migrated_manifests": migrated,
        "onboarding": (
            "Copy archive_docs/examples/connector-template, fill fixtures, "
            "run python -m archive_sync.connectors.cli check. Register the factory only."
        ),
        "proof_note": (
            "Contributor package writes account-scoped cards, replay is idempotent, "
            "and an incompatible sdk_version never writes. Remaining adapters are listed, not claimed migrated."
        ),
    }


register(
    Scenario(
        id="p08.contributor_template",
        suite="p08",
        product_guarantee=(
            "A copied template package passes fixture replay and quality checks; "
            "incompatible versions fail before write; remaining legacy adapters are named"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p08_contributor,
    )
)
