"""P08-A sample connector through SDK write and available engine exact-read."""

from __future__ import annotations

import time
from typing import Any

from archive_cli.engine_factory import ContainedCanonicalReader, resolve_archive_identity, trusted_local_access
from archive_engine.errors import IncompatibleContractError
from archive_engine.service import ArchiveEngineService
from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector, execute_from_manifest
from archive_sync.connectors.sample import SAMPLE_ACCOUNT_ALPHA, SAMPLE_CONNECTOR_ID, sample_manifest
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.acceptance.scenarios.baseline import ScenarioAssertionError


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def run_p08_sample(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    identity = resolve_archive_identity(runtime.vault, schema_binding="warehouse:ppa+index_schema_v9")
    access = trusted_local_access(identity.archive_id)
    writer = ContainedVaultWriter(runtime.vault)

    first = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=identity,
        access=access,
        writer=writer,
        cursor={},
        run_id="p08a-acceptance",
    )
    if first.created_count != 2:
        raise ScenarioAssertionError(f"expected two created cards, got {first.created_count}")
    replay = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=identity,
        access=access,
        writer=writer,
        cursor={},
        run_id="p08a-acceptance-replay",
    )
    if first.uids != replay.uids:
        raise ScenarioAssertionError(f"replay UIDs changed: {first.uids!r} vs {replay.uids!r}")
    if replay.created_count != 0:
        raise ScenarioAssertionError("replay created a duplicate card")

    attempts_before = writer.write_attempts
    bad = sample_manifest().to_payload()
    bad["sdk_version"] = "99"
    try:
        execute_from_manifest(bad, identity=identity, access=access, writer=writer, cursor={})
    except IncompatibleContractError:
        pass
    else:
        raise ScenarioAssertionError("malformed manifest was accepted")
    if writer.write_attempts != attempts_before:
        raise ScenarioAssertionError("malformed manifest reached the writer")

    primary = next(
        persist
        for persist, proposal in zip(first.persists, first.proposals)
        if proposal.identity.account_scope == SAMPLE_ACCOUNT_ALPHA
    )
    service = ArchiveEngineService(
        identity=identity,
        access=access,
        lookup=_UidLookup(writer.uid_to_rel),
        reader=ContainedCanonicalReader(runtime.vault),
    )
    read = service.read_exact(primary.uid)
    if not read.found or primary.uid not in read.content or "email_message" not in read.content:
        raise ScenarioAssertionError(f"engine exact-read missed {primary.uid}")

    return {
        "id": "p08.sample_sdk_tracer",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "sample_card_uid": primary.uid,
        "sample_rel_path": primary.rel_path,
        "uids": list(first.uids),
        "manifest": first.manifest.to_payload(),
        "proposals": [proposal.identity.to_payload() for proposal in first.proposals],
        "changes": [change.to_payload() for change in first.changes],
        "committed_cursor": dict(first.committed_cursor or {}),
        "replay_uids": list(replay.uids),
        "replay_created_count": replay.created_count,
        "engine_read_found": read.found,
        "p02_wiring": "pending",
        "p03_wiring": "pending",
        "proof_note": (
            "P08-A writes a real canonical card and reads it through the P06 exact-read "
            "service. P03 maintain receipts and P02 serving publication are pending P08-B."
        ),
    }


register(
    Scenario(
        id="p08.sample_sdk_tracer",
        suite="p08",
        product_guarantee=(
            "A fixture-backed connector writes an account-scoped typed card that the "
            "engine can read; replay keeps the same UID; a bad manifest never writes"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p08_sample,
    )
)
