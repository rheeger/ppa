"""P06-D acceptance: late features converge through the installed engine."""

from __future__ import annotations

import inspect
import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from archive_cli.engine_factory import EngineBurstBridge
from archive_cli.store import DefaultArchiveStore
from archive_sync.connectors.replay import (
    BURST_FRESHNESS_INVALIDATED,
    BURST_FRESHNESS_UNKNOWN,
    PendingScope,
    persist_pending_scope,
)
from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector
from archive_sync.connectors.sample import SAMPLE_CONNECTOR_ID
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import BASELINE_REL_PATH, init_vault, write_baseline_person_card
from archive_tests.acceptance.p09_install_support import (
    build_or_load_release,
    install_isolated,
    release_wheels,
    run_installed,
)
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_engine_feature_convergence import (
    THREAD_UID,
    MemoryServing,
    _store,
    _thread_rows,
    test_append_edit_delete_duplicate_retire_only_changed_bursts,
)

REPO = Path(__file__).resolve().parents[3]


def run_p06_convergence(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    write_baseline_person_card(runtime.vault, owned_root=runtime.root)

    early = _store(runtime.vault, attach_bursts=False)
    connector_first = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=early.runtime.identity,
        access=early.access,
        writer=ContainedVaultWriter(runtime.vault),
        cursor={},
        run_id="p06d-accept-connector-first",
    )
    persist_pending_scope(
        runtime.vault,
        PendingScope(
            source="gmail.thread",
            account_scope="alpha@example.test",
            thread_id=THREAD_UID,
            event_identity="evt-p06d-accept",
        ),
    )
    late = _store(runtime.vault, attach_bursts=True)
    drained = late.drain_pending_scopes()
    if connector_first.burst_freshness != BURST_FRESHNESS_UNKNOWN:
        raise AssertionError(f"connector-first freshness should be unknown: {connector_first.burst_freshness}")
    if not drained or any(not item.scheduled for item in drained):
        raise AssertionError(f"late resolver left pending unscheduled: {drained!r}")

    bursts_first = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=late.runtime.identity,
        access=late.access,
        writer=ContainedVaultWriter(runtime.vault),
        cursor={},
        run_id="p06d-accept-bursts-first",
        burst_resolver=EngineBurstBridge(),
    )
    if bursts_first.burst_freshness != BURST_FRESHNESS_INVALIDATED:
        raise AssertionError(f"bursts-first freshness should be invalidated: {bursts_first.burst_freshness}")

    test_append_edit_delete_duplicate_retire_only_changed_bursts()

    serving = MemoryServing(_thread_rows(), generation_id="gen-p06d-accept")
    store = _store(runtime.vault, serving=serving)
    search = store.search("use port 8432", limit=8)
    evidence = store.evidence(query="use port 8432", limit=8)
    if not any(row.get("card_uid") == THREAD_UID for row in (search.get("rows") or [])):
        raise AssertionError(f"current reply missing from search: {search}")
    generation = store.runtime.retrieval.generation()
    chain = {
        "event": "reply",
        "revision": next((item.revision for item in connector_first.persists if item.revision), ""),
        "chunk": "current-reply",
        "generation": generation,
        "citation": "use port 8432",
    }
    search_src = inspect.getsource(DefaultArchiveStore.search)
    if "self.runtime.search" not in search_src or "self.index.search" in search_src:
        raise AssertionError("store.search restored concrete-store routing")

    release_dir = Path(runtime.root).resolve().parent / "release"
    planned = REPO / "logs" / "plans" / "p06" / "P06-D" / "release"
    if (planned / "release-manifest.json").is_file():
        release_dir = planned
    manifest = build_or_load_release(repo=REPO, output=release_dir)
    wheels = release_wheels(release_dir, manifest)
    dest = Path(tempfile.mkdtemp(prefix="ppa-p06d-install-"))
    installed = install_isolated(
        dest=dest,
        wheels=wheels,
        lock=REPO / "requirements" / "runtime-py312.lock",
        repo=REPO,
        extra_env=dict(runtime.env),
    )
    cli_read = run_installed(installed, ["read", BASELINE_REL_PATH], extra_env=dict(runtime.env))
    if cli_read.returncode != 0:
        raise AssertionError(cli_read.stderr or cli_read.stdout)
    cli_payload = json.loads(cli_read.stdout)
    if not cli_payload.get("found"):
        raise AssertionError(f"installed CLI missed fixture card: {cli_payload}")

    script = (
        "import json, logging\n"
        "from archive_cli.commands.read import read\n"
        "from archive_cli.store import DefaultArchiveStore\n"
        f"store = DefaultArchiveStore(vault={str(runtime.vault)!r})\n"
        f"payload = read({BASELINE_REL_PATH!r}, store=store, logger=logging.getLogger('ppa'))\n"
        "print(json.dumps({'found': bool(payload.get('found'))}))\n"
    )
    mcp = subprocess.run(
        [str(installed.python), "-c", script],
        cwd=str(installed.root),
        env={**installed.env, **dict(runtime.env)},
        text=True,
        capture_output=True,
        check=False,
    )
    if mcp.returncode != 0:
        raise AssertionError(mcp.stderr or mcp.stdout)
    mcp_payload = json.loads(mcp.stdout.strip().splitlines()[-1])
    if not mcp_payload.get("found"):
        raise AssertionError(f"installed read helper missed fixture: {mcp_payload}")

    early.close()
    late.close()
    store.close()
    payload = {
        "id": "p06.convergence.installed_engine",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "feature_arrival": {
            "connector_before_bursts": {
                "freshness_before": connector_first.burst_freshness,
                "pending_after_drain": [item.to_payload() for item in drained],
            },
            "bursts_before_connector": {
                "freshness": bursts_first.burst_freshness,
                "burst_keys": list(bursts_first.burst_keys),
            },
        },
        "event_revision_chunk_generation_citation": chain,
        "evidence_hit_count": len(evidence.get("hits") or []),
        "extension_path": installed.extension_path,
        "wheel_hashes": installed.wheel_hashes,
        "release_sha": manifest.get("git_sha"),
        "cli_found": bool(cli_payload.get("found")),
        "mcp_found": bool(mcp_payload.get("found")),
        "production_proven": False,
    }
    artifact = runtime.root.parent / "p06-convergence.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p06.convergence.installed_engine",
        suite="p06",
        product_guarantee=(
            "A connector reply converges through pending drain, burst invalidation, "
            "and the installed CLI/MCP context query"
        ),
        proof_tier="isolated_integration",
        fixture_seed=6,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("p06-convergence.json",),
        run=run_p06_convergence,
    )
)
