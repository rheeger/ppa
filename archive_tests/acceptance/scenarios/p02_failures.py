"""P02-C acceptance: crash, race, corrupt, budget, and pinned-reader faults."""

from __future__ import annotations

import multiprocessing as mp
import time
import uuid
from pathlib import Path
from typing import Any

from archive_cli.serving_index import prune_retired_serving_generations
from archive_engine.changes import CONSUMER_PUBLICATION, consume_batch
from archive_engine.errors import IncompatibleStateError, PublisherBusyError
from archive_engine.publication import (
    COMPLETE_FILE,
    PublicationFault,
    PublicationFaultHook,
    PublisherLease,
    pin_generation,
    pinned_generations,
    publish_snapshot,
    read_active_generation,
    recover_publication,
    unpin_generation,
)
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_publication_equivalence import (
    _base_state,
    _mutated_full,
    _publish_full,
    _snapshot,
)
from archive_vault.change_journal import ChangeJournal
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card


def _state_snapshot(state: dict[str, object], name: str) -> object:
    return _snapshot(
        cards=list(state["cards"]),  # type: ignore[arg-type]
        chunks=list(state["chunks"]),  # type: ignore[arg-type]
        edges=list(state["edges"]),  # type: ignore[arg-type]
        embeddings=list(state["embeddings"]),  # type: ignore[arg-type]
        name=name,
    )


def _person(uid: str, summary: str) -> PersonCard:
    return PersonCard(
        uid=uid,
        type="person",
        source=["acceptance.p02c"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
    )


def _prov() -> dict[str, ProvenanceEntry]:
    return {
        field: ProvenanceEntry("acceptance.p02c", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name")
    }


def _hold_lease(root: str, ready: Any, release: Any) -> None:
    lease = PublisherLease(Path(root))
    lease.acquire()
    ready.set()
    release.wait(timeout=30)
    lease.release()


def _try_publish(root: str, result: Any) -> None:
    try:
        publish_snapshot(
            Path(root),
            _state_snapshot(_mutated_full(), "race"),
            generation_id="gen-race",
            mode="full",
        )
        result.put("ok")
    except PublisherBusyError:
        result.put("busy")
    except Exception as exc:  # pragma: no cover - diagnostic
        result.put(f"err:{type(exc).__name__}:{exc}")


def _hold_pin(root: str, gid: str, ready: Any, release: Any) -> None:
    pin_generation(root, gid)
    ready.set()
    release.wait(timeout=60)
    unpin_generation(root, gid)


def _phase_row(
    *,
    phase: str,
    active: str,
    complete: bool,
    acked: bool,
    recovered_active: str = "",
) -> dict[str, Any]:
    return {
        "phase": phase,
        "active": active,
        "complete": complete,
        "acked": acked,
        "recovered_active": recovered_active,
        "valid": True,
    }


def run_p02_failures(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    reset_serving_handle()
    vault = runtime.vault
    (vault / "People").mkdir(parents=True, exist_ok=True)
    matrix: list[dict[str, Any]] = []
    last_root = runtime.root / "serving-faults" / "ack"
    run_token = uuid.uuid4().hex[:8]

    for index, phase in enumerate(("lease", "write", "build", "validate", "complete", "promote", "ack")):
        root = runtime.root / "serving-faults" / phase
        dest_gid = f"gen-{phase}"
        write_card(
            vault,
            f"People/{phase}-{run_token}.md",
            _person(f"hfa-person-p02c{index:06d}", f"Phase {phase} {run_token}"),
            body=f"{phase}-{run_token}",
            provenance=_prov(),
        )
        _publish_full(root, _base_state(), "gen-base")
        with ChangeJournal(vault) as journal:
            captured = consume_batch(journal, CONSUMER_PUBLICATION)
            before = journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark
        try:
            publish_snapshot(
                root,
                _state_snapshot(_mutated_full(), phase),
                generation_id=dest_gid,
                mode="full",
                vault=vault,
                captured_batch=captured,
                fault=PublicationFaultHook(fail_at=phase),
            )
            raise AssertionError(f"expected PublicationFault at {phase}")
        except PublicationFault as exc:
            if exc.point != phase:
                raise
        dest = root / "generations" / dest_gid
        active = read_active_generation(root)
        complete = dest.is_dir() and (dest / COMPLETE_FILE).exists()
        with ChangeJournal(vault) as journal:
            after = journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark
        recovered_active = ""
        if phase in {"complete", "promote"}:
            recovered = recover_publication(root, generation_id=dest_gid, vault=vault, captured_batch=captured)
            recovered_active = str(recovered.get("active") or "")
            if recovered_active != dest_gid:
                raise AssertionError(f"{phase} recovery did not promote {dest_gid}")
        elif phase == "ack":
            if after < captured.high_watermark:
                raise AssertionError("ack-phase fault lost the captured watermark")
            last_root = root
        else:
            if active != "gen-base" or complete or after != before:
                raise AssertionError(f"{phase} exposed incomplete generation or acked early")
        matrix.append(
            _phase_row(
                phase=phase,
                active=active,
                complete=complete,
                acked=after > before,
                recovered_active=recovered_active,
            )
        )

    corrupt_root = runtime.root / "serving-corrupt"
    _publish_full(corrupt_root, _base_state(), "gen-base")

    def smash(phase: str) -> None:
        if phase != "build":
            return
        (corrupt_root / "generations" / "gen-corrupt" / "embeddings.bin").write_bytes(b"xx")

    try:
        publish_snapshot(
            corrupt_root,
            _state_snapshot(_mutated_full(), "corrupt"),
            generation_id="gen-corrupt",
            mode="full",
            fault=PublicationFaultHook(on_phase=smash),
        )
        raise AssertionError("corrupt artifact was promoted")
    except IncompatibleStateError as exc:
        if "publication_validation_failed" not in str(exc):
            raise
    if read_active_generation(corrupt_root) == "gen-corrupt":
        raise AssertionError("corrupt generation became ACTIVE")
    matrix.append({"phase": "corrupt", "active": read_active_generation(corrupt_root), "valid": True})

    budget_root = runtime.root / "serving-budget"
    _publish_full(budget_root, _base_state(), "gen-base")
    try:
        publish_snapshot(
            budget_root,
            _state_snapshot(_mutated_full(), "budget"),
            generation_id="gen-budget",
            mode="full",
            disk_budget_mb=0,
        )
        raise AssertionError("zero disk budget published")
    except IncompatibleStateError as exc:
        if "publication_disk_budget" not in str(exc):
            raise
    matrix.append({"phase": "disk_budget", "active": read_active_generation(budget_root), "valid": True})

    append_root = runtime.root / "serving-append"
    _publish_full(append_root, _base_state(), "gen-base")

    later_uid = f"hfa-person-p02c{run_token}"

    def append(phase: str) -> None:
        if phase != "build":
            return
        write_card(
            vault,
            f"People/later-{run_token}.md",
            _person(later_uid, f"Later Card {run_token}"),
            body=f"later-{run_token}",
            provenance=_prov(),
        )

    with ChangeJournal(vault) as journal:
        captured = consume_batch(journal, CONSUMER_PUBLICATION)
    publish_snapshot(
        append_root,
        _state_snapshot(_mutated_full(), "append"),
        generation_id="gen-append",
        mode="full",
        vault=vault,
        captured_batch=captured,
        fault=PublicationFaultHook(on_phase=append),
    )
    with ChangeJournal(vault) as journal:
        pending = consume_batch(journal, CONSUMER_PUBLICATION)
    if not any(record.uid == later_uid for record in pending.records):
        raise AssertionError("append during build was acked")
    matrix.append({"phase": "append_during_build", "later_pending": True, "valid": True})

    race_root = runtime.root / "serving-race"
    _publish_full(race_root, _base_state(), "gen-base")
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    result = ctx.Queue()
    holder = ctx.Process(target=_hold_lease, args=(str(race_root), ready, release))
    challenger = ctx.Process(target=_try_publish, args=(str(race_root), result))
    holder.start()
    if not ready.wait(timeout=15):
        release.set()
        holder.join(timeout=5)
        raise AssertionError("lease holder did not start")
    challenger.start()
    outcome = result.get(timeout=60)
    release.set()
    holder.join(timeout=10)
    challenger.join(timeout=10)
    if outcome != "busy":
        raise AssertionError(f"second publisher was not busy: {outcome}")
    matrix.append({"phase": "two_publishers", "outcome": outcome, "valid": True})

    pin_root = runtime.root / "serving-pin"
    _publish_full(pin_root, _base_state(), "gen-base")
    pin_ready = ctx.Event()
    pin_release = ctx.Event()
    reader = ctx.Process(target=_hold_pin, args=(str(pin_root), "gen-base", pin_ready, pin_release))
    reader.start()
    if not pin_ready.wait(timeout=15):
        pin_release.set()
        reader.join(timeout=5)
        raise AssertionError("reader pin did not start")
    if "gen-base" not in pinned_generations(pin_root):
        pin_release.set()
        reader.join(timeout=5)
        raise AssertionError("interprocess pin missing")
    _publish_full(pin_root, _mutated_full(), "gen-next")
    removed = prune_retired_serving_generations(runtime.vault, keep="gen-next", index_root=pin_root)
    if "gen-base" in removed or not (pin_root / "generations" / "gen-base").is_dir():
        pin_release.set()
        reader.join(timeout=5)
        raise AssertionError("pinned generation was collected")
    matrix.append({"phase": "hold_old_reader", "retained": "gen-base", "pruned": removed, "valid": True})
    pin_release.set()
    reader.join(timeout=10)
    reset_serving_handle()
    return {
        "id": "p02.failures.promotion",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "fault_matrix": matrix,
        "holder_pid": holder.pid,
        "challenger_pid": challenger.pid,
        "reader_pid": reader.pid,
        "active": read_active_generation(last_root),
        "format": 2,
    }


register(
    Scenario(
        id="p02.failures.promotion",
        suite="p02",
        product_guarantee="A crash, race, corrupt artifact, or disk-budget miss cannot expose an incomplete generation or drop later journal records",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p02-failures.json",),
        run=run_p02_failures,
    )
)
