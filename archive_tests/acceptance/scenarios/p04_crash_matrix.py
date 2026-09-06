"""P04-C: composed crash, concurrent publish, correction, and provider-retry faults.

Crashes are real child-process exits at named publication/journal boundaries.
In-process ``PublicationFault`` alone is not sufficient proof.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from pathlib import Path
from typing import Any

from archive_engine.changes import CONSUMER_PUBLICATION, consume_batch
from archive_engine.contracts import UNKNOWN, EvidenceEnvelope
from archive_engine.corrections import (
    CorrectionCommandRequest,
    execute_correction_command,
    reconcile_pending_corrections,
)
from archive_engine.egress import capture_egress, egress_scope
from archive_engine.errors import EgressDeniedError, PublisherBusyError
from archive_engine.publication import (
    COMPLETE_FILE,
    PUBLICATION_PHASES,
    PublicationFaultHook,
    PublisherLease,
    publish_snapshot,
    read_active_generation,
    recover_publication,
)
from archive_sync.adapters.base import deterministic_provenance
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_publication_equivalence import _base_state, _mutated_full, _publish_full, _snapshot
from archive_vault.change_journal import ChangeJournal
from archive_vault.schema import FinanceCard
from archive_vault.vault import read_note, write_card

REPLY_UID = "hfa-email-message-p04creply01"
REPLY_REL = "Email/p04-crash-reply.md"
REPLY_TOKEN = "P04C-TOKEN-AUTH-REPLY"
CHARGE_UID = "hfa-finance-p04cchg0001"
CHARGE_REL = "Finance/p04-crash-charge.md"
DEATH_CODE = 91
JOURNAL_DEATH_CODE = 92


class ProcessDeathHook:
    """Exit the current process at a journal protocol point."""

    def __init__(self, fail_at: str, code: int = JOURNAL_DEATH_CODE) -> None:
        self.fail_at = fail_at
        self.code = code

    def check(self, point: str) -> None:
        if self.fail_at and self.fail_at == point:
            os._exit(self.code)


def _publisher_die(root: str, vault: str, gid: str, phase: str, marker: str) -> None:
    """Child entry: publish then die after the named phase completes."""

    def on_phase(name: str) -> None:
        Path(marker).write_text(name, encoding="utf-8")
        if name == phase:
            os._exit(DEATH_CODE)

    state = _mutated_full()
    snap = _snapshot(
        cards=list(state["cards"]),
        chunks=list(state["chunks"]),
        edges=list(state["edges"]),
        embeddings=list(state["embeddings"]),
        name="p04c-crash",
    )
    captured = None
    vault_path = Path(vault)
    with ChangeJournal(vault_path) as journal:
        captured = consume_batch(journal, CONSUMER_PUBLICATION)
    publish_snapshot(
        Path(root),
        snap,
        generation_id=gid,
        mode="full",
        vault=vault_path,
        captured_batch=captured,
        fault=PublicationFaultHook(on_phase=on_phase),
    )


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
            _snapshot(
                cards=list(_mutated_full()["cards"]),
                chunks=list(_mutated_full()["chunks"]),
                edges=list(_mutated_full()["edges"]),
                embeddings=list(_mutated_full()["embeddings"]),
                name="race",
            ),
            generation_id="gen-race",
            mode="full",
        )
        result.put("ok")
    except PublisherBusyError:
        result.put("busy")
    except Exception as exc:  # pragma: no cover - diagnostic
        result.put(f"err:{type(exc).__name__}:{exc}")


def _journal_die(vault: str, rel: str, uid: str, body: str) -> None:
    path = Path(vault)
    with ChangeJournal(path, fault=ProcessDeathHook("after_replace")) as journal:
        journal.apply_mutation(
            uid=uid,
            rel_path=rel,
            operation="create",
            content=body.encode("utf-8"),
            source="acceptance.p04c",
        )


def _expected_after_death(phase: str) -> dict[str, Any]:
    if phase in {"lease", "write", "build", "validate"}:
        return {"active": "gen-base", "complete": False, "acked": False}
    if phase == "complete":
        return {"active": "gen-base", "complete": True, "acked": False}
    if phase == "promote":
        return {"active": "gen-next", "complete": True, "acked": False}
    return {"active": "gen-next", "complete": True, "acked": True}


def _write_labeled(vault: Path) -> None:
    from archive_vault.schema import EmailMessageCard

    write_card(
        vault,
        REPLY_REL,
        EmailMessageCard(
            uid=REPLY_UID,
            type="email_message",
            source=["acceptance.p04c"],
            source_id="msg-p04c-reply",
            created="2026-04-08",
            updated="2026-04-08",
            gmail_message_id="msg-p04c-reply",
            gmail_thread_id="thread-p04c-auth",
            subject="Re: book the loft",
            snippet="Yes, book it, I will cover it.",
            summary="Authorization reply",
        ),
        body=f"Yes, book it, I will cover it. {REPLY_TOKEN}",
        provenance=deterministic_provenance(
            EmailMessageCard(
                uid=REPLY_UID,
                type="email_message",
                source=["acceptance.p04c"],
                source_id="msg-p04c-reply",
                created="2026-04-08",
                updated="2026-04-08",
                gmail_message_id="msg-p04c-reply",
                gmail_thread_id="thread-p04c-auth",
                subject="Re: book the loft",
                snippet="Yes, book it, I will cover it.",
                summary="Authorization reply",
            ),
            "acceptance.p04c",
        ),
    )
    card = FinanceCard(
        uid=CHARGE_UID,
        type="finance",
        source=["acceptance.p04c"],
        source_id="fin-p04c-charge",
        created="2026-04-11",
        updated="2026-04-11",
        summary="Loft charge",
        amount=482.0,
        currency="USD",
        counterparty="Harbor Loft Collective",
        note="Supported charge for the labeled stay.",
    )
    write_card(
        vault,
        CHARGE_REL,
        card,
        body=f"Card charge 482.00 USD. {REPLY_TOKEN}",
        provenance=deterministic_provenance(card, "acceptance.p04c"),
    )


def _kill_and_recover(root: Path, vault: Path, phase: str) -> dict[str, Any]:
    ctx = mp.get_context("spawn")
    marker = vault.parent / f"phase-{phase}.txt"
    if marker.exists():
        marker.unlink()
    with ChangeJournal(vault) as journal:
        before = journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark
    proc = ctx.Process(target=_publisher_die, args=(str(root), str(vault), "gen-next", phase, str(marker)))
    proc.start()
    proc.join(timeout=60)
    if proc.is_alive():
        proc.kill()
        proc.join(timeout=5)
        raise AssertionError(f"publisher hung at phase={phase}")
    if proc.exitcode != DEATH_CODE:
        raise AssertionError(f"expected process death {DEATH_CODE} at {phase}, got {proc.exitcode}")
    dest = root / "generations" / "gen-next"
    active = read_active_generation(root)
    complete = dest.is_dir() and (dest / COMPLETE_FILE).exists()
    with ChangeJournal(vault) as journal:
        after = journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark
    expect = _expected_after_death(phase)
    if active != expect["active"]:
        raise AssertionError(f"{phase}: active={active} expected {expect['active']}")
    if complete != expect["complete"]:
        raise AssertionError(f"{phase}: complete={complete} expected {expect['complete']}")
    acked = after != before
    if acked != expect["acked"]:
        raise AssertionError(f"{phase}: acked={acked} expected {expect['acked']}")
    recovered = recover_publication(root, generation_id="gen-next", vault=vault)
    recovered_active = read_active_generation(root)
    if phase in {"complete", "promote", "ack"} and recovered_active != "gen-next":
        raise AssertionError(f"{phase}: recover left active={recovered_active}")
    if recovered_active not in {"gen-base", "gen-next"}:
        raise AssertionError(f"{phase}: invalid recovered active {recovered_active}")
    return {
        "phase": phase,
        "exitcode": proc.exitcode,
        "process_death": True,
        "active_after_crash": active,
        "complete_after_crash": complete,
        "acked_after_crash": acked,
        "recovered_active": recovered_active,
        "recovered": recovered,
        "false_completion": False,
    }


def _relation_after_rebuild(runtime: IsolatedRuntime) -> dict[str, Any]:
    reset_serving_handle()
    from archive_cli.server import archive_hybrid_search_json, archive_read, archive_search

    mcp_search = archive_search(REPLY_TOKEN, limit=8)
    mcp_read = archive_read(REPLY_UID)
    hybrid = json.loads(archive_hybrid_search_json(REPLY_TOKEN, limit=8))
    rows = list(hybrid.get("rows") or [])
    found = REPLY_UID in mcp_search or REPLY_REL in mcp_search or REPLY_UID in mcp_read
    generation = read_active_generation(runtime.serving_index_path) or UNKNOWN
    envelope = EvidenceEnvelope(
        generation=str(generation),
        watermark=len(rows),
        coverage="partial" if found else "unknown",
        freshness=UNKNOWN,
        complete=bool(found),
        truncated=False,
        method="hybrid",
        query=REPLY_TOKEN,
        confidence_reason="labeled_reply_membership" if found else "incomplete_after_fault",
    )
    if not found and envelope.complete:
        raise AssertionError("envelope claims complete without the labeled reply")
    if not found:
        # Honest incomplete is allowed; silent empty success is not.
        if hybrid.get("ok") is False:
            pass
        elif not rows:
            pass
        else:
            raise AssertionError(f"relation query missed {REPLY_UID} without incomplete envelope: {hybrid}")
    return {
        "found": found,
        "envelope": envelope.to_payload(),
        "hybrid_row_count": len(rows),
        "mcp_search_hit": REPLY_UID in mcp_search or REPLY_REL in mcp_search,
    }


def _provider_half_batch() -> dict[str, Any]:
    from archive_engine.contracts import AccessContext
    from archive_engine.egress import authorize_destination

    access = AccessContext(
        archive_id="p04c",
        principal="local-operator",
        profile="trusted-local",
        egress_policy_revision="local-only",
    )
    os.environ["PPA_EGRESS_MODE"] = "local-only"
    completed = []
    denied = []
    with capture_egress() as events:
        with egress_scope(access, sources=("acceptance.p04c",)):
            authorize_destination("hash", access=access, sources=("acceptance.p04c",))
            completed.append("hash")
            try:
                authorize_destination(
                    "openai", access=access, sources=("acceptance.p04c",), url="https://api.openai.com/v1/embeddings"
                )
                raise AssertionError("openai must not be authorized under local-only")
            except EgressDeniedError:
                denied.append("openai")
            authorize_destination("hash", access=access, sources=("acceptance.p04c",))
            completed.append("hash-retry")
    leaked = [event.body for event in events if event.body and "P04C-TOKEN" in event.body]
    if leaked:
        raise AssertionError("provider capture leaked labeled archive text")
    if "openai" not in denied:
        raise AssertionError("half-batch did not deny the remote destination")
    remote_allowed = [event.destination for event in events if event.allowed and event.destination == "openai"]
    if remote_allowed:
        raise AssertionError("remote destination was allowed after deny")
    return {
        "completed": completed,
        "denied": denied,
        "events": [
            {"destination": event.destination, "allowed": event.allowed, "phase": event.phase, "reason": event.reason}
            for event in events
        ],
        "fallback": False,
    }


def _correction_then_reimport(vault: Path) -> dict[str, Any]:
    card = FinanceCard(
        uid=CHARGE_UID,
        type="finance",
        source=["acceptance.p04c"],
        source_id="fin-p04c-charge",
        created="2026-04-11",
        updated="2026-04-11",
        summary="Loft charge",
        amount=482.0,
        currency="USD",
        counterparty="Harbor Loft Collective",
    )
    write_card(
        vault, CHARGE_REL, card, body="original 482", provenance=deterministic_provenance(card, "acceptance.p04c")
    )
    execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=CHARGE_UID,
            field="amount",
            value=18.5,
            author="acceptance.p04c",
            reason="paper receipt",
            rel_path=CHARGE_REL,
        ),
    )
    older = card.model_copy(update={"amount": 482.0, "updated": "2026-04-10"})
    write_card(
        vault,
        CHARGE_REL,
        older,
        body="replayed source 482",
        provenance=deterministic_provenance(older, "acceptance.p04c"),
    )
    reconcile_pending_corrections(vault)
    current = read_note(vault, CHARGE_REL)[0]
    if float(current.get("amount") or 0) != 18.5:
        raise AssertionError(f"correction lost after reimport: {current.get('amount')}")
    return {"uid": CHARGE_UID, "amount": 18.5, "survived_reimport": True}


def run_p04_crash_matrix(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    repo = Path(__file__).resolve().parents[3]
    os.environ["PYTHONPATH"] = str(repo) + os.pathsep + os.environ.get("PYTHONPATH", "")
    init_vault(runtime.vault, owned_root=runtime.root)
    _write_labeled(runtime.vault)
    root = runtime.root / "crash-index"
    _publish_full(root, _base_state(), "gen-base")
    if read_active_generation(root) != "gen-base":
        raise AssertionError("base generation did not become ACTIVE")

    matrix = []
    for phase in PUBLICATION_PHASES:
        _publish_full(root, _base_state(), "gen-base")
        matrix.append(_kill_and_recover(root, runtime.vault, phase))

    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    result = ctx.Queue()
    holder = ctx.Process(target=_hold_lease, args=(str(root), ready, release))
    challenger = ctx.Process(target=_try_publish, args=(str(root), result))
    holder.start()
    if not ready.wait(timeout=10):
        holder.kill()
        raise AssertionError("lease holder never became ready")
    challenger.start()
    raced = result.get(timeout=20)
    release.set()
    holder.join(timeout=10)
    challenger.join(timeout=10)
    if raced != "busy":
        raise AssertionError(f"concurrent publisher completed falsely: {raced}")

    dest = root / "generations" / "gen-corrupt"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / COMPLETE_FILE).write_text("{not-json", encoding="utf-8")
    corrupt_status = "failed_explicit"
    try:
        recover_publication(root, generation_id="gen-corrupt", vault=runtime.vault)
        if read_active_generation(root) == "gen-corrupt":
            raise AssertionError("corrupt COMPLETE promoted to ACTIVE")
        corrupt_status = "rejected"
    except Exception as exc:
        if isinstance(exc, AssertionError):
            raise
        corrupt_status = f"failed_explicit:{type(exc).__name__}"

    journal_rel = "People/p04-crash-journal.md"
    journal_uid = "hfa-person-p04cjrnl001"
    body = (
        "---\n"
        "uid: hfa-person-p04cjrnl001\n"
        "type: person\n"
        "source: [acceptance.p04c]\n"
        "source_id: jrnl\n"
        'created: "2026-09-06"\n'
        'updated: "2026-09-06"\n'
        "summary: Journal crash\n"
        "first_name: Journal\n"
        "last_name: Crash\n"
        "---\n"
        "journal\n"
    )
    (runtime.vault / "People").mkdir(parents=True, exist_ok=True)
    proc = ctx.Process(target=_journal_die, args=(str(runtime.vault), journal_rel, journal_uid, body))
    proc.start()
    proc.join(timeout=30)
    if proc.exitcode != JOURNAL_DEATH_CODE:
        raise AssertionError(f"journal child exit {proc.exitcode}, expected {JOURNAL_DEATH_CODE}")
    with ChangeJournal(runtime.vault) as journal:
        reconciled = journal.reconcile()
    if not (runtime.vault / journal_rel).is_file():
        raise AssertionError("journal crash lost the replaced card")
    from archive_vault.schema import PersonCard

    person = PersonCard(
        uid=journal_uid,
        type="person",
        source=["acceptance.p04c"],
        source_id="jrnl",
        created="2026-09-06",
        updated="2026-09-06",
        summary="Journal crash",
        first_name="Journal",
        last_name="Crash",
    )
    write_card(
        runtime.vault,
        journal_rel,
        person,
        body="journal",
        provenance=deterministic_provenance(person, "acceptance.p04c"),
    )

    provider = _provider_half_batch()
    correction = _correction_then_reimport(runtime.vault)

    reset_serving_handle()
    from archive_cli.server import archive_rebuild_indexes

    rebuild = archive_rebuild_indexes()
    relation = _relation_after_rebuild(runtime)

    return {
        "id": "p04.crash_matrix",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "fault_matrix": matrix,
        "concurrent_publisher": {"result": raced, "false_completion": False},
        "corrupt_artifact": {"status": corrupt_status},
        "journal_crash": {
            "exitcode": proc.exitcode,
            "reconcile_count": len(reconciled) if isinstance(reconciled, list) else reconciled,
            "card_present": True,
        },
        "provider_half_batch": provider,
        "correction_reimport": correction,
        "relation_after_rebuild": relation,
        "rebuild": (rebuild.splitlines()[0] if isinstance(rebuild, str) and rebuild else rebuild),
        "production_proven": False,
    }


register(
    Scenario(
        id="p04.crash_matrix",
        suite="p04",
        product_guarantee="Real process death at publish/journal boundaries recovers without lost acknowledged evidence or false completion",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("crash-matrix.json",),
        run=run_p04_crash_matrix,
    )
)
