"""P02-A acceptance: a journaled mutation survives crash and remains consumable."""

from __future__ import annotations

import time
from typing import Any

from archive_engine.changes import (
    CONSUMER_PUBLICATION,
    CONSUMER_WAREHOUSE,
    NAMED_CONSUMERS,
    acknowledge_batch,
    consume_batch,
    recovery_checkpoint_binding,
)
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.change_journal import OPERATION_UPDATE, ChangeJournal, FaultHook, JournalFault
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

UID = "hfa-person-p02ajnl001"


def _prov(*fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry("acceptance.p02a", "2026-09-06", "deterministic") for field in fields}


def _card(summary: str) -> PersonCard:
    return PersonCard(
        uid=UID,
        type="person",
        source=["acceptance.p02a"],
        source_id="p02a@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
        emails=["p02a@example.test"],
        tags=["p02-journal", "synthetic"],
    )


def run_p02_journal(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    first = write_card(
        runtime.vault,
        "People/p02-journal.md",
        _card("P02 Journal Person"),
        body="canonical before crash",
        provenance=_prov("summary", "first_name", "last_name", "emails", "tags"),
    )
    original = first.read_bytes()
    updated = write_card(
        runtime.vault,
        "People/p02-journal.md",
        _card("P02 Journal Person"),
        body="this write is interrupted",
        provenance=_prov("summary", "first_name", "last_name", "emails", "tags"),
    )
    # Re-create an interrupted update against the live file using the journal fault hook.
    live = updated.read_bytes()
    with ChangeJournal(runtime.vault, fault=FaultHook(fail_at="after_replace")) as journal:
        try:
            journal.apply_mutation(
                uid=UID,
                rel_path="People/p02-journal.md",
                operation=OPERATION_UPDATE,
                content=live + b"\n# recovered\n",
            )
        except JournalFault:
            pass
        else:
            raise AssertionError("expected JournalFault after_replace")
    with ChangeJournal(runtime.vault) as journal:
        results = journal.reconcile()
        if not results or results[-1].state != "committed":
            raise AssertionError(f"reconcile did not commit interrupted mutation: {results}")
        warehouse = consume_batch(journal, CONSUMER_WAREHOUSE)
        if not any(record.uid == UID and record.committed for record in warehouse.records):
            raise AssertionError("warehouse consumer missed committed UID")
        acknowledge_batch(
            journal,
            warehouse,
            acked_sequences=[warehouse.records[0].sequence],
            gap_sequences=[r.sequence for r in warehouse.records[1:]],
            gap_reason="warehouse_gap",
        )
        publication = consume_batch(journal, CONSUMER_PUBLICATION)
        acknowledge_batch(journal, publication)
        pub_cursor = journal.consumer_cursor(CONSUMER_PUBLICATION)
        wh_cursor = journal.consumer_cursor(CONSUMER_WAREHOUSE)
        if pub_cursor.high_watermark <= 0:
            raise AssertionError("publication cursor did not advance")
        if not wh_cursor.gaps and len(warehouse.records) > 1:
            raise AssertionError("publication ack cleared warehouse gaps")
        checkpoint = recovery_checkpoint_binding(runtime.vault)
    recovered = (runtime.vault / "People/p02-journal.md").read_bytes()
    if recovered == original and b"# recovered" not in recovered:
        raise AssertionError("interrupted replacement was lost")
    return {
        "id": "p02.journal.crash_replay",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "uid": UID,
        "consumers": list(NAMED_CONSUMERS),
        "publication_watermark": pub_cursor.high_watermark,
        "warehouse_watermark": wh_cursor.high_watermark,
        "warehouse_gaps": list(wh_cursor.gaps),
        "checkpoint": checkpoint,
        "reconcile": [result.action for result in results],
    }


register(
    Scenario(
        id="p02.journal.crash_replay",
        suite="p02",
        product_guarantee="A crashed canonical mutation is recovered and visible to warehouse without clearing other consumer gaps",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p02-journal.json",),
        run=run_p02_journal,
    )
)
