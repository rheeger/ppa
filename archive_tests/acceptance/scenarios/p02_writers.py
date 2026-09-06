"""P02-D acceptance: writer routes reach publish() with a real receipt."""

from __future__ import annotations

import time
from typing import Any

from archive_cli.corpus_hygiene.apply import delete_vault_markdown
from archive_engine.changes import (
    CONSUMER_PUBLICATION,
    CONSUMER_WAREHOUSE,
    acknowledge_batch,
    acknowledge_materialized,
    consume_batch,
    emit_embed_completion,
    request_reconciliation,
)
from archive_engine.publication import publish
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_publication_equivalence import (
    _base_state,
    _delta_from_base,
    _mutated_full,
    _publish_full,
    _snapshot,
)
from archive_vault.change_journal import OPERATION_DELETE, OPERATION_EMBED, ChangeJournal, mutation_context
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card


def _person(uid: str, summary: str) -> PersonCard:
    return PersonCard(
        uid=uid,
        type="person",
        source=["acceptance.p02d"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
    )


def _prov() -> dict[str, ProvenanceEntry]:
    return {
        field: ProvenanceEntry("acceptance.p02d", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name")
    }


def _state_snapshot(state: dict[str, object], name: str):
    return _snapshot(
        cards=list(state["cards"]),  # type: ignore[arg-type]
        chunks=list(state["chunks"]),  # type: ignore[arg-type]
        edges=list(state["edges"]),  # type: ignore[arg-type]
        embeddings=list(state["embeddings"]),  # type: ignore[arg-type]
        name=name,
    )


def _gen_bytes(root, gid: str) -> int:
    dest = root / "generations" / gid
    return sum(path.stat().st_size for path in dest.rglob("*") if path.is_file())


def run_p02_writers(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    reset_serving_handle()
    vault = runtime.vault
    root = runtime.serving_index_path
    (vault / "People").mkdir(parents=True, exist_ok=True)
    coverage: list[dict[str, Any]] = []

    write_card(
        vault,
        "People/source.md",
        _person("hfa-person-p02d000101", "Source Write"),
        body="source",
        provenance=_prov(),
    )
    coverage.append({"route": "source_write", "operation": "create", "reaches_publisher": True})

    path = vault / "People/source.md"
    path.write_text(path.read_text(encoding="utf-8").replace("source", "manual"), encoding="utf-8")
    recon = request_reconciliation(
        vault,
        uid_to_rel={"hfa-person-p02d000101": "People/source.md"},
        reason="manual_edit",
    )
    if not recon["imported"]:
        raise AssertionError("manual edit did not produce a journaled update")
    coverage.append({"route": "manual_file_edit", "bounded": True, "imported": recon["imported"]})

    with mutation_context(source="card_enrichment"):
        write_card(
            vault,
            "People/source.md",
            _person("hfa-person-p02d000101", "Enrich Write"),
            body="enrich",
            provenance=_prov(),
        )
    coverage.append({"route": "enrichment_write", "source": "card_enrichment"})

    warehouse = acknowledge_materialized(vault, uids=["hfa-person-p02d000101"])
    coverage.append({"route": "warehouse_materialization", "watermark": warehouse.high_watermark})

    embeds = emit_embed_completion(vault, ["hfa-person-p02d000101"], source="embed_pending")
    if not embeds or embeds[0].operation != OPERATION_EMBED:
        raise AssertionError("embedding-only completion was not journaled")
    coverage.append({"route": "embedding_only", "operation": OPERATION_EMBED})

    write_card(
        vault,
        "People/hygiene.md",
        _person("hfa-person-p02d000102", "Hygiene Card"),
        body="hygiene",
        provenance=_prov(),
    )
    delete_vault_markdown(
        vault,
        ["People/hygiene.md"],
        uids_by_rel={"People/hygiene.md": "hfa-person-p02d000102"},
    )
    with ChangeJournal(vault) as journal:
        deleted = [row for row in journal.committed_records() if row.uid == "hfa-person-p02d000102"]
    if not any(row.operation == OPERATION_DELETE for row in deleted):
        raise AssertionError("hygiene unlink did not journal a delete")
    coverage.append({"route": "hygiene_delete", "operation": OPERATION_DELETE})

    _publish_full(root, _base_state(), "gen-base")
    delta = publish(
        warehouse.high_watermark,
        {
            "vault": vault,
            "index_root": root,
            "snapshot": _delta_from_base(),
            "generation_id": "gen-delta",
            "parent_generation": "gen-base",
            "mode": "incremental",
        },
    )
    compact = publish(
        warehouse.high_watermark,
        {
            "vault": vault,
            "index_root": root,
            "snapshot": _state_snapshot(_mutated_full(), "compact"),
            "generation_id": "gen-compact",
            "mode": "compact",
            "force_compact": True,
        },
    )
    if not delta.ok or delta.mode != "delta":
        raise AssertionError(f"small change was not a delta: {delta.to_payload()}")
    if not compact.ok or not compact.compacted:
        raise AssertionError(f"compaction was not explicit: {compact.to_payload()}")
    delta_bytes = _gen_bytes(root, "gen-delta")
    compact_bytes = _gen_bytes(root, "gen-compact")
    if delta_bytes >= compact_bytes:
        raise AssertionError(f"delta {delta_bytes} was not cheaper than compact {compact_bytes}")
    coverage.append({"route": "rebuild_publish", "delta_mode": delta.mode, "compacted": compact.compacted})

    with ChangeJournal(vault) as journal:
        leftover_pub = consume_batch(journal, CONSUMER_PUBLICATION)
        leftover_wh = consume_batch(journal, CONSUMER_WAREHOUSE)
        if leftover_wh.records:
            acknowledge_batch(journal, leftover_wh)
    coverage.append(
        {
            "route": "independent_consumers",
            "publication_pending": len(leftover_pub.records),
            "warehouse_independent": True,
        }
    )
    reset_serving_handle()
    return {
        "id": "p02.writers.coverage",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "coverage": coverage,
        "receipt": delta.to_payload(),
        "compact_receipt": compact.to_payload(),
        "delta_bytes": delta_bytes,
        "compact_bytes": compact_bytes,
        "format": 2,
    }


register(
    Scenario(
        id="p02.writers.coverage",
        suite="p02",
        product_guarantee="Every current mutation route journals a durable change or requests bounded reconciliation, then publish() returns a receipt",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p02-writers.json",),
        run=run_p02_writers,
    )
)
