"""P02-D: every mutation route journals or requests bounded reconciliation."""

from __future__ import annotations

from pathlib import Path

import pytest

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
from archive_tests.test_publication_equivalence import (
    _base_state,
    _delta_from_base,
    _mutated_full,
    _publish_full,
    _snapshot,
)
from archive_vault.change_journal import (
    OPERATION_DELETE,
    OPERATION_EMBED,
    OPERATION_UPDATE,
    ChangeJournal,
    mutation_context,
)
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")


def _person(uid: str, summary: str) -> PersonCard:
    return PersonCard(
        uid=uid,
        type="person",
        source=["test.p02d"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
    )


def _prov() -> dict[str, ProvenanceEntry]:
    return {
        field: ProvenanceEntry("test.p02d", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name")
    }


def _write(vault: Path, uid: str, summary: str, rel: str = "People/writer.md") -> Path:
    (vault / "People").mkdir(parents=True, exist_ok=True)
    return write_card(vault, rel, _person(uid, summary), body=f"{summary} body", provenance=_prov())


def _state_snapshot(state: dict[str, object], name: str):
    return _snapshot(
        cards=list(state["cards"]),  # type: ignore[arg-type]
        chunks=list(state["chunks"]),  # type: ignore[arg-type]
        edges=list(state["edges"]),  # type: ignore[arg-type]
        embeddings=list(state["embeddings"]),  # type: ignore[arg-type]
        name=name,
    )


def _gen_bytes(root: Path, gid: str) -> int:
    dest = root / "generations" / gid
    return sum(path.stat().st_size for path in dest.rglob("*") if path.is_file())


def test_source_write_reaches_publisher(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    root = tmp_path / "idx"
    _write(vault, "hfa-person-p02d000001", "Source Write")
    _publish_full(root, _base_state(), "gen-base")
    receipt = publish(
        1,
        {
            "vault": vault,
            "index_root": root,
            "snapshot": _state_snapshot(_mutated_full(), "src"),
            "generation_id": "gen-src",
            "mode": "full",
        },
    )
    assert receipt.ok
    assert receipt.generation_id == "gen-src"
    assert receipt.eligible_checkpoint == 1
    with ChangeJournal(vault) as journal:
        pending = consume_batch(journal, CONSUMER_PUBLICATION)
    assert pending.high_watermark >= 1
    assert not any(record.uid == "hfa-person-p02d000001" for record in pending.records)


def test_manual_file_edit_requests_bounded_reconciliation(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    path = _write(vault, "hfa-person-p02d000002", "Manual Edit")
    path.write_text(path.read_text(encoding="utf-8").replace("Manual Edit body", "Edited body"), encoding="utf-8")
    report = request_reconciliation(
        vault,
        uid_to_rel={"hfa-person-p02d000002": "People/writer.md"},
        reason="manual_edit",
    )
    assert report["bounded"] is True
    assert report["imported"]
    with ChangeJournal(vault) as journal:
        records = [row for row in journal.committed_records() if row.uid == "hfa-person-p02d000002"]
    assert any(row.operation == OPERATION_UPDATE for row in records)
    assert max(row.sequence for row in records) in report["imported"]


def test_enrichment_write_sets_mutation_context(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write(vault, "hfa-person-p02d000003", "Enrich First")
    with mutation_context(source="card_enrichment"):
        _write(vault, "hfa-person-p02d000003", "Enrich Next")
    with ChangeJournal(vault) as journal:
        latest = [row for row in journal.committed_records() if row.uid == "hfa-person-p02d000003"][-1]
    assert latest.source == "card_enrichment"
    assert latest.operation == OPERATION_UPDATE


def test_warehouse_materialization_acks_independently(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write(vault, "hfa-person-p02d000004", "Warehouse Card")
    cursor = acknowledge_materialized(vault, uids=["hfa-person-p02d000004"])
    assert cursor.high_watermark >= 1
    with ChangeJournal(vault) as journal:
        pub = consume_batch(journal, CONSUMER_PUBLICATION)
        warehouse = journal.consumer_cursor(CONSUMER_WAREHOUSE)
    assert any(record.uid == "hfa-person-p02d000004" for record in pub.records)
    assert warehouse.high_watermark >= 1


def test_embed_completion_is_journaled(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write(vault, "hfa-person-p02d000005", "Embed Card")
    records = emit_embed_completion(vault, ["hfa-person-p02d000005"], source="embed_pending")
    assert records and records[0].operation == OPERATION_EMBED
    with ChangeJournal(vault) as journal:
        batch = consume_batch(journal, CONSUMER_PUBLICATION)
    assert any(record.operation == OPERATION_EMBED for record in batch.records)


def test_hygiene_delete_journals_tombstone(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write(vault, "hfa-person-p02d000006", "Hygiene Card", rel="People/hygiene.md")
    deleted = delete_vault_markdown(
        vault,
        ["People/hygiene.md"],
        uids_by_rel={"People/hygiene.md": "hfa-person-p02d000006"},
    )
    assert deleted == 1
    assert not (vault / "People/hygiene.md").exists()
    with ChangeJournal(vault) as journal:
        records = [row for row in journal.committed_records() if row.uid == "hfa-person-p02d000006"]
    assert any(row.operation == OPERATION_DELETE for row in records)


def test_rebuild_route_does_not_skip_unknown_dirty(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write(vault, "hfa-person-p02d000007", "Rebuild Card")
    acknowledge_materialized(vault, uids=["hfa-person-p02d000007"])
    with ChangeJournal(vault) as journal:
        leftover = consume_batch(journal, CONSUMER_WAREHOUSE)
    assert leftover.records == () or all(record.uid != "hfa-person-p02d000007" for record in leftover.records)


def test_small_change_is_bounded_delta_with_explicit_compaction(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    root = tmp_path / "idx"
    _write(vault, "hfa-person-p02d000008", "Delta Card")
    _publish_full(root, _base_state(), "gen-base")
    delta = publish(
        1,
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
        1,
        {
            "vault": vault,
            "index_root": root,
            "snapshot": _state_snapshot(_mutated_full(), "compact"),
            "generation_id": "gen-compact",
            "mode": "compact",
            "force_compact": True,
        },
    )
    assert delta.ok and delta.mode == "delta" and not delta.compacted
    assert compact.ok and compact.compacted
    assert _gen_bytes(root, "gen-delta") < _gen_bytes(root, "gen-compact")
    assert (root / "generations" / "gen-base").is_dir()


def test_named_consumers_ack_independently(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    _write(vault, "hfa-person-p02d000009", "Consumer Card")
    with ChangeJournal(vault) as journal:
        pub = consume_batch(journal, CONSUMER_PUBLICATION)
        acknowledge_batch(journal, pub)
        warehouse = consume_batch(journal, CONSUMER_WAREHOUSE)
        assert warehouse.records
        assert journal.consumer_cursor(CONSUMER_WAREHOUSE).high_watermark == 0
        acknowledge_batch(journal, warehouse)
        assert journal.consumer_cursor(CONSUMER_WAREHOUSE).high_watermark >= 1
