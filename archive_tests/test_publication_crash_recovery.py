"""P02-C: kill the publisher at each phase; previous ACTIVE stays valid."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_engine.changes import CONSUMER_PUBLICATION, consume_batch
from archive_engine.errors import IncompatibleStateError
from archive_engine.publication import (
    COMPLETE_FILE,
    PublicationFault,
    PublicationFaultHook,
    read_active_generation,
    recover_publication,
    publish_snapshot,
)
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

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")


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
        source=["test.p02c"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
    )


def _prov() -> dict[str, ProvenanceEntry]:
    return {
        field: ProvenanceEntry("test.p02c", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name")
    }


@pytest.mark.parametrize("phase", ["lease", "write", "build", "validate", "complete", "promote", "ack"])
def test_kill_publisher_at_each_phase_keeps_previous_or_recovers(tmp_path: Path, phase: str) -> None:
    root = tmp_path / "idx"
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    write_card(vault, "People/keep.md", _person("hfa-person-p02c000001", "Keep One"), body="one", provenance=_prov())
    _publish_full(root, _base_state(), "gen-base")
    assert read_active_generation(root) == "gen-base"
    with ChangeJournal(vault) as journal:
        captured = consume_batch(journal, CONSUMER_PUBLICATION)
        before = journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark
    try:
        publish_snapshot(
            root,
            _state_snapshot(_mutated_full(), "crash"),
            generation_id="gen-next",
            mode="full",
            vault=vault,
            captured_batch=captured,
            fault=PublicationFaultHook(fail_at=phase),
        )
    except PublicationFault as exc:
        assert exc.point == phase
    else:
        raise AssertionError(f"expected PublicationFault at {phase}")
    dest = root / "generations" / "gen-next"
    active = read_active_generation(root)
    complete = dest.is_dir() and (dest / COMPLETE_FILE).exists()
    with ChangeJournal(vault) as journal:
        after_crash = journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark
    if phase in {"lease", "write", "build", "validate"}:
        assert active == "gen-base"
        assert not complete
        assert after_crash == before
    elif phase == "complete":
        assert active == "gen-base"
        assert complete
        assert after_crash == before
        recovered = recover_publication(root, generation_id="gen-next", vault=vault, captured_batch=captured)
        assert recovered["promoted"] is True
        assert read_active_generation(root) == "gen-next"
        with ChangeJournal(vault) as journal:
            assert journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark >= captured.high_watermark
    elif phase == "promote":
        assert active == "gen-next"
        assert complete
        assert after_crash == before
        recovered = recover_publication(root, generation_id="gen-next", vault=vault, captured_batch=captured)
        assert recovered["acked"] is True
        with ChangeJournal(vault) as journal:
            assert journal.consumer_cursor(CONSUMER_PUBLICATION).high_watermark >= captured.high_watermark
    else:
        assert active == "gen-next"
        assert after_crash >= captured.high_watermark


def test_corrupt_artifact_is_not_promoted(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    _publish_full(root, _base_state(), "gen-base")

    def smash(phase: str) -> None:
        if phase != "build":
            return
        dest = root / "generations" / "gen-bad"
        (dest / "embeddings.bin").write_bytes(b"xx")

    with pytest.raises(IncompatibleStateError, match="publication_validation_failed"):
        publish_snapshot(
            root,
            _state_snapshot(_mutated_full(), "bad"),
            generation_id="gen-bad",
            mode="full",
            fault=PublicationFaultHook(on_phase=smash, fail_at=None),
        )
    # smash runs on every phase including validate; ACTIVE must stay
    assert read_active_generation(root) == "gen-base"
    assert not (root / "generations" / "gen-bad" / COMPLETE_FILE).exists()


def test_disk_budget_fails_closed_before_promotion(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    _publish_full(root, _base_state(), "gen-base")
    with pytest.raises(IncompatibleStateError, match="publication_disk_budget"):
        publish_snapshot(
            root,
            _state_snapshot(_mutated_full(), "budget"),
            generation_id="gen-budget",
            mode="full",
            disk_budget_mb=0,
        )
    assert read_active_generation(root) == "gen-base"
    assert not (root / "generations" / "gen-budget" / COMPLETE_FILE).exists()


def test_append_during_build_is_not_acked(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    write_card(vault, "People/first.md", _person("hfa-person-p02c000002", "First Card"), body="first", provenance=_prov())
    _publish_full(root, _base_state(), "gen-base")

    def append(phase: str) -> None:
        if phase != "build":
            return
        write_card(
            vault,
            "People/later.md",
            _person("hfa-person-p02c000003", "Later Card"),
            body="later",
            provenance=_prov(),
        )

    with ChangeJournal(vault) as journal:
        captured = consume_batch(journal, CONSUMER_PUBLICATION)
        captured_ids = {record.sequence for record in captured.records}
    receipt = publish_snapshot(
        root,
        _state_snapshot(_mutated_full(), "append"),
        generation_id="gen-append",
        mode="full",
        vault=vault,
        captured_batch=captured,
        fault=PublicationFaultHook(on_phase=append),
    )
    assert receipt.generation_id == "gen-append"
    with ChangeJournal(vault) as journal:
        cursor = journal.consumer_cursor(CONSUMER_PUBLICATION)
        pending = consume_batch(journal, CONSUMER_PUBLICATION)
    later = [record for record in pending.records if record.uid == "hfa-person-p02c000003"]
    assert later, "later mutation must remain pending"
    assert later[0].sequence not in captured_ids
    assert later[0].sequence > captured.high_watermark
    assert later[0].sequence not in {record.sequence for record in captured.records}
    assert receipt.acked_watermark <= captured.high_watermark
    assert cursor.high_watermark <= captured.high_watermark


def test_replay_does_not_ack_beyond_captured(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    write_card(vault, "People/first.md", _person("hfa-person-p02c000004", "First Card"), body="first", provenance=_prov())
    _publish_full(root, _base_state(), "gen-base")
    with ChangeJournal(vault) as journal:
        captured = consume_batch(journal, CONSUMER_PUBLICATION)
    try:
        publish_snapshot(
            root,
            _state_snapshot(_mutated_full(), "replay"),
            generation_id="gen-replay",
            mode="full",
            vault=vault,
            captured_batch=captured,
            fault=PublicationFaultHook(fail_at="promote"),
        )
    except PublicationFault:
        pass
    write_card(vault, "People/extra.md", _person("hfa-person-p02c000005", "Extra Card"), body="extra", provenance=_prov())
    recover_publication(root, generation_id="gen-replay", vault=vault, captured_batch=captured)
    with ChangeJournal(vault) as journal:
        pending = consume_batch(journal, CONSUMER_PUBLICATION)
    assert any(record.uid == "hfa-person-p02c000005" for record in pending.records)
