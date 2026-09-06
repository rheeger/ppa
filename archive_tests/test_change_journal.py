"""P02-A: canonical journal, crash/replay, and independent consumer cursors."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from archive_cli.migrations import discover_migrations
from archive_engine.changes import (
    CONSUMER_PUBLICATION,
    CONSUMER_WAREHOUSE,
    NAMED_CONSUMERS,
    acknowledge_batch,
    consume_batch,
    named_consumers,
    recovery_checkpoint_binding,
)
from archive_engine.contracts import ChangeBatch, ChangeRecord
from archive_vault.change_journal import (
    OPERATION_CREATE,
    OPERATION_UPDATE,
    ChangeJournal,
    FaultHook,
    JournalFault,
    RevisionConflict,
    content_revision,
    idempotency_key,
)
from archive_vault.paths import PathEscapeError
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import delete_card, write_card


def _person(uid: str, summary: str) -> tuple[PersonCard, dict[str, ProvenanceEntry]]:
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test.journal"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
    )
    prov = {
        "summary": ProvenanceEntry("test.journal", "2026-09-06", "deterministic"),
        "first_name": ProvenanceEntry("test.journal", "2026-09-06", "deterministic"),
        "last_name": ProvenanceEntry("test.journal", "2026-09-06", "deterministic"),
    }
    return card, prov


def _write(vault: Path, uid: str, summary: str, rel: str = "People/journal.md") -> Path:
    card, prov = _person(uid, summary)
    return write_card(vault, rel, card, body=f"{summary} body", provenance=prov)


def test_named_consumers_include_publication_and_future_slots() -> None:
    names = named_consumers()
    assert names == NAMED_CONSUMERS
    for required in (
        "publication",
        "warehouse",
        "vectors",
        "graph",
        "seed-link",
        "enrichment",
        "claims",
    ):
        assert required in names


def test_write_card_commits_journal_record(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    _write(vault, "hfa-person-journal0001", "Journal Alpha")
    with ChangeJournal(vault) as journal:
        records = journal.committed_records()
        assert len(records) == 1
        rec = records[0]
        assert isinstance(rec, ChangeRecord)
        assert rec.uid == "hfa-person-journal0001"
        assert rec.operation == OPERATION_CREATE
        assert rec.committed is True
        assert rec.after_revision
        batch = consume_batch(journal, CONSUMER_WAREHOUSE)
        assert batch.consumer_name == "warehouse"
        assert batch.records[0].uid == rec.uid
        assert batch.high_watermark == rec.sequence


def test_idempotent_mutation_key_does_not_duplicate(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    path = _write(vault, "hfa-person-journal0002", "Journal Beta")
    payload = path.read_bytes()
    with ChangeJournal(vault) as journal:
        first = journal.committed_records()[0]
        second = journal.apply_mutation(
            uid="hfa-person-journal0002",
            rel_path="People/journal.md",
            operation=OPERATION_CREATE,
            content=payload,
        )
        assert second.mutation_id == first.mutation_id
        assert second.sequence == first.sequence
        assert journal.committed_records()[0].mutation_id == first.mutation_id
        same_key = journal.apply_mutation(
            uid="hfa-person-journal0002",
            rel_path="People/journal.md",
            operation=OPERATION_CREATE,
            content=payload,
        )
        assert same_key.mutation_id == first.mutation_id
        assert idempotency_key(first.uid, first.operation, first.before_revision, first.after_revision)


def test_crash_before_replace_keeps_old_file(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    path = _write(vault, "hfa-person-journal0003", "Journal Gamma")
    original = path.read_bytes()
    card, prov = _person("hfa-person-journal0003", "Journal Gamma Two")
    from archive_vault.provenance import write_provenance
    from archive_vault.schema import card_to_frontmatter, validate_card_strict
    from archive_vault.yaml_parser import render_card

    validated = validate_card_strict(card.model_dump(mode="python"))
    content = render_card(card_to_frontmatter(validated), write_provenance("new-body", prov)).encode()
    with ChangeJournal(vault, fault=FaultHook(fail_at="after_prepare")) as journal:
        with pytest.raises(JournalFault):
            journal.apply_mutation(
                uid="hfa-person-journal0003",
                rel_path="People/journal.md",
                operation=OPERATION_UPDATE,
                content=content,
            )
    assert path.read_bytes() == original
    with ChangeJournal(vault) as journal:
        results = journal.reconcile()
        assert results
        assert results[0].action == "incomplete_prepare"
        assert results[0].state == "prepared"
        prepared = journal.mutation_by_id(results[0].mutation_id)
        assert prepared is not None
        assert prepared.committed is False
        resumed = journal.apply_mutation(
            uid="hfa-person-journal0003",
            rel_path="People/journal.md",
            operation=OPERATION_UPDATE,
            content=content,
        )
        assert resumed.committed is True
    assert path.read_bytes() == content


def test_crash_after_replace_reconcile_commits(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    path = _write(vault, "hfa-person-journal0004", "Journal Delta")
    card, prov = _person("hfa-person-journal0004", "Journal Delta Two")
    from archive_vault.provenance import write_provenance
    from archive_vault.schema import card_to_frontmatter, validate_card_strict
    from archive_vault.yaml_parser import render_card

    validated = validate_card_strict(card.model_dump(mode="python"))
    content = render_card(card_to_frontmatter(validated), write_provenance("after-replace", prov)).encode()
    with ChangeJournal(vault, fault=FaultHook(fail_at="after_replace")) as journal:
        with pytest.raises(JournalFault):
            journal.apply_mutation(
                uid="hfa-person-journal0004",
                rel_path="People/journal.md",
                operation=OPERATION_UPDATE,
                content=content,
            )
    assert path.read_bytes() == content
    with ChangeJournal(vault) as journal:
        results = journal.reconcile()
        assert results[0].action == "commit"
        rec = journal.committed_records()[-1]
        assert rec.uid == "hfa-person-journal0004"
        assert rec.after_revision == content_revision(content)
        assert rec.committed is True


def test_crash_after_commit_replay_is_idempotent(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    _write(vault, "hfa-person-journal0005", "Journal Epsilon")
    with ChangeJournal(vault) as journal:
        rec = journal.committed_records()[0]
        batch = consume_batch(journal, CONSUMER_WAREHOUSE)
        assert batch.records[0].mutation_id == rec.mutation_id
        first = acknowledge_batch(journal, batch)
        second = acknowledge_batch(journal, batch)
        assert first.high_watermark == rec.sequence
        assert second.high_watermark == rec.sequence
        assert first.acked_sequences == second.acked_sequences
        replay = consume_batch(journal, CONSUMER_WAREHOUSE)
        assert replay.records == ()


def test_publication_ack_does_not_clear_warehouse_gap(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    _write(vault, "hfa-person-journal0006", "Journal Zeta One", rel="People/one.md")
    _write(vault, "hfa-person-journal0007", "Journal Zeta Two", rel="People/two.md")
    with ChangeJournal(vault) as journal:
        pub = consume_batch(journal, CONSUMER_PUBLICATION)
        wh = consume_batch(journal, CONSUMER_WAREHOUSE)
        assert [r.uid for r in pub.records] == [r.uid for r in wh.records]
        assert len(pub.records) == 2
        acknowledge_batch(journal, pub)
        acknowledge_batch(
            journal,
            wh,
            acked_sequences=[wh.records[0].sequence],
            gap_sequences=[wh.records[1].sequence],
            gap_reason="warehouse_pending",
        )
        pub_cursor = journal.consumer_cursor(CONSUMER_PUBLICATION)
        wh_cursor = journal.consumer_cursor(CONSUMER_WAREHOUSE)
        assert pub_cursor.high_watermark == pub.records[-1].sequence
        assert pub_cursor.gaps == ()
        assert wh_cursor.high_watermark == wh.records[0].sequence
        assert wh_cursor.gaps == (wh.records[1].sequence,)


def test_two_writers_serialize_revisions(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    _write(vault, "hfa-person-journal0008", "Journal Eta")
    errors: list[BaseException] = []

    def _writer(suffix: str) -> None:
        try:
            card, prov = _person("hfa-person-journal0008", f"Journal Eta {suffix}")
            write_card(vault, "People/journal.md", card, body=suffix, provenance=prov)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_writer, args=(label,)) for label in ("A", "B")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    with ChangeJournal(vault) as journal:
        committed = [r for r in journal.committed_records() if r.uid == "hfa-person-journal0008"]
        assert len(committed) >= 2
        latest = committed[-1]
        live = content_revision((vault / "People/journal.md").read_bytes())
        assert latest.after_revision == live
        befores = {r.before_revision for r in committed[1:]}
        afters = {r.after_revision for r in committed[:-1]}
        assert befores & afters


def test_stale_before_revision_conflicts(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    path = _write(vault, "hfa-person-journal0009", "Journal Theta")
    stale = path.read_bytes()
    card, prov = _person("hfa-person-journal0009", "Journal Theta Two")
    write_card(vault, "People/journal.md", card, body="newer", provenance=prov)
    with ChangeJournal(vault) as journal:
        with pytest.raises(RevisionConflict):
            journal.apply_mutation(
                uid="hfa-person-journal0009",
                rel_path="People/journal.md",
                operation=OPERATION_UPDATE,
                content=stale,
                expected_before=content_revision(stale),
            )


def test_delete_retains_tombstone(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    path = _write(vault, "hfa-person-journal0010", "Journal Iota")
    delete_card(vault, "People/journal.md")
    assert not path.exists()
    with ChangeJournal(vault) as journal:
        deleted = [r for r in journal.committed_records() if r.operation == "delete"]
        assert deleted
        assert deleted[-1].uid == "hfa-person-journal0010"
        assert deleted[-1].after_revision == ""
        assert deleted[-1].committed is True


def test_legacy_dirty_import_skips_journaled_uids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    _write(vault, "hfa-person-journal0011", "Journal Kappa")
    from archive_cli.errors import ServingIndexUnavailableError
    from archive_cli.serving_index import mark_serving_index_dirty, read_dirty_uids

    root = tmp_path / "rust-search-index"
    root.mkdir()
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    monkeypatch.setattr(
        "archive_cli.serving_index._crate",
        lambda: (_ for _ in ()).throw(ServingIndexUnavailableError("serving_index_unavailable")),
    )
    mark_serving_index_dirty(vault, "legacy_writer", ["hfa-person-journal0011", "hfa-person-journal0012"])
    assert "hfa-person-journal0011" in read_dirty_uids(vault)
    assert "hfa-person-journal0012" in read_dirty_uids(vault)
    with ChangeJournal(vault) as journal:
        uids = [r.uid for r in journal.committed_records()]
        assert uids.count("hfa-person-journal0011") == 1
        assert "hfa-person-journal0012" in uids
        legacy = [r for r in journal.committed_records() if r.operation == "legacy_dirty"]
        assert any(r.uid == "hfa-person-journal0012" for r in legacy)


def test_journal_rejects_path_escape(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    with ChangeJournal(vault) as journal:
        with pytest.raises(PathEscapeError):
            journal.apply_mutation(
                uid="hfa-person-escape0001",
                rel_path="../outside/pwned.md",
                operation=OPERATION_CREATE,
                content=b"nope",
            )
    assert not (outside / "pwned.md").exists()


def test_recovery_checkpoint_hook_is_available(tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    _write(vault, "hfa-person-journal0013", "Journal Lambda")
    binding = recovery_checkpoint_binding(vault)
    assert binding["archive_id"]["status"] == "available"
    assert binding["checkpoint"]["status"] == "available"
    assert binding["checkpoint"]["reason"] == "p02_change_journal"
    assert binding["checkpoint"]["value"]["high_watermark"] >= 1
    assert set(binding["checkpoint"]["value"]["consumers"]) == set(NAMED_CONSUMERS)


def test_migration_009_is_reserved() -> None:
    migrations = discover_migrations()
    versions = [item.version for item in migrations]
    assert 9 in versions
    named = [item for item in migrations if item.version == 9]
    assert named[0].name == "change_consumers"


def test_change_batch_requires_consumer_name() -> None:
    rec = ChangeRecord(
        archive_id="aid",
        sequence=1,
        mutation_id="m1",
        uid="u1",
        operation="create",
        before_revision="",
        after_revision="abc",
        source="test",
        account="",
        run_id="",
        committed=True,
    )
    batch = ChangeBatch(high_watermark=1, consumer_name="warehouse", records=(rec,))
    assert batch.consumer_name == "warehouse"
    payload = batch.to_payload()
    loaded = ChangeBatch.from_payload(payload)
    assert loaded.consumer_name == "warehouse"


@pytest.mark.integration
def test_migration_009_bootstrap_parity(pgvector_dsn: str) -> None:
    from archive_cli.change_consumers import ensure_change_consumer_tables
    from archive_cli.index_store import PostgresArchiveIndex
    from archive_cli.migrate import MigrationRunner

    vault = Path(".")
    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = "archive_p02a_consumers"
    index.bootstrap()
    with index._connect() as conn:
        rows = conn.execute(
            f"SELECT consumer_name FROM {index.schema}.change_consumers ORDER BY consumer_name"
        ).fetchall()
        names = {row[0] if not isinstance(row, dict) else row["consumer_name"] for row in rows}
        assert names == set(NAMED_CONSUMERS)
        runner = MigrationRunner(conn, index.schema)
        status = runner.status()
        applied = set(runner.applied_versions())
        assert 9 in applied
        assert status["pending_count"] == 0
        ensure_change_consumer_tables(conn, index.schema)
