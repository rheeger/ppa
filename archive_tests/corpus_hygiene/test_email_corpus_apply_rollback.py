"""Section B — email corpus hygiene apply/rollback tests."""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.apply import (
    copy_rollback_kit,
    delete_vault_markdown,
    restore_rollback_kit,
    run_email_corpus_apply,
    write_rollback_json,
)
from archive_cli.corpus_hygiene.census import CensusContext, run_email_census_dry_run
from archive_cli.corpus_hygiene.classification_reuse import EmailThreadRecord, thread_from_frontmatter
from archive_cli.corpus_hygiene.decision_io import decisions_artifact_path, load_decision_records_jsonl
from archive_cli.corpus_hygiene.decisions import EmailCorpusDecisionRecord
from archive_cli.corpus_hygiene.guards import guard_corpus_hygiene_apply
from archive_cli.corpus_hygiene.rollback import run_email_corpus_rollback
from archive_cli.corpus_hygiene.state_store import (
    CORPUS_STATE_ACTIVE,
    CORPUS_STATE_QUARANTINE,
    CORPUS_STATE_SUPPRESSED,
    _uids_for_corpus_states,
    active_corpus_sql_filter,
    all_card_uids_for_records,
    card_uids_for_decision,
    get_card_corpus_state,
    is_card_retrieval_active,
    removal_uids_for_records,
)
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.validation_gates.constants import GATE_RUN_STATUS_PASSED, GATE_SYNTHETIC_FIXTURES
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.guards import GateRefusalError
from archive_sync.gmail_promotion.ledger import FilePromotionLedger, default_ledger_path
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

_FIXTURE_RUN_ID = "section-b-apply-test"


def test_active_corpus_sql_filter_hides_suppressed_only() -> None:
    sql = active_corpus_sql_filter(schema="archive", card_alias="c")
    assert CORPUS_STATE_SUPPRESSED in sql
    assert CORPUS_STATE_QUARANTINE not in sql


def test_removal_uids_are_suppressed_only() -> None:
    census = run_email_census_dry_run(
        _fixture_threads(),
        context=CensusContext(decision_run_id=_FIXTURE_RUN_ID, engine_mode="n/a"),
    )
    removed = set(removal_uids_for_records(census.records))
    assert "uid-mkt" in removed
    assert "uid-msg-1" in removed
    assert "uid-derived" not in removed
    assert "meal-order-1" not in removed
    assert "uid-txn" not in removed


def _thread(**kwargs: object) -> EmailThreadRecord:
    defaults = {
        "thread_uid": "uid-default",
        "gmail_thread_id": "g-default",
        "account_email": "owner@example.com",
        "source_key": "gmail-messages:owner@example.com",
    }
    defaults.update(kwargs)
    return EmailThreadRecord(**defaults)  # type: ignore[arg-type]


def _fixture_threads() -> list[EmailThreadRecord]:
    return [
        _thread(
            thread_uid="uid-mkt",
            gmail_thread_id="g-mkt",
            message_uids=("uid-msg-1",),
            label_ids=("CATEGORY_PROMOTIONS",),
            triage_classification="marketing",
            triage_confidence=0.91,
        ),
        _thread(
            thread_uid="uid-txn",
            gmail_thread_id="g-txn",
            triage_classification="transactional",
            triage_confidence=0.84,
            triage_card_types=("meal_order",),
        ),
        _thread(
            thread_uid="uid-derived",
            gmail_thread_id="g-derived",
            label_ids=("CATEGORY_PROMOTIONS",),
            triage_classification="marketing",
            triage_confidence=0.91,
            derived_uids=("meal-order-1",),
        ),
    ]


def _bootstrap_schema(dsn: str, tmp_path: Path, schema: str) -> PostgresArchiveIndex:
    idx = PostgresArchiveIndex(vault=tmp_path, dsn=dsn)
    idx.schema = schema
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        idx._create_schema(conn)
        runner = MigrationRunner(conn, schema)
        runner.ensure_table()
        runner.run()
    return idx


def _seed_cards(idx: PostgresArchiveIndex, uids: list[str]) -> None:
    with idx._connect() as conn:
        for uid in uids:
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.cards (uid, type, rel_path, summary, slug, content_hash, search_text)
                VALUES (%s, 'email_thread', %s, %s, %s, %s, %s)
                ON CONFLICT (uid) DO NOTHING
                """,
                (uid, f"email/{uid}.md", f"summary {uid}", uid, f"hash-{uid}", uid),
            )
        conn.commit()


def _write_note(vault: Path, rel_path: str, body: str = "# note\n") -> Path:
    path = vault / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def _register_dry_run_gate(
    idx: PostgresArchiveIndex,
    *,
    run_id: str,
    archive_instance: str,
    vault_path: str,
) -> None:
    with idx._connect() as conn:
        registry = GateRegistry(conn, idx.schema)
        record = registry.create_run(
            gate=GATE_SYNTHETIC_FIXTURES,
            archive_instance=archive_instance,
            vault_path=vault_path,
            index_schema=idx.schema,
            engine_mode="n/a",
            policy_version=EMAIL_PROMOTION_POLICY_VERSION,
            run_id=run_id,
        )
        registry.complete_run(record.run_id, status=GATE_RUN_STATUS_PASSED)


def _write_dry_run_artifacts(repo_root: Path, run_id: str, threads: list[EmailThreadRecord]) -> None:
    ctx = CensusContext(
        decision_run_id=run_id,
        engine_mode="n/a",
        archive_instance="fixture:test",
    )
    run_email_census_dry_run(threads, context=ctx, repo_root=repo_root)


@pytest.mark.integration
class TestEmailCorpusApplyRollback:
    SCHEMA = "ppa_b_apply"

    def test_apply_and_rollback_restore_active_state(
        self,
        pgvector_dsn: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        idx = _bootstrap_schema(pgvector_dsn, tmp_path, self.SCHEMA)
        threads = _fixture_threads()
        uids = ["uid-mkt", "uid-msg-1", "uid-txn", "uid-derived"]
        _seed_cards(idx, uids)

        archive_instance = f"fixture:{self.SCHEMA}@127.0.0.1:5432/archive@{tmp_path.name}"
        _register_dry_run_gate(
            idx,
            run_id=_FIXTURE_RUN_ID,
            archive_instance=archive_instance,
            vault_path=str(tmp_path),
        )
        _write_dry_run_artifacts(tmp_path, _FIXTURE_RUN_ID, threads)

        with idx._connect() as conn:
            registry = GateRegistry(conn, idx.schema)
            _record, decisions_path = guard_corpus_hygiene_apply(
                registry,
                decision_run_id=_FIXTURE_RUN_ID,
                archive_instance=archive_instance,
                repo_root=tmp_path,
                instance_role="fixture",
            )
            records_path = decisions_artifact_path(tmp_path, _FIXTURE_RUN_ID)
            assert decisions_path == records_path
            records = load_decision_records_jsonl(decisions_path)
            apply_result = run_email_corpus_apply(
                conn,
                idx.schema,
                records,
                decision_run_id=_FIXTURE_RUN_ID,
                archive_instance=archive_instance,
                vault_path=str(tmp_path),
                engine_mode="n/a",
                repo_root=tmp_path,
                ccs_only=True,
            )

            assert apply_result.counts.threads_applied == 3
            assert apply_result.vault_markdown_deleted is False
            assert get_card_corpus_state(conn, idx.schema, "uid-mkt") == "suppressed"
            assert get_card_corpus_state(conn, idx.schema, "uid-txn") == "active"
            assert get_card_corpus_state(conn, idx.schema, "uid-derived") == "quarantine"
            assert get_card_corpus_state(conn, idx.schema, "meal-order-1") == CORPUS_STATE_QUARANTINE
            assert not is_card_retrieval_active(conn, idx.schema, "uid-mkt")
            assert is_card_retrieval_active(conn, idx.schema, "uid-txn")
            assert is_card_retrieval_active(conn, idx.schema, "uid-derived")
            assert is_card_retrieval_active(conn, idx.schema, "meal-order-1")
            rollback_payload = json.loads(Path(apply_result.rollback_path).read_text(encoding="utf-8"))
            assert "meal-order-1" in rollback_payload["card_uids"]

            rollback_result = run_email_corpus_rollback(
                conn,
                idx.schema,
                decision_run_id=_FIXTURE_RUN_ID,
                archive_instance=archive_instance,
                vault_path=str(tmp_path),
                engine_mode="n/a",
                repo_root=tmp_path,
            )
            assert rollback_result.counts.cards_restored >= 3
            assert get_card_corpus_state(conn, idx.schema, "uid-mkt") == CORPUS_STATE_ACTIVE
            assert is_card_retrieval_active(conn, idx.schema, "uid-mkt")

    def test_apply_refused_without_gate_evidence(
        self,
        pgvector_dsn: str,
        tmp_path: Path,
    ) -> None:
        idx = _bootstrap_schema(pgvector_dsn, tmp_path, self.SCHEMA + "_guard")
        archive_instance = f"fixture:{idx.schema}@127.0.0.1:5432/archive@{tmp_path.name}"
        with idx._connect() as conn:
            registry = GateRegistry(conn, idx.schema)
            with pytest.raises(GateRefusalError, match="Unknown decision_run_id"):
                guard_corpus_hygiene_apply(
                    registry,
                    decision_run_id="missing-run",
                    archive_instance=archive_instance,
                    repo_root=tmp_path,
                )


def test_decision_record_required_fields_for_apply() -> None:
    threads = [
        _thread(
            thread_uid="uid-mkt",
            triage_classification="marketing",
            triage_confidence=0.91,
            label_ids=("CATEGORY_PROMOTIONS",),
        )
    ]
    census = run_email_census_dry_run(
        threads,
        context=CensusContext(decision_run_id=_FIXTURE_RUN_ID, engine_mode="n/a"),
    )
    rec = census.records[0]
    for field in (
        "decision_run_id",
        "classification_source",
        "corpus_decision",
        "processor_decision",
        "policy_version",
    ):
        assert getattr(rec, field)


def test_card_uids_for_decision_includes_derived() -> None:
    census = run_email_census_dry_run(
        _fixture_threads(),
        context=CensusContext(decision_run_id=_FIXTURE_RUN_ID, engine_mode="n/a"),
    )
    derived = next(rec for rec in census.records if rec.thread_uid == "uid-derived")
    uids = card_uids_for_decision(derived)
    assert "uid-derived" in uids
    assert "meal-order-1" in uids


def _minimal_decision_record(**kwargs: object) -> EmailCorpusDecisionRecord:
    defaults: dict[str, object] = {
        "decision_run_id": "r",
        "source_key": "sk",
        "account_email": "a@b.c",
        "gmail_thread_id": "g",
        "gmail_history_id": "",
        "thread_body_sha": "",
        "thread_uid": "t",
        "message_uids": (),
        "attachment_uids": (),
        "derived_uids": (),
        "classification": None,
        "canonical_classification": "marketing",
        "confidence": 0.9,
        "card_types": (),
        "classification_source": "test",
        "classify_prompt_version": "",
        "classify_model": "",
        "policy_version": "v",
        "previous_corpus_state": CORPUS_STATE_ACTIVE,
        "corpus_decision": CORPUS_STATE_SUPPRESSED,
        "processor_decision": "suppress",
        "decision_reason": "",
        "decision_signals": (),
    }
    defaults.update(kwargs)
    return EmailCorpusDecisionRecord(**defaults)  # type: ignore[arg-type]


def test_uid_collect_hot_path_uses_set_membership() -> None:
    for fn in (card_uids_for_decision, _uids_for_corpus_states, all_card_uids_for_records):
        src = inspect.getsource(fn)
        assert "if uid not in uids" not in src
        assert "uid not in uids:" not in src
        assert "seen: set[str]" in src
        assert "uid not in seen" in src


def test_card_uids_for_decision_first_seen_order_and_dedupe() -> None:
    rec = _minimal_decision_record(
        thread_uid="thread-1",
        message_uids=("thread-1", "msg-1", "msg-1"),
        attachment_uids=("att-1",),
        derived_uids=("thread-1", "derived-1"),
    )
    assert card_uids_for_decision(rec) == ["thread-1", "msg-1", "att-1", "derived-1"]


def test_removal_uids_large_n_set_collect() -> None:
    n = 2500
    records = [
        _minimal_decision_record(
            thread_uid=f"t-{i}",
            message_uids=(f"m-{i}", f"t-{i}"),
            corpus_decision=CORPUS_STATE_SUPPRESSED,
        )
        for i in range(n)
    ]
    uids = removal_uids_for_records(records)
    assert len(uids) == n * 2
    assert uids[0] == "t-0"
    assert uids[1] == "m-0"
    assert uids[-2] == f"t-{n - 1}"
    assert uids[-1] == f"m-{n - 1}"


def test_write_rollback_json_large_uid_set(tmp_path: Path) -> None:
    n = 5000
    card_uids = [f"card-{i:05d}" for i in range(n)]
    removed = [f"rm-{i:05d}" for i in range(n // 2)]
    path = tmp_path / "artifacts" / "rollback.json"
    written = write_rollback_json(
        path,
        decision_run_id="large-n",
        archive_instance="fixture:test",
        card_uids=card_uids,
        removed_card_uids=removed,
        vault_markdown_deleted=True,
        rollback_kit_path="/kit",
        ccs_only=False,
    )
    assert written == path
    assert path.is_file()
    assert not path.with_name("rollback.json.tmp").exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["card_uids"] == sorted(card_uids)
    assert payload["removed_card_uids"] == removed
    assert payload["decision_run_id"] == "large-n"
    assert payload["vault_markdown_deleted"] is True
    assert payload["rollback_kit_path"] == "/kit"


def test_apply_persists_rollback_json_via_streamed_writer() -> None:
    src = inspect.getsource(run_email_corpus_apply)
    assert "write_rollback_json(" in src
    assert "json.dumps(rollback_payload" not in src
    assert src.count("write_rollback_json(") >= 2


def test_thread_from_frontmatter_keeps_attachment_and_derived_uids() -> None:
    thread = thread_from_frontmatter(
        "email/uid-derived.md",
        {
            "uid": "uid-derived",
            "messages": ["[[uid-msg-1]]"],
            "attachments": ["[[att-1]]"],
            "derived_uids": ["meal-order-1"],
        },
    )
    assert thread.message_uids == ("uid-msg-1",)
    assert thread.attachment_uids == ("att-1",)
    assert thread.derived_uids == ("meal-order-1",)


def test_vault_remove_deletes_files_and_appends_ledger(tmp_path: Path) -> None:
    rel = "email/uid-mkt.md"
    note = _write_note(tmp_path, rel, "# marketing\n")
    deleted = delete_vault_markdown(tmp_path, [rel], progress_every=1)
    assert deleted == 1
    assert not note.exists()

    census = run_email_census_dry_run(
        [_fixture_threads()[0]],
        context=CensusContext(decision_run_id=_FIXTURE_RUN_ID, engine_mode="n/a"),
    )
    from archive_cli.corpus_hygiene.apply import append_promotion_ledger

    appended = append_promotion_ledger(tmp_path, census.records)
    assert appended == 1
    ledger = FilePromotionLedger(default_ledger_path(tmp_path))
    assert ledger.get_thread_state("g-mkt") == "suppressed"
    assert ledger.get_thread_state("g-unknown") == CORPUS_STATE_ACTIVE


def test_rollback_kit_restore_small_n(tmp_path: Path) -> None:
    rels = [f"email/uid-{i}.md" for i in range(3)]
    for rel in rels:
        _write_note(tmp_path, rel, f"# {rel}\n")
    kit, copied = copy_rollback_kit(tmp_path, rels, decision_run_id=_FIXTURE_RUN_ID, limit=20)
    assert len(copied) == 3
    assert (kit / "kit_manifest.json").is_file()
    for rel in rels:
        (tmp_path / rel).unlink()
        assert not (tmp_path / rel).exists()
    restored = restore_rollback_kit(tmp_path, _FIXTURE_RUN_ID)
    assert restored == 3
    for rel in rels:
        assert (tmp_path / rel).is_file()


@pytest.mark.integration
class TestEmailCorpusVaultRemove:
    SCHEMA = "ppa_b_vault_remove"

    def test_apply_deletes_notes_purges_index_and_writes_ledger(
        self,
        pgvector_dsn: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        idx = _bootstrap_schema(pgvector_dsn, tmp_path, self.SCHEMA)
        threads = _fixture_threads()
        uids = ["uid-mkt", "uid-msg-1", "uid-txn", "uid-derived", "meal-order-1"]
        _seed_cards(idx, uids)
        for uid in uids:
            _write_note(tmp_path, f"email/{uid}.md", f"# {uid}\n")
        with idx._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {idx.schema}.chunks
                    (chunk_key, card_uid, rel_path, chunk_type, chunk_index, content, content_hash)
                VALUES (%s, %s, %s, 'body', 0, 'mkt body', 'hash-chunk-mkt')
                """,
                ("ck-uid-mkt", "uid-mkt", "email/uid-mkt.md"),
            )
            conn.commit()

        archive_instance = f"fixture:{self.SCHEMA}@127.0.0.1:5432/archive@{tmp_path.name}"
        _register_dry_run_gate(
            idx,
            run_id=_FIXTURE_RUN_ID,
            archive_instance=archive_instance,
            vault_path=str(tmp_path),
        )
        _write_dry_run_artifacts(tmp_path, _FIXTURE_RUN_ID, threads)

        with idx._connect() as conn:
            registry = GateRegistry(conn, idx.schema)
            _record, decisions_path = guard_corpus_hygiene_apply(
                registry,
                decision_run_id=_FIXTURE_RUN_ID,
                archive_instance=archive_instance,
                repo_root=tmp_path,
                instance_role="fixture",
            )
            records = load_decision_records_jsonl(decisions_path)
            apply_result = run_email_corpus_apply(
                conn,
                idx.schema,
                records,
                decision_run_id=_FIXTURE_RUN_ID,
                archive_instance=archive_instance,
                vault_path=str(tmp_path),
                engine_mode="n/a",
                repo_root=tmp_path,
            )

            assert apply_result.vault_markdown_deleted is True
            assert apply_result.counts.files_deleted == 2
            report = json.loads(Path(apply_result.artifact_paths["report"]).read_text(encoding="utf-8"))
            assert report["details"]["safety"]["vault_markdown_deleted"] is True
            summary = Path(apply_result.artifact_paths["summary"]).read_text(encoding="utf-8")
            assert "vault markdown deleted: yes" in summary
            rollback_payload = json.loads(Path(apply_result.rollback_path).read_text(encoding="utf-8"))
            assert "meal-order-1" in rollback_payload["card_uids"]
            assert "meal-order-1" not in rollback_payload["removed_card_uids"]
            assert "uid-derived" not in rollback_payload["removed_card_uids"]
            assert "uid-mkt" in rollback_payload["removed_card_uids"]

            assert not (tmp_path / "email/uid-mkt.md").exists()
            assert not (tmp_path / "email/uid-msg-1.md").exists()
            assert (tmp_path / "email/uid-derived.md").is_file()
            assert (tmp_path / "email/meal-order-1.md").is_file()
            assert (tmp_path / "email/uid-txn.md").is_file()

            gone = conn.execute(
                f"SELECT uid FROM {idx.schema}.cards WHERE uid = ANY(%s)",
                (["uid-mkt", "uid-msg-1"],),
            ).fetchall()
            assert gone == []
            kept = conn.execute(
                f"SELECT uid FROM {idx.schema}.cards WHERE uid = ANY(%s) ORDER BY uid",
                (["uid-derived", "meal-order-1", "uid-txn"],),
            ).fetchall()
            assert [str(row["uid"]) for row in kept] == ["meal-order-1", "uid-derived", "uid-txn"]
            assert get_card_corpus_state(conn, idx.schema, "uid-derived") == CORPUS_STATE_QUARANTINE
            assert get_card_corpus_state(conn, idx.schema, "meal-order-1") == CORPUS_STATE_QUARANTINE
            assert is_card_retrieval_active(conn, idx.schema, "uid-derived")
            leftover_chunks = conn.execute(
                f"SELECT chunk_key FROM {idx.schema}.chunks WHERE card_uid = %s",
                ("uid-mkt",),
            ).fetchall()
            assert leftover_chunks == []

            ledger = FilePromotionLedger(default_ledger_path(tmp_path))
            assert ledger.get_thread_state("g-mkt") == "suppressed"
            assert ledger.get_thread_state("g-derived") == CORPUS_STATE_ACTIVE
            assert ledger.get_thread_state("g-txn") == CORPUS_STATE_ACTIVE

            rollback_result = run_email_corpus_rollback(
                conn,
                idx.schema,
                decision_run_id=_FIXTURE_RUN_ID,
                archive_instance=archive_instance,
                vault_path=str(tmp_path),
                engine_mode="n/a",
                repo_root=tmp_path,
            )
            assert rollback_result.counts.kit_files_restored >= 1
            assert rollback_result.counts.kit_files_restored <= 20
            assert (tmp_path / "email/uid-mkt.md").is_file()
            assert (tmp_path / "email/uid-txn.md").is_file()
            rb_report = json.loads(Path(rollback_result.artifact_paths["report"]).read_text(encoding="utf-8"))
            assert rb_report["details"]["safety"]["vault_markdown_deleted"] is True
            assert rb_report["details"]["kit_files_restored"] == rollback_result.counts.kit_files_restored
