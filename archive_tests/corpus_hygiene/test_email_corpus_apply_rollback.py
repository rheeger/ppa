"""Section B — email corpus hygiene apply/rollback tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.apply import run_email_corpus_apply
from archive_cli.corpus_hygiene.census import CensusContext, run_email_census_dry_run
from archive_cli.corpus_hygiene.classification_reuse import EmailThreadRecord
from archive_cli.corpus_hygiene.decision_io import decisions_artifact_path, load_decision_records_jsonl
from archive_cli.corpus_hygiene.guards import guard_corpus_hygiene_apply
from archive_cli.corpus_hygiene.rollback import run_email_corpus_rollback
from archive_cli.corpus_hygiene.state_store import (
    CORPUS_STATE_ACTIVE,
    get_card_corpus_state,
    is_card_retrieval_active,
)
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.validation_gates.constants import GATE_RUN_STATUS_PASSED, GATE_SYNTHETIC_FIXTURES
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.guards import GateRefusalError
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

_FIXTURE_RUN_ID = "section-b-apply-test"


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
            )

            assert apply_result.counts.threads_applied == 3
            assert get_card_corpus_state(conn, idx.schema, "uid-mkt") == "suppressed"
            assert get_card_corpus_state(conn, idx.schema, "uid-txn") == "active"
            assert get_card_corpus_state(conn, idx.schema, "uid-derived") == "quarantine"
            assert get_card_corpus_state(conn, idx.schema, "meal-order-1") == CORPUS_STATE_ACTIVE
            assert not is_card_retrieval_active(conn, idx.schema, "uid-mkt")
            assert is_card_retrieval_active(conn, idx.schema, "uid-txn")

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
