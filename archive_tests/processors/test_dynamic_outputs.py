"""P03-C — derived outputs report real UIDs/revisions and stay bounded."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from archive_engine.contracts import OutputRevision
from archive_sync.adapters.base import deterministic_provenance
from archive_sync.extractors.amazon import AmazonExtractor
from archive_sync.extractors.base import EmailExtractor, TemplateVersion
from archive_sync.extractors.registry import ExtractorRegistry
from archive_sync.extractors.runner import ExtractionRunner
from archive_sync.llm_enrichment.card_enrichment_runner import (
    DERIVED_ENRICHMENT_TAG,
    apply_deterministic_derived_enrichment,
)
from archive_sync.processors.batch import ProcessorPlanItem
from archive_sync.processors.constants import (
    CORPUS_ACTIVE,
    INPUT_STATUS_COMPLETE,
    INPUT_STATUS_PENDING,
    MAX_SELF_INVALIDATING_REVISIONS,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    SKIP_NONCONVERGENT,
)
from archive_sync.processors.dirty_io import enqueue_output_snapshots
from archive_sync.processors.runner import (
    BatchExecuteResult,
    ExecuteContext,
    _execute_linkers,
    _execute_typed_extraction,
    _result_with_outputs,
    run_processors,
)
from archive_sync.processors.staleness import ProcessorInputSnapshot
from archive_sync.processors.state_store import ProcessorStateStore
from archive_tests.archive_sync.extractors.conftest import write_email_to_vault
from archive_tests.archive_sync.extractors.test_amazon import AMAZON_ORDER_BODY
from archive_vault.schema import PurchaseCard
from archive_vault.vault import read_note_by_uid, write_card

EMAIL_UID = "hfa-email-message-p03c01"
UNAFFECTED_UID = "hfa-person-p03cunaffected"
ORDER_NUMBER = "112-1234567-1234567"


def _vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in (
        "People",
        "Email",
        "Transactions/Purchases",
        "Entities/Organizations",
        "_templates",
        ".obsidian",
        "_meta",
    ):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    return vault


def _amazon_frontmatter() -> dict:
    return {
        "uid": EMAIL_UID,
        "type": "email_message",
        "source": ["gmail"],
        "source_id": "gmail.msg.p03c01",
        "created": "2024-03-15",
        "updated": "2024-03-15",
        "summary": "Your Amazon.com order confirmation",
        "gmail_message_id": "msgid-p03c01",
        "gmail_thread_id": "thread-p03c01",
        "account_email": "me@example.com",
        "from_email": "auto-confirm@amazon.com",
        "to_emails": ["me@example.com"],
        "subject": "Your Amazon.com order confirmation",
        "sent_at": "2024-03-15T14:30:00-08:00",
        "people": [],
        "orgs": [],
        "tags": [],
    }


def _snap(uid: str, *, card_type: str = "email_message", decision: str = "typed_extraction") -> ProcessorInputSnapshot:
    return ProcessorInputSnapshot(
        input_uid=uid,
        card_type=card_type,
        corpus_state=CORPUS_ACTIVE,
        processor_decision=decision,
        field_values={
            "body_sha": uid,
            "thread_uid": uid,
            "frontmatter_hash": uid,
            "chunk_hash": uid,
            "source_hash": uid,
            "target_hash": uid,
            "corpus_state": CORPUS_ACTIVE,
            "processor_decision": decision,
        },
        source_dirty=True,
        upstream_complete=True,
    )


def test_extraction_reports_created_output_uids(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    write_email_to_vault(str(vault), "Email/2024-03/p03c01.md", _amazon_frontmatter(), AMAZON_ORDER_BODY)
    from archive_sync.extractors.registry import build_default_registry

    metrics = ExtractionRunner(
        str(vault),
        registry=build_default_registry(),
        dry_run=False,
        workers=1,
        uid_allowlist={EMAIL_UID},
    ).run()
    assert metrics.extracted_cards == 1
    assert metrics.created
    created = metrics.created[0]
    expected = AmazonExtractor().generate_derived_uid(EMAIL_UID, ORDER_NUMBER)
    assert created.source_uid == EMAIL_UID
    assert created.output_uid == expected
    assert created.revision
    assert created.operation == "created"
    assert not metrics.failed_source_uids


def test_no_extraction_is_not_failure(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    fm = _amazon_frontmatter()
    fm["from_email"] = "noreply@example.com"
    fm["subject"] = "Lunch plans"
    write_email_to_vault(str(vault), "Email/2024-03/p03c-none.md", fm, "See you at noon.")
    from archive_sync.extractors.registry import build_default_registry

    metrics = ExtractionRunner(
        str(vault),
        registry=build_default_registry(),
        dry_run=False,
        workers=1,
        uid_allowlist={EMAIL_UID},
    ).run()
    assert EMAIL_UID in metrics.no_extraction_source_uids
    assert not metrics.failed_source_uids
    assert metrics.extracted_cards == 0

    ctx = ExecuteContext(vault_path=str(vault), apply=True, dry_run=False)
    item = ProcessorPlanItem(
        processor_key=PROCESSOR_EMAIL_TYPED_EXTRACTION,
        input_uid=EMAIL_UID,
        current_input_hash="rev-1",
        input_revision="rev-1",
        processor_version="email-typed-extraction-v1",
    )
    out = _execute_typed_extraction(ctx, [item])
    assert out.results[0].valid_no_output
    assert out.results[0].status == INPUT_STATUS_COMPLETE
    assert out.results[0].receipt is not None
    assert out.results[0].receipt.outputs == ()


def test_extraction_exception_is_failure(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    write_email_to_vault(str(vault), "Email/2024-03/p03c-boom.md", _amazon_frontmatter(), AMAZON_ORDER_BODY)

    class BoomExtractor(EmailExtractor):
        extractor_id = "boom"
        sender_patterns = [r".*@amazon\.com$"]
        output_card_type = "purchase"

        def template_versions(self):
            def boom(_fm, _body):
                raise RuntimeError("forced extract failure")

            return [TemplateVersion("t", ("2000-01-01", "2099-12-31"), boom)]

    registry = ExtractorRegistry()
    registry.register(BoomExtractor())
    metrics = ExtractionRunner(
        str(vault),
        registry=registry,
        dry_run=False,
        workers=1,
        uid_allowlist={EMAIL_UID},
    ).run()
    assert EMAIL_UID in metrics.failed_source_uids
    assert EMAIL_UID not in metrics.no_extraction_source_uids


def test_enqueue_output_snapshots_is_scoped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []

    def fake_resolve(uids, **kwargs):
        seen.append([str(uid) for uid in uids])
        return []

    monkeypatch.setattr("archive_sync.processors.dirty_io.resolve_snapshots_for_uids", fake_resolve)
    enqueue_output_snapshots(["hfa-purchase-abc"], vault_path=tmp_path)
    assert seen == [["hfa-purchase-abc"]]


def test_extraction_rerun_is_noop(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    write_email_to_vault(str(vault), "Email/2024-03/p03c01.md", _amazon_frontmatter(), AMAZON_ORDER_BODY)
    from archive_sync.extractors.registry import build_default_registry

    registry = build_default_registry()
    first = ExtractionRunner(str(vault), registry=registry, dry_run=False, workers=1, uid_allowlist={EMAIL_UID}).run()
    second = ExtractionRunner(str(vault), registry=registry, dry_run=False, workers=1, uid_allowlist={EMAIL_UID}).run()
    assert first.extracted_cards == 1
    assert second.extracted_cards == 0
    assert second.unchanged
    assert second.unchanged[0].output_uid == first.created[0].output_uid


def test_derived_enrichment_is_idempotent(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    uid = "hfa-purchase-p03cenr01"
    card = PurchaseCard(
        uid=uid,
        type="purchase",
        source=["email_extraction"],
        source_id=uid,
        created="2024-03-15",
        updated="2024-03-15",
        summary="Amazon order 1",
        vendor="Amazon",
        order_number=ORDER_NUMBER,
    )
    write_card(
        str(vault),
        f"Transactions/Purchases/2024-03/{uid}.md",
        card,
        "# Amazon order 1\n",
        deterministic_provenance(card, "test"),
    )
    first = apply_deterministic_derived_enrichment(vault, [uid])
    assert uid in first.enriched_card_uids
    note = read_note_by_uid(str(vault), uid)
    assert note is not None
    assert DERIVED_ENRICHMENT_TAG in (note[1].get("tags") or [])
    second = apply_deterministic_derived_enrichment(vault, [uid])
    assert second.vault_writes == 0
    assert uid in second.unchanged_card_uids


def test_linkers_run_deterministic_without_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    def fake_refresh(index, **kwargs):
        called.update(kwargs)
        return {
            "jobs_completed": 1,
            "jobs_failed": 0,
            "output_uids": ["hfa-organization-amazon"],
            "created_candidate_ids": ["9"],
        }

    monkeypatch.setattr("archive_cli.seed_links.run_incremental_link_refresh", fake_refresh)
    monkeypatch.setattr("archive_cli.index_config.get_rebuild_workers", lambda: 1)
    ctx = ExecuteContext(
        vault_path=".",
        store=SimpleNamespace(index=object()),
        apply=True,
        dry_run=False,
        provider_available=False,
        allow_broad_llm=False,
    )
    item = ProcessorPlanItem(
        processor_key=PROCESSOR_LINKERS,
        input_uid="hfa-purchase-1",
        current_input_hash="h",
        input_revision="h",
        processor_version="linkers-v1",
    )
    out = _execute_linkers(ctx, [item])
    assert called["include_llm"] is False
    assert out.results[0].status == INPUT_STATUS_COMPLETE
    assert out.results[0].output_uids == ["hfa-organization-amazon"]
    assert "provider_links_pending" in out.warnings


def test_nonconvergent_processor_is_pending_not_skipped(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    writes = {"n": 0}

    def fake_executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        writes["n"] += 1
        revision = f"rev-{writes['n']}"
        return BatchExecuteResult(
            results=[
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_COMPLETE,
                    outputs=(OutputRevision(uid="hfa-loop-out01", revision=revision),),
                )
                for item in items
            ]
        )

    result = run_processors(
        inputs=[_snap(EMAIL_UID, card_type="person", decision="")],
        vault_path=str(vault),
        state_store=ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json"),
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        provider_available=False,
        run_id="p03c-nonconvergent",
        batch_executor=fake_executor,
    )
    pending = [item for item in result.item_results if item.skip_reason == SKIP_NONCONVERGENT]
    assert pending
    assert all(item.status == INPUT_STATUS_PENDING for item in pending)
    assert writes["n"] <= MAX_SELF_INVALIDATING_REVISIONS + 2


def test_unaffected_card_not_enqueued(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    write_email_to_vault(str(vault), "Email/2024-03/p03c01.md", _amazon_frontmatter(), AMAZON_ORDER_BODY)
    seen: list[str] = []

    def fake_executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        seen.extend(item.input_uid for item in items)
        purchase = AmazonExtractor().generate_derived_uid(EMAIL_UID, ORDER_NUMBER)
        return BatchExecuteResult(
            results=[
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_COMPLETE,
                    outputs=(OutputRevision(uid=purchase, revision="r1"),)
                    if item.processor_key == PROCESSOR_EMAIL_TYPED_EXTRACTION
                    else (OutputRevision(uid=item.input_uid, revision="r1"),),
                )
                for item in items
            ]
        )

    run_processors(
        inputs=[_snap(EMAIL_UID)],
        vault_path=str(vault),
        state_store=ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json"),
        apply=True,
        dry_run=False,
        provider_available=False,
        run_id="p03c-scoped",
        batch_executor=fake_executor,
    )
    assert EMAIL_UID in seen
    assert UNAFFECTED_UID not in seen


def test_typed_extraction_adapter_returns_output_revisions(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    write_email_to_vault(str(vault), "Email/2024-03/p03c01.md", _amazon_frontmatter(), AMAZON_ORDER_BODY)
    ctx = ExecuteContext(vault_path=str(vault), apply=True, dry_run=False)
    item = ProcessorPlanItem(
        processor_key=PROCESSOR_EMAIL_TYPED_EXTRACTION,
        input_uid=EMAIL_UID,
        current_input_hash="rev-1",
        input_revision="rev-1",
        processor_version="email-typed-extraction-v1",
    )
    out = _execute_typed_extraction(ctx, [item])
    assert out.results[0].status == INPUT_STATUS_COMPLETE
    assert out.results[0].receipt is not None
    assert out.results[0].receipt.outputs
    expected = AmazonExtractor().generate_derived_uid(EMAIL_UID, ORDER_NUMBER)
    assert out.results[0].output_uids == [expected]
    assert out.results[0].receipt.outputs[0].revision
