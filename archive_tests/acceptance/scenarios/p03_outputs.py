"""P03-C acceptance: one source receipt creates derived outputs to a revision fixed point."""

from __future__ import annotations

import time
from typing import Any

from archive_sync.extractors.amazon import AmazonExtractor
from archive_sync.llm_enrichment.card_enrichment_runner import DERIVED_ENRICHMENT_TAG
from archive_tests.acceptance.environment import IsolatedRuntime, inspect_warehouse_card
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.archive_sync.extractors.conftest import write_email_to_vault
from archive_tests.archive_sync.extractors.test_amazon import AMAZON_ORDER_BODY
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.uid import generate_uid
from archive_vault.vault import read_note_by_uid, write_card

EMAIL_UID = "hfa-email-message-p03cacc1"
PERSON_UID = "hfa-person-p03caccunaffected"
ORDER_NUMBER = "112-1234567-1234567"


class ScenarioAssertionError(AssertionError):
    """P03-C product assertion failed."""


def _write_person(vault) -> None:
    card = PersonCard(
        uid=PERSON_UID,
        type="person",
        source=["acceptance.p03c"],
        source_id=f"{PERSON_UID}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="P03 C Unaffected",
        first_name="P03",
        last_name="Unaffected",
        emails=[f"{PERSON_UID}@example.test"],
        tags=["p03-acceptance", "synthetic"],
    )
    prov = {
        field: ProvenanceEntry("acceptance.p03c", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name", "emails", "tags")
    }
    write_card(vault, "People/p03-unaffected.md", card, body="Unaffected P03-C fixture.", provenance=prov)


def _write_amazon_email(vault) -> None:
    fm = {
        "uid": EMAIL_UID,
        "type": "email_message",
        "source": ["gmail"],
        "source_id": "gmail.msg.p03cacc1",
        "created": "2024-03-15",
        "updated": "2024-03-15",
        "summary": "Your Amazon.com order confirmation",
        "gmail_message_id": "msgid-p03cacc1",
        "gmail_thread_id": "thread-p03cacc1",
        "account_email": "me@example.com",
        "from_email": "auto-confirm@amazon.com",
        "to_emails": ["me@example.com"],
        "subject": "Your Amazon.com order confirmation",
        "sent_at": "2024-03-15T14:30:00-08:00",
        "people": [],
        "orgs": [],
        "tags": [],
    }
    write_email_to_vault(str(vault), "Email/2024-03/p03c-amazon.md", fm, AMAZON_ORDER_BODY)


def _email_snap():
    from archive_sync.processors.constants import CORPUS_ACTIVE
    from archive_sync.processors.staleness import ProcessorInputSnapshot

    return ProcessorInputSnapshot(
        input_uid=EMAIL_UID,
        card_type="email_message",
        corpus_state=CORPUS_ACTIVE,
        processor_decision="typed_extraction",
        field_values={
            "body_sha": EMAIL_UID,
            "thread_uid": EMAIL_UID,
            "frontmatter_hash": EMAIL_UID,
            "chunk_hash": EMAIL_UID,
            "source_hash": EMAIL_UID,
            "target_hash": EMAIL_UID,
            "corpus_state": CORPUS_ACTIVE,
            "processor_decision": "typed_extraction",
        },
        source_dirty=True,
        upstream_complete=True,
    )


def _person_snap():
    from archive_sync.processors.constants import CORPUS_ACTIVE
    from archive_sync.processors.staleness import ProcessorInputSnapshot

    return ProcessorInputSnapshot(
        input_uid=PERSON_UID,
        card_type="person",
        corpus_state=CORPUS_ACTIVE,
        field_values={
            "body_sha": PERSON_UID,
            "frontmatter_hash": PERSON_UID,
            "chunk_hash": PERSON_UID,
            "source_hash": PERSON_UID,
            "target_hash": PERSON_UID,
            "corpus_state": CORPUS_ACTIVE,
        },
        source_dirty=True,
        upstream_complete=True,
    )


def run_p03_outputs(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    _write_person(runtime.vault)
    _write_amazon_email(runtime.vault)

    from archive_cli.store import DefaultArchiveStore
    from archive_engine.contracts import OutputRevision
    from archive_sync.processors.batch import ProcessorPlanItem
    from archive_sync.processors.constants import (
        INPUT_STATUS_COMPLETE,
        INPUT_STATUS_PENDING,
        PROCESSOR_EMAIL_TYPED_EXTRACTION,
        PROCESSOR_MATERIALIZATION,
        SKIP_NONCONVERGENT,
    )
    from archive_sync.processors.runner import (
        BatchExecuteResult,
        ExecuteContext,
        _result_with_outputs,
        run_processors,
    )
    from archive_sync.processors.state_store import ProcessorStateStore

    store = DefaultArchiveStore(vault=runtime.vault)
    store.bootstrap()
    state = ProcessorStateStore(None, meta_path=runtime.vault / "_meta" / "processors.json")

    person_first = run_processors(
        inputs=[_person_snap()],
        vault_path=str(runtime.vault),
        store=store,
        state_store=state,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03c-person-baseline",
    )
    person_before = inspect_warehouse_card(runtime.dsn, runtime.schema, PERSON_UID)
    if person_before is None:
        raise ScenarioAssertionError(f"person baseline missing: {person_first.report.status}")

    purchase_uid = AmazonExtractor().generate_derived_uid(EMAIL_UID, ORDER_NUMBER)
    org_uid = generate_uid("organization", "entity-resolution", "amazon.com")

    result = run_processors(
        inputs=[_email_snap()],
        vault_path=str(runtime.vault),
        store=store,
        state_store=state,
        apply=True,
        dry_run=False,
        provider_available=True,
        allow_broad_llm=False,
        run_id="p03c-outputs",
    )

    purchase_note = read_note_by_uid(str(runtime.vault), purchase_uid)
    if purchase_note is None:
        raise ScenarioAssertionError(f"purchase card not created: {purchase_uid}")
    if DERIVED_ENRICHMENT_TAG not in (purchase_note[1].get("tags") or []):
        raise ScenarioAssertionError(f"purchase was not enriched: {purchase_note[1].get('tags')}")
    org_note = read_note_by_uid(str(runtime.vault), org_uid)
    if org_note is None:
        raise ScenarioAssertionError(f"amazon org not created: {org_uid}")

    purchase_row = inspect_warehouse_card(runtime.dsn, runtime.schema, purchase_uid)
    org_row = inspect_warehouse_card(runtime.dsn, runtime.schema, org_uid)
    if purchase_row is None or org_row is None:
        raise ScenarioAssertionError(f"derived warehouse rows missing purchase={purchase_row} org={org_row}")

    person_after = inspect_warehouse_card(runtime.dsn, runtime.schema, PERSON_UID)
    if person_after != person_before:
        raise ScenarioAssertionError(f"unaffected person changed {person_before} -> {person_after}")

    extraction = [
        item
        for item in result.item_results
        if item.processor_key == PROCESSOR_EMAIL_TYPED_EXTRACTION and item.input_uid == EMAIL_UID
    ]
    if not extraction or not extraction[0].receipt or not extraction[0].receipt.outputs:
        raise ScenarioAssertionError(f"extraction receipt missing outputs: {extraction}")
    if extraction[0].output_uids != [purchase_uid]:
        raise ScenarioAssertionError(f"extraction outputs {extraction[0].output_uids} != {[purchase_uid]}")

    rerun = run_processors(
        inputs=[_email_snap()],
        vault_path=str(runtime.vault),
        store=store,
        state_store=state,
        apply=True,
        dry_run=False,
        provider_available=True,
        allow_broad_llm=False,
        run_id="p03c-outputs-rerun",
    )
    rerun_extraction = [
        item
        for item in rerun.item_results
        if item.processor_key == PROCESSOR_EMAIL_TYPED_EXTRACTION and item.input_uid == EMAIL_UID
    ]
    created_again = [
        item
        for item in rerun_extraction
        if item.status == INPUT_STATUS_COMPLETE
        and item.receipt
        and item.receipt.outputs
        and not item.valid_no_output
        and not item.already_current
    ]
    if created_again:
        raise ScenarioAssertionError(f"rerun created outputs again: {created_again}")

    loop_state = ProcessorStateStore(None, meta_path=runtime.vault / "_meta" / "processors-loop.json")
    writes = {"n": 0}

    def looping_executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        writes["n"] += 1
        return BatchExecuteResult(
            results=[
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_COMPLETE,
                    outputs=(OutputRevision(uid="hfa-loop-outp03c", revision=f"rev-{writes['n']}"),),
                )
                for item in items
            ]
        )

    looped = run_processors(
        inputs=[_person_snap()],
        vault_path=str(runtime.vault),
        store=store,
        state_store=loop_state,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        provider_available=False,
        run_id="p03c-nonconvergent",
        batch_executor=looping_executor,
    )
    pending = [item for item in looped.item_results if item.skip_reason == SKIP_NONCONVERGENT]
    if not pending or any(item.status != INPUT_STATUS_PENDING for item in pending):
        raise ScenarioAssertionError(f"nonconvergent run hid work as skipped: {looped.item_results}")

    chain = [EMAIL_UID, purchase_uid, org_uid]
    return {
        "id": "p03.outputs_revision_fixed_point",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "output_uid_chain": chain,
        "purchase_uid": purchase_uid,
        "org_uid": org_uid,
        "extraction_outputs": [out.to_payload() for out in extraction[0].receipt.outputs],
        "person_before": person_before,
        "person_after": person_after,
        "feedback_generations": result.feedback_generations,
        "capability_markers": list(result.capability_markers),
        "rerun_already_current_or_noop": True,
        "nonconvergent_pending": len(pending),
        "nonconvergent_writes": writes["n"],
        "report_status": result.report.status,
        "schema": runtime.schema,
    }


register(
    Scenario(
        id="p03.outputs_revision_fixed_point",
        suite="p03",
        product_guarantee=(
            "One source receipt creates a purchase, resolves an entity, changes "
            "enrichment, and updates affected warehouse outputs in a bounded run"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p03_outputs,
    )
)
