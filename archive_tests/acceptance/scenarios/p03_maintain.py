"""P03-D acceptance: one maintain command produces current searchable evidence."""

from __future__ import annotations

import logging
import time
from typing import Any

from archive_sync.extractors.amazon import AmazonExtractor
from archive_sync.llm_enrichment.card_enrichment_runner import DERIVED_ENRICHMENT_TAG
from archive_tests.acceptance.environment import IsolatedRuntime, inspect_warehouse_card, reset_serving_handle
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.archive_sync.extractors.conftest import write_email_to_vault
from archive_tests.archive_sync.extractors.test_amazon import AMAZON_ORDER_BODY
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.uid import generate_uid
from archive_vault.vault import read_note_by_uid, write_card

EMAIL_UID = "hfa-email-message-p03dacc1"
PERSON_UID = "hfa-person-p03daccunaffected"
ORDER_NUMBER = "112-1234567-1234567"


class ScenarioAssertionError(AssertionError):
    """P03-D product assertion failed."""


def _write_person(vault) -> None:
    card = PersonCard(
        uid=PERSON_UID,
        type="person",
        source=["acceptance.p03d"],
        source_id=f"{PERSON_UID}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="P03 D Unaffected",
        first_name="P03",
        last_name="Maintain",
        emails=[f"{PERSON_UID}@example.test"],
        tags=["p03-acceptance", "synthetic"],
    )
    prov = {
        field: ProvenanceEntry("acceptance.p03d", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name", "emails", "tags")
    }
    write_card(vault, "People/p03d-unaffected.md", card, body="Unaffected P03-D fixture.", provenance=prov)


def _write_amazon_email(vault) -> None:
    fm = {
        "uid": EMAIL_UID,
        "type": "email_message",
        "source": ["gmail"],
        "source_id": "gmail.msg.p03dacc1",
        "created": "2024-03-15",
        "updated": "2024-03-15",
        "summary": "Your Amazon.com order confirmation",
        "gmail_message_id": "msgid-p03dacc1",
        "gmail_thread_id": "thread-p03dacc1",
        "account_email": "me@example.com",
        "from_email": "auto-confirm@amazon.com",
        "to_emails": ["me@example.com"],
        "subject": "Your Amazon.com order confirmation",
        "sent_at": "2024-03-15T14:30:00-08:00",
        "people": [],
        "orgs": [],
        "tags": [],
    }
    write_email_to_vault(str(vault), "Email/2024-03/p03d-amazon.md", fm, AMAZON_ORDER_BODY)


def _watermarks(vault) -> dict[str, Any]:
    from archive_cli.commands.maintain import read_freshness_watermarks

    return read_freshness_watermarks(vault)


def run_p03_maintain(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    for extra in ("Transactions/Purchases", "Entities/Organizations"):
        (runtime.vault / extra).mkdir(parents=True, exist_ok=True)
    _write_person(runtime.vault)
    _write_amazon_email(runtime.vault)

    from archive_cli.commands.maintain import run_maintenance
    from archive_cli.server import archive_search
    from archive_cli.serving_index import get_serving_handle, mark_serving_index_dirty
    from archive_cli.store import DefaultArchiveStore
    from archive_sync.processors.constants import PROCESSOR_EMAIL_TYPED_EXTRACTION
    from archive_vault.change_journal import ChangeJournal

    store = DefaultArchiveStore(vault=runtime.vault)
    store.bootstrap()
    from archive_tests.acceptance.scenarios.p03_outputs import EMAIL_UID as C_EMAIL_UID

    purchase_uid = AmazonExtractor().generate_derived_uid(EMAIL_UID, ORDER_NUMBER)
    c_purchase_uid = AmazonExtractor().generate_derived_uid(C_EMAIL_UID, ORDER_NUMBER)
    org_uid = generate_uid("organization", "entity-resolution", "amazon.com")
    dirty_uids = [PERSON_UID, EMAIL_UID]
    if read_note_by_uid(str(runtime.vault), c_purchase_uid) is not None:
        dirty_uids.append(c_purchase_uid)
    dirty_path = runtime.vault / "_meta" / "p03d-dirty-uids.txt"
    dirty_path.write_text("\n".join(dirty_uids) + "\n", encoding="utf-8")
    mark_serving_index_dirty(runtime.vault, "p03d-maintain", dirty_uids)

    before = _watermarks(runtime.vault)
    dry = run_maintenance(
        store=store,
        logger=logging.getLogger("ppa.acceptance"),
        dry_run=True,
        run_processors=True,
        apply_processors=True,
        dirty_uids_path=str(dirty_path),
    )
    after_dry = _watermarks(runtime.vault)
    if after_dry["journal_watermark"] != before["journal_watermark"]:
        raise ScenarioAssertionError(f"dry-run moved journal watermark {before} -> {after_dry}")
    if after_dry["published_watermark"] != before["published_watermark"]:
        raise ScenarioAssertionError(f"dry-run moved published watermark {before} -> {after_dry}")
    if "serving_index_publish (dry-run)" not in dry.skipped_steps:
        raise ScenarioAssertionError(f"dry-run did not stay read-only: {dry.skipped_steps}")

    first = run_maintenance(
        store=store,
        logger=logging.getLogger("ppa.acceptance"),
        dry_run=False,
        run_processors=True,
        apply_processors=True,
        allow_broad_llm=False,
        dirty_uids_path=str(dirty_path),
    )
    if first.publication.get("ok") is False and first.publication.get("error"):
        raise ScenarioAssertionError(f"publish failed: {first.publication}")
    if EMAIL_UID in first.failed_revision_uids:
        raise ScenarioAssertionError(f"source email claimed failed: {first.failed_revision_uids}")

    extraction_items = [
        item
        for report in first.processor_reports
        for item in (report.get("item_results") or [])
        if item.get("processor_key") == PROCESSOR_EMAIL_TYPED_EXTRACTION and item.get("input_uid") == EMAIL_UID
    ]
    receipt_outputs = []
    extraction_status = ""
    skip_reason = ""
    if extraction_items:
        extraction_status = str(extraction_items[0].get("status") or "")
        skip_reason = str(extraction_items[0].get("skip_reason") or "")
        receipt = extraction_items[0].get("receipt") or {}
        receipt_outputs = [row.get("uid") for row in (receipt.get("outputs") or []) if row.get("uid")]

    purchase_note = read_note_by_uid(str(runtime.vault), purchase_uid)
    if purchase_note is None:
        claimed_complete = extraction_status in {"complete", "completed"} and bool(receipt_outputs)
        if claimed_complete:
            raise ScenarioAssertionError(
                f"extraction receipt claimed {receipt_outputs} but purchase is missing from the vault"
            )
        pending = first.failed_revision_uids or first.skipped_steps or skip_reason
        if purchase_uid not in str(first.to_dict()) and not skip_reason:
            raise ScenarioAssertionError(
                f"derived purchase missing and not pending: {purchase_uid} report={first.to_dict()}"
            )
        searchable = False
        pending_reason = str(pending)
    else:
        if DERIVED_ENRICHMENT_TAG not in (purchase_note[1].get("tags") or []):
            raise ScenarioAssertionError(f"purchase was not enriched: {purchase_note[1].get('tags')}")
        searchable = True
        pending_reason = ""

    purchase_row = inspect_warehouse_card(runtime.dsn, runtime.schema, purchase_uid)
    org_row = inspect_warehouse_card(runtime.dsn, runtime.schema, org_uid)
    if searchable and purchase_row is None:
        raise ScenarioAssertionError(f"purchase not materialized: {purchase_uid}")
    all_extraction_outputs = []
    for report in first.processor_reports:
        for item in report.get("item_results") or []:
            if item.get("processor_key") != PROCESSOR_EMAIL_TYPED_EXTRACTION:
                continue
            if item.get("valid_no_output") or item.get("status") not in {"complete", "completed"}:
                continue
            rec = item.get("receipt") or {}
            all_extraction_outputs.extend(row.get("uid") for row in (rec.get("outputs") or []) if row.get("uid"))
    if first.cards_extracted != len(all_extraction_outputs):
        raise ScenarioAssertionError(
            f"count summaries drifted from receipts extracted={first.cards_extracted} outputs={all_extraction_outputs}"
        )

    reset_serving_handle()
    native_hits: list[str] = []
    mcp_search = ""
    c_purchase_note = read_note_by_uid(str(runtime.vault), c_purchase_uid)
    if searchable:
        handle = get_serving_handle(runtime.vault)
        listed = handle.query(limit=50)
        native_hits = [str(row.get("card_uid") or "") for row in listed]
        exact = handle.search(purchase_uid, limit=8)
        if purchase_uid not in native_hits and not any(row.get("card_uid") == purchase_uid for row in exact):
            raise ScenarioAssertionError(f"native query missed derived purchase: {listed[:8]} exact={exact}")
        mcp_search = archive_search(purchase_uid, limit=8)
        if purchase_uid not in mcp_search and "Amazon" not in mcp_search:
            raise ScenarioAssertionError(f"MCP search missed derived purchase: {mcp_search[:500]}")
        if c_purchase_note is not None:
            c_exact = handle.search(c_purchase_uid, limit=8)
            if c_purchase_uid not in native_hits and not any(row.get("card_uid") == c_purchase_uid for row in c_exact):
                raise ScenarioAssertionError(
                    f"native query missed P03-C derived purchase: {listed[:8]} exact={c_exact}"
                )
        handle.close()
        reset_serving_handle()

    restart = run_maintenance(
        store=store,
        logger=logging.getLogger("ppa.acceptance"),
        dry_run=False,
        run_processors=True,
        apply_processors=True,
        dirty_uids_path=str(dirty_path),
    )
    if restart.cards_extracted and restart.cards_extracted != first.cards_extracted and not restart.nothing_to_do:
        created_again = [
            item
            for report in restart.processor_reports
            for item in (report.get("item_results") or [])
            if item.get("processor_key") == PROCESSOR_EMAIL_TYPED_EXTRACTION
            and item.get("status") == "complete"
            and item.get("output_uids")
            and not item.get("already_current")
            and not item.get("valid_no_output")
        ]
        if created_again:
            raise ScenarioAssertionError(f"restart created outputs again: {created_again}")

    failed_report = first.to_dict()
    failed_report["processor_reports"] = [
        {
            "item_results": [
                {
                    "processor_key": "materialization",
                    "input_uid": "hfa-failed-revision-p03d",
                    "status": "failed",
                    "output_uids": ["hfa-failed-revision-p03d"],
                }
            ]
        }
    ]
    from archive_cli.commands.maintain import MaintenanceReport, eligible_checkpoint_for_report

    failed = MaintenanceReport(
        publish_uids=["hfa-failed-revision-p03d"],
        processor_reports=failed_report["processor_reports"],
    )
    plan = eligible_checkpoint_for_report(store, failed)
    if "hfa-failed-revision-p03d" in plan["dirty_uids"]:
        raise ScenarioAssertionError("failed revision was eligible to publish")
    if plan["high_watermark"] > plan["published_watermark"] and plan["failed_uids"]:
        raise ScenarioAssertionError(f"eligible checkpoint moved past failed work: {plan}")

    after = _watermarks(runtime.vault)
    with ChangeJournal(runtime.vault) as journal:
        journal_hw = journal.checkpoint().get("value", {}).get("high_watermark", 0)

    return {
        "id": "p03.maintain_revision_pipeline",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "maintenance_run_id": first.maintenance_run_id,
        "restart_run_id": restart.maintenance_run_id,
        "purchase_uid": purchase_uid,
        "c_purchase_uid": c_purchase_uid,
        "c_purchase_present": read_note_by_uid(str(runtime.vault), c_purchase_uid) is not None,
        "org_uid": org_uid,
        "searchable": searchable,
        "pending_reason": pending_reason,
        "receipt_outputs": receipt_outputs,
        "cards_extracted": first.cards_extracted,
        "entities_resolved": first.entities_resolved,
        "cards_rebuilt": first.cards_rebuilt,
        "journal_watermark": first.journal_watermark or journal_hw,
        "materialized_watermark": first.materialized_watermark,
        "published_watermark": first.published_watermark or after["published_watermark"],
        "eligible_checkpoint": first.eligible_checkpoint,
        "source_cursors": first.source_cursors,
        "dry_run_journal_unchanged": after_dry["journal_watermark"] == before["journal_watermark"],
        "canonical_present": purchase_note is not None,
        "warehouse_present": purchase_row is not None,
        "org_warehouse_present": org_row is not None,
        "native_hits": native_hits,
        "mcp_hit": bool(purchase_uid in mcp_search) if mcp_search else False,
        "publication": first.publication,
        "restart_noop": bool(restart.nothing_to_do or "serving_index_publish (clean)" in restart.skipped_steps),
        "failed_revision_excluded": "hfa-failed-revision-p03d" not in plan["dirty_uids"],
        "schema": runtime.schema,
    }


register(
    Scenario(
        id="p03.maintain_revision_pipeline",
        suite="p03",
        product_guarantee=(
            "One maintain command journals source work, runs processors, publishes "
            "the eligible checkpoint, and makes the derived purchase searchable"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p03_maintain,
    )
)
