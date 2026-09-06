"""P08-B: existing Gmail/calendar connectors through the SDK on synthetic items."""

from __future__ import annotations

import time
from typing import Any

from archive_cli.engine_factory import ContainedCanonicalReader, resolve_archive_identity, trusted_local_access
from archive_engine.corrections import CorrectionCommandRequest, apply_override
from archive_engine.service import ArchiveEngineService
from archive_sync.adapters.base import FetchedBatch
from archive_sync.adapters.calendar_events import CalendarEventsAdapter, _event_uid
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _message_uid
from archive_sync.connectors.legacy import CALENDAR_CONNECTOR_ID, GMAIL_CONNECTOR_ID, run_legacy_connector
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.acceptance.scenarios.baseline import ScenarioAssertionError
from archive_vault.provenance import PROVENANCE_METHOD_HUMAN
from archive_vault.vault import read_note

ACCOUNT_ALPHA = "alpha@example.test"
ACCOUNT_BETA = "beta@example.test"
SHARED_MESSAGE_ID = "shared-msg-001"
SHARED_EVENT_ID = "shared-event-001"


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def _gmail_item(*, account: str, subject: str) -> dict[str, object]:
    return {
        "kind": "message",
        "message_id": SHARED_MESSAGE_ID,
        "thread_id": "thread-shared",
        "account_email": account,
        "created": "2026-03-08",
        "sent_at": "2026-03-08T15:00:00Z",
        "subject": subject,
        "snippet": subject,
        "from_email": "sender@example.test",
        "to_emails": [account],
        "body": f"synthetic body for {account}",
    }


def _calendar_item(*, account: str, title: str) -> dict[str, object]:
    return {
        "event_id": SHARED_EVENT_ID,
        "calendar_id": "primary",
        "account_email": account,
        "title": title,
        "start_at": "2026-03-08T15:00:00Z",
        "end_at": "2026-03-08T16:00:00Z",
    }


def _stub(adapter, items: list[dict[str, object]], cursor_patch: dict[str, object]) -> None:
    def fetch_batches(vault_path, cursor, config=None, **kwargs):
        yield FetchedBatch(items=list(items), cursor_patch=dict(cursor_patch), commit_cursor=True)

    adapter.fetch_batches = fetch_batches  # type: ignore[method-assign]


def run_p08_existing(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    identity = resolve_archive_identity(runtime.vault, schema_binding="warehouse:ppa+index_schema_v9")
    access = trusted_local_access(identity.archive_id)

    gmail_items = [
        _gmail_item(account=ACCOUNT_ALPHA, subject="Provider subject"),
        _gmail_item(account=ACCOUNT_BETA, subject="Other account"),
    ]
    gmail_adapter = GmailMessagesAdapter()
    old_gmail = {item["account_email"]: gmail_adapter.to_card(item)[0].uid for item in gmail_items}
    _stub(gmail_adapter, gmail_items, {"history_id": "hist-9"})
    gmail = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=identity,
        access=access,
        vault=runtime.vault,
        adapter=gmail_adapter,
        run_id="p08b-acceptance-gmail",
    )
    if set(gmail.uids) != set(old_gmail.values()):
        raise ScenarioAssertionError(f"Gmail SDK UIDs drifted from to_card: {gmail.uids!r} vs {old_gmail!r}")
    if old_gmail[ACCOUNT_ALPHA] == old_gmail[ACCOUNT_BETA]:
        raise ScenarioAssertionError("same provider message id collided across accounts")
    if gmail.created_count != 2 or not gmail.changes or not gmail.receipts:
        raise ScenarioAssertionError("Gmail SDK run did not emit creates/changes/receipts")
    if gmail.pending_p02_wiring != "wired" or gmail.pending_p03_wiring != "wired":
        raise ScenarioAssertionError("Gmail P02/P03 wiring is still pending")
    if gmail.committed_cursor != {"history_id": "hist-9"}:
        raise ScenarioAssertionError(f"cursor committed before persist or drifted: {gmail.committed_cursor!r}")

    alpha = next(
        persist
        for persist, proposal in zip(gmail.persists, gmail.proposals)
        if proposal.identity.account_scope == ACCOUNT_ALPHA
    )
    apply_override(
        runtime.vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=alpha.uid,
            field="subject",
            value="Corrected subject",
            author="acceptance.p08b",
            reason="manual title correction",
            rel_path=alpha.rel_path,
        ),
    )
    replay = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=identity,
        access=access,
        vault=runtime.vault,
        adapter=gmail_adapter,
        run_id="p08b-acceptance-replay",
    )
    if replay.uids != gmail.uids or replay.created_count != 0:
        raise ScenarioAssertionError("Gmail replay duplicated or changed UIDs")
    frontmatter, _, provenance = read_note(runtime.vault, alpha.rel_path)
    if frontmatter["subject"] != "Corrected subject":
        raise ScenarioAssertionError(f"correction did not survive replay: {frontmatter['subject']!r}")
    if provenance["subject"].method != PROVENANCE_METHOD_HUMAN:
        raise ScenarioAssertionError("correction provenance was relabeled")

    calendar_adapter = CalendarEventsAdapter()
    cal_items = [
        _calendar_item(account=ACCOUNT_ALPHA, title="Alpha event"),
        _calendar_item(account=ACCOUNT_BETA, title="Beta event"),
    ]
    old_cal = {calendar_adapter.to_card(item)[0].uid for item in cal_items}
    expected_cal = {
        _event_uid(ACCOUNT_ALPHA, "primary", SHARED_EVENT_ID),
        _event_uid(ACCOUNT_BETA, "primary", SHARED_EVENT_ID),
    }
    if old_cal != expected_cal:
        raise ScenarioAssertionError(f"calendar to_card UIDs drifted: {old_cal!r}")
    _stub(calendar_adapter, cal_items, {"sync_token": "sync-9"})
    calendar = run_legacy_connector(
        CALENDAR_CONNECTOR_ID,
        identity=identity,
        access=access,
        vault=runtime.vault,
        adapter=calendar_adapter,
        run_id="p08b-acceptance-calendar",
    )
    if set(calendar.uids) != old_cal:
        raise ScenarioAssertionError(f"calendar SDK UIDs drifted: {calendar.uids!r} vs {old_cal!r}")

    service = ArchiveEngineService(
        identity=identity,
        access=access,
        lookup=_UidLookup({alpha.uid: alpha.rel_path}),
        reader=ContainedCanonicalReader(runtime.vault),
    )
    read = service.read_exact(alpha.uid)
    if not read.found or alpha.uid not in read.content:
        raise ScenarioAssertionError(f"engine exact-read missed {alpha.uid}")

    return {
        "id": "p08.existing_gmail_calendar",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "old_gmail_uids": old_gmail,
        "new_gmail_uids": list(gmail.uids),
        "old_calendar_uids": sorted(old_cal),
        "new_calendar_uids": list(calendar.uids),
        "alpha_uid": alpha.uid,
        "expected_alpha_uid": _message_uid(ACCOUNT_ALPHA, SHARED_MESSAGE_ID),
        "replay_created_count": replay.created_count,
        "corrected_subject": frontmatter["subject"],
        "committed_cursor": dict(gmail.committed_cursor or {}),
        "change_ids": [change.mutation_id for change in gmail.changes],
        "receipt_processors": [receipt.processor for receipt in gmail.receipts],
        "engine_read_found": read.found,
        "p02_wiring": gmail.pending_p02_wiring,
        "p03_wiring": gmail.pending_p03_wiring,
        "proof_note": (
            "Gmail and calendar parse through adapter.to_card; the SDK only wraps "
            "fetch/normalize/persist. Same provider IDs stay account-scoped. "
            "P07 overrides survive replay. Runtime emits ChangeRecords and OutputReceipts."
        ),
    }


register(
    Scenario(
        id="p08.existing_gmail_calendar",
        suite="p08",
        product_guarantee=(
            "Existing Gmail and calendar connectors keep their UIDs through the SDK, "
            "keep two accounts distinct, and preserve a manual correction on replay"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p08_existing,
    )
)
