"""P08-C: reply refreshes thread context; lifecycle matrix stays fail-closed."""

from __future__ import annotations

import time
from typing import Any

from archive_cli.engine_factory import ContainedCanonicalReader, resolve_archive_identity, trusted_local_access
from archive_engine.service import ArchiveEngineService
from archive_sync.adapters.base import FetchedBatch
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _message_uid, _thread_uid, thread_event_uids
from archive_sync.connectors.legacy import GMAIL_CONNECTOR_ID, run_legacy_connector
from archive_sync.connectors.replay import (
    BURST_FRESHNESS_UNKNOWN,
    CURSOR_EXPIRED,
    RETENTION_PROVIDER_TOMBSTONE,
    FixtureBurstResolver,
    LifecycleRunner,
    PendingScope,
    ThreadEvent,
    apply_provider_tombstone,
    attach_resolver,
    inspect_cursor,
    migrate_cursor,
    persist_pending_scope,
    select_latest_events,
)
from archive_sync.connectors.runtime import ContainedVaultWriter
from archive_sync.connectors.sample import SAMPLE_CONNECTOR_ID
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.acceptance.scenarios.baseline import ScenarioAssertionError

ACCOUNT = "alpha@example.test"
THREAD_ID = "thread-shared"
MSG_ROOT = "msg-root"
MSG_REPLY = "msg-reply"
UNRELATED = "thread-other"


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def _item(message_id: str, subject: str) -> dict[str, object]:
    return {
        "kind": "message",
        "message_id": message_id,
        "thread_id": THREAD_ID,
        "account_email": ACCOUNT,
        "created": "2026-03-08",
        "sent_at": "2026-03-08T15:00:00Z",
        "subject": subject,
        "body": subject,
    }


def _thread(messages: list[str]) -> dict[str, object]:
    return {
        "kind": "thread",
        "thread_id": THREAD_ID,
        "account_email": ACCOUNT,
        "created": "2026-03-08",
        "subject": "Board",
        "messages": [f"[[{uid}]]" for uid in messages],
        "message_count": len(messages),
    }


def run_p08_lifecycle(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    identity = resolve_archive_identity(runtime.vault, schema_binding="warehouse:ppa+index_schema_v9")
    access = trusted_local_access(identity.archive_id)

    v1 = migrate_cursor({"history_id": "h1", "page_token": None}, from_version="1")
    if v1.version != "2" or v1.payload.get("history_id") != "h1":
        raise ScenarioAssertionError(f"cursor migration lost history: {v1.to_payload()!r}")
    expired = inspect_cursor({"expired": True, "history_id": "h1"})
    if expired.status != CURSOR_EXPIRED:
        raise ScenarioAssertionError("expired cursor was not explicit")

    older = ThreadEvent(THREAD_ID, (MSG_REPLY,), "1", ACCOUNT, "evt-1")
    newer = ThreadEvent(THREAD_ID, (MSG_REPLY,), "3", ACCOUNT, "evt-1")
    if select_latest_events((newer, older))[0].provider_revision != "3":
        raise ScenarioAssertionError("out-of-order events did not keep the latest revision")
    tombstoned = apply_provider_tombstone({"uid": "keep-me", "body": "evidence"})
    if tombstoned.get("retention") != RETENTION_PROVIDER_TOMBSTONE or "body" not in tombstoned:
        raise ScenarioAssertionError("provider tombstone erased evidence")

    sample_vault = runtime.root / "p08c-sample"
    init_vault(sample_vault, owned_root=runtime.root)
    sample_identity = resolve_archive_identity(sample_vault, schema_binding="warehouse:ppa+index_schema_v9")
    sample_access = trusted_local_access(sample_identity.archive_id)
    writer = ContainedVaultWriter(sample_vault)
    sample = LifecycleRunner(
        SAMPLE_CONNECTOR_ID,
        vault=sample_vault,
        identity=sample_identity,
        access=sample_access,
        writer=writer,
    )
    first = sample.run(cursor={}, run_id="p08c-accept-sample")
    if first.run is None or first.run.created_count != 2:
        raise ScenarioAssertionError("sample lifecycle write failed")
    replay = sample.run(cursor={}, run_id="p08c-accept-replay")
    if replay.run is None or replay.run.created_count != 0 or replay.run.uids != first.run.uids:
        raise ScenarioAssertionError("replay duplicated or reminted UIDs")
    interrupted = sample.run(cursor={}, run_id="p08c-accept-interrupt", fault_after=1)
    if not interrupted.interrupted or not interrupted.cards_preserved:
        raise ScenarioAssertionError("interrupt lost cards or committed a cursor")
    rolled = sample.run(cursor=first.cursor.payload, run_id="p08c-accept-bad", next_connector_version="9.0.0")
    if not rolled.rolled_back or rolled.cursor.status != "incompatible":
        raise ScenarioAssertionError("incompatible upgrade did not roll back")

    thread_uid = _thread_uid(ACCOUNT, THREAD_ID)
    root_uid = _message_uid(ACCOUNT, MSG_ROOT)
    reply_uid = _message_uid(ACCOUNT, MSG_REPLY)
    unrelated = _thread_uid(ACCOUNT, UNRELATED)
    adapter = GmailMessagesAdapter()
    items = [
        _thread([root_uid, reply_uid]),
        _item(MSG_ROOT, "Board"),
        _item(MSG_REPLY, "Re: Board"),
    ]

    def fetch_batches(vault_path, cursor, config=None, **kwargs):
        yield FetchedBatch(items=list(items), cursor_patch={"history_id": "hist-reply"}, commit_cursor=True)

    adapter.fetch_batches = fetch_batches  # type: ignore[method-assign]

    def execute(connector_id, **kwargs):
        return run_legacy_connector(
            connector_id,
            identity=identity,
            access=access,
            vault=runtime.vault,
            adapter=adapter,
            cursor=kwargs.get("cursor"),
            run_id=str(kwargs.get("run_id") or "p08c-accept-reply"),
        )

    gmail = LifecycleRunner(
        GMAIL_CONNECTOR_ID,
        vault=runtime.vault,
        identity=identity,
        access=access,
        writer=ContainedVaultWriter(runtime.vault),
    )
    reply = gmail.run(
        cursor={"history_id": None},
        run_id="p08c-accept-reply",
        events=(
            ThreadEvent(
                parent_thread_id=THREAD_ID,
                changed_message_ids=(MSG_REPLY,),
                provider_revision="2",
                account_scope=ACCOUNT,
                event_identity="gmail.thread:thread-shared:reply",
                kind="reply",
            ),
        ),
        thread_uid=thread_uid,
        message_uids=(root_uid, reply_uid),
        unrelated_uids=(unrelated,),
        execute=execute,
    )
    expected = thread_event_uids(ACCOUNT, THREAD_ID, (MSG_ROOT, MSG_REPLY))
    if reply.burst_freshness != BURST_FRESHNESS_UNKNOWN:
        raise ScenarioAssertionError(f"burst freshness should be unknown without resolver: {reply.burst_freshness}")
    if unrelated in reply.dirty_uids or not set(expected) <= set(reply.dirty_uids):
        raise ScenarioAssertionError(f"dirty UIDs wrong: {reply.dirty_uids!r}")
    if reply.run is None:
        raise ScenarioAssertionError("gmail reply run produced no persist")
    service = ArchiveEngineService(
        identity=identity,
        access=access,
        lookup=_UidLookup({item.uid: item.rel_path for item in reply.run.persists}),
        reader=ContainedCanonicalReader(runtime.vault),
    )
    thread_read = service.read_exact(thread_uid)
    reply_read = service.read_exact(reply_uid)
    if not thread_read.found or reply_uid not in thread_read.content:
        raise ScenarioAssertionError("reply is not visible inside the thread card")
    if not reply_read.found:
        raise ScenarioAssertionError("reply card was not readable")

    persist_pending_scope(
        runtime.vault,
        PendingScope(source="gmail.thread", account_scope=ACCOUNT, thread_id=THREAD_ID, event_identity="evt-pending"),
    )
    scheduled = attach_resolver(runtime.vault, FixtureBurstResolver())
    pending = next((item for item in scheduled if item.event_identity == "evt-pending"), None)
    if pending is None or not pending.scheduled:
        raise ScenarioAssertionError(
            f"pending scope was not scheduled once: {[item.to_payload() for item in scheduled]!r}"
        )

    return {
        "id": "p08.lifecycle_replay",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "old_cursor": {"history_id": "h1", "page_token": None, "cursor_version": "1"},
        "new_cursor": v1.to_payload(),
        "dirty_uids": list(reply.dirty_uids),
        "unrelated_untouched": unrelated not in reply.dirty_uids,
        "thread_uid": thread_uid,
        "reply_uid": reply_uid,
        "burst_freshness": reply.burst_freshness,
        "expired_status": expired.status,
        "replay_created_count": replay.run.created_count if replay.run else -1,
        "interrupted": interrupted.interrupted,
        "rolled_back": rolled.rolled_back,
        "tombstone_retention": tombstoned.get("retention"),
        "scheduled_event_identities": [item.event_identity for item in scheduled],
        "matrix": {
            "replay": "pass",
            "interrupt": "pass",
            "reorder": "pass",
            "expired_cursor": "pass",
            "upgrade_rollback": "pass",
            "thread_reply": "pass",
            "unrelated_untouched": "pass",
            "tombstone_vs_forget": "pass",
            "burst_freshness_unknown": "pass",
            "pending_scope_schedule_once": "pass",
        },
        "proof_note": (
            "Reply dirties the thread and reply UIDs. Burst keys stay unknown until a "
            "resolver is attached. Expired cursors and incompatible upgrades fail closed "
            "without deleting cards."
        ),
    }


register(
    Scenario(
        id="p08.lifecycle_replay",
        suite="p08",
        product_guarantee=(
            "A reply is visible in its thread; replay/interrupt/expired cursor/upgrade "
            "do not duplicate or delete history; burst freshness is unknown without a resolver"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p08_lifecycle,
    )
)
