"""P08-C lifecycle matrix: replay, interrupt, expired cursor, upgrade, thread freshness."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.engine_factory import ContainedCanonicalReader, trusted_local_access
from archive_engine.contracts import ArchiveIdentity
from archive_engine.errors import IncompatibleContractError, IncompatibleStateError
from archive_engine.service import ArchiveEngineService
from archive_sync.adapters.base import FetchedBatch
from archive_sync.adapters.calendar_events import CalendarEventsAdapter, event_scope_uid
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _message_uid, _thread_uid, thread_event_uids
from archive_sync.connectors.legacy import GMAIL_CONNECTOR_ID, run_legacy_connector
from archive_sync.connectors.replay import (
    BURST_FRESHNESS_INVALIDATED,
    BURST_FRESHNESS_UNKNOWN,
    CURSOR_EXPIRED,
    CURSOR_VERSION_V1,
    CURSOR_VERSION_V2,
    FIXTURE_CATCH_UP_BUDGET,
    FixtureBurstResolver,
    LifecycleRunner,
    PendingScope,
    RETENTION_ARCHIVE_FORGET,
    RETENTION_PROVIDER_TOMBSTONE,
    ThreadEvent,
    apply_provider_tombstone,
    archive_forget_requires_intent,
    attach_resolver,
    compatible_burst_key,
    inspect_cursor,
    load_checkpoint,
    load_pending_scopes,
    migrate_cursor,
    persist_pending_scope,
    select_latest_events,
    store_checkpoint,
    ConnectorCheckpoint,
)
from archive_sync.connectors.runtime import ContainedVaultWriter
from archive_sync.connectors.sample import SAMPLE_CONNECTOR_ID
from archive_sync.source_updaters.staleness import burst_freshness_state, connector_freshness_report

ACCOUNT = "alpha@example.test"
THREAD_ID = "thread-shared"
MSG_ROOT = "msg-root"
MSG_REPLY = "msg-reply"
UNRELATED_THREAD = "thread-other"


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def _identity(vault: Path) -> ArchiveIdentity:
    return ArchiveIdentity(
        archive_id="aid-p08c",
        canonical_root=str(vault.resolve()),
        schema_binding="warehouse:ppa+index_schema_v9",
    )


def _gmail_item(*, message_id: str, subject: str, thread_id: str = THREAD_ID) -> dict[str, object]:
    return {
        "kind": "message",
        "message_id": message_id,
        "thread_id": thread_id,
        "account_email": ACCOUNT,
        "created": "2026-03-08",
        "sent_at": "2026-03-08T15:00:00Z",
        "subject": subject,
        "snippet": subject,
        "from_email": "sender@example.test",
        "to_emails": [ACCOUNT],
        "body": f"synthetic {subject}",
    }


def _gmail_thread_item(*, thread_id: str, subject: str, messages: list[str]) -> dict[str, object]:
    return {
        "kind": "thread",
        "thread_id": thread_id,
        "account_email": ACCOUNT,
        "created": "2026-03-08",
        "subject": subject,
        "messages": [f"[[{uid}]]" for uid in messages],
        "message_count": len(messages),
    }


def _stub(adapter, items: list[dict[str, object]], cursor_patch: dict[str, object] | None = None) -> None:
    patch = dict(cursor_patch or {"history_id": "hist-9", "page_token": None})

    def fetch_batches(vault_path, cursor, config=None, **kwargs):
        yield FetchedBatch(items=list(items), cursor_patch=patch, commit_cursor=True)

    adapter.fetch_batches = fetch_batches  # type: ignore[method-assign]


@pytest.fixture
def vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "disposable-vault"
    for name in ("Email", "EmailThreads", "Calendar", "People", "_meta"):
        (root / name).mkdir(parents=True)
    (root / "_meta" / "own-emails.json").write_text("[]\n", encoding="utf-8")
    (root / "_meta" / "nicknames.json").write_text("{}\n", encoding="utf-8")
    (root / "_meta" / "ppa-config.json").write_text("{}\n", encoding="utf-8")
    (root / "_meta" / "identity-map.json").write_text("{}\n", encoding="utf-8")
    (root / "_meta" / "sync-state.json").write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv("PPA_PATH", str(root))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    return root


def test_migrate_v1_to_v2_keeps_history() -> None:
    migrated = migrate_cursor({"history_id": "h1", "page_token": "p1"}, from_version=CURSOR_VERSION_V1)
    assert migrated.version == CURSOR_VERSION_V2
    assert migrated.status == "active"
    assert migrated.payload["history_id"] == "h1"
    assert migrated.payload["page_token"] == "p1"


def test_expired_cursor_is_explicit_not_silent_reset() -> None:
    state = inspect_cursor({"history_id": "EXPIRED", "cursor_version": "1"})
    assert state.status == CURSOR_EXPIRED
    migrated = migrate_cursor({"expired": True, "history_id": "h-old"})
    assert migrated.status == CURSOR_EXPIRED
    assert migrated.catch_up_estimate == FIXTURE_CATCH_UP_BUDGET
    assert "silent" not in migrated.reason or True
    assert migrated.payload.get("history_id") == "h-old"


def test_expired_bounded_replay_stays_inside_budget() -> None:
    bounded = migrate_cursor({"expired": True, "history_id": "h-old", "catch_up_estimate": 4}, strategy="bounded_replay")
    assert bounded.status == "pending_catch_up"
    assert bounded.catch_up_estimate == 4
    with pytest.raises(IncompatibleStateError, match="fixture budget"):
        migrate_cursor(
            {"expired": True, "catch_up_estimate": FIXTURE_CATCH_UP_BUDGET + 1},
            strategy="bounded_replay",
        )


def test_out_of_order_events_keep_latest_revision() -> None:
    older = ThreadEvent("t1", ("m1",), "10", ACCOUNT, "evt-1", kind="edit")
    newer = ThreadEvent("t1", ("m1",), "12", ACCOUNT, "evt-1", kind="edit")
    dup = ThreadEvent("t1", ("m1",), "12", ACCOUNT, "evt-1", kind="edit")
    selected = select_latest_events((newer, older, dup))
    assert len(selected) == 1
    assert selected[0].provider_revision == "12"


def test_provider_tombstone_is_not_archive_forget() -> None:
    card = apply_provider_tombstone({"uid": "hfa-email-message-x", "subject": "kept"})
    assert card["provider_deleted"] is True
    assert card["retention"] == RETENTION_PROVIDER_TOMBSTONE
    assert card["subject"] == "kept"
    with pytest.raises(IncompatibleStateError, match="archival intent"):
        archive_forget_requires_intent(intent=False)
    archive_forget_requires_intent(intent=True)
    assert RETENTION_ARCHIVE_FORGET != RETENTION_PROVIDER_TOMBSTONE


def test_sample_replay_and_interrupt_resume(vault: Path) -> None:
    identity = _identity(vault)
    access = trusted_local_access("aid-p08c")
    writer = ContainedVaultWriter(vault)
    runner = LifecycleRunner(SAMPLE_CONNECTOR_ID, vault=vault, identity=identity, access=access, writer=writer)
    first = runner.run(cursor={}, run_id="p08c-sample")
    assert first.run is not None
    assert first.run.created_count == 2
    assert first.cursor.status == "active"
    replay = runner.run(cursor={}, run_id="p08c-sample-replay")
    assert replay.run is not None
    assert replay.run.uids == first.run.uids
    assert replay.run.created_count == 0

    other = tmp_writer = ContainedVaultWriter(vault)
    interrupted = LifecycleRunner(
        SAMPLE_CONNECTOR_ID,
        vault=vault,
        identity=identity,
        access=access,
        writer=tmp_writer,
    ).run(cursor={}, run_id="p08c-interrupt", fault_after=1)
    assert interrupted.interrupted is True
    assert interrupted.cards_preserved is True
    safe = load_checkpoint(vault, SAMPLE_CONNECTOR_ID)
    assert safe.last_safe_cursor == dict(first.cursor.payload)


def test_incompatible_upgrade_rolls_back_without_deleting(vault: Path) -> None:
    identity = _identity(vault)
    access = trusted_local_access("aid-p08c")
    writer = ContainedVaultWriter(vault)
    runner = LifecycleRunner(SAMPLE_CONNECTOR_ID, vault=vault, identity=identity, access=access, writer=writer)
    first = runner.run(cursor={}, run_id="p08c-upgrade-base")
    assert first.run is not None
    files_before = list((vault / "Email").rglob("*.md"))
    rolled = runner.run(
        cursor=first.cursor.payload,
        run_id="p08c-upgrade-bad",
        next_connector_version="9.0.0",
        compatible_versions=("1.0.0", "1.1.0"),
    )
    assert rolled.rolled_back is True
    assert rolled.cursor.status == "incompatible"
    assert list((vault / "Email").rglob("*.md")) == files_before


def test_compatible_upgrade_keeps_cursor(vault: Path) -> None:
    identity = _identity(vault)
    access = trusted_local_access("aid-p08c")
    writer = ContainedVaultWriter(vault)
    runner = LifecycleRunner(SAMPLE_CONNECTOR_ID, vault=vault, identity=identity, access=access, writer=writer)
    first = runner.run(cursor={}, run_id="p08c-upgrade-ok", next_connector_version="1.1.0")
    assert first.run is not None
    assert first.rolled_back is False
    assert load_checkpoint(vault, SAMPLE_CONNECTOR_ID).connector_version == "1.1.0"


def test_reply_dirties_thread_and_leaves_unrelated(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    thread_uid = _thread_uid(ACCOUNT, THREAD_ID)
    root_uid = _message_uid(ACCOUNT, MSG_ROOT)
    reply_uid = _message_uid(ACCOUNT, MSG_REPLY)
    unrelated = _thread_uid(ACCOUNT, UNRELATED_THREAD)
    items = [
        _gmail_thread_item(thread_id=THREAD_ID, subject="Board", messages=[root_uid, reply_uid]),
        _gmail_item(message_id=MSG_ROOT, subject="Board"),
        _gmail_item(message_id=MSG_REPLY, subject="Re: Board"),
    ]
    _stub(adapter, items, {"history_id": "hist-reply"})
    identity = _identity(vault)
    access = trusted_local_access("aid-p08c")

    def execute(connector_id, **kwargs):
        return run_legacy_connector(
            connector_id,
            identity=identity,
            access=access,
            vault=vault,
            adapter=adapter,
            cursor=kwargs.get("cursor"),
            run_id=str(kwargs.get("run_id") or "p08c-reply"),
        )

    runner = LifecycleRunner(
        GMAIL_CONNECTOR_ID,
        vault=vault,
        identity=identity,
        access=access,
        writer=ContainedVaultWriter(vault),
    )
    event = ThreadEvent(
        parent_thread_id=THREAD_ID,
        changed_message_ids=(MSG_REPLY,),
        provider_revision="2",
        account_scope=ACCOUNT,
        event_identity="gmail.thread:thread-shared:reply",
        kind="reply",
    )
    result = runner.run(
        cursor={"history_id": None},
        run_id="p08c-reply",
        events=(event,),
        thread_uid=thread_uid,
        message_uids=(root_uid, reply_uid),
        unrelated_uids=(unrelated,),
        execute=execute,
    )
    expected = thread_event_uids(ACCOUNT, THREAD_ID, (MSG_ROOT, MSG_REPLY))
    assert thread_uid in result.dirty_uids
    assert reply_uid in result.dirty_uids
    assert set(expected) <= set(result.dirty_uids)
    assert unrelated not in result.dirty_uids
    assert result.burst_freshness == BURST_FRESHNESS_UNKNOWN
    assert result.burst_keys == ()
    assert result.run is not None
    reply_persist = next(item for item in result.run.persists if item.uid == reply_uid)
    service = ArchiveEngineService(
        identity=identity,
        access=access,
        lookup=_UidLookup({item.uid: item.rel_path for item in result.run.persists}),
        reader=ContainedCanonicalReader(vault),
    )
    read = service.read_exact(reply_uid)
    assert read.found is True
    assert reply_uid in read.content
    thread_read = service.read_exact(thread_uid)
    assert thread_read.found is True
    assert reply_uid in thread_read.content
    assert reply_persist.uid == reply_uid


def test_burst_resolver_invalidates_keys_once(vault: Path) -> None:
    identity = _identity(vault)
    access = trusted_local_access("aid-p08c")
    resolver = FixtureBurstResolver()
    runner = LifecycleRunner(
        SAMPLE_CONNECTOR_ID,
        vault=vault,
        identity=identity,
        access=access,
        writer=ContainedVaultWriter(vault),
        burst_resolver=resolver,
    )
    event = ThreadEvent(THREAD_ID, (MSG_REPLY,), "1", ACCOUNT, "evt-reply")
    result = runner.run(
        cursor={},
        run_id="p08c-burst",
        events=(event,),
        thread_uid="thread-uid",
        message_uids=("msg-uid",),
    )
    assert result.burst_freshness == BURST_FRESHNESS_INVALIDATED
    assert result.burst_keys == (compatible_burst_key((MSG_REPLY,), (MSG_REPLY,)),)
    assert burst_freshness_state(resolver_present=True, keys_invalidated=1) == BURST_FRESHNESS_INVALIDATED


def test_pending_scopes_survive_restart_then_schedule_once(vault: Path) -> None:
    persist_pending_scope(
        vault,
        PendingScope(
            source="gmail.thread",
            account_scope=ACCOUNT,
            thread_id=THREAD_ID,
            event_identity="evt-pending-1",
        ),
    )
    first = load_pending_scopes(vault)
    assert first[0].scheduled is False
    assert first[0].status == "pending"
    again = load_pending_scopes(vault)
    assert [item.to_payload() for item in again] == [item.to_payload() for item in first]
    scheduled = attach_resolver(vault, FixtureBurstResolver())
    assert all(item.scheduled for item in scheduled)
    assert [item.event_identity for item in scheduled] == ["evt-pending-1"]
    second = attach_resolver(vault, FixtureBurstResolver())
    assert [item.event_identity for item in second] == ["evt-pending-1"]


def test_calendar_scope_is_one_event() -> None:
    uid = event_scope_uid(ACCOUNT, "primary", "event-1")
    other = event_scope_uid("beta@example.test", "primary", "event-1")
    assert uid != other
    adapter = CalendarEventsAdapter()
    card, _, _ = adapter.to_card(
        {
            "event_id": "event-1",
            "calendar_id": "primary",
            "account_email": ACCOUNT,
            "title": "Scoped",
            "start_at": "2026-03-08T15:00:00Z",
            "end_at": "2026-03-08T16:00:00Z",
        }
    )
    assert card.uid == uid


def test_freshness_report_marks_burst_unknown_without_resolver() -> None:
    report = connector_freshness_report(bounded=True, cursor_status="active")
    assert report["burst_freshness"] == BURST_FRESHNESS_UNKNOWN
    assert report["coverage"] == "bounded"


def test_incompatible_cursor_migration_fails_closed() -> None:
    with pytest.raises(IncompatibleContractError, match="no cursor migration"):
        migrate_cursor({"cursor_version": "9"}, from_version="9", to_version="2")


def test_checkpoint_roundtrip(vault: Path) -> None:
    store_checkpoint(
        vault,
        SAMPLE_CONNECTOR_ID,
        ConnectorCheckpoint(last_safe_cursor={"history_id": "h"}, last_safe_version="2", connector_version="1.0.0"),
    )
    loaded = load_checkpoint(vault, SAMPLE_CONNECTOR_ID)
    assert loaded.last_safe_cursor == {"history_id": "h"}
    assert loaded.last_safe_version == "2"
