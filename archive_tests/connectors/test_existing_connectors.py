"""P08-B: Gmail and calendar through the shared SDK. Synthetic provider items only."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.engine_factory import ContainedCanonicalReader, trusted_local_access
from archive_engine.contracts import ArchiveIdentity
from archive_engine.corrections import CorrectionCommandRequest, apply_override
from archive_engine.service import ArchiveEngineService
from archive_sync.adapter_contracts import ADAPTER_SPECS, get_adapter_spec
from archive_sync.adapters.base import FetchedBatch
from archive_sync.adapters.calendar_events import CalendarEventsAdapter, _event_uid
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _message_uid
from archive_sync.connectors.legacy import (
    CALENDAR_CONNECTOR_ID,
    GMAIL_CONNECTOR_ID,
    adapter_for_source,
    adapter_spec_for,
    declaration_for_calendar,
    declaration_for_gmail,
    run_legacy_connector,
)
from archive_sync.connectors.registry import known_connectors
from archive_sync.source_updaters.declarations import declaration_for_adapter_source_id
from archive_sync.source_updaters.runner import build_adapter
from archive_vault.provenance import PROVENANCE_METHOD_HUMAN
from archive_vault.vault import read_note

ACCOUNT_ALPHA = "alpha@example.test"
ACCOUNT_BETA = "beta@example.test"
SHARED_MESSAGE_ID = "shared-msg-001"
SHARED_EVENT_ID = "shared-event-001"
CURSOR_AFTER = {"history_id": "hist-9", "page_token": None}


class _UidLookup:
    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def resolve_rel_path(self, uid: str) -> str | None:
        return self._mapping.get(uid)


def _identity(vault: Path) -> ArchiveIdentity:
    return ArchiveIdentity(
        archive_id="aid-p08b",
        canonical_root=str(vault.resolve()),
        schema_binding="warehouse:ppa+index_schema_v9",
    )


def _gmail_message_item(*, account: str, message_id: str, subject: str) -> dict[str, object]:
    return {
        "kind": "message",
        "message_id": message_id,
        "thread_id": "thread-shared",
        "account_email": account,
        "created": "2026-03-08",
        "sent_at": "2026-03-08T15:00:00Z",
        "subject": subject,
        "snippet": subject,
        "from_email": "sender@example.test",
        "to_emails": [account],
        "participant_emails": ["sender@example.test", account],
        "body": f"synthetic body for {account}",
    }


def _calendar_item(*, account: str, event_id: str, title: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "calendar_id": "primary",
        "account_email": account,
        "title": title,
        "start_at": "2026-03-08T15:00:00Z",
        "end_at": "2026-03-08T16:00:00Z",
        "source_messages": [],
    }


def _stub_batches(adapter, items: list[dict[str, object]], cursor_patch: dict[str, object] | None = None) -> None:
    patch = dict(cursor_patch or CURSOR_AFTER)

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


def test_gmail_and_calendar_are_registered() -> None:
    assert GMAIL_CONNECTOR_ID in known_connectors()
    assert CALENDAR_CONNECTOR_ID in known_connectors()


def test_dispatch_and_specs_come_from_legacy() -> None:
    assert type(adapter_for_source(GMAIL_CONNECTOR_ID)).__name__ == "GmailMessagesAdapter"
    assert type(adapter_for_source(CALENDAR_CONNECTOR_ID)).__name__ == "CalendarEventsAdapter"
    assert type(build_adapter(GMAIL_CONNECTOR_ID)).__name__ == "GmailMessagesAdapter"
    assert type(build_adapter(CALENDAR_CONNECTOR_ID)).__name__ == "CalendarEventsAdapter"
    assert get_adapter_spec(GMAIL_CONNECTOR_ID) == ADAPTER_SPECS[GMAIL_CONNECTOR_ID]
    assert get_adapter_spec(CALENDAR_CONNECTOR_ID) == ADAPTER_SPECS[CALENDAR_CONNECTOR_ID]
    assert adapter_spec_for(GMAIL_CONNECTOR_ID) == ADAPTER_SPECS[GMAIL_CONNECTOR_ID]
    assert declaration_for_adapter_source_id(GMAIL_CONNECTOR_ID, scope=ACCOUNT_ALPHA) == declaration_for_gmail(
        ACCOUNT_ALPHA
    )
    assert declaration_for_adapter_source_id(CALENDAR_CONNECTOR_ID, scope=ACCOUNT_ALPHA) == declaration_for_calendar(
        ACCOUNT_ALPHA
    )


def test_old_to_card_uid_matches_sdk_persist(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    item = _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Project")
    card, _, _ = adapter.to_card(item)
    _stub_batches(adapter, [item])
    result = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        account_email=ACCOUNT_ALPHA,
        run_id="p08b-compare",
    )
    assert result.created_count == 1
    assert result.uids == (card.uid,)
    assert card.uid == _message_uid(ACCOUNT_ALPHA, SHARED_MESSAGE_ID)
    assert result.persists[0].rel_path.endswith(f"{card.uid}.md")


def test_two_accounts_same_provider_id_stay_distinct(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    items = [
        _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Alpha"),
        _gmail_message_item(account=ACCOUNT_BETA, message_id=SHARED_MESSAGE_ID, subject="Beta"),
    ]
    old_uids = {adapter.to_card(item)[0].uid for item in items}
    _stub_batches(adapter, items)
    result = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        run_id="p08b-accounts",
    )
    assert len(result.uids) == 2
    assert set(result.uids) == old_uids
    assert result.uids[0] != result.uids[1]
    scopes = {proposal.identity.account_scope for proposal in result.proposals}
    providers = {proposal.identity.provider_object_id for proposal in result.proposals}
    assert scopes == {ACCOUNT_ALPHA, ACCOUNT_BETA}
    assert providers == {SHARED_MESSAGE_ID}


def test_calendar_two_accounts_same_event_id(vault: Path) -> None:
    adapter = CalendarEventsAdapter()
    items = [
        _calendar_item(account=ACCOUNT_ALPHA, event_id=SHARED_EVENT_ID, title="Alpha event"),
        _calendar_item(account=ACCOUNT_BETA, event_id=SHARED_EVENT_ID, title="Beta event"),
    ]
    old_uids = {adapter.to_card(item)[0].uid for item in items}
    assert old_uids == {
        _event_uid(ACCOUNT_ALPHA, "primary", SHARED_EVENT_ID),
        _event_uid(ACCOUNT_BETA, "primary", SHARED_EVENT_ID),
    }
    _stub_batches(adapter, items, {"sync_token": "sync-9", "page_token": None})
    result = run_legacy_connector(
        CALENDAR_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        run_id="p08b-cal",
    )
    assert set(result.uids) == old_uids
    assert result.created_count == 2


def test_replay_same_uid_no_duplicate(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    item = _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Replay")
    _stub_batches(adapter, [item])
    first = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        run_id="p08b-first",
    )
    replay = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        run_id="p08b-replay",
    )
    assert first.uids == replay.uids
    assert replay.created_count == 0
    files = list((vault / "Email").rglob("*.md"))
    assert len(files) == 1


def test_cursor_commits_after_persist(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    item = _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Cursor")
    _stub_batches(adapter, [item], CURSOR_AFTER)
    result = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        cursor={"history_id": None},
        run_id="p08b-cursor",
    )
    assert result.created_count == 1
    assert result.committed_cursor == CURSOR_AFTER
    assert result.proposed_cursor == CURSOR_AFTER


def test_runtime_emits_change_records_and_receipts(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    item = _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Receipts")
    _stub_batches(adapter, [item])
    result = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=_identity(vault),
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        run_id="p08b-receipts",
    )
    assert result.pending_p02_wiring == "wired"
    assert result.pending_p03_wiring == "wired"
    assert len(result.changes) == 1
    assert result.changes[0].committed is True
    assert result.changes[0].uid == result.uids[0]
    assert result.changes[0].account == ACCOUNT_ALPHA
    assert len(result.receipts) == 1
    assert result.receipts[0].processor == f"connector.{GMAIL_CONNECTOR_ID}"
    assert result.receipts[0].status == "completed"
    assert result.receipts[0].input_uid == result.uids[0]


def test_manual_correction_survives_sdk_replay(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    item = _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Provider subject")
    _stub_batches(adapter, [item])
    identity = _identity(vault)
    access = trusted_local_access("aid-p08b")
    first = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=identity,
        access=access,
        vault=vault,
        adapter=adapter,
        run_id="p08b-before-correction",
    )
    persist = first.persists[0]
    apply_override(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=persist.uid,
            field="subject",
            value="Corrected subject",
            author="p08b",
            reason="manual title correction",
            rel_path=persist.rel_path,
        ),
    )
    replay = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=identity,
        access=access,
        vault=vault,
        adapter=adapter,
        run_id="p08b-after-correction",
    )
    assert replay.uids == first.uids
    frontmatter, _, provenance = read_note(vault, persist.rel_path)
    assert frontmatter["subject"] == "Corrected subject"
    assert provenance["subject"].method == PROVENANCE_METHOD_HUMAN
    assert frontmatter["source"] == ["gmail.message"]


def test_engine_can_read_sdk_card(vault: Path) -> None:
    adapter = GmailMessagesAdapter()
    item = _gmail_message_item(account=ACCOUNT_ALPHA, message_id=SHARED_MESSAGE_ID, subject="Readable")
    _stub_batches(adapter, [item])
    identity = _identity(vault)
    result = run_legacy_connector(
        GMAIL_CONNECTOR_ID,
        identity=identity,
        access=trusted_local_access("aid-p08b"),
        vault=vault,
        adapter=adapter,
        run_id="p08b-read",
    )
    persist = result.persists[0]
    service = ArchiveEngineService(
        identity=identity,
        access=trusted_local_access("aid-p08b"),
        lookup=_UidLookup({persist.uid: persist.rel_path}),
        reader=ContainedCanonicalReader(vault),
    )
    read = service.read_exact(persist.uid)
    assert read.found is True
    assert persist.uid in read.content
    assert "email_message" in read.content


def test_adapters_opt_into_sdk() -> None:
    assert GmailMessagesAdapter.uses_connector_sdk is True
    assert CalendarEventsAdapter.uses_connector_sdk is True


def test_no_live_google_in_legacy_module() -> None:
    text = (
        Path(__file__)
        .resolve()
        .parents[2]
        .joinpath("archive_sync", "connectors", "legacy.py")
        .read_text(encoding="utf-8")
    )
    assert "gmail.googleapis" not in text
    assert "accounts.google.com" not in text
    assert "COPY " not in text
    assert "psycopg" not in text
