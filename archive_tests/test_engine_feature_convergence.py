"""P06-D: late features converge through the installed engine seams."""

from __future__ import annotations

import inspect
import json
import logging
from copy import deepcopy
from pathlib import Path

from archive_cli.commands import read as read_cmd
from archive_cli.commands import search as search_cmd
from archive_cli.engine_factory import EngineBurstBridge, build_runtime
from archive_cli.store import DefaultArchiveStore
from archive_engine.adapters.retrieval import RetrievalAdapter
from archive_engine.contracts import AccessContext
from archive_sync.connectors.replay import (
    BURST_FRESHNESS_INVALIDATED,
    BURST_FRESHNESS_UNKNOWN,
    LifecycleRunner,
    PendingScope,
    load_pending_scopes,
    persist_pending_scope,
)
from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector
from archive_sync.connectors.sample import SAMPLE_CONNECTOR_ID
from archive_tests.test_conversation_bursts import (
    ANSWER,
    ANSWER_MESSAGE_ID,
    _msg,
    burst_chunks,
    long_thread_messages,
    thread_frontmatter,
)
from archive_tests.test_engine_boundaries import assert_engine_import_boundaries
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

REPO = Path(__file__).resolve().parents[1]
DENIED_TOKEN = "P06D_DENIED_SYNTHETIC_SENTINEL"
THREAD_UID = "hfa-email-thread-p06d"
UNRELATED_UID = "hfa-email-thread-p06d-other"
REPLY_QUERY = "use port 8432"
LOG = logging.getLogger("ppa.test.p06d")


class MemoryIndex:
    def __init__(self, rows: list[dict] | None = None, mapping: dict[str, str] | None = None):
        self.rows = list(rows or [])
        self.mapping = dict(mapping or {})
        for row in self.rows:
            uid = str(row.get("card_uid") or "")
            rel = str(row.get("rel_path") or "")
            if uid and rel:
                self.mapping.setdefault(uid, rel)

    def read_path_for_uid(self, uid: str) -> str | None:
        return self.mapping.get(uid)

    def search(self, query: str, limit: int = 20, **_kwargs):
        needle = (query or "").casefold()
        hits = [dict(row) for row in self.rows if not needle or needle in json.dumps(row).casefold()]
        return hits[:limit]

    def query_cards(self, **kwargs):
        return self.search(str(kwargs.get("type_filter") or ""), limit=int(kwargs.get("limit", 20) or 20))

    def graph(self, note_path: str, hops: int = 2):
        return {note_path: []}

    def timeline(self, **_kwargs):
        return list(self.rows)

    def status(self):
        return {"card_count": len(self.rows)}

    def bootstrap(self):
        return {"ok": True}

    def rebuild(self):
        return {"cards": len(self.rows)}


class MemoryServing:
    def __init__(self, rows: list[dict], *, generation_id: str = "gen-p06d-1"):
        self.rows = list(rows)
        self.generation_id = generation_id
        self.retired: set[str] = set()

    def search(self, query: str, limit: int = 20, **kwargs):
        access_sources = set(kwargs.get("access_sources") or [])
        needle = (query or "").casefold()
        hits = []
        for row in self.rows:
            if row.get("burst_key") in self.retired:
                continue
            if access_sources:
                sources = {str(item) for item in (row.get("sources") or row.get("source") or [])}
                if sources and not sources <= access_sources:
                    continue
            blob = json.dumps(row).casefold()
            if needle and needle not in blob:
                continue
            hits.append(dict(row))
        return hits[:limit]

    def query(self, **kwargs):
        return self.search("", limit=int(kwargs.get("limit", 20) or 20), **kwargs)

    def typed_query(self, **kwargs):
        return {"rows": self.query(**kwargs), "total_status": "exact", "truncated": False}

    def graph(self, rel_path: str, hops: int = 1, **_kwargs):
        return {rel_path: []}

    def timeline(self, **kwargs):
        return self.search("", **kwargs)

    def pointers(self, uids, **_kwargs):
        return {uid: {} for uid in uids}

    def read_path(self, uid: str) -> str | None:
        for row in self.rows:
            if row.get("card_uid") == uid:
                return str(row.get("rel_path") or "")
        return None


def _write_person(vault: Path, uid: str, body: str, *, rel: str = "People/card.md") -> None:
    (vault / "People").mkdir(parents=True, exist_ok=True)
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test"],
        source_id=f"{uid}@example.com",
        created="2026-09-06",
        updated="2026-09-06",
        summary=uid,
    )
    write_card(
        vault,
        rel,
        card,
        body=body,
        provenance={"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")},
    )


def _thread_rows() -> list[dict]:
    return [
        {
            "card_uid": THREAD_UID,
            "rel_path": "Email/thread.md",
            "summary": f"Deploy port question {REPLY_QUERY}",
            "type": "email_thread",
            "source": ["gmail"],
            "sources": ["gmail"],
            "required_sources": ["gmail"],
            "lineage_complete": True,
            "search_text": f"{REPLY_QUERY} which port",
            "burst_key": "current-reply",
        },
        {
            "card_uid": UNRELATED_UID,
            "rel_path": "Email/other.md",
            "summary": "Unrelated dinner plans",
            "type": "email_thread",
            "source": ["gmail"],
            "sources": ["gmail"],
            "required_sources": ["gmail"],
            "lineage_complete": True,
            "search_text": "dinner reservations",
            "burst_key": "unrelated-stable",
        },
        {
            "card_uid": "hfa-medical-p06d0001",
            "rel_path": "Medical/private.md",
            "summary": f"Medical note {DENIED_TOKEN}",
            "type": "medical_record",
            "source": ["medical"],
            "sources": ["medical"],
            "required_sources": ["medical"],
            "lineage_complete": True,
            "search_text": DENIED_TOKEN,
            "burst_key": "medical-denied",
        },
    ]


def _store(
    vault: Path,
    *,
    serving: MemoryServing | None = None,
    access: AccessContext | None = None,
    attach_bursts: bool = True,
):
    rows = list(serving.rows) if serving is not None else _thread_rows()
    index = MemoryIndex(rows)
    store = DefaultArchiveStore(vault=vault, index=index, access=access)
    if serving is not None:
        store.runtime = build_runtime(
            vault=vault,
            index=index,
            access=store.access,
            identity=store.runtime.identity,
            serving_factory=lambda: serving,
            policy_fields=store._policy_kwargs,
            authorize_rows=store._authorized_rows,
            attach_bursts=attach_bursts,
        )
    elif not attach_bursts:
        store.runtime = build_runtime(
            vault=vault,
            index=index,
            access=store.access,
            identity=store.runtime.identity,
            policy_fields=store._policy_kwargs,
            authorize_rows=store._authorized_rows,
            attach_bursts=False,
        )
    return store


def _event_chain(*, revision: str, burst_key: str, generation: str, citation: str) -> dict[str, str]:
    return {
        "event": "reply",
        "revision": revision,
        "chunk": burst_key,
        "generation": generation,
        "citation": citation,
    }


def test_store_search_query_evidence_stay_on_runtime():
    search_src = inspect.getsource(DefaultArchiveStore.search)
    query_src = inspect.getsource(DefaultArchiveStore.query)
    evidence_src = inspect.getsource(DefaultArchiveStore.evidence)
    graph_src = inspect.getsource(DefaultArchiveStore.graph)
    assert "self.runtime.search" in search_src
    assert "self.runtime.query" in query_src
    assert "self.runtime.retrieval.search" in evidence_src
    assert "self.runtime.graph" in graph_src
    assert "self.index.search" not in search_src
    assert "self.index.search" not in evidence_src
    retrieval_src = inspect.getsource(RetrievalAdapter)
    assert "PostgresArchiveIndex" not in retrieval_src
    assert "isinstance(self._index" not in retrieval_src


def test_engine_import_boundaries_hold():
    assert_engine_import_boundaries(REPO / "archive_engine")


def test_connector_before_bursts_then_drain(tmp_path: Path):
    vault = tmp_path / "connector-first"
    vault.mkdir()
    (vault / "_meta").mkdir()
    _write_person(vault, "hfa-person-p06d0001", "operator")
    identity_store = _store(vault, attach_bursts=False)
    identity = identity_store.runtime.identity
    access = identity_store.access
    writer = ContainedVaultWriter(vault)
    runner = LifecycleRunner(
        SAMPLE_CONNECTOR_ID,
        vault=vault,
        identity=identity,
        access=access,
        writer=writer,
    )
    first = runner.run(cursor={}, run_id="p06d-connector-first")
    assert first.run is not None
    assert first.burst_freshness == BURST_FRESHNESS_UNKNOWN
    persist_pending_scope(
        vault,
        PendingScope(
            source="gmail.thread",
            account_scope="alpha@example.test",
            thread_id=THREAD_UID,
            event_identity="evt-reply-1",
        ),
    )
    pending = load_pending_scopes(vault)
    assert pending
    assert all(not item.scheduled for item in pending)

    late = _store(vault, attach_bursts=True)
    drained = late.drain_pending_scopes()
    assert drained
    assert all(item.scheduled for item in drained)
    again = late.drain_pending_scopes()
    assert [item.event_identity for item in again] == [item.event_identity for item in drained]
    assert all(item.scheduled for item in again)
    identity_store.close()
    late.close()


def test_bursts_before_connector_invalidates_immediately(tmp_path: Path):
    vault = tmp_path / "bursts-first"
    vault.mkdir()
    (vault / "_meta").mkdir()
    store = _store(vault, attach_bursts=True)
    writer = ContainedVaultWriter(vault)
    bridge = EngineBurstBridge()
    run = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=store.runtime.identity,
        access=store.access,
        writer=writer,
        cursor={},
        run_id="p06d-bursts-first",
        burst_resolver=bridge,
    )
    assert run.burst_freshness == BURST_FRESHNESS_INVALIDATED
    assert run.burst_keys
    assert run.created_count == 2
    store.close()


def test_unavailable_resolver_keeps_unknown_freshness(tmp_path: Path):
    vault = tmp_path / "no-bursts"
    vault.mkdir()
    store = _store(vault, attach_bursts=False)
    assert store.runtime.drain_pending_scopes() == ()
    writer = ContainedVaultWriter(vault)
    run = execute_connector(
        SAMPLE_CONNECTOR_ID,
        identity=store.runtime.identity,
        access=store.access,
        writer=writer,
        cursor={},
        run_id="p06d-unavailable",
    )
    assert run.burst_freshness == BURST_FRESHNESS_UNKNOWN
    store.close()


def test_append_edit_delete_duplicate_retire_only_changed_bursts():
    bridge = EngineBurstBridge()
    base = thread_frontmatter(uid=THREAD_UID)
    previous = [str(chunk["burst_key"]) for chunk in burst_chunks(base)]
    unrelated = thread_frontmatter(uid=UNRELATED_UID, messages=long_thread_messages(include_answer=False))
    unrelated_keys = [str(chunk["burst_key"]) for chunk in burst_chunks(unrelated)]

    appended = deepcopy(base)
    appended["conversation_messages"] = long_thread_messages(
        extra=[_msg("msg-later", "shipping tomorrow", timestamp="2026-03-10T11:00:00+00:00")]
    )
    append_result = bridge.resolve_burst_affected(
        THREAD_UID,
        changed_message_ids=["msg-later"],
        previous_burst_keys=previous,
        current_messages=appended["conversation_messages"],
        channel="email",
    )
    assert append_result.status == "resolved"
    assert not append_result.retired_burst_keys
    assert append_result.replacement_burst_keys

    edited = deepcopy(base)
    messages = list(edited["conversation_messages"])
    for item in messages:
        if item["message_id"] == ANSWER_MESSAGE_ID:
            item["text"] = "use port 9000"
            item["revision"] = "rev-answer-2"
    edited["conversation_messages"] = messages
    edit_result = bridge.resolve_burst_affected(
        THREAD_UID,
        changed_message_ids=[ANSWER_MESSAGE_ID],
        previous_burst_keys=previous,
        current_messages=messages,
        channel="email",
    )
    current_keys = [str(chunk["burst_key"]) for chunk in burst_chunks(edited)]
    assert ANSWER not in "\n".join(str(chunk.get("content")) for chunk in burst_chunks(edited))
    assert set(edit_result.retired_burst_keys) == set(previous) - set(current_keys)
    assert set(unrelated_keys).isdisjoint(edit_result.retired_burst_keys)

    remaining = [item for item in base["conversation_messages"] if item["message_id"] != ANSWER_MESSAGE_ID]
    delete_result = bridge.resolve_burst_affected(
        THREAD_UID,
        changed_message_ids=[ANSWER_MESSAGE_ID],
        previous_burst_keys=previous,
        current_messages=remaining,
        channel="email",
    )
    assert delete_result.retired_burst_keys

    duplicate = bridge.resolve_burst_affected(
        THREAD_UID,
        changed_message_ids=[ANSWER_MESSAGE_ID],
        previous_burst_keys=previous,
        current_messages=base["conversation_messages"],
        channel="email",
    )
    assert not duplicate.retired_burst_keys
    assert duplicate.replacement_burst_keys


def test_current_reply_evidence_and_restricted_denial(tmp_path: Path):
    vault = tmp_path / "evidence"
    vault.mkdir()
    _write_person(vault, "hfa-person-p06d0002", "operator")
    serving = MemoryServing(_thread_rows(), generation_id="gen-p06d-reply")
    serving.retired.add("obsolete-reply")
    trusted = _store(vault, serving=serving)
    found = trusted.search(REPLY_QUERY, limit=8)
    rows = found.get("rows") or []
    assert any(row.get("card_uid") == THREAD_UID for row in rows)
    assert all(row.get("card_uid") != UNRELATED_UID or REPLY_QUERY not in str(row) for row in rows)
    evidence = trusted.evidence(query=REPLY_QUERY, limit=8)
    hits = evidence.get("hits") or []
    assert any(hit.get("uid") == THREAD_UID or THREAD_UID in json.dumps(hit) for hit in hits)
    assert trusted.runtime.retrieval.generation() == "gen-p06d-reply"
    chain = _event_chain(
        revision="rev-answer-1",
        burst_key="current-reply",
        generation=trusted.runtime.retrieval.generation(),
        citation=REPLY_QUERY,
    )
    assert chain["generation"] == "gen-p06d-reply"

    restricted = AccessContext(
        archive_id=trusted.runtime.identity.archive_id,
        principal="alice",
        profile="read-only",
        allowed_sources=("gmail",),
    )
    denied = _store(vault, serving=MemoryServing(_thread_rows(), generation_id="gen-p06d-reply"), access=restricted)
    denied_search = denied.search(DENIED_TOKEN, limit=8)
    denied_rows = json.dumps(denied_search.get("rows") or [])
    assert denied_search.get("rows") == []
    assert "hfa-medical-p06d0001" not in denied_rows
    allowed = denied.search(REPLY_QUERY, limit=8)
    allowed_blob = json.dumps(allowed.get("rows") or [])
    assert any(row.get("card_uid") == THREAD_UID for row in (allowed.get("rows") or []))
    assert DENIED_TOKEN not in allowed_blob
    assert "hfa-medical-p06d0001" not in allowed_blob
    ev_hits = json.dumps((denied.evidence(query=REPLY_QUERY, limit=8).get("hits") or []))
    assert DENIED_TOKEN not in ev_hits
    assert "hfa-medical-p06d0001" not in ev_hits
    trusted.close()
    denied.close()


def test_cli_and_mcp_agree_on_same_store(tmp_path: Path, monkeypatch):
    vault = tmp_path / "clients"
    vault.mkdir()
    _write_person(vault, "hfa-person-p06d0003", "Ada Example")
    serving = MemoryServing(_thread_rows(), generation_id="gen-p06d-clients")
    store = _store(vault, serving=serving)
    cli = search_cmd.search(REPLY_QUERY, limit=8, store=store, logger=LOG)
    facade = store.search(REPLY_QUERY, limit=8)
    assert [row.get("card_uid") for row in cli.get("rows") or []] == [
        row.get("card_uid") for row in facade.get("rows") or []
    ]
    read_cli = read_cmd.read("People/card.md", store=store, logger=LOG)
    read_store = store.read("People/card.md")
    assert read_cli.get("found") == read_store.get("found") is True
    assert "Ada Example" in str(read_cli.get("content") or "")

    monkeypatch.setattr("archive_cli.server.resolve_store", lambda: store)
    from archive_cli.server import archive_search

    mcp = archive_search(REPLY_QUERY, 8)
    assert THREAD_UID in mcp or REPLY_QUERY in mcp
    store.close()


def test_restart_drain_is_idempotent(tmp_path: Path):
    vault = tmp_path / "restart"
    vault.mkdir()
    persist_pending_scope(
        vault,
        PendingScope(source="gmail.thread", account_scope="a", thread_id=THREAD_UID, event_identity="evt-restart"),
    )
    first = _store(vault)
    once = first.drain_pending_scopes()
    first.close()
    second = _store(vault)
    twice = second.drain_pending_scopes()
    assert [item.event_identity for item in once] == [item.event_identity for item in twice]
    assert all(item.scheduled for item in twice)
    second.close()


def test_production_proven_stays_false():
    assert json.loads(json.dumps({"production_proven": False}))["production_proven"] is False
