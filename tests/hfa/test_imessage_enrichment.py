from __future__ import annotations

import json

import pytest
from hfa.imessage_enrichment import IMessageThreadSummaryEnrichment
from hfa.provenance import ProvenanceEntry
from hfa.schema import IMessageMessageCard, IMessageThreadCard
from hfa.thread_hash import compute_imessage_thread_body_sha
from hfa.vault import write_card


class StubProvider:
    name = "stub"

    def __init__(self, model: str = "stub-model", response: str = "Family coordinated dinner for Sunday."):
        self.model = model
        self.response = response
        self.prompts: list[str] = []

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        self.prompts.append(prompt)
        return self.response


def test_imessage_thread_summary_enrichment_tracks_message_content_hash(tmp_vault, monkeypatch):
    message_one = IMessageMessageCard(
        uid="hfa-imessage-message-111111111111",
        type="imessage_message",
        source=["imessage.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="hello one",
        imessage_message_id="message-1",
        imessage_chat_id="chat-1",
        sender_handle="alice@example.com",
        participant_handles=["alice@example.com"],
        sent_at="2026-03-08T10:00:00Z",
    )
    message_two = IMessageMessageCard(
        uid="hfa-imessage-message-222222222222",
        type="imessage_message",
        source=["imessage.message"],
        source_id="message-2",
        created="2026-03-08",
        updated="2026-03-08",
        summary="hello two",
        imessage_message_id="message-2",
        imessage_chat_id="chat-1",
        sender_handle="alice@example.com",
        participant_handles=["alice@example.com"],
        sent_at="2026-03-08T11:00:00Z",
    )
    thread = IMessageThreadCard(
        uid="hfa-imessage-thread-111111111111",
        type="imessage_thread",
        source=["imessage.thread"],
        source_id="chat-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Alice Chat",
        imessage_chat_id="chat-1",
        participant_handles=["alice@example.com"],
        messages=["[[hfa-imessage-message-111111111111]]", "[[hfa-imessage-message-222222222222]]"],
    )

    write_card(tmp_vault, "IMessage/2026-03/hfa-imessage-message-111111111111.md", message_one, body="Dinner on Sunday?", provenance={
        "summary": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "imessage_message_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "imessage_chat_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "sender_handle": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "participant_handles": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "sent_at": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
    })
    write_card(tmp_vault, "IMessage/2026-03/hfa-imessage-message-222222222222.md", message_two, body="Yes, 7pm works.", provenance={
        "summary": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "imessage_message_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "imessage_chat_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "sender_handle": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "participant_handles": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "sent_at": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
    })

    provider = StubProvider()
    monkeypatch.setattr("hfa.imessage_enrichment.get_provider_chain", lambda vault_path: [provider])

    step = IMessageThreadSummaryEnrichment()
    assert step.should_run(thread, "", {}, str(tmp_vault)) is True
    updates = step.run(thread, "", str(tmp_vault))
    summary, prov = updates["thread_summary"]
    assert "Sunday" in summary
    assert prov.method == "llm"
    assert prov.input_hash
    thread.thread_body_sha = compute_imessage_thread_body_sha(thread, tmp_vault)
    assert prov.input_hash == thread.thread_body_sha

    existing_provenance = {"thread_summary": prov}
    monkeypatch.setattr(
        "hfa.imessage_enrichment.compute_imessage_thread_body_sha",
        lambda card, vault_path: pytest.fail("should not recompute hash when cached thread_body_sha matches"),
    )
    assert step.should_run(thread, "", existing_provenance, str(tmp_vault)) is False

    write_card(tmp_vault, "IMessage/2026-03/hfa-imessage-message-222222222222.md", message_two, body="Yes, 8pm works instead.", provenance={
        "summary": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "imessage_message_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "imessage_chat_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "sender_handle": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "participant_handles": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        "sent_at": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
    })
    monkeypatch.setattr("hfa.imessage_enrichment.compute_imessage_thread_body_sha", compute_imessage_thread_body_sha)
    thread.thread_body_sha = compute_imessage_thread_body_sha(thread, tmp_vault)
    assert step.should_run(thread, "", existing_provenance, str(tmp_vault)) is True


def test_imessage_thread_summary_enrichment_can_disable_sha_cache(tmp_vault, monkeypatch):
    message = IMessageMessageCard(
        uid="hfa-imessage-message-333333333333",
        type="imessage_message",
        source=["imessage.message"],
        source_id="message-3",
        created="2026-03-08",
        updated="2026-03-08",
        summary="hello",
        imessage_message_id="message-3",
        imessage_chat_id="chat-2",
        sender_handle="alice@example.com",
        participant_handles=["alice@example.com"],
        sent_at="2026-03-08T12:00:00Z",
    )
    thread = IMessageThreadCard(
        uid="hfa-imessage-thread-222222222222",
        type="imessage_thread",
        source=["imessage.thread"],
        source_id="chat-2",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Alice Chat",
        imessage_chat_id="chat-2",
        participant_handles=["alice@example.com"],
        messages=["[[hfa-imessage-message-333333333333]]"],
    )
    write_card(
        tmp_vault,
        "IMessage/2026-03/hfa-imessage-message-333333333333.md",
        message,
        body="Nothing changed here.",
        provenance={
            "summary": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
            "imessage_message_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
            "imessage_chat_id": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
            "sender_handle": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
            "participant_handles": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
            "sent_at": ProvenanceEntry("imessage.message", "2026-03-08", "deterministic"),
        },
    )
    thread.thread_body_sha = compute_imessage_thread_body_sha(thread, tmp_vault)
    existing_provenance = {
        "thread_summary": ProvenanceEntry(
            source="imessage-thread-summary",
            date="2026-03-08",
            method="llm",
            model="stub-model",
            enrichment_version=1,
            input_hash=thread.thread_body_sha,
        )
    }
    (tmp_vault / "_meta" / "ppa-config.json").write_text(
        json.dumps({"imessage_thread_body_sha_cache_enabled": False}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "hfa.imessage_enrichment.compute_imessage_thread_body_sha",
        lambda card, vault_path: pytest.fail("cache-disabled path should not consult cached thread sha shortcut"),
    )
    step = IMessageThreadSummaryEnrichment()
    assert step.should_run(thread, "", existing_provenance, str(tmp_vault)) is True
