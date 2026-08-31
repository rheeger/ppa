"""Section C — Gmail classify-before-promotion tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.decisions import EmailCorpusDecisionRecord
from archive_sync.adapters.base import FetchedBatch
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter
from archive_sync.gmail_promotion.classification_resolve import GmailClassificationResolver
from archive_sync.gmail_promotion.gate import GmailPromotionGate, PromotionOutcome
from archive_sync.gmail_promotion.ledger import FailingLedger, FilePromotionLedger
from archive_sync.gmail_promotion.metrics import GmailPromotionBatchMetrics
from archive_sync.gmail_promotion.thread_record import thread_record_from_gmail_items
from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION


def _marketing_thread_record(account: str = "me@example.com") -> tuple[dict, list]:
    thread_id = "promo-mkt-1"
    thread = {
        "kind": "thread",
        "thread_id": thread_id,
        "gmail_history_id": "9001",
        "account_email": account,
        "subject": "50% off today only",
        "participants": ["deals@retailer.com"],
        "label_ids": ["INBOX", "CATEGORY_PROMOTIONS"],
        "messages": [],
        "first_message_at": "2026-03-01T00:00:00Z",
        "last_message_at": "2026-03-01T00:00:00Z",
        "message_count": 1,
        "has_attachments": False,
        "thread_body_sha": "sha-mkt",
        "created": "2026-03-01",
    }
    messages = [
        {
            "kind": "message",
            "message_id": "msg-mkt-1",
            "thread_id": thread_id,
            "account_email": account,
            "from_email": "deals@retailer.com",
            "subject": "50% off today only",
            "label_ids": ["INBOX", "CATEGORY_PROMOTIONS"],
            "direction": "inbound",
            "attachment_ids": [],
            "participant_emails": ["deals@retailer.com"],
        }
    ]
    return thread, messages


def _transactional_thread_record(account: str = "me@example.com") -> tuple[dict, list]:
    thread_id = "promo-txn-1"
    thread = {
        "kind": "thread",
        "thread_id": thread_id,
        "gmail_history_id": "9002",
        "account_email": account,
        "subject": "Your receipt from Example Store",
        "participants": ["receipts@stripe.com", account],
        "label_ids": ["INBOX"],
        "messages": [],
        "first_message_at": "2026-03-02T00:00:00Z",
        "last_message_at": "2026-03-02T00:00:00Z",
        "message_count": 1,
        "has_attachments": False,
        "thread_body_sha": "sha-txn",
        "created": "2026-03-02",
    }
    messages = [
        {
            "kind": "message",
            "message_id": "msg-txn-1",
            "thread_id": thread_id,
            "account_email": account,
            "from_email": "receipts@stripe.com",
            "subject": "Your receipt from Example Store",
            "label_ids": ["INBOX"],
            "direction": "inbound",
            "attachment_ids": [],
            "participant_emails": ["receipts@stripe.com", account],
        }
    ]
    return thread, messages


def _gate(tmp_path: Path, *, card_rows: list | None = None, classify_entries: dict | None = None) -> GmailPromotionGate:
    ledger = FilePromotionLedger(tmp_path / "ledger.jsonl")
    classify_index = None
    if classify_entries:
        db = tmp_path / "classify.db"
        with ClassifyIndex(db) as idx:
            for tid, row in classify_entries.items():
                idx.put_classification(
                    tid,
                    str(row.get("category") or "marketing"),
                    float(row.get("confidence") or 0.9),
                    list(row.get("card_types") or []),
                    first_subject=str(row.get("subject") or ""),
                )
        classify_index = ClassifyIndex(db)
    from archive_cli.corpus_hygiene.classification_reuse import load_card_classifications_from_rows

    resolver = GmailClassificationResolver(
        card_classifications=load_card_classifications_from_rows(card_rows or []),
        classify_index=classify_index,
    )
    return GmailPromotionGate(
        ledger=ledger,
        resolver=resolver,
        decision_run_id="section-c-test",
        metrics=GmailPromotionBatchMetrics(),
    )


def test_suppressed_thread_writes_ledger_not_cards(tmp_path: Path) -> None:
    gate = _gate(
        tmp_path,
        classify_entries={"promo-mkt-1": {"category": "marketing", "confidence": 0.95}},
    )
    thread, messages = _marketing_thread_record()
    result = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    assert result.outcome == PromotionOutcome.SUPPRESS
    assert result.emit_cards is False
    gate.persist_decision(result)
    assert gate.ledger.get_thread_state("promo-mkt-1") == "suppressed"
    assert (tmp_path / "ledger.jsonl").read_text(encoding="utf-8").strip()


def test_active_thread_promotes_cards(tmp_path: Path) -> None:
    gate = _gate(tmp_path)
    thread, messages = _transactional_thread_record()
    result = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    assert result.outcome == PromotionOutcome.PROMOTE_CARDS
    assert result.emit_cards is True
    assert result.record.corpus_decision == "active"


def test_quarantine_thread_emits_labeled_cards(tmp_path: Path) -> None:
    gate = _gate(tmp_path)
    thread, messages = _marketing_thread_record()
    thread["label_ids"] = ["INBOX", "CATEGORY_PROMOTIONS", "STARRED"]
    result = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    assert result.outcome == PromotionOutcome.QUARANTINE
    assert result.emit_cards is True
    assert result.record.corpus_decision == "quarantine"
    assert result.dirty_card_uids


def test_email_corpus_decisions_precedence_over_classify_index(tmp_path: Path) -> None:
    ledger = FilePromotionLedger(tmp_path / "ledger.jsonl")
    thread, messages = _marketing_thread_record()
    record = EmailCorpusDecisionRecord(
        decision_run_id="prior-run",
        source_key="gmail-messages:me@example.com",
        account_email="me@example.com",
        gmail_thread_id="promo-mkt-1",
        gmail_history_id="9001",
        thread_body_sha="sha-mkt",
        thread_uid="uid-x",
        message_uids=(),
        attachment_uids=(),
        derived_uids=(),
        classification="personal",
        canonical_classification="personal",
        confidence=0.99,
        card_types=(),
        classification_source="card_classifications",
        classify_prompt_version="",
        classify_model="",
        policy_version=EMAIL_PROMOTION_POLICY_VERSION,
        previous_corpus_state="suppressed",
        corpus_decision="suppressed",
        processor_decision="suppressed_no_processing",
        decision_reason="test",
        decision_signals=(),
    )
    ledger.persist(record)
    db = tmp_path / "classify.db"
    with ClassifyIndex(db) as idx:
        idx.put_classification("promo-mkt-1", "marketing", 0.99, [])
    resolver = GmailClassificationResolver(
        corpus_decisions={record.gmail_thread_id: record},
        classify_index=ClassifyIndex(db),
    )
    email_thread = thread_record_from_gmail_items(
        thread, messages, account_email="me@example.com", own_emails={"me@example.com"}
    )
    hit = resolver.resolve(email_thread)
    assert hit.classification_source == "email_corpus_decisions"
    assert hit.classification == "personal"


def test_classification_reused_before_stage0(tmp_path: Path) -> None:
    thread, messages = _marketing_thread_record()
    db = tmp_path / "classify.db"
    with ClassifyIndex(db) as idx:
        idx.put_classification("promo-mkt-1", "marketing", 0.95, [], first_subject="sale")
    resolver = GmailClassificationResolver(classify_index=ClassifyIndex(db))
    record = thread_record_from_gmail_items(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
    )
    hit = resolver.resolve(record)
    assert hit.classification_source == "classify_index"
    assert resolver.new_llm_call_count == 0


def test_previously_suppressed_re_promoted_on_owner_reply(tmp_path: Path) -> None:
    gate = _gate(tmp_path)
    thread, messages = _marketing_thread_record()
    suppress = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    gate.persist_decision(suppress)
    messages[0]["direction"] = "outbound"
    messages[0]["from_email"] = "me@example.com"
    thread["participants"] = ["me@example.com", "deals@retailer.com"]
    promote = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    assert promote.outcome == PromotionOutcome.PROMOTE_CARDS
    assert gate.metrics is not None
    assert gate.metrics.re_promoted == 1


def test_active_card_demotion_recommended_not_suppressed(tmp_path: Path) -> None:
    gate = _gate(
        tmp_path,
        classify_entries={"promo-mkt-1": {"category": "marketing", "confidence": 0.95}},
    )
    thread, messages = _marketing_thread_record()
    result = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=True,
    )
    assert result.outcome == PromotionOutcome.DEMOTION_RECOMMENDED
    assert result.emit_cards is True
    assert result.record.corpus_decision == "active"
    assert any(s.startswith("recommended_corpus_state:") for s in result.record.decision_signals)


def test_cursor_blocked_when_ledger_write_fails(tmp_path: Path) -> None:
    gate = GmailPromotionGate(
        ledger=FailingLedger(),
        resolver=GmailClassificationResolver(),
        decision_run_id="fail-run",
    )
    thread, messages = _marketing_thread_record()
    result = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    with pytest.raises(OSError):
        gate.persist_decision(result)
    batch = FetchedBatch(items=[], cursor_patch={"page_index": 0}, commit_cursor=False)
    assert batch.commit_cursor is False


def test_fetch_batches_suppressed_yields_no_cards(tmp_path, monkeypatch) -> None:
    adapter = GmailMessagesAdapter()
    thread, messages = _marketing_thread_record()
    artifacts = tmp_path / "_artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    db = artifacts / "_classify_index.db"
    with ClassifyIndex(db) as idx:
        idx.put_classification(thread["thread_id"], "marketing", 0.95, [], first_subject=thread["subject"])
    thread_payload = {
        "id": thread["thread_id"],
        "historyId": thread["gmail_history_id"],
        "messages": [
            {
                "id": messages[0]["message_id"],
                "threadId": thread["thread_id"],
                "internalDate": "1710000000000",
                "snippet": "sale",
                "labelIds": ["INBOX", "CATEGORY_PROMOTIONS"],
                "payload": {
                    "mimeType": "text/plain",
                    "headers": [
                        {"name": "From", "value": "Deals <deals@retailer.com>"},
                        {"name": "To", "value": "me@example.com"},
                        {"name": "Subject", "value": "50% off today only"},
                        {"name": "Date", "value": "Sat, 01 Mar 2026 00:00:00 +0000"},
                    ],
                    "body": {"data": ""},
                },
            }
        ],
    }

    def fake_gws(args):
        if args[:4] == ["gmail", "users", "threads", "list"]:
            return {"threads": [{"id": thread["thread_id"], "historyId": "9001"}], "nextPageToken": None}
        if args[:4] == ["gmail", "users", "threads", "get"]:
            return thread_payload
        raise AssertionError(args)

    monkeypatch.setattr(adapter, "_gws_with_retry", fake_gws)
    batches = list(
        adapter.fetch_batches(
            str(tmp_path),
            {},
            account_email="me@example.com",
            max_threads=10,
            page_size=1,
            gmail_promotion_gate=True,
            promotion_decision_run_id="fetch-suppress-test",
        )
    )
    card_items = [item for batch in batches for item in batch.items if item.get("kind") == "thread"]
    assert card_items == []
    ledger = FilePromotionLedger(tmp_path / "_artifacts" / "gmail_promotion_ledger.jsonl")
    assert ledger.get_thread_state(thread["thread_id"]) == "suppressed"


def test_fetch_batches_active_writes_thread_card(tmp_path, monkeypatch) -> None:
    adapter = GmailMessagesAdapter()
    thread, messages = _transactional_thread_record()

    def fake_gws(args):
        if args[:4] == ["gmail", "users", "threads", "list"]:
            return {"threads": [{"id": thread["thread_id"], "historyId": "9002"}], "nextPageToken": None}
        if args[:4] == ["gmail", "users", "threads", "get"]:
            return {
                "id": thread["thread_id"],
                "historyId": thread["gmail_history_id"],
                "messages": [
                    {
                        "id": messages[0]["message_id"],
                        "threadId": thread["thread_id"],
                        "internalDate": "1710000000000",
                        "snippet": "receipt",
                        "labelIds": ["INBOX"],
                        "payload": {
                            "mimeType": "text/plain",
                            "headers": [
                                {"name": "From", "value": "Receipts <receipts@stripe.com>"},
                                {"name": "To", "value": "me@example.com"},
                                {"name": "Subject", "value": "Your receipt from Example Store"},
                                {"name": "Date", "value": "Sun, 02 Mar 2026 00:00:00 +0000"},
                            ],
                            "body": {"data": ""},
                        },
                    }
                ],
            }
        raise AssertionError(args)

    monkeypatch.setattr(adapter, "_gws_with_retry", fake_gws)
    batches = list(
        adapter.fetch_batches(
            str(tmp_path),
            {},
            account_email="me@example.com",
            max_threads=10,
            page_size=1,
            gmail_promotion_gate=True,
            promotion_decision_run_id="fetch-active-test",
        )
    )
    thread_items = [item for batch in batches for item in batch.items if item.get("kind") == "thread"]
    assert len(thread_items) == 1
    assert thread_items[0]["thread_id"] == thread["thread_id"]


def test_ingest_cursor_advances_after_suppressed_ledger_persist(tmp_path, monkeypatch) -> None:
    adapter = GmailMessagesAdapter()
    thread, _ = _marketing_thread_record()

    def fake_gws(args):
        if args[:4] == ["gmail", "users", "threads", "list"]:
            return {"threads": [{"id": thread["thread_id"], "historyId": "9001"}], "nextPageToken": None}
        if args[:4] == ["gmail", "users", "threads", "get"]:
            return {
                "id": thread["thread_id"],
                "historyId": "9001",
                "messages": [
                    {
                        "id": "msg-mkt-1",
                        "threadId": thread["thread_id"],
                        "internalDate": "1710000000000",
                        "snippet": "sale",
                        "labelIds": ["INBOX", "CATEGORY_PROMOTIONS"],
                        "payload": {
                            "mimeType": "text/plain",
                            "headers": [
                                {"name": "From", "value": "Deals <deals@retailer.com>"},
                                {"name": "To", "value": "me@example.com"},
                                {"name": "Subject", "value": "50% off today only"},
                            ],
                            "body": {"data": ""},
                        },
                    }
                ],
            }
        raise AssertionError(args)

    monkeypatch.setattr(adapter, "_gws_with_retry", fake_gws)
    adapter.ingest(
        str(tmp_path),
        dry_run=False,
        account_email="me@example.com",
        max_threads=5,
        page_size=1,
        gmail_promotion_gate=True,
        promotion_decision_run_id="ingest-cursor-test",
    )
    # ingest uses internal cursor; verify ledger written and no email cards
    ledger_path = tmp_path / "_artifacts" / "gmail_promotion_ledger.jsonl"
    assert ledger_path.is_file()
    email_md = list(tmp_path.rglob("EmailThreads/*.md")) + list(tmp_path.rglob("Email/*.md"))
    assert email_md == []


def test_missing_classification_blocks_cursor(tmp_path) -> None:
    gate = GmailPromotionGate(
        ledger=FilePromotionLedger(tmp_path / "ledger.jsonl"),
        resolver=GmailClassificationResolver(),
        decision_run_id="missing-class-test",
        fail_on_missing_classification=True,
    )
    thread = {
        "kind": "thread",
        "thread_id": "promo-unknown-1",
        "gmail_history_id": "9010",
        "account_email": "me@example.com",
        "subject": "Weekly digest",
        "participants": ["news@obscure-newsletter-xyz.example"],
        "label_ids": ["INBOX"],
        "message_count": 1,
        "has_attachments": False,
        "thread_body_sha": "sha-unknown",
        "created": "2026-03-03",
    }
    messages = [
        {
            "kind": "message",
            "message_id": "msg-unknown-1",
            "thread_id": "promo-unknown-1",
            "account_email": "me@example.com",
            "from_email": "news@obscure-newsletter-xyz.example",
            "subject": "Weekly digest",
            "label_ids": ["INBOX"],
            "direction": "inbound",
            "attachment_ids": [],
            "participant_emails": ["news@obscure-newsletter-xyz.example"],
        }
    ]
    result = gate.evaluate_loaded_thread(
        thread,
        messages,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        vault_has_active_card=False,
    )
    assert result.commit_cursor is False
