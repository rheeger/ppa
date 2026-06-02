"""Build corpus hygiene thread records from Gmail adapter payloads."""

from __future__ import annotations

from typing import Any

from archive_cli.corpus_hygiene.classification_reuse import EmailThreadRecord


def thread_record_from_gmail_items(
    thread_record: dict[str, Any],
    message_records: list[dict[str, Any]],
    *,
    account_email: str,
    own_emails: set[str],
    previous_corpus_state: str = "active",
    triage_classification: str = "",
    triage_confidence: float = 0.0,
) -> EmailThreadRecord:
    """Map Gmail adapter dicts to ``EmailThreadRecord`` for policy evaluation."""

    thread_id = str(thread_record.get("thread_id", "")).strip()
    account = str(thread_record.get("account_email", account_email)).strip().lower()
    owner = account
    from_emails: list[str] = []
    for msg in message_records:
        fe = str(msg.get("from_email", "")).strip().lower()
        if fe and fe not in from_emails:
            from_emails.append(fe)
    participants = tuple(str(x).strip().lower() for x in thread_record.get("participants", []) if str(x).strip())
    owner_sent = any(
        str(msg.get("direction", "")).strip() == "outbound" or str(msg.get("from_email", "")).strip().lower() in own_emails
        for msg in message_records
    )
    owner_replied = owner_sent or any(str(msg.get("in_reply_to", "")).strip() for msg in message_records)
    message_uids = tuple(
        str(msg.get("message_id", "")).strip()
        for msg in message_records
        if str(msg.get("message_id", "")).strip()
    )
    # UIDs are computed by adapter helpers at ingest; use deterministic placeholders for ledger.
    from archive_sync.adapters.gmail_messages import _attachment_uid, _message_uid, _thread_uid

    thread_uid = _thread_uid(account, thread_id)
    msg_uids = tuple(_message_uid(account, mid) for mid in message_uids)
    att_uids: list[str] = []
    for msg in message_records:
        mid = str(msg.get("message_id", "")).strip()
        for att in msg.get("attachment_ids") or []:
            aid = str(att).strip()
            if mid and aid:
                uid = _attachment_uid(account, mid, aid)
                if uid not in att_uids:
                    att_uids.append(uid)

    calendar_hints = bool(
        thread_record.get("invite_ical_uids")
        or thread_record.get("invite_event_id_hints")
        or any(msg.get("invite_ical_uid") or msg.get("invite_event_id_hint") for msg in message_records)
    )

    return EmailThreadRecord(
        thread_uid=thread_uid,
        gmail_thread_id=thread_id,
        gmail_history_id=str(thread_record.get("gmail_history_id", "")).strip(),
        thread_body_sha=str(thread_record.get("thread_body_sha", "")).strip(),
        account_email=account,
        source_key=f"gmail-messages:{account}",
        subject=str(thread_record.get("subject", "")).strip(),
        from_emails=tuple(from_emails),
        participant_emails=participants,
        owner_email=owner,
        label_ids=tuple(str(x) for x in thread_record.get("label_ids", []) if str(x).strip()),
        message_count=int(thread_record.get("message_count", 0) or len(message_records)),
        first_message_at=str(thread_record.get("first_message_at", "")).strip(),
        last_message_at=str(thread_record.get("last_message_at", "")).strip(),
        has_attachments=bool(thread_record.get("has_attachments")),
        calendar_event_hints=calendar_hints,
        owner_sent_message=owner_sent,
        owner_replied=owner_replied,
        message_uids=msg_uids,
        attachment_uids=tuple(att_uids),
        previous_corpus_state=previous_corpus_state,
        triage_classification=triage_classification,
        triage_confidence=triage_confidence,
    )
