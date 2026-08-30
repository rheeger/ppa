"""Classification reuse for email corpus hygiene (Section B precedence rules)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from archive_auth import INTERNAL_DOMAINS
from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.known_senders import (
    NOISE_DOMAINS,
    TRANSACTIONAL_DOMAINS,
    _email_domain,
    _is_marketing_subject,
    classify_thread_prefilter,
)


@dataclass(frozen=True, slots=True)
class ReusedClassification:
    classification: str
    confidence: float
    card_types: tuple[str, ...]
    classification_source: str
    classify_prompt_version: str = ""
    classify_model: str = ""


@dataclass(frozen=True, slots=True)
class EmailThreadRecord:
    """Minimal thread shape for dry-run census (fixtures or vault cache)."""

    thread_uid: str
    gmail_thread_id: str
    gmail_history_id: str = ""
    thread_body_sha: str = ""
    account_email: str = ""
    source_key: str = ""
    subject: str = ""
    from_emails: tuple[str, ...] = ()
    participant_emails: tuple[str, ...] = ()
    owner_email: str = ""
    label_ids: tuple[str, ...] = ()
    message_count: int = 0
    first_message_at: str = ""
    last_message_at: str = ""
    has_attachments: bool = False
    calendar_event_hints: bool = False
    owner_sent_message: bool = False
    owner_replied: bool = False
    message_uids: tuple[str, ...] = ()
    attachment_uids: tuple[str, ...] = ()
    derived_uids: tuple[str, ...] = ()
    previous_corpus_state: str = "active"
    triage_classification: str = ""
    triage_confidence: float = 0.0
    triage_card_types: tuple[str, ...] = ()
    triage_classify_model: str = ""


def _parse_card_types(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, (list, tuple)):
        return tuple(str(x) for x in raw if str(x).strip())
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return ()
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return tuple(str(x) for x in parsed if str(x).strip())
            except json.JSONDecodeError:
                pass
        return (s,)
    return ()


def _hit(
    classification: str,
    confidence: float,
    card_types: tuple[str, ...],
    source: str,
    *,
    classify_prompt_version: str = "",
    classify_model: str = "",
) -> ReusedClassification:
    return ReusedClassification(
        classification=classification,
        confidence=float(confidence),
        card_types=card_types,
        classification_source=source,
        classify_prompt_version=classify_prompt_version,
        classify_model=classify_model,
    )


def stage0_classification(
    from_emails: tuple[str, ...],
    subjects: tuple[str, ...],
    *,
    user_domains: frozenset[str] | None = None,
) -> ReusedClassification | None:
    """Map deterministic Stage 0 gates to a reusable classification."""

    decision, card_types = classify_thread_prefilter(
        list(from_emails),
        list(subjects),
        user_domains=user_domains or frozenset(INTERNAL_DOMAINS),
    )
    if decision == "fast_track":
        return _hit("transactional", 0.85, tuple(card_types), "stage0")
    if decision != "skip":
        return None

    for fe in from_emails:
        dom = _email_domain(fe)
        if dom in NOISE_DOMAINS or dom in TRANSACTIONAL_DOMAINS:
            if dom in TRANSACTIONAL_DOMAINS and subjects and all(_is_marketing_subject(s) for s in subjects if s):
                return _hit("marketing", 0.85, (), "stage0")
            if dom in NOISE_DOMAINS:
                return _hit("noise", 0.85, (), "stage0")

    if subjects and all(_is_marketing_subject(s) for s in subjects if s):
        return _hit("marketing", 0.85, (), "stage0")

    work_domains = user_domains or frozenset(INTERNAL_DOMAINS)
    if from_emails and all(
        _email_domain(fe) in work_domains or _email_domain(fe) in {"gmail.com", "googlemail.com"}
        for fe in from_emails
    ):
        return _hit("personal", 0.75, (), "stage0")

    return _hit("noise", 0.80, (), "stage0")


class ClassificationReuseLoader:
    """Resolve classification using Section B precedence (no LLM by default)."""

    def __init__(
        self,
        *,
        card_classifications: dict[str, ReusedClassification] | None = None,
        classify_index: ClassifyIndex | None = None,
        allow_new_llm: bool = False,
        llm_classify_fn: Callable[[EmailThreadRecord], ReusedClassification | None] | None = None,
        user_domains: frozenset[str] | None = None,
    ) -> None:
        self._card_classifications = card_classifications or {}
        self._classify_index = classify_index
        self._classify_by_thread_id: dict[str, dict[str, Any]] = (
            classify_index.dump_all() if classify_index is not None else {}
        )
        self._allow_new_llm = allow_new_llm
        self._llm_classify_fn = llm_classify_fn
        self._user_domains = user_domains
        self.source_counts: dict[str, int] = {}
        self.new_llm_call_count = 0
        self.missing_classification_count = 0

    def resolve(self, thread: EmailThreadRecord) -> ReusedClassification:
        hit = self._resolve_inner(thread)
        src = hit.classification_source
        self.source_counts[src] = self.source_counts.get(src, 0) + 1
        return hit

    def _resolve_inner(self, thread: EmailThreadRecord) -> ReusedClassification:
        if thread.thread_uid and thread.thread_uid in self._card_classifications:
            return self._card_classifications[thread.thread_uid]

        tid = thread.gmail_thread_id.strip()
        if tid:
            cached = self._classify_by_thread_id.get(tid)
            if cached:
                return _hit(
                    str(cached.get("category") or ""),
                    float(cached.get("confidence") or 0.0),
                    tuple(_parse_card_types(cached.get("card_types"))),
                    "classify_index",
                    classify_prompt_version=str(cached.get("classify_prompt_version") or ""),
                    classify_model=str(cached.get("classify_model") or ""),
                )

        if thread.triage_classification.strip():
            return _hit(
                thread.triage_classification.strip(),
                float(thread.triage_confidence or 0.0),
                thread.triage_card_types,
                "frontmatter",
                classify_model=thread.triage_classify_model,
            )

        subjects = (thread.subject,) if thread.subject else ()
        stage0 = stage0_classification(thread.from_emails, subjects, user_domains=self._user_domains)
        if stage0 is not None:
            return stage0

        if self._allow_new_llm and self._llm_classify_fn is not None:
            llm_hit = self._llm_classify_fn(thread)
            if llm_hit is not None:
                self.new_llm_call_count += 1
                return llm_hit

        self.missing_classification_count += 1
        return _hit("", 0.0, (), "missing")


def load_card_classifications_from_rows(rows: list[dict[str, Any]]) -> dict[str, ReusedClassification]:
    out: dict[str, ReusedClassification] = {}
    for row in rows:
        uid = str(row.get("card_uid") or row.get("thread_uid") or "").strip()
        if not uid:
            continue
        out[uid] = _hit(
            str(row.get("classification") or ""),
            float(row.get("confidence") or 0.0),
            _parse_card_types(row.get("card_types")),
            "card_classifications",
            classify_model=str(row.get("classify_model") or ""),
        )
    return out


def discover_classify_index_db(vault_path: Path) -> Path | None:
    artifacts = Path(vault_path) / "_artifacts"
    if not artifacts.is_dir():
        return None
    candidates = sorted(artifacts.glob("_classify_index*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def open_classify_index(vault_path: Path | None) -> ClassifyIndex | None:
    if vault_path is None:
        return None
    db = discover_classify_index_db(vault_path)
    if db is None or not db.is_file():
        return None
    return ClassifyIndex(db)


def _normalize_email_tuple(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        value = raw.strip().lower()
        return (value,) if value else ()
    if isinstance(raw, (list, tuple)):
        out: list[str] = []
        for item in raw:
            value = str(item).strip().lower()
            if value and value not in out:
                out.append(value)
        return tuple(out)
    return ()


def _uid_from_ref(raw: str) -> str:
    """Normalize a wikilink or bare UID to a card UID."""

    value = raw.strip()
    if not value:
        return ""
    if value.startswith("[[") and value.endswith("]]"):
        value = value[2:-2].strip()
    return value.split("|", 1)[0].strip()


def _uids_from_frontmatter_list(raw: Any) -> tuple[str, ...]:
    """Extract card UIDs from a frontmatter list (wikilinks or bare UIDs)."""

    if raw is None:
        return ()
    if isinstance(raw, str):
        items: list[Any] = [raw]
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        return ()
    out: list[str] = []
    for item in items:
        uid = _uid_from_ref(str(item))
        if uid and uid not in out:
            out.append(uid)
    return tuple(out)


def _from_emails_from_frontmatter(fm: dict[str, Any]) -> tuple[str, ...]:
    """Collect From addresses stored on a thread card (if any)."""

    emails = list(_normalize_email_tuple(fm.get("from_emails")))
    for key in ("from_email", "from"):
        for email in _normalize_email_tuple(fm.get(key)):
            if email not in emails:
                emails.append(email)
    return tuple(emails)


def infer_owner_outbound(
    *,
    owner_email: str,
    explicit_owner_sent: bool = False,
    explicit_owner_replied: bool = False,
    from_emails: tuple[str, ...] = (),
    message_directions: tuple[str, ...] = (),
) -> tuple[bool, bool]:
    """Infer (owner_sent_message, owner_replied) from real outbound signals only.

    Owner appearing in ``participants`` is **not** outbound — mailbox owners are
    listed on inbound mail too. Prefer explicit flags, From=owner, or outbound
    message direction.
    """

    owner = owner_email.strip().lower()
    owner_sent = bool(explicit_owner_sent)
    if not owner_sent and owner:
        owner_sent = any(fe.strip().lower() == owner for fe in from_emails if fe.strip())
    if not owner_sent:
        owner_sent = any(str(d).strip().lower() == "outbound" for d in message_directions)
    owner_replied = bool(explicit_owner_replied)
    return owner_sent, owner_replied


def thread_from_frontmatter(
    rel_path: str,
    fm: dict[str, Any],
    *,
    owner_email: str = "",
    message_from_emails: tuple[str, ...] = (),
    message_directions: tuple[str, ...] = (),
) -> EmailThreadRecord:
    """Build a thread record from vault frontmatter.

    ``owner_sent_message`` / ``owner_replied`` require real outbound signals
    (explicit flags, From=owner, or outbound direction) — not merely
    ``owner ∈ participants``.
    """

    uid = str(fm.get("uid") or Path(rel_path).stem)
    participants = tuple(str(x).strip().lower() for x in (fm.get("participants") or []) if str(x).strip())
    owner = (owner_email or str(fm.get("account_email") or "")).strip().lower()
    message_uids = _uids_from_frontmatter_list(fm.get("message_uids") or fm.get("messages"))
    attachment_uids = _uids_from_frontmatter_list(fm.get("attachment_uids") or fm.get("attachments"))
    derived_uids = _uids_from_frontmatter_list(fm.get("derived_uids") or fm.get("derived_cards"))
    from_emails = _from_emails_from_frontmatter(fm)
    for email in message_from_emails:
        value = email.strip().lower()
        if value and value not in from_emails:
            from_emails = (*from_emails, value)
    owner_sent, owner_replied = infer_owner_outbound(
        owner_email=owner,
        explicit_owner_sent=bool(fm.get("owner_sent_message")),
        explicit_owner_replied=bool(fm.get("owner_replied")),
        from_emails=from_emails,
        message_directions=message_directions,
    )
    return EmailThreadRecord(
        thread_uid=uid,
        gmail_thread_id=str(fm.get("gmail_thread_id") or ""),
        gmail_history_id=str(fm.get("gmail_history_id") or ""),
        thread_body_sha=str(fm.get("thread_body_sha") or ""),
        account_email=str(fm.get("account_email") or owner_email or ""),
        source_key=f"gmail-messages:{fm.get('account_email') or owner_email or 'unknown'}",
        subject=str(fm.get("subject") or ""),
        from_emails=from_emails,
        participant_emails=participants,
        owner_email=owner,
        label_ids=tuple(str(x) for x in (fm.get("label_ids") or [])),
        message_count=int(fm.get("message_count") or len(message_uids) or 0),
        first_message_at=str(fm.get("first_message_at") or ""),
        last_message_at=str(fm.get("last_message_at") or ""),
        has_attachments=bool(fm.get("has_attachments")),
        calendar_event_hints=bool(fm.get("calendar_events") or fm.get("invite_event_id_hints")),
        owner_sent_message=owner_sent,
        owner_replied=owner_replied,
        message_uids=message_uids,
        attachment_uids=attachment_uids,
        derived_uids=derived_uids,
        triage_classification=str(fm.get("triage_classification") or ""),
        triage_confidence=float(fm.get("triage_confidence") or 0.0),
        triage_card_types=_parse_card_types(fm.get("triage_card_types")),
        triage_classify_model=str(fm.get("triage_classify_model") or ""),
        previous_corpus_state="active",
    )
