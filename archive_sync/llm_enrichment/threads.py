"""Thread assembly for LLM enrichment — vault scan cache first, filesystem fallback."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive_mcp.vault_cache import VaultScanCache
from archive_sync.extractors.preprocessing import clean_email_body
from hfa.vault import (iter_note_paths, read_note_file,
                       read_note_frontmatter_file)

logger = logging.getLogger("ppa.llm_enrichment.threads")

# Conservative cap vs 128k context (plan): leave room for system prompt + schema + output.
_EXTRACTION_TOKEN_BUDGET = 100_000


@dataclass(frozen=True)
class ThreadStub:
    """Frontmatter-only row for grouping (Tier 1)."""

    uid: str
    rel_path: str
    gmail_thread_id: str
    sent_at: str
    from_email: str
    from_name: str
    subject: str
    snippet: str
    direction: str
    participant_emails: tuple[str, ...] = ()


@dataclass
class ThreadMessage:
    uid: str
    rel_path: str
    from_email: str
    from_name: str
    sent_at: str
    subject: str
    body: str
    direction: str


@dataclass
class ThreadDocument:
    thread_id: str
    messages: list[ThreadMessage]
    subject: str
    participants: list[str]
    date_range: tuple[str, str]
    message_count: int
    total_chars: int
    content_hash: str


def _fm_thread_id(fm: dict[str, Any], uid: str) -> str:
    tid = str(fm.get("gmail_thread_id") or "").strip()
    return tid if tid else f"_singleton:{uid}"


def thread_stub_from_frontmatter(rel_path: str, fm: dict[str, Any]) -> ThreadStub | None:
    if str(fm.get("type") or "") != "email_message":
        return None
    uid = str(fm.get("uid") or "").strip()
    if not uid:
        return None
    pe = fm.get("participant_emails") or []
    emails_t: tuple[str, ...] = ()
    if isinstance(pe, list):
        emails_t = tuple(str(x).strip().lower() for x in pe if str(x).strip())
    return ThreadStub(
        uid=uid,
        rel_path=rel_path,
        gmail_thread_id=_fm_thread_id(fm, uid),
        sent_at=str(fm.get("sent_at") or "").strip(),
        from_email=str(fm.get("from_email") or "").strip().lower(),
        from_name=str(fm.get("from_name") or "").strip(),
        subject=str(fm.get("subject") or "").strip(),
        snippet=str(fm.get("snippet") or "").strip(),
        direction=str(fm.get("direction") or "").strip().lower(),
        participant_emails=emails_t,
    )


def email_message_stubs_from_sqlite(cache_db: Path) -> list[ThreadStub]:
    """Load ``email_message`` stubs from a vault scan cache SQLite file (no vault walk)."""

    if not cache_db.exists():
        return []
    stubs: list[ThreadStub] = []
    conn = sqlite3.connect(str(cache_db), timeout=60.0)
    try:
        rows = conn.execute(
            "SELECT rel_path, frontmatter_json FROM notes WHERE card_type = ?",
            ("email_message",),
        ).fetchall()
    finally:
        conn.close()
    for rel_path, fj in rows:
        try:
            fm = json.loads(fj)
        except json.JSONDecodeError:
            continue
        if not isinstance(fm, dict):
            continue
        stub = thread_stub_from_frontmatter(str(rel_path), fm)
        if stub:
            stubs.append(stub)
    return stubs


def stubs_from_filesystem_walk(vault: Path) -> list[ThreadStub]:
    """Fallback: walk ``Email/`` and read frontmatter only."""

    vault = Path(vault)
    out: list[ThreadStub] = []
    for rel in iter_note_paths(vault):
        if not rel.parts or rel.parts[0] != "Email":
            continue
        path = vault / rel
        try:
            note = read_note_frontmatter_file(path, vault_root=vault)
        except OSError:
            continue
        stub = thread_stub_from_frontmatter(rel.as_posix(), note.frontmatter)
        if stub:
            out.append(stub)
    return out


def load_email_stubs_for_vault(vault: Path) -> list[ThreadStub]:
    """Prefer vault scan cache SQLite; fall back to filesystem walk if missing or empty."""

    vault = Path(vault).resolve()
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    if cache_path.exists():
        stubs = email_message_stubs_from_sqlite(cache_path)
        if stubs:
            logger.info("thread-stubs from vault-scan-cache path=%s count=%d", cache_path, len(stubs))
            return stubs
    logger.info("thread-stubs fallback Email/ walk vault=%s", vault)
    return stubs_from_filesystem_walk(vault)


def build_thread_index(stubs: list[ThreadStub]) -> dict[str, list[ThreadStub]]:
    """Group stubs by ``gmail_thread_id`` (or synthetic singleton id), sorted by ``sent_at``."""

    buckets: dict[str, list[ThreadStub]] = {}
    for s in stubs:
        buckets.setdefault(s.gmail_thread_id, []).append(s)
    for group in buckets.values():
        group.sort(key=lambda x: (x.sent_at or "", x.uid))
    return buckets


def build_thread_index_from_cache(cache_db: Path) -> dict[str, list[ThreadStub]]:
    """Convenience: load from a vault-scan-cache path and group."""

    return build_thread_index(email_message_stubs_from_sqlite(cache_db))


def _content_hash_messages(messages: list[ThreadMessage]) -> str:
    blob = "\n---\n".join(f"{m.uid}\n{m.body}" for m in messages)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _load_body_for_stub(
    s: ThreadStub,
    vault: Path,
    *,
    scan_cache: VaultScanCache | None,
) -> str:
    if scan_cache is not None and scan_cache.tier() >= 2:
        try:
            return scan_cache.body_for_rel_path(s.rel_path)
        except ValueError:
            pass
    parsed = read_note_file(vault / s.rel_path, vault_root=vault)
    return parsed.body


def hydrate_thread(
    stubs: list[ThreadStub],
    vault: Path,
    *,
    scan_cache: VaultScanCache | None = None,
) -> ThreadDocument:
    """Load full bodies for stubs and build a :class:`ThreadDocument`."""

    vault = Path(vault)
    ordered = sorted(stubs, key=lambda x: (x.sent_at or "", x.uid))
    messages: list[ThreadMessage] = []
    for s in ordered:
        raw = _load_body_for_stub(s, vault, scan_cache=scan_cache)
        body = clean_email_body(raw)
        messages.append(
            ThreadMessage(
                uid=s.uid,
                rel_path=s.rel_path,
                from_email=s.from_email,
                from_name=s.from_name,
                sent_at=s.sent_at,
                subject=s.subject,
                body=body,
                direction=s.direction,
            )
        )
    seen: list[str] = []
    added: set[str] = set()
    for s in ordered:
        if s.from_email and s.from_email not in added:
            added.add(s.from_email)
            seen.append(s.from_email)
        for e in s.participant_emails:
            if e not in added:
                added.add(e)
                seen.append(e)
    subj = ordered[0].subject if ordered else ""
    dates = [m.sent_at for m in messages if m.sent_at]
    dr = (min(dates), max(dates)) if dates else ("", "")
    tid = ordered[0].gmail_thread_id if ordered else ""
    total_chars = sum(len(m.body) for m in messages)
    return ThreadDocument(
        thread_id=tid,
        messages=messages,
        subject=subj,
        participants=seen,
        date_range=dr,
        message_count=len(messages),
        total_chars=total_chars,
        content_hash=_content_hash_messages(messages),
    )


def approx_token_count(text: str) -> int:
    return int(len(text.split()) * 1.3)


def render_thread_for_triage(stubs: list[ThreadStub]) -> str:
    """Short representation from stubs only (no file reads)."""

    if not stubs:
        return ""
    ordered = sorted(stubs, key=lambda x: (x.sent_at or "", x.uid))
    first = ordered[0]
    last = ordered[-1]
    lines = [
        f"Thread id: {first.gmail_thread_id}",
        f"Subject: {first.subject}",
        f"Messages: {len(stubs)}",
        "",
        "First message snippet:",
        (first.snippet[:300] if first.snippet else "(no snippet)"),
        "",
    ]
    if len(ordered) > 1:
        sn = last.snippet or ""
        lines += ["Last message snippet:", (sn[-200:] if sn else "(no snippet)"), ""]
    pe: set[str] = set()
    for s in ordered:
        if s.from_email:
            pe.add(s.from_email)
        pe.update(s.participant_emails)
    if pe:
        lines.append("Participants: " + ", ".join(sorted(pe)))
    return "\n".join(lines).strip()


def render_thread_for_extraction(thread: ThreadDocument) -> str:
    """Full thread text for extraction; truncates very long threads (first 5 + last 5)."""

    msgs = thread.messages

    def chunk(msg_index: int, m: ThreadMessage) -> str:
        return (
            f"[MSG {msg_index}] uid={m.uid} sent_at={m.sent_at} "
            f"from={m.from_name} <{m.from_email}>\n"
            f"Subject: {m.subject}\n---\n{m.body}"
        )

    parts = [chunk(i + 1, m) for i, m in enumerate(msgs)]
    combined = "\n\n".join(parts)
    if approx_token_count(combined) <= _EXTRACTION_TOKEN_BUDGET:
        return combined

    if len(msgs) > 10:
        omitted = len(msgs) - 10
        logger.warning(
            "thread truncated thread_id=%s omitted_messages=%d (first 5 + last 5)",
            thread.thread_id,
            omitted,
        )
        out: list[str] = []
        for i in range(5):
            out.append(chunk(i + 1, msgs[i]))
        out.append(f"[... {omitted} messages omitted for length ...]")
        for k, m in enumerate(msgs[-5:]):
            out.append(chunk(len(msgs) - 5 + k + 1, m))
        return "\n\n".join(out)

    # Few messages but huge bodies — rough character cap
    cap_chars = int(_EXTRACTION_TOKEN_BUDGET * 4)
    if len(combined) > cap_chars:
        logger.warning(
            "thread truncated thread_id=%s (few messages, oversized bodies)",
            thread.thread_id,
        )
        half = cap_chars // 2
        return combined[:half] + "\n\n[... middle omitted for length ...]\n\n" + combined[-half:]
    return combined
