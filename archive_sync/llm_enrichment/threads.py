"""Thread assembly for LLM enrichment — vault scan cache first, filesystem fallback."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive_cli.vault_cache import VaultScanCache
from archive_sync.extractors.preprocessing import clean_email_body, strip_reply_artifacts
from archive_vault.thread_hash import slug_from_wikilink
from archive_vault.vault import find_note_by_slug, iter_note_paths, read_note_file, read_note_frontmatter_file

logger = logging.getLogger("ppa.llm_enrichment.threads")

# Room for system prompt + JSON output; Gemini Flash family supports large context — use it for long mail.
_EXTRACTION_TOKEN_BUDGET = 250_000


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
    """Walk ``Email/`` and build thread stubs.

    When ``PPA_ENGINE=rust`` and a tier-2 cache exists, reads from SQLite with no per-file I/O.
    """

    vault = Path(vault)

    from archive_cli.ppa_engine import ppa_engine

    if ppa_engine() == "rust":
        from archive_vault.vault import _tier2_cache_path

        cache_path = _tier2_cache_path(vault)
        if cache_path is not None:
            try:
                import archive_crate

                rows = archive_crate.frontmatter_dicts_from_cache(
                    str(cache_path), types=["email_message"], prefix="Email/",
                )
                out: list[ThreadStub] = []
                for row in rows:
                    stub = thread_stub_from_frontmatter(row["rel_path"], row["frontmatter"])
                    if stub:
                        out.append(stub)
                return out
            except Exception:
                pass

    out = []
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


@dataclass
class MessageStubIndex:
    """Bulk in-memory index for iMessage / Beeper message card resolution.

    Built once per run from a single SQL query; eliminates per-wikilink
    SQLite lookups during eligibility scanning and hydration.
    """

    slug_to_rel: dict[str, str]
    rel_to_fm: dict[str, dict[str, Any]]

    def resolve_slug(self, slug: str) -> str | None:
        s = (slug or "").strip()
        if not s:
            return None
        return (
            self.slug_to_rel.get(s)
            or self.slug_to_rel.get(s.replace(" ", "-"))
            or self.slug_to_rel.get(s.replace(" ", "_"))
        )

    def frontmatter(self, rel_path: str) -> dict[str, Any] | None:
        return self.rel_to_fm.get(rel_path)


@dataclass
class ParticipantNameResolver:
    """Maps sender handles (phone / email) → human-readable names.

    Built once per run from person cards + thread ``people`` wikilinks.
    """

    handle_to_name: dict[str, str]
    slug_to_name: dict[str, str]

    def resolve(self, handle: str) -> str | None:
        h = (handle or "").strip()
        if not h:
            return None
        return self.handle_to_name.get(h) or self.handle_to_name.get(h.lstrip("+"))

    def names_for_thread(
        self,
        thread_card: dict[str, Any],
    ) -> dict[str, str]:
        """Build handle→name mapping for a specific thread using its ``people`` wikilinks + fallback."""

        out: dict[str, str] = {}
        people_links = thread_card.get("people") or []
        handles = thread_card.get("participant_handles") or []
        if not isinstance(people_links, list):
            people_links = []
        if not isinstance(handles, list):
            handles = []

        for wl in people_links:
            slug = str(wl).strip()
            if slug.startswith("[[") and slug.endswith("]]"):
                slug = slug[2:-2].split("|", 1)[0].strip()
            name = self.slug_to_name.get(slug)
            if not name:
                name = slug.replace("-", " ").replace("_", " ").title()
            # Try to pair with a handle via person card phone/email
            # Person card phones may lack '+' prefix; normalize both sides
            for h in handles:
                h_str = str(h).strip()
                if h_str in out:
                    continue
                h_norm = h_str.lstrip("+")
                existing = self.handle_to_name.get(h_str) or self.handle_to_name.get(h_norm)
                if existing and existing == name:
                    out[h_str] = name
                    break

        for h in handles:
            h_str = str(h).strip()
            if h_str not in out:
                resolved = self.resolve(h_str)
                if resolved:
                    out[h_str] = resolved

        return out


def build_participant_name_resolver(scan_cache: VaultScanCache) -> ParticipantNameResolver:
    """One SQL query over person cards → handle-to-name + slug-to-name maps."""

    import time as _time

    t0 = _time.perf_counter()
    with scan_cache._lock:
        rows = scan_cache._conn.execute(
            "SELECT slug, frontmatter_json FROM notes WHERE card_type = 'person'",
        ).fetchall()

    handle_to_name: dict[str, str] = {}
    slug_to_name: dict[str, str] = {}

    for slug_raw, fj_raw in rows:
        slug = str(slug_raw)
        try:
            fm = json.loads(fj_raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(fm, dict):
            continue
        name = str(fm.get("summary") or "").strip()
        if not name:
            continue
        slug_to_name[slug] = name
        for phone in fm.get("phones") or []:
            p = str(phone).strip()
            if p:
                handle_to_name[p] = name
                handle_to_name[p.lstrip("+")] = name
                clean = "".join(c for c in p if c.isdigit())
                if clean:
                    handle_to_name[clean] = name
                    handle_to_name["+" + clean] = name
        for email in fm.get("emails") or []:
            e = str(email).strip().lower()
            if e:
                handle_to_name[e] = name

    elapsed = _time.perf_counter() - t0
    logger.info(
        "participant-name-resolver built handles=%d person_slugs=%d elapsed=%.2fs",
        len(handle_to_name),
        len(slug_to_name),
        elapsed,
    )
    return ParticipantNameResolver(handle_to_name=handle_to_name, slug_to_name=slug_to_name)


def build_message_stub_index(scan_cache: VaultScanCache) -> MessageStubIndex:
    """One SQL query → in-memory slug/frontmatter dicts for imessage_message + beeper_message."""

    import time as _time

    t0 = _time.perf_counter()
    with scan_cache._lock:
        rows = scan_cache._conn.execute(
            "SELECT slug, rel_path, frontmatter_json FROM notes "
            "WHERE card_type IN ('imessage_message', 'beeper_message')",
        ).fetchall()

    slug_to_rel: dict[str, str] = {}
    rel_to_fm: dict[str, dict[str, Any]] = {}
    for slug_raw, rp_raw, fj_raw in rows:
        slug = str(slug_raw)
        rp = str(rp_raw)
        slug_to_rel.setdefault(slug, rp)
        try:
            fm = json.loads(fj_raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(fm, dict):
            rel_to_fm[rp] = fm

    elapsed = _time.perf_counter() - t0
    logger.info(
        "message-stub-index built slugs=%d frontmatters=%d elapsed=%.2fs",
        len(slug_to_rel),
        len(rel_to_fm),
        elapsed,
    )
    return MessageStubIndex(slug_to_rel=slug_to_rel, rel_to_fm=rel_to_fm)


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
        body = strip_reply_artifacts(clean_email_body(raw))
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


def _render_extraction_blocks(msgs: list[ThreadMessage], bodies: list[str]) -> str:
    out: list[str] = []
    for i, m in enumerate(msgs):
        out.append(
            f"[MSG {i + 1}] uid={m.uid} sent_at={m.sent_at} "
            f"from={m.from_name} <{m.from_email}>\n"
            f"Subject: {m.subject}\n---\n{bodies[i]}"
        )
    return "\n\n".join(out)


def imessage_thread_content_hash(messages: list[ThreadMessage]) -> str:
    """SHA-256 over ordered message uids + bodies (cache invalidation)."""

    blob = "\n---\n".join(f"{m.uid}\n{m.body}" for m in messages)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _resolve_message_rel_path(
    wikilink: str,
    vault: Path,
    *,
    scan_cache: VaultScanCache | None,
) -> str | None:
    slug = slug_from_wikilink(str(wikilink))
    if not slug:
        return None
    if scan_cache is not None:
        hit = scan_cache.rel_path_for_slug(slug)
        if hit:
            return hit
    note = find_note_by_slug(vault, slug)
    if note is None:
        return None
    try:
        return str(note.resolve().relative_to(Path(vault).resolve()))
    except ValueError:
        return None


def _load_body_imessage(
    rel_path: str,
    vault: Path,
    *,
    scan_cache: VaultScanCache | None,
) -> str:
    if scan_cache is not None and scan_cache.tier() >= 2:
        try:
            return scan_cache.body_for_rel_path(rel_path)
        except ValueError:
            pass
    parsed = read_note_file(vault / rel_path, vault_root=vault)
    return parsed.body


def hydrate_imessage_thread(
    thread_card: dict[str, Any],
    vault: Path,
    *,
    scan_cache: VaultScanCache | None = None,
    msg_index: MessageStubIndex | None = None,
    handle_names: dict[str, str] | None = None,
) -> list[ThreadMessage]:
    """Load full message bodies for an iMessage/Beeper thread.

    When *msg_index* is provided, slug→rel_path and frontmatter come from
    in-memory dicts (fast).  Falls back to per-link scan_cache / filesystem
    when the index is not available.

    *handle_names* maps sender handles (phone/email) → human-readable names
    so ``from_name`` is e.g. "Jordan Rochelson" instead of "+15162254830".
    """

    vault = Path(vault).resolve()
    raw_links = thread_card.get("messages") or []
    if not isinstance(raw_links, list):
        return []
    _names = handle_names or {}
    messages: list[ThreadMessage] = []
    for wikilink in raw_links:
        slug = slug_from_wikilink(str(wikilink))
        if not slug:
            continue

        rel: str | None = None
        if msg_index is not None:
            rel = msg_index.resolve_slug(slug)
        if rel is None:
            rel = _resolve_message_rel_path(wikilink, vault, scan_cache=scan_cache)
        if not rel:
            continue

        fm: dict[str, Any] | None = None
        if msg_index is not None:
            fm = msg_index.frontmatter(rel)
        if fm is None:
            try:
                if scan_cache is not None:
                    fm = scan_cache.frontmatter_for_rel_path(rel)
                else:
                    fm = read_note_frontmatter_file(vault / rel, vault_root=vault).frontmatter
            except (KeyError, OSError):
                continue
        if fm is None:
            continue
        ctype = str(fm.get("type") or "")
        if ctype not in ("imessage_message", "beeper_message"):
            continue
        body = _load_body_imessage(rel, vault, scan_cache=scan_cache)
        uid = str(fm.get("uid") or "").strip()
        sent_at = str(fm.get("sent_at") or "").strip()
        is_from_me = bool(fm.get("is_from_me"))
        direction = "sent" if is_from_me else "received"
        if ctype == "beeper_message":
            from_handle = str(fm.get("sender_identifier") or "").strip()
            from_name = str(fm.get("sender_name") or "").strip()
            if not from_name:
                from_name = _names.get(from_handle) or from_handle
        else:
            from_handle = str(fm.get("sender_handle") or "").strip()
            from_name = _names.get(from_handle) or from_handle
        subj = str(fm.get("subject") or "").strip()
        messages.append(
            ThreadMessage(
                uid=uid or rel,
                rel_path=rel,
                from_email=from_handle,
                from_name=from_name,
                sent_at=sent_at,
                subject=subj,
                body=body.strip(),
                direction=direction,
            )
        )
    messages.sort(key=lambda m: (m.sent_at or "", m.uid))
    return messages


def chunk_thread_messages(
    messages: list[ThreadMessage],
    *,
    chunk_size: int = 800,
    overlap: int = 50,
) -> list[list[ThreadMessage]]:
    """Split into overlapping windows (large threads → multiple LLM calls)."""

    if not messages:
        return []
    if len(messages) <= chunk_size:
        return [messages]
    out: list[list[ThreadMessage]] = []
    step = max(1, chunk_size - overlap)
    i = 0
    while i < len(messages):
        out.append(messages[i : i + chunk_size])
        if i + chunk_size >= len(messages):
            break
        i += step
    return out


def render_imessage_chunk_for_llm(
    messages: list[ThreadMessage],
    *,
    display_label: str,
    context_header: str,
) -> str:
    """Chronological lines: ``[ME]`` or ``[Name]`` + timestamp + body."""

    lines = [
        f"Chat: {display_label}",
        context_header.strip(),
        "",
        "--- MESSAGES ---",
        "",
    ]
    for m in messages:
        ts = m.sent_at[:19] if len(m.sent_at) >= 19 else (m.sent_at or "?")
        if m.direction == "sent":
            who = "ME"
        else:
            who = (m.from_name or m.from_email or "contact").strip() or "contact"
        lines.append(f"[{who}] {ts}")
        lines.append(m.body)
        lines.append("")
    return "\n".join(lines).strip()


def render_thread_for_extraction(thread: ThreadDocument) -> str:
    """Render full thread for LLM: prefer **all messages** with equal per-message caps; last resort first+last."""

    msgs = thread.messages
    if not msgs:
        return ""

    bodies = [m.body for m in msgs]
    combined = _render_extraction_blocks(msgs, bodies)
    if approx_token_count(combined) <= _EXTRACTION_TOKEN_BUDGET:
        return combined

    max_body = max(len(b) for b in bodies)
    lo, hi = 0, max_body + 1
    best_trial: str | None = None
    best_cap: int | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        capped = [b[:mid] if len(b) > mid else b for b in bodies]
        trial = _render_extraction_blocks(msgs, capped)
        if approx_token_count(trial) <= _EXTRACTION_TOKEN_BUDGET:
            best_trial = trial
            best_cap = mid
            lo = mid + 1
        else:
            hi = mid - 1

    if best_trial is not None:
        if best_cap is not None and best_cap < max_body:
            logger.warning(
                "thread thread_id=%s per_message_cap_chars=%d messages=%d",
                thread.thread_id,
                best_cap,
                len(msgs),
            )
        return best_trial

    # Even empty bodies exceed budget (extremely many messages): drop middle.
    if len(msgs) > 10:
        omitted = len(msgs) - 10
        logger.warning(
            "thread truncated thread_id=%s omitted_messages=%d (first 5 + last 5; token budget)",
            thread.thread_id,
            omitted,
        )
        slot_msgs = msgs[:5] + msgs[-5:]
        slot_bodies = [m.body for m in slot_msgs]
        sub = _render_extraction_blocks(slot_msgs, slot_bodies)
        if approx_token_count(sub) <= _EXTRACTION_TOKEN_BUDGET:
            return sub
        max_s = max(len(b) for b in slot_bodies)
        lo2, hi2 = 0, max_s + 1
        best2: str | None = None
        best_c2: int | None = None
        while lo2 <= hi2:
            mid = (lo2 + hi2) // 2
            cap = [b[:mid] if len(b) > mid else b for b in slot_bodies]
            trial = _render_extraction_blocks(slot_msgs, cap)
            if approx_token_count(trial) <= _EXTRACTION_TOKEN_BUDGET:
                best2 = trial
                best_c2 = mid
                lo2 = mid + 1
            else:
                hi2 = mid - 1
        if best2 is not None:
            if best_c2 is not None and best_c2 < max_s:
                logger.warning(
                    "thread thread_id=%s per_message_cap_chars=%d (first+last window)",
                    thread.thread_id,
                    best_c2,
                )
            return best2

    # ≤10 messages, bodies still huge — cut concatenated blob in the middle
    cap_chars = int(_EXTRACTION_TOKEN_BUDGET * 4)
    if len(combined) > cap_chars:
        logger.warning(
            "thread truncated thread_id=%s (few messages, oversized bodies)",
            thread.thread_id,
        )
        half = cap_chars // 2
        return (
            combined[:half]
            + "\n\n[... middle omitted for length ...]\n\n"
            + combined[-half:]
        )
    return combined
