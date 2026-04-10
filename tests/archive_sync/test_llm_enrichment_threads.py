"""Tests for archive_sync.llm_enrichment.threads."""

from __future__ import annotations

from pathlib import Path

import pytest
from archive_mcp.vault_cache import VaultScanCache
from archive_sync.llm_enrichment.threads import (
    ThreadStub, build_thread_index, build_thread_index_from_cache,
    email_message_stubs_from_sqlite, hydrate_thread,
    load_email_stubs_for_vault, render_thread_for_extraction,
    render_thread_for_triage, stubs_from_filesystem_walk,
    thread_stub_from_frontmatter)
from tests.fixtures import load_fixture_vault


def test_thread_stub_from_frontmatter_filters_type() -> None:
    assert thread_stub_from_frontmatter("x.md", {"type": "person"}) is None


def test_build_thread_index_groups_and_sorts() -> None:
    a = ThreadStub(
        uid="u1",
        rel_path="Email/a.md",
        gmail_thread_id="t1",
        sent_at="2025-01-02",
        from_email="a@x.com",
        from_name="A",
        subject="S",
        snippet="sn",
        direction="inbound",
    )
    b = ThreadStub(
        uid="u2",
        rel_path="Email/b.md",
        gmail_thread_id="t1",
        sent_at="2025-01-01",
        from_email="b@x.com",
        from_name="B",
        subject="S",
        snippet="sn2",
        direction="inbound",
    )
    idx = build_thread_index([a, b])
    assert list(idx.keys()) == ["t1"]
    assert [x.uid for x in idx["t1"]] == ["u2", "u1"]


def test_render_thread_for_triage_includes_snippets() -> None:
    stubs = [
        ThreadStub(
            uid="u1",
            rel_path="Email/1.md",
            gmail_thread_id="tid",
            sent_at="2025-01-01",
            from_email="a@e.com",
            from_name="A",
            subject="Subj",
            snippet="first snippet text " * 20,
            direction="inbound",
        ),
        ThreadStub(
            uid="u2",
            rel_path="Email/2.md",
            gmail_thread_id="tid",
            sent_at="2025-01-02",
            from_email="b@e.com",
            from_name="B",
            subject="Subj",
            snippet="last end snippet " * 30,
            direction="inbound",
        ),
    ]
    text = render_thread_for_triage(stubs)
    assert "Thread id: tid" in text
    assert "Messages: 2" in text
    assert "Participants:" in text


def test_fixture_vault_email_roundtrip(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=False)
    cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    stubs_sql = email_message_stubs_from_sqlite(cache_path)
    stubs_fs = stubs_from_filesystem_walk(vault)
    assert len(stubs_sql) == len(stubs_fs)
    idx = build_thread_index_from_cache(cache_path)
    assert idx
    tid = next(iter(idx))
    doc = hydrate_thread(idx[tid], vault, scan_cache=cache)
    assert doc.message_count >= 1
    assert doc.content_hash
    assert "Q3" in render_thread_for_extraction(doc) or "planning" in render_thread_for_extraction(doc).lower()


def test_load_email_stubs_prefers_cache(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=False)
    VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    stubs = load_email_stubs_for_vault(vault)
    assert any(s.uid for s in stubs)


def test_extraction_truncates_long_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_sync.llm_enrichment import threads as T

    # Force per-message capping (full 12-message render would otherwise fit the default budget).
    monkeypatch.setattr(T, "_EXTRACTION_TOKEN_BUDGET", 12_000)

    n_msgs = 12
    body = "word " * 8000
    msgs = [
        T.ThreadMessage(
            uid=f"u{i}",
            rel_path=f"Email/{i}.md",
            from_email="x@y.com",
            from_name="X",
            sent_at=f"2025-01-{i+1:02d}",
            subject="S",
            body=body,
            direction="inbound",
        )
        for i in range(n_msgs)
    ]
    doc = T.ThreadDocument(
        thread_id="big",
        messages=msgs,
        subject="S",
        participants=["x@y.com"],
        date_range=("2025-01-01", "2025-01-12"),
        message_count=n_msgs,
        total_chars=sum(len(m.body) for m in msgs),
        content_hash="x",
    )
    rendered = render_thread_for_extraction(doc)
    # Prefer all messages with equal per-message caps (not first+last only).
    assert rendered.count("[MSG ") == 12
    assert len(rendered) < sum(len(m.body) for m in msgs)
