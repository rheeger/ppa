"""Compact evidence listing: no full bodies, dated hits, composition in --help."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_server import FakeIndex, _seed_vault

import archive_cli.commands._resolve as resolve_mod
from archive_cli.card_traversal import (
    assert_compact_payload,
    compact_hits,
    narrative_outline,
    stack_pointers_from_frontmatter,
    uids_from_frontmatter_list,
)
from archive_cli.commands import evidence as evidence_cmd
from archive_cli.commands import formatters as fmt
from archive_cli.commands import read as read_cmd
from archive_cli.mcp_instructions import CARD_STACK_PLAYBOOK_HELP, TOOL_DESCRIPTIONS
from archive_cli.server import archive_evidence, archive_read
from archive_cli.store import DefaultArchiveStore

PPA_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def tmp_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "Email").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://archive:archive@localhost:5432/archive")
    return vault


@pytest.fixture
def fake_index(monkeypatch: pytest.MonkeyPatch) -> FakeIndex:
    fake = FakeIndex()
    monkeypatch.setattr(resolve_mod, "get_index", lambda vault=None: fake)
    return fake


@pytest.fixture
def command_store(tmp_vault: Path, fake_index: FakeIndex) -> DefaultArchiveStore:
    return DefaultArchiveStore(vault=tmp_vault, index=fake_index)


def test_uids_from_wikilinks_and_frontmatter() -> None:
    assert uids_from_frontmatter_list(["[[hfa-email-attachment-aaa]]", "hfa-doc-bbb"]) == [
        "hfa-email-attachment-aaa",
        "hfa-doc-bbb",
    ]
    ptrs = stack_pointers_from_frontmatter(
        {
            "attachments": ["[[hfa-email-attachment-aaa]]"],
            "duplicates": ["[[hfa-doc-bbb]]"],
            "message": "[[hfa-email-message-ccc]]",
        }
    )
    assert ptrs["attachment_uids"] == ["hfa-email-attachment-aaa"]
    assert ptrs["duplicate_uids"] == ["hfa-doc-bbb"]
    assert ptrs["parent_uid"] == "hfa-email-message-ccc"


def test_compact_hits_drop_full_attachment_body() -> None:
    rows = [
        {
            "card_uid": "hfa-email-message-111",
            "rel_path": "Email/2024/hfa-email-message-111.md",
            "summary": "Lease PDF from Sarah",
            "type": "email_message",
            "activity_at": "2024-06-02T10:00:00+00:00",
            "matched_by": "lexical",
            "preview": "FULL OCR PAGE 1 " + ("lorem " * 200),
            "content": "must not appear",
        },
        {
            "card_uid": "hfa-email-attachment-222",
            "rel_path": "EmailAttachments/2024/hfa-email-attachment-222.md",
            "summary": "lease.pdf",
            "type": "email_attachment",
            "activity_at": "2024-06-01T09:00:00+00:00",
            "matched_by": "lexical",
            "preview": "EXTRACTED MARKDOWN FROM PDF " + ("x" * 500),
        },
    ]
    pointers = {
        "hfa-email-message-111": {
            "attachment_uids": ["hfa-email-attachment-222"],
            "duplicate_uids": [],
            "parent_uid": "",
        },
        "hfa-email-attachment-222": {
            "attachment_uids": [],
            "duplicate_uids": ["hfa-document-333"],
            "parent_uid": "hfa-email-message-111",
        },
    }
    hits = compact_hits(rows, pointers_by_uid=pointers, question="Sarah lease")
    payload = {"hits": hits}
    assert_compact_payload(payload)
    dumped = str(payload)
    assert "FULL OCR" not in dumped
    assert "EXTRACTED MARKDOWN" not in dumped
    assert "must not appear" not in dumped
    assert [h["date"] for h in hits] == ["2024-06-01", "2024-06-02"]
    assert hits[0]["parent_uid"] == "hfa-email-message-111"
    assert hits[1]["attachment_uids"] == ["hfa-email-attachment-222"]
    outline = narrative_outline(hits)
    assert "2024-06-01" in outline
    assert "hfa-email-attachment-222" in outline
    assert "FULL OCR" not in outline
    text = fmt.format_evidence(payload)
    assert "FULL OCR" not in text
    assert "hfa-email-message-111" in text


def test_evidence_command_compact_and_dated(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.evidence")
    result = evidence_cmd.evidence(
        query="Jane",
        limit=8,
        narrative=True,
        store=command_store,
        logger=log,
    )
    assert_compact_payload(result)
    hits = result["hits"]
    assert hits
    assert hits[0]["date"] == "2026-03-06"
    assert hits[0]["uid"] == "hfa-person-aaaabbbbcccc"
    assert "preview" not in hits[0]
    assert "FULL ATTACHMENT OCR" not in str(result)
    assert "2026-03-06" in result["narrative"]
    assert "hfa-person-aaaabbbbcccc" in result["narrative"]


def test_archive_evidence_mcp_text_is_compact(tmp_vault: Path, fake_index: FakeIndex) -> None:
    _seed_vault(tmp_vault)
    out = archive_evidence(query="Jane", narrative=True)
    assert "hfa-person-aaaabbbbcccc" in out
    assert "2026-03-06" in out
    assert "FULL ATTACHMENT OCR" not in out
    assert "Narrative:" in out


def test_read_flags_are_links_only(tmp_vault: Path, fake_index: FakeIndex, command_store: DefaultArchiveStore) -> None:
    rel = "Email/2024/hfa-email-message-stack.md"
    path = tmp_vault / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    ocr_blob = "PDF OCR PAGE " + ("secret-extract " * 80)
    path.write_text(
        f"""---
uid: hfa-email-message-stack
type: email_message
source: [gmail]
created: '2024-06-02'
summary: Stack parent
attachments: ['[[hfa-email-attachment-aaa]]']
duplicates: ['[[hfa-document-bbb]]']
---

Body of the email only.

## Attachment dump that must not be re-emitted as other cards
{ocr_blob}
""",
        encoding="utf-8",
    )
    orig = fake_index.read_path_for_uid

    def _read_path(uid: str):
        if uid == "hfa-email-message-stack":
            return rel
        return orig(uid)

    fake_index.read_path_for_uid = _read_path  # type: ignore[method-assign]
    log = logging.getLogger("test.read")
    result = read_cmd.read(
        "hfa-email-message-stack",
        store=command_store,
        logger=log,
        include_attachment_uids=True,
        include_duplicate_uids=True,
    )
    assert result["found"] is True
    assert result["attachment_uids"] == ["hfa-email-attachment-aaa"]
    assert result["duplicate_uids"] == ["hfa-document-bbb"]
    mcp_out = archive_read(
        "hfa-email-message-stack",
        include_attachment_uids=True,
        include_duplicate_uids=True,
    )
    assert "hfa-email-attachment-aaa" in mcp_out
    assert "links only" in mcp_out
    footer = mcp_out.split("links only", 1)[1]
    assert "secret-extract" not in footer
    assert "hfa-email-attachment-aaa" in footer


def test_cli_help_includes_composition() -> None:
    env = {**os.environ, "PPA_PATH": "/tmp/ppa-help-vault"}
    for cmd in ("--help", "evidence", "search", "read", "query", "timeline", "hybrid-search"):
        args = [sys.executable, "-m", "archive_cli"]
        if cmd == "--help":
            args.append("--help")
        else:
            args.extend([cmd, "--help"])
        out = subprocess.check_output(args, cwd=str(PPA_ROOT), env=env, text=True)
        lowered = out.lower()
        assert "compose" in lowered or "follow" in lowered
        assert "search" in lowered or "hybrid" in lowered or "query" in lowered
        if cmd == "evidence":
            assert "compact" in lowered
            assert "raise" in lowered or "wider" in lowered
        if cmd in {"search", "hybrid-search", "query"}:
            assert "never use" not in lowered
            assert "discovery only" not in lowered


def test_mcp_tool_docs_include_composition_needles() -> None:
    for name in ("archive_search", "archive_evidence", "archive_read", "archive_hybrid_search", "archive_query"):
        desc = TOOL_DESCRIPTIONS[name]
        assert len(desc) > 80
    evidence = TOOL_DESCRIPTIONS["archive_evidence"].lower()
    assert "compact" in evidence
    assert "raise" in evidence
    assert CARD_STACK_PLAYBOOK_HELP
    assert "compose" in CARD_STACK_PLAYBOOK_HELP.lower()
