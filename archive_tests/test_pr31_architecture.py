from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_identity_resolution_does_not_clone_person_index() -> None:
    text = (ROOT / "archive_engine" / "identity_resolution.py").read_text(encoding="utf-8")
    tree = ast.parse(text)
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
        elif isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
    assert "archive_cli.commands.identity_repair" not in names
    assert "PersonIndex" not in text


def test_serving_export_does_not_promote() -> None:
    text = (ROOT / "archive_engine" / "adapters" / "serving_export.py").read_text(encoding="utf-8")
    assert "serving_index_publish" not in text
    assert "acknowledge_captured" not in text


def test_conversation_links_are_proposed() -> None:
    text = (ROOT / "archive_engine" / "conversation_links.py").read_text(encoding="utf-8")
    assert "proposed_link" in text
    assert "serving_index_publish" not in text
    assert "persist_json_state" in text


def test_identity_queue_is_journaled() -> None:
    text = (ROOT / "archive_cli" / "commands" / "identity_repair.py").read_text(encoding="utf-8")
    assert "dedup-candidates.json" not in text
    assert "IDENTITY_PROPOSALS" in text or "persist_json_state" in text


def test_thread_projection_owns_rollup() -> None:
    text = (ROOT / "archive_cli" / "commands" / "identity_repair.py").read_text(encoding="utf-8")
    assert "project_from_rows" in text or "drain_pending" in text
    maintain = (ROOT / "archive_cli" / "commands" / "maintain.py").read_text(encoding="utf-8")
    assert "drain_pending" in maintain
    assert "pending_thread_uids" in maintain


def test_repair_merge_calls_corrections() -> None:
    text = (ROOT / "archive_cli" / "commands" / "identity_repair.py").read_text(encoding="utf-8")
    assert "merge_identities" in text
    assert "redirect_to" not in text or "merge_identities" in text
