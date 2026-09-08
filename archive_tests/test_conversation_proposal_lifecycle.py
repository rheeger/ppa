from __future__ import annotations

from archive_cli.commands.identity_repair import emit_same_conversation_edges
from archive_engine.conversation_links import enumerate_pairs, preview_or_apply


def test_enumerate_all_pairs_not_first_element() -> None:
    threads = [
        {"uid": "e1", "people": ["[[pat]]"], "participant_handles": ["pat@example.com"]},
        {"uid": "e2", "people": ["[[pat]]"], "participant_handles": ["pat+work@example.com"]},
        {"uid": "p1", "people": ["[[pat]]"], "participant_handles": ["+15551230000"]},
        {"uid": "p2", "people": ["[[pat]]"], "participant_handles": ["+15551239999"]},
    ]
    result = enumerate_pairs(threads)
    assert result["count"] == 4
    assert result["truncated"] is False


def test_preview_writes_nothing(tmp_path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir(parents=True, exist_ok=True)
    result = preview_or_apply(
        vault,
        [{"uid": "e1", "people": ["[[pat]]"], "participant_handles": ["pat@example.com"]}],
        apply=False,
    )
    assert result["applied"] is False
    assert not (vault / "_meta" / "conversation-proposals.json").exists()
    assert not (vault / "_meta" / "same-conversation.json").exists()
    assert not (vault / "_meta" / "change-journal.sqlite3").exists()


def test_apply_journals_proposals(tmp_path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    result = preview_or_apply(
        vault,
        [
            {"uid": "e1", "people": ["[[pat]]"], "participant_handles": ["pat@example.com"]},
            {"uid": "p1", "people": ["[[pat]]"], "participant_handles": ["+15551230000"]},
        ],
        apply=True,
    )
    assert result["applied"] is True
    assert result["mutation_id"]
    assert (vault / "_meta" / "conversation-proposals.json").is_file()
    assert (vault / "_meta" / "change-journal.sqlite3").is_file()


def test_cli_preview_honors_apply_false(tmp_path) -> None:
    vault = tmp_path / "vault"
    (vault / "Messages").mkdir(parents=True, exist_ok=True)
    result = emit_same_conversation_edges(vault, apply=False)
    assert result["applied"] is False
    assert result["path"] == ""
    assert not (vault / "_meta").exists() or not (vault / "_meta" / "same-conversation.json").exists()
