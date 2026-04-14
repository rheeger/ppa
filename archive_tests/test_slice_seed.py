"""Tests for stratified seed slicer."""

from __future__ import annotations

import json
from pathlib import Path

from archive_cli.test_slice import (
    SliceConfig,
    _build_uid_by_stem,
    _closure_single_seed,
    load_slice_config,
    slice_seed_vault,
)
from archive_tests.fixtures import load_fixture_vault
from archive_vault.vault import read_note_file


def test_slice_config_json_schema(tmp_path: Path) -> None:
    cfg_path = Path("archive_tests/slice_config.json")
    cfg = load_slice_config(cfg_path)
    assert isinstance(cfg.target_percent, float)
    assert isinstance(cfg.primary_user_uid, str)


def test_stratified_slicer_covers_types(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=True)
    out = tmp_path / "slice"
    res = slice_seed_vault(
        src,
        out,
        SliceConfig(target_percent=100.0, min_cards_per_type=1, cluster_cap=5000),
    )
    assert res.selected_card_count > 0
    assert res.orphaned_wikilinks == 0


def test_slice_reproducibility(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=False)
    cfg = SliceConfig(target_percent=50.0, min_cards_per_type=1, cluster_cap=5000)
    o1 = tmp_path / "o1"
    o2 = tmp_path / "o2"
    slice_seed_vault(src, o1, cfg)
    slice_seed_vault(src, o2, cfg)
    c1 = sorted((p.relative_to(o1).as_posix() for p in o1.rglob("*.md")))
    c2 = sorted((p.relative_to(o2).as_posix() for p in o2.rglob("*.md")))
    assert c1 == c2


def test_per_seed_closure_pulls_thread_messages(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=True)
    rel_by_uid = {}
    for note_path in src.rglob("*.md"):
        rel = note_path.relative_to(src)
        note = read_note_file(note_path, vault_root=src)
        rel_by_uid[str(note.frontmatter["uid"])] = rel
    uid_by_stem = _build_uid_by_stem(src, rel_by_uid)
    seen = _closure_single_seed(
        src,
        "hfa-email-thread-fix001abc",
        rel_by_uid=rel_by_uid,
        uid_by_stem=uid_by_stem,
        cluster_cap=5000,
        already_included=set(),
    )
    assert seen is not None
    assert "hfa-email-thread-fix001abc" in seen
    assert "hfa-email-message-fix001abc" in seen
    assert any(uid in seen for uid in {"hfa-person-fixjane001", "hfa-person-im-jane", "hfa-person-graph-jane"})
    assert any(uid in seen for uid in {"hfa-person-fixrobbie01", "hfa-person-graph-robbie"})


def test_primary_user_always_in_slice(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=True)
    out = tmp_path / "slice"
    cfg = SliceConfig(
        target_percent=5.0,
        min_cards_per_type=1,
        cluster_cap=5,
        primary_user_uid="hfa-person-fixrobbie01",
    )
    res = slice_seed_vault(src, out, cfg)
    assert res.selected_card_count > 0
    robbie = out / "People" / "person_2.md"
    assert robbie.exists()
    note = read_note_file(robbie, vault_root=out)
    assert note.frontmatter["uid"] == "hfa-person-fixrobbie01"


def test_primary_user_not_dropped_by_cap(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=True)
    out = tmp_path / "slice"
    cfg = SliceConfig(
        target_percent=0.0,
        min_cards_per_type=0,
        cluster_cap=1,
        primary_user_uid="hfa-person-fixrobbie01",
    )
    res = slice_seed_vault(src, out, cfg)
    assert res.selected_card_count >= 1
    robbie = read_note_file(out / "People" / "person_2.md", vault_root=out)
    assert robbie.frontmatter["uid"] == "hfa-person-fixrobbie01"


def test_zero_orphans_on_fixture_vault(tmp_path: Path) -> None:
    src = load_fixture_vault(tmp_path / "src", include_graphs=True)
    out = tmp_path / "slice"
    res = slice_seed_vault(
        src,
        out,
        SliceConfig(
            target_percent=100.0,
            min_cards_per_type=1,
            cluster_cap=5000,
            primary_user_uid="hfa-person-fixrobbie01",
        ),
    )
    assert res.orphaned_wikilinks == 0


def test_cluster_cap_drops_single_hub_seed(tmp_path: Path) -> None:
    src = tmp_path / "src"
    load_fixture_vault(src, include_graphs=False)
    thread_dir = src / "EmailThreads" / "extra"
    message_dir = src / "Email" / "extra"
    people_dir = src / "People" / "extra"
    thread_dir.mkdir(parents=True, exist_ok=True)
    message_dir.mkdir(parents=True, exist_ok=True)
    people_dir.mkdir(parents=True, exist_ok=True)

    for idx in range(6):
        (people_dir / f"hub-person-{idx}.md").write_text(
            "\n".join(
                [
                    "---",
                    f"uid: hfa-person-hub-{idx}",
                    "type: person",
                    "source:",
                    "  - contacts.apple",
                    f"source_id: hub{idx}@example.com",
                    'created: "2025-06-01"',
                    'updated: "2025-06-15"',
                    f'summary: "Hub Person {idx}"',
                    f"first_name: Hub{idx}",
                    "last_name: Person",
                    "people: []",
                    "orgs: []",
                    "tags: []",
                    "---",
                    "",
                    f"Hub person {idx}.",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    (thread_dir / "hub-thread.md").write_text(
        "\n".join(
            [
                "---",
                "uid: hfa-email-thread-hub",
                "type: email_thread",
                "source:",
                "  - gmail",
                "source_id: gmail.thread.hub",
                'created: "2025-06-15"',
                'updated: "2025-06-15"',
                'summary: "Hub Thread"',
                "gmail_thread_id: thread_hub",
                "messages:",
                "  - hfa-email-message-hub",
                "people:",
                "  - \"[[Hub Person 0]]\"",
                "  - \"[[Hub Person 1]]\"",
                "  - \"[[Hub Person 2]]\"",
                "  - \"[[Hub Person 3]]\"",
                "  - \"[[Hub Person 4]]\"",
                "  - \"[[Hub Person 5]]\"",
                "orgs: []",
                "tags: []",
                "---",
                "",
                "Hub thread body.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (message_dir / "hub-message.md").write_text(
        "\n".join(
            [
                "---",
                "uid: hfa-email-message-hub",
                "type: email_message",
                "source:",
                "  - gmail",
                "source_id: gmail.msg.hub",
                'created: "2025-06-15"',
                'updated: "2025-06-15"',
                'summary: "Hub Message"',
                "gmail_message_id: msg_hub",
                "gmail_thread_id: thread_hub",
                "from_email: hub@example.com",
                "to_emails:",
                "  - robbie@example.com",
                'subject: "Hub Message"',
                'sent_at: "2025-06-15T14:30:00-07:00"',
                'thread: "[[hfa-email-thread-hub]]"',
                "people:",
                "  - \"[[Hub Person 0]]\"",
                "  - \"[[Hub Person 1]]\"",
                "  - \"[[Hub Person 2]]\"",
                "  - \"[[Hub Person 3]]\"",
                "  - \"[[Hub Person 4]]\"",
                "  - \"[[Hub Person 5]]\"",
                "orgs: []",
                "tags: []",
                "---",
                "",
                "Hub message body.",
                "",
            ]
        ),
        encoding="utf-8",
    )

    (thread_dir / "small-thread.md").write_text(
        "\n".join(
            [
                "---",
                "uid: hfa-email-thread-small",
                "type: email_thread",
                "source:",
                "  - gmail",
                "source_id: gmail.thread.small",
                'created: "2025-06-15"',
                'updated: "2025-06-15"',
                'summary: "Small Thread"',
                "gmail_thread_id: thread_small",
                "messages:",
                "  - hfa-email-message-small",
                "people:",
                "  - \"[[Jane Smith]]\"",
                "orgs: []",
                "tags: []",
                "---",
                "",
                "Small thread body.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (message_dir / "small-message.md").write_text(
        "\n".join(
            [
                "---",
                "uid: hfa-email-message-small",
                "type: email_message",
                "source:",
                "  - gmail",
                "source_id: gmail.msg.small",
                'created: "2025-06-15"',
                'updated: "2025-06-15"',
                'summary: "Small Message"',
                "gmail_message_id: msg_small",
                "gmail_thread_id: thread_small",
                "from_email: jane@example.com",
                "to_emails:",
                "  - robbie@example.com",
                'subject: "Small Message"',
                'sent_at: "2025-06-15T14:30:00-07:00"',
                'thread: "[[hfa-email-thread-small]]"',
                "people:",
                "  - \"[[Jane Smith]]\"",
                "  - \"[[Robbie Heeger]]\"",
                "orgs: []",
                "tags: []",
                "---",
                "",
                "Small message body.",
                "",
            ]
        ),
        encoding="utf-8",
    )

    cfg_path = tmp_path / "slice_config.json"
    cfg_path.write_text(
        json.dumps(
            {
                "vault_commit": "",
                "snapshot_date": "2026-04-01",
                "seed_uids_by_type": {
                    "email_thread": [
                        "hfa-email-thread-hub",
                        "hfa-email-thread-small",
                    ]
                },
                "cluster_cap": 5,
                "min_cards_per_type": 1,
                "target_percent": 10,
                "primary_user_uid": "hfa-person-fixrobbie01",
            }
        ),
        encoding="utf-8",
    )
    cfg = load_slice_config(cfg_path)
    out = tmp_path / "slice"
    slice_seed_vault(src, out, cfg)
    thread_uids = {
        read_note_file(path, vault_root=out).frontmatter["uid"]
        for path in out.rglob("*.md")
        if path.parts and "EmailThreads" in path.parts
    }
    assert "hfa-email-thread-small" in thread_uids
    assert "hfa-email-thread-hub" not in thread_uids
