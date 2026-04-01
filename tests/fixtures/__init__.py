"""Fixture loading helpers for the PPA test infrastructure.

Convention: every card fixture is a .md file with YAML frontmatter + markdown body,
matching the vault's actual format. Fixtures are auto-discovered by glob.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from hfa.card_contracts import CARD_TYPE_SPECS
from hfa.vault import read_note_file

FIXTURES_DIR = Path(__file__).parent
CARDS_DIR = FIXTURES_DIR / "cards"
GRAPHS_DIR = FIXTURES_DIR / "graphs"
EDGE_CASES_DIR = FIXTURES_DIR / "edge_cases"


def iter_card_fixture_paths() -> list[Path]:
    return sorted(CARDS_DIR.glob("*.md"))


def iter_graph_fixture_sets() -> dict[str, list[Path]]:
    result: dict[str, list[Path]] = {}
    for graph_dir in sorted(GRAPHS_DIR.iterdir()):
        if graph_dir.is_dir():
            result[graph_dir.name] = sorted(graph_dir.glob("*.md"))
    return result


def _type_to_dir(card_type: str) -> str:
    spec = CARD_TYPE_SPECS.get(card_type)
    return spec.rel_path_family if spec else "Documents"


def load_fixture_vault(target: Path, *, include_graphs: bool = False) -> Path:
    """Copy all card fixtures into a properly structured tmp vault directory."""
    for d in [
        "People",
        "Finance",
        "Medical",
        "Vaccinations",
        "Calendar",
        "Email",
        "EmailThreads",
        "EmailAttachments",
        "IMessage",
        "IMessageThreads",
        "IMessageAttachments",
        "Beeper",
        "BeeperThreads",
        "BeeperAttachments",
        "Photos",
        "Documents",
        "MeetingTranscripts",
        "GitRepos",
        "GitCommits",
        "GitThreads",
        "GitMessages",
        "Attachments",
        "_templates",
        ".obsidian",
        "_meta",
    ]:
        (target / d).mkdir(parents=True, exist_ok=True)
    (target / "_meta" / "identity-map.json").write_text("{}")
    (target / "_meta" / "sync-state.json").write_text("{}")
    for fixture_path in iter_card_fixture_paths():
        note = read_note_file(fixture_path, vault_root=FIXTURES_DIR)
        card_type = str(note.frontmatter.get("type", ""))
        dest_dir = _type_to_dir(card_type)
        dest = target / dest_dir / fixture_path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fixture_path, dest)
    if include_graphs:
        for _graph_name, paths in iter_graph_fixture_sets().items():
            for p in paths:
                note = read_note_file(p, vault_root=FIXTURES_DIR)
                card_type = str(note.frontmatter.get("type", ""))
                dest_dir = _type_to_dir(card_type)
                dest = target / dest_dir / p.name
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest)
    return target
