from pathlib import Path

from archive_cli.mcp_instructions import TOOL_DESCRIPTIONS, build_server_instructions

JOB_ROOT = Path(__file__).resolve().parents[1] / ".cursor" / "skills" / "archive-query"
JOB_FILES = (
    "SKILL.md",
    "identify-person.md",
    "census-channels.md",
    "read-a-stack.md",
    "answer-a-fact.md",
    "reconstruct-a-story.md",
)


def test_archive_query_job_files_exist() -> None:
    for name in JOB_FILES:
        path = JOB_ROOT / name
        assert path.is_file(), path
        text = path.read_text(encoding="utf-8")
        assert len(text) > 80


def test_mcp_router_names_every_job_file() -> None:
    text = build_server_instructions("Test Archives")
    for name in JOB_FILES:
        if name == "SKILL.md":
            assert "archive-query" in text
            continue
        assert name in text
    assert "candidate" in TOOL_DESCRIPTIONS["archive_person"].lower()
