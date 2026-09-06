"""Fail-closed MCP tool profiles. Isolated env only; no warehouse or seed vault."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.server import (
    _VALID_TOOL_PROFILES,
    _tool_profile_error,
    archive_read,
    archive_rebuild_indexes,
    archive_search,
)

PPA_ROOT = Path(__file__).resolve().parents[1]


def test_valid_profiles_are_closed_enum() -> None:
    assert _VALID_TOOL_PROFILES == frozenset({"full", "read-only", "remote-read", "admin-only"})


def test_unset_profile_defaults_to_explicit_full(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_MCP_TOOL_PROFILE", raising=False)
    assert _tool_profile_error("archive_search") is None
    assert _tool_profile_error("archive_read") is None
    assert _tool_profile_error("archive_rebuild_indexes") is None


def test_explicit_full_allows_all_tools(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_MCP_TOOL_PROFILE", "full")
    assert _tool_profile_error("archive_search") is None
    assert _tool_profile_error("archive_rebuild_indexes") is None


def test_read_only_and_remote_read_keep_allow_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_MCP_TOOL_PROFILE", "read-only")
    assert _tool_profile_error("archive_search") is None
    assert _tool_profile_error("archive_read") is None
    assert _tool_profile_error("archive_rebuild_indexes") == "Tool disabled by PPA_MCP_TOOL_PROFILE=read-only"

    monkeypatch.setenv("PPA_MCP_TOOL_PROFILE", "remote-read")
    assert _tool_profile_error("archive_search") is None
    assert _tool_profile_error("archive_read") == "Tool disabled by PPA_MCP_TOOL_PROFILE=remote-read"
    assert _tool_profile_error("archive_rebuild_indexes") == "Tool disabled by PPA_MCP_TOOL_PROFILE=remote-read"


@pytest.mark.parametrize("raw", ["", "   ", "typo", "FULLL", "unrestricted", "all"])
def test_empty_and_unknown_profiles_fail_closed(raw: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_MCP_TOOL_PROFILE", raw)
    err = _tool_profile_error("archive_search")
    assert err is not None
    assert err.startswith("Invalid PPA_MCP_TOOL_PROFILE=")
    assert "Valid profiles:" in err
    assert "full" in err
    assert "read-only" in err
    assert "remote-read" in err
    assert _tool_profile_error("archive_rebuild_indexes") == err
    assert _tool_profile_error("archive_read") == err


def test_unknown_profile_does_not_run_tools(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_MCP_TOOL_PROFILE", "not-a-profile")
    search = archive_search("anything")
    read = archive_read("People/jane-smith.md")
    rebuild = archive_rebuild_indexes()
    assert search.startswith("Invalid PPA_MCP_TOOL_PROFILE=")
    assert read.startswith("Invalid PPA_MCP_TOOL_PROFILE=")
    assert rebuild.startswith("Invalid PPA_MCP_TOOL_PROFILE=")


def test_subprocess_invalid_profile_fails_closed(tmp_path: Path) -> None:
    env = {
        **os.environ,
        "PPA_MCP_TOOL_PROFILE": "definitely-invalid",
        "PPA_EMBEDDING_PROVIDER": "hash",
        "PPA_ENGINE": "rust",
        "PPA_PATH": str(tmp_path / "unused-vault"),
        "PPA_INDEX_DSN": "postgresql://unused:unused@127.0.0.1:1/unused",
        "PYTHONPATH": str(PPA_ROOT),
    }
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PPA_CONFIG_PATH", None)
    script = (
        "from archive_cli.server import archive_search, archive_read, _tool_profile_error\n"
        "print(_tool_profile_error('archive_search'))\n"
        "print(archive_search('probe'))\n"
        "print(archive_read('People/x.md'))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "Invalid PPA_MCP_TOOL_PROFILE=" in proc.stdout
    assert "definitely-invalid" in proc.stdout
    assert "Tool disabled" not in proc.stdout
    # Must not look like an unrestricted miss/hit.
    assert "No matches" not in proc.stdout
    assert "Not found" not in proc.stdout
