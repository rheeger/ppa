"""PR31 release-contract tests. Integration-owned; never the living archive."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_release_manifest_lists_hashed_wheels() -> None:
    repo = Path(__file__).resolve().parents[1]
    lock = repo / "requirements" / "runtime-py312.lock"
    assert lock.is_file()
    text = lock.read_text(encoding="utf-8")
    assert "--hash=" in text
    builder = repo / "archive_scripts" / "build_release.py"
    assert builder.is_file()


def test_mcp_client_uses_installed_sdk() -> None:
    from archive_tests.acceptance.p31_mcp import _CLIENT

    assert "ClientSession" in _CLIENT
    assert "stdio_client" in _CLIENT
    assert "call_tool" in _CLIENT


@pytest.mark.integration
def test_installed_product_path_requires_owned_runtime() -> None:
    pytest.skip("run via archive_tests.acceptance --suite p31; IsolatedRuntime owns PG")
