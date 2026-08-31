"""Tests for authenticated HTTP MCP serve."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.http_serve import (
    DEFAULT_TOKEN_FILE,
    bearer_authorized,
    resolve_http_auth_token,
    write_http_auth_token,
)

PPA_ROOT = Path(__file__).resolve().parents[1]


def test_bearer_authorized_accepts_exact_token() -> None:
    token = "a" * 32
    assert bearer_authorized(f"Bearer {token}", token) is True


def test_bearer_authorized_rejects_wrong_or_missing() -> None:
    token = "a" * 32
    assert bearer_authorized("Bearer bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", token) is False
    assert bearer_authorized("Bearer ", token) is False
    assert bearer_authorized(token, token) is False
    assert bearer_authorized("Bearer " + token, "") is False


def test_resolve_http_auth_token_prefers_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PPA_MCP_AUTH_TOKEN", "from-env")
    monkeypatch.setenv("PPA_MCP_AUTH_TOKEN_FILE", str(tmp_path / "missing"))
    assert resolve_http_auth_token() == "from-env"


def test_resolve_http_auth_token_reads_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    token_path = tmp_path / "token"
    token_path.write_text("file-token\n", encoding="utf-8")
    monkeypatch.delenv("PPA_MCP_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("PPA_MCP_AUTH_TOKEN_FILE", str(token_path))
    assert resolve_http_auth_token() == "file-token"


def test_write_http_auth_token_sets_mode(tmp_path: Path) -> None:
    dest = tmp_path / "mcp-http-token"
    wrote = write_http_auth_token("secret-token", dest)
    assert wrote.read_text(encoding="utf-8").strip() == "secret-token"
    assert (wrote.stat().st_mode & 0o777) == 0o600


def test_http_serve_exits_without_token() -> None:
    env = {**os.environ, "PPA_MCP_HTTP": "1"}
    env.pop("PPA_MCP_AUTH_TOKEN", None)
    env["PPA_MCP_AUTH_TOKEN_FILE"] = str(Path("/tmp/ppa-missing-http-token"))
    proc = subprocess.run(
        [sys.executable, "-m", "archive_cli", "serve", "--http"],
        cwd=str(PPA_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2
    assert "PPA_MCP_AUTH_TOKEN" in proc.stderr


def test_mcp_config_http_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_MCP_HTTP_URL", "https://ginger-m4-max.tail0c38c5.ts.net/mcp")
    out = subprocess.check_output(
        [sys.executable, "-m", "archive_cli", "mcp-config"],
        cwd=str(PPA_ROOT),
        env={**os.environ, "PPA_MCP_HTTP_URL": "https://ginger-m4-max.tail0c38c5.ts.net/mcp"},
        text=True,
    )
    data = json.loads(out)
    inner = next(iter(data["mcpServers"].values()))
    assert inner["url"].endswith("/mcp")
    assert "Authorization" in inner["headers"]
    assert DEFAULT_TOKEN_FILE.name == "mcp-http-token"
