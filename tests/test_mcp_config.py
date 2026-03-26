"""Tests for ppa mcp-config emission."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

PPA_ROOT = Path(__file__).resolve().parents[1]


def test_mcp_config_emits_json_without_secret_named_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PPA_INDEX_SCHEMA", "archive_seed")
    monkeypatch.setenv("PPA_PATH", str(tmp_path))
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("PPA_OPENAI_API_KEY", raising=False)

    out = subprocess.check_output(
        [sys.executable, "-m", "archive_mcp", "mcp-config"],
        cwd=str(PPA_ROOT),
        env={**os.environ, "PPA_INDEX_SCHEMA": "archive_seed", "PPA_PATH": str(tmp_path), "PPA_EMBEDDING_PROVIDER": "hash"},
        text=True,
    )
    data = json.loads(out)
    assert "mcpServers" in data
    inner = next(iter(data["mcpServers"].values()))
    assert inner["command"] == "ppa"
    assert inner["args"] == ["serve"]
    env_block = inner["env"]
    assert env_block.get("PPA_PATH") == str(tmp_path)
    assert "OPENAI_API_KEY" not in out


def test_mcp_config_strips_ppa_key_like_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_INDEX_SCHEMA", "x")
    monkeypatch.setenv("PPA_FAKE_API_KEY", "should-not-appear")
    out = subprocess.check_output(
        [sys.executable, "-m", "archive_mcp", "mcp-config"],
        cwd=str(PPA_ROOT),
        env={**os.environ, "PPA_INDEX_SCHEMA": "x", "PPA_FAKE_API_KEY": "should-not-appear"},
        text=True,
    )
    data = json.loads(out)
    inner = next(iter(data["mcpServers"].values()))
    assert "PPA_FAKE_API_KEY" not in inner["env"]


def test_mcp_config_tunnel_args_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_MCP_TUNNEL_HOST", "arnold@192.168.50.27")
    out = subprocess.check_output(
        [sys.executable, "-m", "archive_mcp", "mcp-config"],
        cwd=str(PPA_ROOT),
        env={**os.environ, "PPA_MCP_TUNNEL_HOST": "arnold@192.168.50.27"},
        text=True,
    )
    data = json.loads(out)
    inner = next(iter(data["mcpServers"].values()))
    assert inner["args"] == ["serve", "--tunnel", "arnold@192.168.50.27"]
