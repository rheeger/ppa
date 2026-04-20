"""Tests for ``ppa linker scaffold`` (no Postgres)."""

from __future__ import annotations

import compileall
from pathlib import Path
from types import SimpleNamespace

from archive_cli.linker_cli import cmd_scaffold


def test_linker_scaffold_writes_syntax_ok(tmp_path: Path) -> None:
    out = tmp_path / "zz_scaffold_foo.py"
    ns = SimpleNamespace(
        module="zzScaffoldFooLinker",
        source_types="email_message",
        emits="related_to",
        out=str(out),
        force=False,
    )
    assert cmd_scaffold(ns) == 0
    assert out.is_file()
    body = out.read_text(encoding="utf-8")
    assert "zz_scaffold_foo_linker_candidates" in body
    assert "MODULE_ZZ_SCAFFOLD_FOO_LINKER" in body
    assert compileall.compile_file(str(out), quiet=1)


def test_linker_scaffold_refuses_overwrite_without_force(tmp_path: Path) -> None:
    out = tmp_path / "dup.py"
    out.write_text("# existing\n", encoding="utf-8")
    ns = SimpleNamespace(
        module="dupLinker",
        source_types="",
        emits="",
        out=str(out),
        force=False,
    )
    assert cmd_scaffold(ns) == 2
