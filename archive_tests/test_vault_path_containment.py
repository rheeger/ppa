"""Adversarial vault containment: isolated fixtures only, never real private files."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_cli.commands import attachments as attachments_cmd
from archive_cli.store import DefaultArchiveStore
from archive_vault.paths import PathEscapeError, atomic_write_contained, resolve_contained_path
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import update_frontmatter_fields, write_card

SENTINEL = "P05A_SENTINEL_HARMLESS_OUTSIDE_VAULT"
PPA_ROOT = Path(__file__).resolve().parents[1]


def _person() -> tuple[PersonCard, dict[str, ProvenanceEntry]]:
    card = PersonCard(
        uid="hfa-person-containment01",
        type="person",
        source=["test"],
        source_id="containment@example.com",
        created="2026-09-06",
        updated="2026-09-06",
        summary="Containment Fixture",
    )
    prov = {"summary": ProvenanceEntry("test", "2026-09-06", "deterministic")}
    return card, prov


@pytest.fixture
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    vault = tmp_path / "disposable-vault"
    (vault / "People").mkdir(parents=True)
    (vault / "EmailAttachments" / "2026-09").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.md"
    sentinel.write_text(SENTINEL + "\n", encoding="utf-8")
    card, prov = _person()
    write_card(vault, "People/containment-fixture.md", card, body="inside-ok", provenance=prov)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.delenv("PPA_CONFIG_PATH", raising=False)
    return {"vault": vault, "outside": outside, "sentinel": sentinel, "root": tmp_path}


class _UidIndex:
    def __init__(self, rel_path: str | None):
        self._rel_path = rel_path

    def read_path_for_uid(self, uid: str) -> str | None:
        return self._rel_path


def _assert_no_sentinel(*parts: str) -> None:
    blob = "\n".join(parts)
    assert SENTINEL not in blob


def test_resolver_rejects_absolute_traversal_and_nul(isolated: dict[str, Path]) -> None:
    vault = isolated["vault"]
    sentinel = isolated["sentinel"]
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, str(sentinel), purpose="read")
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "../outside/sentinel.md", purpose="read")
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "People/../../outside/sentinel.md", purpose="read")
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "People/foo\x00.md", purpose="read")
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "", purpose="read")


def test_resolver_allows_contained_read_and_internal_symlink(isolated: dict[str, Path]) -> None:
    vault = isolated["vault"]
    alias_dir = vault / "Alias"
    alias_dir.mkdir()
    (alias_dir / "fixture.md").symlink_to(Path("..") / "People" / "containment-fixture.md")
    path = resolve_contained_path(vault, "Alias/fixture.md", purpose="read")
    assert path.is_file()
    assert SENTINEL not in path.read_text(encoding="utf-8")
    assert "inside-ok" in path.read_text(encoding="utf-8")


def test_resolver_rejects_symlink_escape_and_symlink_parent(isolated: dict[str, Path]) -> None:
    vault = isolated["vault"]
    leak = vault / "People" / "leak.md"
    leak.symlink_to(isolated["sentinel"])
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "People/leak.md", purpose="read")
    escaped = vault / "EscapedPeople"
    escaped.symlink_to(isolated["outside"])
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "EscapedPeople/sentinel.md", purpose="read")


def test_resolver_write_denies_symlink_and_escape(isolated: dict[str, Path]) -> None:
    vault = isolated["vault"]
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "../outside/pwned.md", purpose="write", create_parents=True)
    with pytest.raises(PathEscapeError):
        atomic_write_contained(vault, "../outside/pwned.md", b"nope")
    assert not (isolated["outside"] / "pwned.md").exists()
    escaped = vault / "OutDir"
    escaped.symlink_to(isolated["outside"])
    with pytest.raises(PathEscapeError):
        resolve_contained_path(vault, "OutDir/pwned.md", purpose="write", create_parents=True)
    assert SENTINEL == isolated["sentinel"].read_text(encoding="utf-8").strip()


def test_store_md_path_cannot_read_outside_sentinel(
    isolated: dict[str, Path], caplog: pytest.LogCaptureFixture
) -> None:
    store = DefaultArchiveStore(vault=isolated["vault"], index=_UidIndex(None))
    caplog.set_level(logging.DEBUG)
    legit = store.read("People/containment-fixture.md")
    assert legit["found"] is True
    assert "inside-ok" in legit["content"]
    _assert_no_sentinel(legit["content"])

    escaped = store.read(str(isolated["sentinel"]))
    assert escaped["found"] is False
    assert escaped["content"] == ""
    traversal = store.read("../outside/sentinel.md")
    assert traversal["found"] is False
    assert traversal["content"] == ""
    _assert_no_sentinel(escaped["content"], traversal["content"], caplog.text)


def test_store_uid_path_cannot_escape(isolated: dict[str, Path]) -> None:
    store = DefaultArchiveStore(vault=isolated["vault"], index=_UidIndex("../outside/sentinel.md"))
    result = store.read("hfa-person-containment01")
    assert result["found"] is False
    assert result["content"] == ""
    _assert_no_sentinel(result["content"])

    good = DefaultArchiveStore(vault=isolated["vault"], index=_UidIndex("People/containment-fixture.md"))
    ok = good.read("hfa-person-containment01")
    assert ok["found"] is True
    assert "inside-ok" in ok["content"]


def test_write_card_and_frontmatter_cannot_escape(isolated: dict[str, Path]) -> None:
    card, prov = _person()
    card.uid = "hfa-person-containment02"
    with pytest.raises(PathEscapeError):
        write_card(isolated["vault"], "../outside/pwned.md", card, provenance=prov)
    assert not (isolated["outside"] / "pwned.md").exists()
    with pytest.raises(FileNotFoundError):
        update_frontmatter_fields(isolated["vault"], "../outside/sentinel.md", {"summary": "nope"})
    assert SENTINEL in isolated["sentinel"].read_text(encoding="utf-8")


def test_fetch_attachment_uses_contained_card_path(isolated: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    rel = "EmailAttachments/2026-09/hfa-email-attachment-contain.md"
    (isolated["vault"] / rel).write_text(
        """---
uid: hfa-email-attachment-contain
type: email_attachment
source: [gmail.attachment]
source_id: test:msg:att
created: '2026-09-06'
summary: fixture.pdf
gmail_message_id: msg-1
gmail_thread_id: thread-1
attachment_id: att-1
account_email: test@example.com
filename: fixture.pdf
mime_type: application/pdf
size_bytes: 4
---
""",
        encoding="utf-8",
    )
    store = DefaultArchiveStore(vault=isolated["vault"], index=_UidIndex(rel))
    log = logging.getLogger("ppa.test.containment")
    ok = attachments_cmd.fetch_attachment(rel, store=store, logger=log)
    assert ok["found"] is True
    denied = attachments_cmd.fetch_attachment(str(isolated["sentinel"]), store=store, logger=log)
    assert denied["found"] is False
    _assert_no_sentinel(str(denied), str(ok.get("attachment")))


def test_cli_subprocess_read_omits_outside_sentinel(isolated: dict[str, Path]) -> None:
    env = {
        **os.environ,
        "PPA_PATH": str(isolated["vault"]),
        "PPA_INDEX_DSN": "postgresql://unused:unused@127.0.0.1:1/unused",
        "PPA_EMBEDDING_PROVIDER": "hash",
        "PPA_ENGINE": "rust",
        "PYTHONPATH": str(PPA_ROOT),
    }
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PPA_CONFIG_PATH", None)
    cases = (
        str(isolated["sentinel"]),
        "../outside/sentinel.md",
        "People/../../outside/sentinel.md",
    )
    for target in cases:
        proc = subprocess.run(
            [sys.executable, "-m", "archive_cli", "read", target],
            cwd=str(isolated["root"]),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        _assert_no_sentinel(proc.stdout, proc.stderr)
        assert '"found": true' not in proc.stdout.lower()
        assert SENTINEL not in proc.stdout

    legit = subprocess.run(
        [sys.executable, "-m", "archive_cli", "read", "People/containment-fixture.md"],
        cwd=str(isolated["root"]),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    _assert_no_sentinel(legit.stdout, legit.stderr)
    assert "inside-ok" in legit.stdout
