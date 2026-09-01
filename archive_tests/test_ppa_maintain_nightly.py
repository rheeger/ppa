"""Tests for the nightly local-seed maintain wrapper."""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "archive_scripts" / "ppa-maintain-nightly.py"
PLIST = REPO_ROOT / "archive_scripts" / "com.rheeger.ppa.maintain-nightly.plist"


def _load_mod():
    spec = importlib.util.spec_from_file_location("ppa_maintain_nightly", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_nightly_wrapper_script_exists() -> None:
    assert SCRIPT.is_file()
    assert PLIST.is_file()


def test_nightly_source_keys_are_live_only() -> None:
    mod = _load_mod()
    keys = mod.nightly_source_keys("rheeger@gmail.com")
    assert keys == [
        "gmail-messages:rheeger@gmail.com",
        "calendar-events:rheeger@gmail.com",
        "otter-transcripts:rheeger@gmail.com",
        "gmail-correspondents:rheeger@gmail.com",
        "contacts:google",
        "file-libraries:documents",
        "beeper:local",
        "imessage:local",
        "github-history:local",
    ]
    joined = " ".join(keys)
    assert "photos" not in joined
    assert "health" not in joined


def test_build_maintain_argv_flags() -> None:
    mod = _load_mod()
    argv = mod.build_maintain_argv(
        python=Path("/tmp/python"),
        log_file=Path("/tmp/logs/ppa-maintain-nightly-20260830.log"),
        source_keys=mod.nightly_source_keys("rheeger@gmail.com"),
    )
    assert argv[:6] == [
        "/tmp/python",
        "-m",
        "archive_cli",
        "--log-file",
        "/tmp/logs/ppa-maintain-nightly-20260830.log",
        "maintain",
    ]
    assert "--run-source-updaters" in argv
    assert "--apply-source-updaters" in argv
    assert "--run-processors" in argv
    assert "--apply-processors" in argv
    assert "--catch-up" not in argv
    assert "--allow-full-embedding" not in argv
    assert "--allow-all-linkers" not in argv
    assert "--allow-broad-llm" not in argv
    assert argv.count("--source-updater") == 9


def test_resolve_dsn_prefers_env_then_port_file(tmp_path: Path) -> None:
    mod = _load_mod()
    port_file = tmp_path / "local-postgres-port"
    port_file.write_text("51234\n", encoding="utf-8")
    assert (
        mod.resolve_dsn(env={"PPA_INDEX_DSN": "postgresql://x@127.0.0.1:9/archive"}, port_file=port_file)
        == "postgresql://x@127.0.0.1:9/archive"
    )
    assert mod.resolve_dsn(env={}, port_file=port_file) == "postgresql://archive:archive@127.0.0.1:51234/archive"
    assert (
        mod.resolve_dsn(env={}, port_file=tmp_path / "missing")
        == "postgresql://archive:archive@127.0.0.1:50731/archive"
    )


def test_maintain_failed_detects_updater_errors() -> None:
    mod = _load_mod()
    assert mod.maintain_failed({}) is None
    assert mod.maintain_failed({"errors": [{"step": "x", "error": "boom"}]})
    assert mod.maintain_failed({"source_updater_reports": [{"source_key": "gmail-messages:x", "status": "blocked"}]})
    assert (
        mod.maintain_failed({"source_updater_reports": [{"source_key": "imessage:local", "status": "success"}]}) is None
    )


def test_render_plist_substitutes_paths() -> None:
    mod = _load_mod()
    rendered = mod.render_plist(repo_root=Path("/repo"), python=Path("/repo/.venv/bin/python"))
    assert "/repo/.venv/bin/python" in rendered
    assert "/repo/archive_scripts/ppa-maintain-nightly.py" in rendered
    assert "<integer>2</integer>" in rendered
    assert "__PPA_REPO__" not in rendered
    assert "RunAtLoad" in rendered


def test_dry_run_exits_zero(tmp_path: Path, monkeypatch, capsys) -> None:
    mod = _load_mod()
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://archive:archive@127.0.0.1:50731/archive")
    monkeypatch.setenv("GOOGLE_ACCOUNT", "rheeger@gmail.com")
    rc = mod.main(["--dry-run"])
    assert rc == 0
    # dry-run logs to stderr, not stdout
    err = capsys.readouterr().err
    assert "--run-source-updaters" in err
    assert "--catch-up" not in err
    assert "photos" not in err.lower() or "parked" in err.lower()


def test_default_log_path_uses_local_date() -> None:
    mod = _load_mod()
    when = datetime(2026, 8, 30, 2, 0, 0)
    path = mod.default_log_path(REPO_ROOT, when)
    assert path == REPO_ROOT / "logs" / "ppa-maintain-nightly-20260830.log"


def test_apply_runtime_env_sets_noninteractive(monkeypatch) -> None:
    mod = _load_mod()
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://archive:archive@127.0.0.1:50731/archive")
    monkeypatch.delenv("PPA_NONINTERACTIVE", raising=False)
    env = mod.apply_runtime_env()
    assert env.get("PPA_NONINTERACTIVE") == "1"
    assert env.get("OTTER_FETCH_MODE") == "mcp"


def test_default_maintain_source_keys_excludes_parked() -> None:
    from archive_sync.source_updaters.runner import default_maintain_source_keys

    keys = default_maintain_source_keys(
        gmail_accounts=("rheeger@gmail.com",),
        calendar_accounts=("rheeger@gmail.com",),
        otter_accounts=("rheeger@gmail.com",),
    )
    assert "photos:local" not in keys
    assert "health:apple-health" not in keys
    assert "gmail-messages:rheeger@gmail.com" in keys
    assert "otter-transcripts:rheeger@gmail.com" in keys
    assert "github-history:local" in keys
