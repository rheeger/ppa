#!/usr/bin/env python3
"""Nightly local-seed maintain — wrap ``ppa maintain``, do not invent a second pipeline.

Nightly IS ``maintain`` with live updater + dirty-processor flags:

    python -m archive_cli --log-file logs/ppa-maintain-nightly-YYYYMMDD.log maintain \\
        --run-source-updaters --apply-source-updaters \\
        --run-processors --apply-processors \\
        --source-updater <live keys>

That sequence already: pulls every executable (non-parked) source → applies new
data → rematerializes dirty UIDs → dirty-embeds via the embedding processor
(``store.embed_pending``) → incremental index. ``ppa maintain`` itself also
skips junk email-attachment cards on Gmail apply, incrementally purges any
that slipped through, and hash-links document/attachment duplicates
(``file_identity``). No separate ``embed-pending`` or ``link-file-duplicates``
step. No ``--catch-up``. No Photos / Apple Health. No ``--allow-full-embedding``
/ IVFFlat / force-full rebuild.

Source keys match ``default_maintain_source_keys`` plus GOOGLE_ACCOUNT expansion
(calendar, contacts, otter, file-libraries, beeper, imessage, gmail-messages,
gmail-correspondents, github). Beeper already default-excludes iMessage /
BlueBubbles. GitHub stage dir is env, not a second command.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

LOG = logging.getLogger("ppa.maintain_nightly")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VAULT = Path.home() / "Archive" / "seed" / "hf-archives-seed-20260307-235127"
DEFAULT_FALLBACK_PYTHON = Path.home() / "Code" / "rheeger" / "ppa-wt-track-a" / ".venv" / "bin" / "python"
DEFAULT_PORT = "50731"
DEFAULT_PORT_FILE = Path.home() / ".ppa" / "local-postgres-port"
DEFAULT_GOOGLE_ACCOUNT = "rheeger@gmail.com"
DEFAULT_GITHUB_STAGE = Path.home() / "Archive" / "raw-data" / "github-history"
DEFAULT_IMESSAGE_SNAPSHOT = Path.home() / "Archive" / "raw-data" / "imessage-snapshots" / "latest"
# Embedding + linker processors resolve via archive_cli.providers.resolve_provider(),
# which reads PPA_ENRICHMENT_MODEL (not PPA_EMBEDDING_*). Unset → llm_provider_unavailable.
DEFAULT_ENRICHMENT_MODEL = "openai:gpt-4o-mini"
LAUNCHD_LABEL = "com.rheeger.ppa.maintain-nightly"
PLIST_NAME = f"{LAUNCHD_LABEL}.plist"
FAILED_UPDATER_STATUSES = frozenset({"failed", "blocked"})

# Live keys maintain already enumerates once GOOGLE_ACCOUNT is set, minus parked.
# Explicit --source-updater keeps nightly deterministic if env is incomplete.
_SCOPED_LIVE = (
    "gmail-messages",
    "calendar-events",
    "otter-transcripts",
    "gmail-correspondents",
)
_FIXED_LIVE = (
    "contacts:google",
    "file-libraries:documents",
    "beeper:local",
    "imessage:local",
    "github-history:local",
)


def configure_wrapper_logging(*, verbose: bool = False, log_file: Path | None = None) -> None:
    """Stderr + optional file, same line format as ``archive_cli.log``."""

    root = logging.getLogger("ppa")
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    if root.handlers:
        return
    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s")
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(fmt)
    root.addHandler(stderr)
    root.propagate = False
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)


def nightly_source_keys(google_account: str) -> list[str]:
    account = google_account.strip()
    if not account or "@" not in account:
        raise ValueError(f"GOOGLE_ACCOUNT must be an email, got {google_account!r}")
    keys = [f"{prefix}:{account}" for prefix in _SCOPED_LIVE]
    keys.extend(_FIXED_LIVE)
    return keys


def default_log_path(repo_root: Path, when: datetime | None = None) -> Path:
    stamp = (when or datetime.now()).strftime("%Y%m%d")
    return repo_root / "logs" / f"ppa-maintain-nightly-{stamp}.log"


def _read_port_file(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if text.isdigit():
        return text
    return None


def resolve_dsn(*, env: dict[str, str] | None = None, port_file: Path = DEFAULT_PORT_FILE) -> str:
    """Same resolution as ``run-local-seed-mcp.sh``: env → port file → 50731."""

    environ = env if env is not None else os.environ
    existing = (environ.get("PPA_INDEX_DSN") or "").strip()
    if existing:
        return existing
    port = (environ.get("PPA_INDEX_PORT") or "").strip() or _read_port_file(port_file) or DEFAULT_PORT
    return f"postgresql://archive:archive@127.0.0.1:{port}/archive"


def _python_can_import(candidate: Path) -> bool:
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        return False
    try:
        proc = subprocess.run(
            [str(candidate), "-c", "import archive_cli"],
            check=False,
            capture_output=True,
            timeout=8,
            cwd=str(REPO_ROOT),
            env={**os.environ, "PYTHONPATH": _pythonpath()},
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _pythonpath() -> str:
    extra = os.environ.get("PYTHONPATH", "")
    return f"{REPO_ROOT}{os.pathsep}{extra}" if extra else str(REPO_ROOT)


def resolve_python(*, env: dict[str, str] | None = None) -> Path:
    environ = env if env is not None else os.environ
    explicit = (environ.get("PPA_PYTHON") or "").strip()
    fallback = Path((environ.get("PPA_PYTHON_FALLBACK") or str(DEFAULT_FALLBACK_PYTHON)).strip())
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.append(REPO_ROOT / ".venv" / "bin" / "python")
    candidates.append(fallback)
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.expanduser()
        if resolved in seen:
            continue
        seen.add(resolved)
        if _python_can_import(resolved):
            return resolved
    if explicit:
        raise FileNotFoundError(f"PPA_PYTHON cannot import archive_cli: {explicit}")
    raise FileNotFoundError(
        f"No working Python for nightly maintain. Set PPA_PYTHON or restore {REPO_ROOT / '.venv' / 'bin' / 'python'}."
    )


def load_openai_key() -> str:
    existing = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if existing:
        return existing
    key_file = Path.home() / ".ppa" / "openai_key.txt"
    try:
        return key_file.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def build_maintain_argv(
    *,
    python: Path,
    log_file: Path,
    source_keys: list[str],
) -> list[str]:
    argv = [
        str(python),
        "-m",
        "archive_cli",
        "--log-file",
        str(log_file),
        "maintain",
        "--run-source-updaters",
        "--apply-source-updaters",
        "--run-processors",
        "--apply-processors",
    ]
    for key in source_keys:
        argv.extend(["--source-updater", key])
    return argv


def maintain_failed(report: dict[str, object]) -> str | None:
    errors = report.get("errors") or []
    if isinstance(errors, list) and errors:
        return f"maintain report errors={len(errors)}"
    updater_reports = report.get("source_updater_reports") or []
    if isinstance(updater_reports, list):
        bad = [
            str(item.get("source_key") or "?")
            for item in updater_reports
            if isinstance(item, dict) and str(item.get("status") or "") in FAILED_UPDATER_STATUSES
        ]
        if bad:
            return f"source updater failed/blocked: {', '.join(bad)}"
    return None


def plist_template_path() -> Path:
    return Path(__file__).resolve().parent / PLIST_NAME


def render_plist(*, repo_root: Path, python: Path, template: str | None = None) -> str:
    text = template if template is not None else plist_template_path().read_text(encoding="utf-8")
    return text.replace("__PPA_REPO__", str(repo_root)).replace("__PPA_PYTHON__", str(python))


def launch_agents_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def install_launchd(*, repo_root: Path, python: Path, load: bool = True) -> Path:
    dest = launch_agents_dir() / PLIST_NAME
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(render_plist(repo_root=repo_root, python=python), encoding="utf-8")
    if load:
        uid = os.getuid()
        domain = f"gui/{uid}"
        subprocess.run(["launchctl", "bootout", f"{domain}/{LAUNCHD_LABEL}"], check=False, capture_output=True)
        subprocess.run(["launchctl", "bootstrap", domain, str(dest)], check=True)
    return dest


def uninstall_launchd() -> None:
    dest = launch_agents_dir() / PLIST_NAME
    uid = os.getuid()
    subprocess.run(
        ["launchctl", "bootout", f"gui/{uid}/{LAUNCHD_LABEL}"],
        check=False,
        capture_output=True,
    )
    if dest.exists():
        dest.unlink()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run nightly local-seed maintain (live source pull + dirty processors). "
            "This is ppa maintain — not a second pipeline."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve env and print the maintain argv; do not run it",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="Write ~/Library/LaunchAgents plist and launchctl bootstrap (2am local)",
    )
    parser.add_argument(
        "--install-plist-only",
        action="store_true",
        help="Write the LaunchAgent plist without launchctl bootstrap",
    )
    parser.add_argument(
        "--uninstall",
        action="store_true",
        help="Remove the LaunchAgent and boot it out",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def apply_runtime_env() -> dict[str, str]:
    """Set local-seed env. Does not mutate Arnold."""

    os.environ.setdefault("PPA_PATH", str(DEFAULT_VAULT))
    os.environ.setdefault("PPA_INDEX_SCHEMA", "ppa")
    os.environ.setdefault("PPA_ARCHIVE_INSTANCE_ROLE", "local-seed")
    os.environ.setdefault("PPA_ENGINE", "rust")
    os.environ.setdefault("PPA_EMBEDDING_PROVIDER", "openai")
    os.environ.setdefault("PPA_EMBEDDING_MODEL", "text-embedding-3-small")
    os.environ.setdefault("PPA_EMBEDDING_VERSION", "1")
    os.environ.setdefault("PPA_ENRICHMENT_MODEL", DEFAULT_ENRICHMENT_MODEL)
    os.environ.setdefault("GOOGLE_ACCOUNT", DEFAULT_GOOGLE_ACCOUNT)
    os.environ.setdefault("OTTER_FETCH_MODE", "mcp")
    os.environ.setdefault("PPA_NONINTERACTIVE", "1")
    os.environ.setdefault("PPA_GITHUB_STAGE_DIR", str(DEFAULT_GITHUB_STAGE))
    os.environ.setdefault("IMESSAGE_SNAPSHOT_DIR", str(DEFAULT_IMESSAGE_SNAPSHOT))
    os.environ["PPA_INDEX_DSN"] = resolve_dsn()
    key = load_openai_key()
    if key:
        os.environ["OPENAI_API_KEY"] = key
    os.environ["PYTHONPATH"] = _pythonpath()
    return os.environ


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    os.chdir(REPO_ROOT)

    if args.uninstall:
        configure_wrapper_logging(verbose=args.verbose)
        uninstall_launchd()
        LOG.info("uninstalled %s", launch_agents_dir() / PLIST_NAME)
        return 0

    if args.install or args.install_plist_only:
        configure_wrapper_logging(verbose=args.verbose)
        apply_runtime_env()
        python = resolve_python()
        dest = install_launchd(repo_root=REPO_ROOT, python=python, load=args.install)
        LOG.info("wrote LaunchAgent dest=%s load=%s", dest, args.install)
        if args.install:
            LOG.info("nightly maintain scheduled for 02:00 local; tail %s", default_log_path(REPO_ROOT))
        else:
            LOG.info(
                "plist written; load with: launchctl bootstrap gui/%s %s",
                os.getuid(),
                dest,
            )
        return 0

    apply_runtime_env()
    log_file = default_log_path(REPO_ROOT)
    configure_wrapper_logging(verbose=args.verbose, log_file=log_file)
    LOG.info("nightly maintain start log_file=%s", log_file)

    google_account = (os.environ.get("GOOGLE_ACCOUNT") or DEFAULT_GOOGLE_ACCOUNT).strip()
    source_keys = nightly_source_keys(google_account)
    python = Path(sys.executable)
    try:
        python = resolve_python()
    except FileNotFoundError:
        if not args.dry_run:
            raise
        LOG.warning("python resolve failed in dry-run; using sys.executable=%s", sys.executable)

    maintain_argv = build_maintain_argv(python=python, log_file=log_file, source_keys=source_keys)
    LOG.info(
        "nightly maintain argv vault=%s schema=%s dsn_port_file=%s sources=%s",
        os.environ.get("PPA_PATH"),
        os.environ.get("PPA_INDEX_SCHEMA"),
        DEFAULT_PORT_FILE,
        ",".join(source_keys),
    )
    LOG.info("nightly maintain command %s", " ".join(maintain_argv))

    if args.dry_run:
        LOG.info("dry-run; not invoking maintain. log_file=%s", log_file)
        return 0

    if not load_openai_key():
        LOG.error("OPENAI_API_KEY missing (set env or ~/.ppa/openai_key.txt); dirty embed will skip")

    json_path = log_file.with_suffix(".json")
    LOG.info("invoking maintain json_report=%s", json_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as jf:
        proc = subprocess.run(
            maintain_argv,
            cwd=str(REPO_ROOT),
            check=False,
            stdout=jf,
            stderr=None,
            env=os.environ.copy(),
        )

    if proc.returncode != 0:
        LOG.error("maintain exited rc=%s log_file=%s", proc.returncode, log_file)
        return proc.returncode

    report: dict[str, object] = {}
    raw = json_path.read_text(encoding="utf-8") if json_path.exists() else ""
    if raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                report = parsed
        except json.JSONDecodeError:
            LOG.error("maintain stdout was not JSON log_file=%s", log_file)
            return 1
    reason = maintain_failed(report)
    if reason:
        LOG.error("maintain failed: %s log_file=%s", reason, log_file)
        return 1
    LOG.info(
        "nightly maintain done nothing_to_do=%s cards_rebuilt=%s updater_runs=%s "
        "junk_purged=%s file_dups_linked=%s log_file=%s",
        report.get("nothing_to_do"),
        report.get("cards_rebuilt"),
        report.get("source_updater_runs"),
        report.get("junk_attachments_purged"),
        report.get("file_duplicates_linked"),
        log_file,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        logging.getLogger("ppa.maintain_nightly").exception("nightly maintain failed")
        sys.stderr.write(f"nightly maintain failed; see {default_log_path(REPO_ROOT)}\n")
        raise SystemExit(1)
