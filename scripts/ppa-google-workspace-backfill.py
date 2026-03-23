#!/usr/bin/env python3
"""Backfill Gmail and Calendar into a multi-account-ready seed vault."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

from archive_sync.adapters.gmail_correspondents import GmailCorrespondentsAdapter
from arnoldlib.accounts import ACCOUNTS
from arnoldlib.google_cli_auth import account_name_from_email

DEFAULT_ACCOUNTS = [
    "rheeger@gmail.com",
    "robbie@endaoment.org",
    "robbie@givingtree.tech",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_accounts(raw_value: str | None) -> list[str]:
    if raw_value:
        requested = [item.strip().lower() for item in raw_value.split(",") if item.strip()]
    else:
        requested = [email for email in DEFAULT_ACCOUNTS if any(email == str(account.get("email", "")).strip().lower() for account in ACCOUNTS.values())]
    seen: set[str] = set()
    ordered: list[str] = []
    for email in requested:
        if email in seen:
            continue
        seen.add(email)
        ordered.append(email)
    return ordered


def _account_env(account_email: str, *, credentials_root: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    candidates = [credentials_root / f"{account_email}.json"]
    account_name = account_name_from_email(account_email)
    if account_name:
        candidates.append(credentials_root / f"{account_name}.json")
    for candidate in candidates:
        if candidate.exists():
            env["GOOGLE_WORKSPACE_CREDENTIALS_PATH"] = str(candidate)
            break
    return env


def _run(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    max_attempts: int = 3,
    retry_delay_seconds: int = 15,
) -> None:
    for attempt in range(1, max(1, max_attempts) + 1):
        print(json.dumps({"timestamp": _now(), "command": command, "attempt": attempt}, indent=2), flush=True)
        completed = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            env={**os.environ, **(env or {})},
            check=False,
        )
        if completed.returncode == 0:
            return
        if attempt >= max_attempts:
            raise RuntimeError(
                f"Command failed with exit code {completed.returncode} after {attempt} attempts: {' '.join(command)}"
            )
        print(
            json.dumps(
                {
                    "timestamp": _now(),
                    "status": "retrying-command",
                    "attempt": attempt,
                    "next_attempt_in_seconds": retry_delay_seconds,
                    "command": command,
                },
                indent=2,
            ),
            flush=True,
        )
        time.sleep(retry_delay_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill multi-account Google Workspace data into the seed vault")
    parser.add_argument("--vault", required=True)
    parser.add_argument("--raw-root", default=str(Path.home() / "Archive" / "raw-data" / "google-workspace"))
    parser.add_argument("--accounts", default="")
    parser.add_argument("--migrate-account", default="rheeger@gmail.com")
    parser.add_argument("--skip-migration", action="store_true")
    parser.add_argument("--calendar-id", default="primary")
    parser.add_argument("--start-year", type=int, default=2004)
    parser.add_argument("--end-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--years-per-window", type=int, default=1)
    parser.add_argument("--gmail-max-concurrent", type=int, default=6)
    parser.add_argument("--gmail-page-size", type=int, default=100)
    parser.add_argument("--gmail-thread-workers", type=int, default=4)
    parser.add_argument("--calendar-max-concurrent", type=int, default=4)
    parser.add_argument("--calendar-page-size", type=int, default=250)
    parser.add_argument("--python-bin", default=sys.executable)
    args = parser.parse_args()

    vault = Path(args.vault).expanduser().resolve()
    raw_root = Path(args.raw_root).expanduser().resolve()
    raw_root.mkdir(parents=True, exist_ok=True)
    credentials_root = raw_root / "credentials"
    credentials_root.mkdir(parents=True, exist_ok=True)

    accounts = _resolve_accounts(args.accounts)
    migrate_account = args.migrate_account.strip().lower()
    additional_accounts = [account for account in accounts if account != migrate_account]

    if not args.skip_migration:
        migration_backup_dir = raw_root / "migrations" / migrate_account / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        _run(
            [
                args.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-migrate-google-account-scope.py"),
                "--vault",
                str(vault),
                "--account-email",
                migrate_account,
                "--backup-dir",
                str(migration_backup_dir),
            ]
        )

    for account_email in additional_accounts:
        gmail_root = raw_root / "gmail" / account_email
        calendar_root = raw_root / "calendar" / account_email / args.calendar_id
        gmail_root.mkdir(parents=True, exist_ok=True)
        calendar_root.mkdir(parents=True, exist_ok=True)

        _run(
            [
                args.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-gmail-extract-parallel.py"),
                "--account-email",
                account_email,
                "--output-root",
                str(gmail_root),
                "--start-year",
                str(args.start_year),
                "--end-year",
                str(args.end_year),
                "--years-per-window",
                str(args.years_per_window),
                "--max-concurrent",
                str(args.gmail_max_concurrent),
                "--page-size",
                str(args.gmail_page_size),
                "--thread-workers",
                str(args.gmail_thread_workers),
                "--python-bin",
                args.python_bin,
            ],
            env=_account_env(account_email, credentials_root=credentials_root),
        )
        _run(
            [
                args.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-gmail-import-spool.py"),
                "--vault",
                str(vault),
                "--spool-dir",
                str(gmail_root),
                "--account-email",
                account_email,
            ],
            env=_account_env(account_email, credentials_root=credentials_root),
        )

        _run(
            [
                args.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-calendar-extract-parallel.py"),
                "--account-email",
                account_email,
                "--calendar-id",
                args.calendar_id,
                "--output-root",
                str(calendar_root),
                "--start-year",
                str(args.start_year),
                "--end-year",
                str(args.end_year),
                "--years-per-window",
                str(args.years_per_window),
                "--max-concurrent",
                str(args.calendar_max_concurrent),
                "--page-size",
                str(args.calendar_page_size),
                "--python-bin",
                args.python_bin,
            ],
            env=_account_env(account_email, credentials_root=credentials_root),
        )
        _run(
            [
                args.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-calendar-import-spool.py"),
                "--vault",
                str(vault),
                "--spool-dir",
                str(calendar_root),
                "--account-email",
                account_email,
                "--calendar-id",
                args.calendar_id,
            ],
            env=_account_env(account_email, credentials_root=credentials_root),
        )

    correspondents_adapter = GmailCorrespondentsAdapter()
    result = correspondents_adapter.ingest(
        str(vault),
        account_email="",
        account_emails=accounts,
    )
    print(
        json.dumps(
            {
                "timestamp": _now(),
                "gmail_correspondents": {
                    "created": result.created,
                    "merged": result.merged,
                    "conflicted": result.conflicted,
                    "skipped": result.skipped,
                    "errors": result.errors,
                },
            },
            indent=2,
        ),
        flush=True,
    )
    if result.errors:
        raise RuntimeError(f"gmail-correspondents aggregate failed with {len(result.errors)} errors")

    _run(
        ["bash", str(REPO_ROOT / "scripts" / "ppa-post-import.sh")],
        env={"PPA_PATH": str(vault), "PYTHON": args.python_bin},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
