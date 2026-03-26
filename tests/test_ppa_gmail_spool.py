from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_gmail_spool_import_rejects_manifest_account_mismatch(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    vault = tmp_path / "hf-archives"
    vault.mkdir()

    spool = tmp_path / "gmail-spool"
    (spool / "threads").mkdir(parents=True)
    (spool / "_meta").mkdir()
    (spool / "_meta" / "manifest.json").write_text(
        json.dumps({"account_email": "wrong@example.com"}),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "ppa-gmail-import-spool.py"),
            "--vault",
            str(vault),
            "--spool-dir",
            str(spool),
            "--account-email",
            "me@example.com",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Spool manifest account mismatch" in (result.stderr + result.stdout)
