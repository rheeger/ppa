"""Restore quarantined notes from a read-only source vault."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from archive_cli.corpus_hygiene.census import CensusContext, run_email_census_dry_run
from archive_cli.corpus_hygiene.classification_reuse import EmailThreadRecord
from archive_cli.corpus_hygiene.restore_quarantine import (
    drop_quarantine_from_ledger,
    rel_paths_from_scan_cache,
    restore_quarantine_notes,
)
from archive_cli.corpus_hygiene.state_store import CORPUS_STATE_QUARANTINE, quarantine_uids_for_records
from archive_sync.gmail_promotion.ledger import FilePromotionLedger, default_ledger_path


def _thread(**kwargs: object) -> EmailThreadRecord:
    defaults = {
        "thread_uid": "uid-default",
        "gmail_thread_id": "g-default",
        "account_email": "owner@example.com",
        "source_key": "gmail-messages:owner@example.com",
    }
    defaults.update(kwargs)
    return EmailThreadRecord(**defaults)  # type: ignore[arg-type]


def _write_cache(path: Path, rows: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE notes (uid TEXT PRIMARY KEY, rel_path TEXT NOT NULL)")
    con.executemany("INSERT INTO notes (uid, rel_path) VALUES (?, ?)", rows)
    con.commit()
    con.close()


def test_quarantine_uids_exclude_suppressed() -> None:
    census = run_email_census_dry_run(
        [
            _thread(
                thread_uid="uid-mkt",
                gmail_thread_id="g-mkt",
                message_uids=("uid-msg-1",),
                label_ids=("CATEGORY_PROMOTIONS",),
                triage_classification="marketing",
                triage_confidence=0.91,
            ),
            _thread(
                thread_uid="uid-derived",
                gmail_thread_id="g-derived",
                label_ids=("CATEGORY_PROMOTIONS",),
                triage_classification="marketing",
                triage_confidence=0.91,
                derived_uids=("meal-order-1",),
            ),
        ],
        context=CensusContext(decision_run_id="restore-q", engine_mode="n/a"),
    )
    q_uids = set(quarantine_uids_for_records(census.records))
    assert "uid-derived" in q_uids
    assert "meal-order-1" in q_uids
    assert "uid-mkt" not in q_uids
    assert "uid-msg-1" not in q_uids


def test_rel_paths_from_scan_cache_skips_artifacts(tmp_path: Path) -> None:
    cache = tmp_path / "cache.sqlite3"
    _write_cache(
        cache,
        [
            ("uid-a", "Email/2020/uid-a.md"),
            ("uid-kit", "_artifacts/hygiene-rollback-kit/x/uid-kit.md"),
        ],
    )
    mapped = rel_paths_from_scan_cache(cache, ["uid-a", "uid-kit", "uid-missing"])
    assert mapped == {"uid-a": "Email/2020/uid-a.md"}


def test_restore_copies_quarantine_only_and_rewrites_ledger(tmp_path: Path) -> None:
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    q_rel = "EmailThreads/2022-09/uid-derived.md"
    s_rel = "Email/2022-09/uid-mkt.md"
    (source / q_rel).parent.mkdir(parents=True, exist_ok=True)
    (source / s_rel).parent.mkdir(parents=True, exist_ok=True)
    (source / q_rel).write_text("quarantine body\n", encoding="utf-8")
    (source / s_rel).write_text("marketing body\n", encoding="utf-8")
    _write_cache(
        source / "_meta" / "vault-scan-cache.sqlite3",
        [
            ("uid-derived", q_rel),
            ("meal-order-1", q_rel),
            ("uid-mkt", s_rel),
        ],
    )

    census = run_email_census_dry_run(
        [
            _thread(
                thread_uid="uid-mkt",
                gmail_thread_id="g-mkt",
                label_ids=("CATEGORY_PROMOTIONS",),
                triage_classification="marketing",
                triage_confidence=0.91,
            ),
            _thread(
                thread_uid="uid-derived",
                gmail_thread_id="g-derived",
                label_ids=("CATEGORY_PROMOTIONS",),
                triage_classification="marketing",
                triage_confidence=0.91,
                derived_uids=("meal-order-1",),
            ),
        ],
        context=CensusContext(decision_run_id="restore-q", engine_mode="n/a"),
    )
    ledger = FilePromotionLedger(default_ledger_path(dest))
    for rec in census.records:
        if rec.gmail_thread_id.strip():
            ledger.persist(rec)

    counts = restore_quarantine_notes(
        census.records,
        source_vault=source,
        dest_vault=dest,
        rematerialize=False,
        progress_every=0,
    )
    assert counts.files_copied == 1
    assert (dest / q_rel).read_text(encoding="utf-8") == "quarantine body\n"
    assert not (dest / s_rel).exists()
    assert (source / q_rel).read_text(encoding="utf-8") == "quarantine body\n"
    replay = FilePromotionLedger(default_ledger_path(dest))
    assert replay.get_thread_state("g-mkt") == "suppressed"
    assert replay.get_thread_state("g-derived") == "active"
    assert counts.ledger_lines_dropped >= 1


def test_drop_quarantine_from_ledger_keeps_suppressed(tmp_path: Path) -> None:
    path = tmp_path / "gmail_promotion_ledger.jsonl"
    path.write_text(
        json.dumps({"gmail_thread_id": "g-s", "corpus_decision": "suppressed"})
        + "\n"
        + json.dumps({"gmail_thread_id": "g-q", "corpus_decision": CORPUS_STATE_QUARANTINE})
        + "\n",
        encoding="utf-8",
    )
    dropped = drop_quarantine_from_ledger(path)
    assert dropped == 1
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [row["gmail_thread_id"] for row in lines] == ["g-s"]
