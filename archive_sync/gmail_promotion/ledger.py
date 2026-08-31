"""Durable promotion ledger for suppressed/quarantine Gmail threads."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from archive_cli.corpus_hygiene.decisions import EmailCorpusDecisionRecord
from archive_cli.corpus_hygiene.state_store import CORPUS_STATE_ACTIVE


class PromotionLedger(Protocol):
    def get_thread_state(self, gmail_thread_id: str) -> str: ...

    def get_decision(self, gmail_thread_id: str) -> EmailCorpusDecisionRecord | None: ...

    def persist(self, record: EmailCorpusDecisionRecord) -> None: ...

    def all_decisions(self) -> list[EmailCorpusDecisionRecord]: ...


@dataclass
class FilePromotionLedger:
    """Append-only JSONL ledger under vault ``_artifacts``."""

    path: Path
    _by_thread: dict[str, EmailCorpusDecisionRecord] | None = None

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            self.path.write_text("", encoding="utf-8")
        self._load()

    def _load(self) -> None:
        from archive_cli.corpus_hygiene.decision_io import record_from_dict

        self._by_thread = {}
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rec = record_from_dict(json.loads(line))
            tid = rec.gmail_thread_id.strip()
            if tid:
                self._by_thread[tid] = rec

    def get_thread_state(self, gmail_thread_id: str) -> str:
        rec = self.get_decision(gmail_thread_id)
        if rec is None:
            return CORPUS_STATE_ACTIVE
        return rec.corpus_decision or CORPUS_STATE_ACTIVE

    def get_decision(self, gmail_thread_id: str) -> EmailCorpusDecisionRecord | None:
        if self._by_thread is None:
            self._load()
        assert self._by_thread is not None
        return self._by_thread.get(gmail_thread_id.strip())

    def persist(self, record: EmailCorpusDecisionRecord) -> None:
        if self._by_thread is None:
            self._load()
        assert self._by_thread is not None
        line = json.dumps(record.to_dict(), sort_keys=True) + "\n"
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
        self._by_thread[record.gmail_thread_id.strip()] = record

    def all_decisions(self) -> list[EmailCorpusDecisionRecord]:
        if self._by_thread is None:
            self._load()
        assert self._by_thread is not None
        return list(self._by_thread.values())


def default_ledger_path(vault_path: str | Path) -> Path:
    return Path(vault_path) / "_artifacts" / "gmail_promotion_ledger.jsonl"


class FailingLedger:
    """Test double that refuses persistence."""

    def get_thread_state(self, gmail_thread_id: str) -> str:
        return CORPUS_STATE_ACTIVE

    def get_decision(self, gmail_thread_id: str) -> EmailCorpusDecisionRecord | None:
        return None

    def persist(self, record: EmailCorpusDecisionRecord) -> None:
        raise OSError("ledger write refused")

    def all_decisions(self) -> list[EmailCorpusDecisionRecord]:
        return []


class CompositePromotionLedger:
    """File ledger for continue-state; Postgres CCS so search can label quarantine."""

    def __init__(self, file_ledger: FilePromotionLedger, db_ledger: DbPromotionLedger) -> None:
        self._file = file_ledger
        self._db = db_ledger

    def get_thread_state(self, gmail_thread_id: str) -> str:
        return self._file.get_thread_state(gmail_thread_id)

    def get_decision(self, gmail_thread_id: str) -> EmailCorpusDecisionRecord | None:
        return self._file.get_decision(gmail_thread_id)

    def persist(self, record: EmailCorpusDecisionRecord) -> None:
        self._file.persist(record)
        self._db.persist(record)

    def all_decisions(self) -> list[EmailCorpusDecisionRecord]:
        return self._file.all_decisions()


class DbPromotionLedger:
    """Optional Postgres-backed ledger via Section B state store."""

    def __init__(self, conn: Any, schema: str, *, decision_run_id: str) -> None:
        self._conn = conn
        self._schema = schema
        self._decision_run_id = decision_run_id
        self._cache: dict[str, EmailCorpusDecisionRecord] = {}

    def get_thread_state(self, gmail_thread_id: str) -> str:
        rec = self.get_decision(gmail_thread_id)
        if rec is None:
            return CORPUS_STATE_ACTIVE
        return rec.corpus_decision

    def get_decision(self, gmail_thread_id: str) -> EmailCorpusDecisionRecord | None:
        return self._cache.get(gmail_thread_id.strip())

    def persist(self, record: EmailCorpusDecisionRecord) -> None:
        from archive_cli.corpus_hygiene.state_store import apply_decision_records

        apply_decision_records(self._conn, self._schema, [record], decision_run_id=self._decision_run_id)
        self._cache[record.gmail_thread_id.strip()] = record

    def all_decisions(self) -> list[EmailCorpusDecisionRecord]:
        return list(self._cache.values())
