#!/usr/bin/env python3
"""Durable supervisor for the initial PPA seed runbook."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

from archive_sync.adapters.apple_health import AppleHealthAdapter
from archive_sync.adapters.calendar_events import CalendarEventsAdapter
from archive_sync.adapters.contacts import ContactsAdapter
from archive_sync.adapters.file_libraries import FileLibrariesAdapter
from archive_sync.adapters.gmail_correspondents import GmailCorrespondentsAdapter
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter
from archive_sync.adapters.medical_records import MedicalRecordsAdapter
from archive_sync.adapters.photos import PhotosAdapter
from archive_vault.sync_state import load_sync_state
from archive_vault.vault import read_note

PHASE_ORDER = [
    "preflight-manifest",
    "preflight-env",
    "prep-readonly",
    "init-canonical",
    "seed-apple-contacts",
    "verify-apple-contacts",
    "seed-google-contacts",
    "verify-google-contacts",
    "seed-medical-records",
    "verify-medical-records",
    "seed-apple-health",
    "verify-apple-health",
    "seed-imessage",
    "verify-imessage",
    "seed-photos",
    "verify-photos",
    "seed-gmail-messages",
    "verify-gmail-messages",
    "seed-calendar-events",
    "verify-calendar-events",
    "seed-gmail-correspondents",
    "seed-file-libraries",
    "verify-file-libraries",
    "canonical-quality-gate",
    "rebuild-derived-index",
    "seed-link-review",
    "link-quality-gate",
    "graph-quality-gate",
    "embed-final-archive",
    "final-acceptance",
]

FULL_CHECKPOINT_PHASES = {
    "init-canonical",
    "verify-apple-contacts",
    "verify-google-contacts",
    "verify-imessage",
    "canonical-quality-gate",
    "final-acceptance",
}

RETRYABLE_PHASES = {
    "prep-readonly",
    "seed-imessage",
    "seed-photos",
    "seed-gmail-messages",
    "seed-calendar-events",
    "seed-gmail-correspondents",
    "seed-file-libraries",
    "embed-final-archive",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _slugify(name: str) -> str:
    return name.replace("_", "-").strip().lower()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _people_count(vault: Path) -> int:
    people_dir = vault / "People"
    if not people_dir.exists():
        return 0
    return len(list(people_dir.glob("*.md")))


def _card_count(vault: Path, root: str) -> int:
    target = vault / root
    if not target.exists():
        return 0
    return len(list(target.rglob("*.md")))


def _cards_with_field(vault: Path, root: str, field_name: str) -> int:
    target = vault / root
    if not target.exists():
        return 0
    count = 0
    for path in target.rglob("*.md"):
        frontmatter, _, _ = read_note(vault, str(path.relative_to(vault)))
        value = frontmatter.get(field_name)
        if value not in ("", [], None, 0, False):
            count += 1
    return count


def _cards_with_exact_people_count(vault: Path, root: str, expected_count: int) -> int:
    target = vault / root
    if not target.exists():
        return 0
    count = 0
    for path in target.rglob("*.md"):
        frontmatter, _, _ = read_note(vault, str(path.relative_to(vault)))
        people = frontmatter.get("people", [])
        if isinstance(people, list) and len(people) == expected_count:
            count += 1
    return count


def _dedup_candidate_count(vault: Path) -> int:
    payload = _read_json(vault / "_meta" / "dedup-candidates.json", [])
    return len(payload) if isinstance(payload, list) else 0


def _validation_error_count(vault: Path) -> int:
    report = _read_json(vault / "_meta" / "validation-report.json", {})
    errors = report.get("errors", []) if isinstance(report, dict) else []
    return len(errors) if isinstance(errors, list) else 0


def _vault_metrics(vault: Path) -> dict[str, int]:
    return {
        "people": _people_count(vault),
        "photos": _card_count(vault, "Photos"),
        "email_threads": _card_count(vault, "EmailThreads"),
        "email_messages": _card_count(vault, "Email"),
        "calendar_events": _card_count(vault, "Calendar"),
        "documents": _card_count(vault, "Documents"),
        "medical_records": _card_count(vault, "Medical"),
        "vaccinations": _card_count(vault, "Vaccinations"),
        "imessage_threads": _card_count(vault, "IMessageThreads"),
        "imessage_messages": _card_count(vault, "IMessage"),
    }


def _sync_cursor(vault: Path, key: str) -> dict[str, Any]:
    state = load_sync_state(vault)
    payload = state.get(key, {}) if isinstance(state, dict) else {}
    return payload if isinstance(payload, dict) else {}


def _gmail_complete(cursor: dict[str, Any]) -> bool:
    return (
        not cursor.get("page_token")
        and not list(cursor.get("page_thread_ids") or [])
        and _coerce_int(cursor.get("page_index")) == 0
    )


def _calendar_complete(cursor: dict[str, Any]) -> bool:
    return not cursor.get("page_token")


def _correspondents_complete(cursor: dict[str, Any]) -> bool:
    return not cursor.get("page_token")


def _imessage_complete(cursor: dict[str, Any]) -> bool:
    target = _coerce_int(cursor.get("snapshot_max_message_rowid"))
    current = _coerce_int(cursor.get("last_completed_message_rowid"))
    return target > 0 and current >= target


def _progress_changed(before: dict[str, Any], after: dict[str, Any], fields: list[str]) -> bool:
    return any(before.get(field) != after.get(field) for field in fields)


def _sample_people_paths(vault: Path, limit: int = 3) -> list[Path]:
    people_dir = vault / "People"
    if not people_dir.exists():
        return []
    return sorted(people_dir.glob("*.md"))[:limit]


def _sample_summaries(vault: Path, limit: int = 3) -> list[str]:
    summaries: list[str] = []
    for path in _sample_people_paths(vault, limit=limit):
        frontmatter, _, _ = read_note(vault, str(path.relative_to(vault)))
        summary = str(frontmatter.get("summary", "")).strip()
        if summary:
            summaries.append(summary)
    return summaries


def _is_vault_initialized(vault: Path) -> bool:
    return (vault / "_meta" / "identity-map.json").exists() and (vault / "_meta" / "sync-state.json").exists()


def _vault_has_content(vault: Path) -> bool:
    if not vault.exists():
        return False
    for child in vault.iterdir():
        if child.name in {".DS_Store"}:
            continue
        if child.is_dir():
            if any(True for _ in child.iterdir()):
                return True
        else:
            return True
    return False


class PhaseFailure(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


@dataclass
class RunManifest:
    run_id: str
    vault_path: str
    log_dir: str
    checkpoint_dir: str
    apple_contacts_vcf: str
    google_account: str
    google_calendar_id: str
    imessage_snapshot_dir: str
    imessage_source_label: str
    photos_library_path: str
    photos_source_label: str
    one_medical_fhir_json: str
    one_medical_ccd_xml: str
    vaccine_pdf: str
    apple_health_export_xml: str
    person_wikilink: str
    archive_index_dsn: str
    embedding_provider: str
    embedding_model: str
    embedding_version: int
    python_bin: str
    full_checkpoint_phases: list[str] = field(default_factory=list)


@dataclass
class RunConfig:
    manifest: RunManifest
    retry_limit: int
    min_free_gb: float
    allow_nonempty_vault: bool
    imessage_batch_size: int
    imessage_workers: int
    gmail_max_threads: int
    gmail_max_messages: int
    gmail_max_attachments: int
    gmail_page_size: int
    gmail_workers: int
    calendar_max_events: int
    correspondents_max_messages: int
    photos_include_private_people: bool
    photos_include_private_labels: bool


class SeedRunbook:
    def __init__(self, config: RunConfig):
        self.config = config
        self.manifest = config.manifest
        self.vault = Path(self.manifest.vault_path).expanduser().resolve()
        self.log_dir = Path(self.manifest.log_dir).expanduser().resolve()
        self.checkpoint_dir = Path(self.manifest.checkpoint_dir).expanduser().resolve()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.log_dir / "run-state.json"
        self.events_path = self.log_dir / "events.jsonl"
        self.manifest_path = self.log_dir / "manifest.json"
        self.current_phase: str | None = None
        self.state = self._load_state()
        _write_json(self.manifest_path, asdict(self.manifest))

    def _load_state(self) -> dict[str, Any]:
        if self.state_path.exists():
            state = _read_json(self.state_path, {})
            if state:
                return state
        phases = {
            phase_id: {
                "status": "pending",
                "attempts": 0,
                "started_at": "",
                "completed_at": "",
                "log_path": str(self._phase_log_path(phase_id)),
                "metrics": {},
                "checkpoints": [],
            }
            for phase_id in PHASE_ORDER
        }
        state = {
            "run_id": self.manifest.run_id,
            "status": "pending",
            "started_at": _now(),
            "completed_at": "",
            "current_phase": "",
            "manifest_path": str(self.manifest_path),
            "phases": phases,
        }
        _write_json(self.state_path, state)
        return state

    def _save_state(self) -> None:
        _write_json(self.state_path, self.state)

    def _record_event(
        self,
        kind: str,
        message: str,
        *,
        phase_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "timestamp": _now(),
            "kind": kind,
            "phase": phase_id or self.current_phase or "",
            "message": message,
            "data": data or {},
        }
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    def _phase_log_path(self, phase_id: str) -> Path:
        index = PHASE_ORDER.index(phase_id) + 1
        return self.log_dir / f"{index:02d}-{phase_id}.log"

    def _log(self, message: str) -> None:
        stamped = f"[{_now()}] {message}"
        print(stamped, flush=True)
        if self.current_phase is not None:
            with self._phase_log_path(self.current_phase).open("a", encoding="utf-8") as handle:
                handle.write(stamped + "\n")

    def _update_phase(self, phase_id: str, *, status: str, metrics: dict[str, Any] | None = None) -> None:
        phase = self.state["phases"][phase_id]
        phase["status"] = status
        if status == "in_progress" and not phase["started_at"]:
            phase["started_at"] = _now()
        if status == "completed":
            phase["completed_at"] = _now()
        if metrics:
            phase["metrics"] = {**phase.get("metrics", {}), **metrics}
        self.state["current_phase"] = phase_id if status == "in_progress" else ""
        if status == "completed" and all(self.state["phases"][item]["status"] == "completed" for item in PHASE_ORDER):
            self.state["status"] = "completed"
            self.state["completed_at"] = _now()
        elif status == "in_progress":
            self.state["status"] = "running"
        self._save_state()

    def _checkpoint(self, phase_id: str, *, metrics: dict[str, Any], full: bool = False) -> Path:
        checkpoint_path = self.checkpoint_dir / phase_id
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        meta_dir = checkpoint_path / "_meta"
        meta_dir.mkdir(exist_ok=True)
        for name in (
            "sync-state.json",
            "identity-map.json",
            "dedup-candidates.json",
            "validation-report.json",
            "enrichment-log.json",
        ):
            source = self.vault / "_meta" / name
            if source.exists():
                shutil.copy2(source, meta_dir / name)
        _write_json(checkpoint_path / "metrics.json", metrics)
        _write_json(checkpoint_path / "vault-counts.json", _vault_metrics(self.vault))
        archive_path = None
        if full:
            archive_path = checkpoint_path / "vault.tar"
            with tarfile.open(archive_path, "w") as archive:
                archive.add(self.vault, arcname=self.vault.name)
        phase = self.state["phases"][phase_id]
        phase.setdefault("checkpoints", []).append(str(checkpoint_path))
        if archive_path is not None:
            phase.setdefault("artifacts", []).append(str(archive_path))
        self._save_state()
        self._record_event(
            "checkpoint",
            f"Checkpoint written for {phase_id}",
            phase_id=phase_id,
            data={"path": str(checkpoint_path)},
        )
        return checkpoint_path

    def _run_command(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        retryable: bool = False,
    ) -> str:
        assert self.current_phase is not None
        resolved_env = os.environ.copy()
        resolved_env.update(env or {})
        self._log("$ " + " ".join(command))
        process = subprocess.Popen(
            command,
            cwd=str(cwd or REPO_ROOT),
            env=resolved_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        lines: list[str] = []
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            with self._phase_log_path(self.current_phase).open("a", encoding="utf-8") as handle:
                handle.write(line)
            lines.append(line)
        exit_code = process.wait()
        output = "".join(lines)
        if exit_code != 0:
            raise PhaseFailure(f"Command failed with exit code {exit_code}", retryable=retryable)
        return output

    @contextmanager
    def _temporary_env(self, **items: str):
        previous = {key: os.environ.get(key) for key in items}
        for key, value in items.items():
            os.environ[key] = value
        try:
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def _doctor_validate(self) -> dict[str, Any]:
        output = self._run_command(
            [
                self.manifest.python_bin,
                "-m",
                "archive_doctor",
                "--vault",
                str(self.vault),
                "validate",
            ]
        )
        report = _read_json(self.vault / "_meta" / "validation-report.json", {})
        if _validation_error_count(self.vault) > 0:
            raise PhaseFailure("Doctor validate reported canonical validation errors")
        return {"output": output, "report": report}

    def _doctor_stats(self) -> str:
        return self._run_command(
            [
                self.manifest.python_bin,
                "-m",
                "archive_doctor",
                "--vault",
                str(self.vault),
                "stats",
            ]
        )

    def _ingest(self, label: str, adapter: Any, *, dry_run: bool = False, **kwargs) -> dict[str, Any]:
        self._log(f"Starting {label}")
        result = adapter.ingest(str(self.vault), dry_run=dry_run, **kwargs)
        metrics = {
            "created": result.created,
            "merged": result.merged,
            "conflicted": result.conflicted,
            "skipped": result.skipped,
            "errors": len(result.errors),
            "skip_details": dict(sorted(result.skip_details.items())),
        }
        self._log(
            f"{label}: created={result.created} merged={result.merged} conflicted={result.conflicted} "
            f"skipped={result.skipped} errors={len(result.errors)}"
        )
        if result.errors:
            raise PhaseFailure(
                f"{label} reported {len(result.errors)} ingest errors",
                retryable=self.current_phase in RETRYABLE_PHASES,
            )
        return metrics

    def _ensure_writable_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(path))
        os.close(fd)
        os.unlink(tmp_name)

    def _disk_free_gb(self, path: Path) -> float:
        usage = shutil.disk_usage(path)
        return usage.free / (1024**3)

    def run(self) -> None:
        for phase_id in PHASE_ORDER:
            if self.state["phases"][phase_id]["status"] == "completed":
                continue
            self.current_phase = phase_id
            self._update_phase(phase_id, status="in_progress")
            phase = self.state["phases"][phase_id]
            while True:
                phase["attempts"] = _coerce_int(phase.get("attempts")) + 1
                self._save_state()
                try:
                    self._record_event(
                        "phase-start",
                        f"Starting {phase_id}",
                        phase_id=phase_id,
                        data={"attempt": phase["attempts"]},
                    )
                    metrics = getattr(self, f"_phase_{phase_id.replace('-', '_')}")()
                    self._update_phase(phase_id, status="completed", metrics=metrics)
                    self._record_event(
                        "phase-complete",
                        f"Completed {phase_id}",
                        phase_id=phase_id,
                        data=metrics,
                    )
                    break
                except PhaseFailure as exc:
                    self._record_event(
                        "phase-error",
                        str(exc),
                        phase_id=phase_id,
                        data={"attempt": phase["attempts"], "retryable": exc.retryable},
                    )
                    self._log(f"{phase_id} failed: {exc}")
                    if exc.retryable and phase["attempts"] <= self.config.retry_limit:
                        self._log(f"Retrying {phase_id} after failure ({phase['attempts']}/{self.config.retry_limit})")
                        time.sleep(2)
                        continue
                    self.state["status"] = "failed"
                    self.state["current_phase"] = phase_id
                    self._save_state()
                    raise
        self.current_phase = None
        self._log("Runbook complete")

    def _phase_preflight_manifest(self) -> dict[str, Any]:
        required = {
            "run_id": self.manifest.run_id,
            "vault_path": self.manifest.vault_path,
            "log_dir": self.manifest.log_dir,
            "checkpoint_dir": self.manifest.checkpoint_dir,
            "apple_contacts_vcf": self.manifest.apple_contacts_vcf,
            "google_account": self.manifest.google_account,
            "google_calendar_id": self.manifest.google_calendar_id,
            "imessage_snapshot_dir": self.manifest.imessage_snapshot_dir,
            "imessage_source_label": self.manifest.imessage_source_label,
            "photos_library_path": self.manifest.photos_library_path,
            "photos_source_label": self.manifest.photos_source_label,
            "one_medical_fhir_json": self.manifest.one_medical_fhir_json,
            "one_medical_ccd_xml": self.manifest.one_medical_ccd_xml,
            "vaccine_pdf": self.manifest.vaccine_pdf,
            "apple_health_export_xml": self.manifest.apple_health_export_xml,
            "person_wikilink": self.manifest.person_wikilink,
            "archive_index_dsn": self.manifest.archive_index_dsn,
            "embedding_provider": self.manifest.embedding_provider,
            "embedding_model": self.manifest.embedding_model,
        }
        missing = [key for key, value in required.items() if not str(value).strip()]
        if missing:
            raise PhaseFailure(f"Missing required manifest values: {', '.join(sorted(missing))}")
        _write_json(self.manifest_path, asdict(self.manifest))
        return {"manifest_path": str(self.manifest_path)}

    def _phase_preflight_env(self) -> dict[str, Any]:
        python_bin = Path(self.manifest.python_bin)
        if not python_bin.exists():
            raise PhaseFailure(f"Python executable not found: {python_bin}")
        apple_vcf = Path(self.manifest.apple_contacts_vcf).expanduser()
        photos_library = Path(self.manifest.photos_library_path).expanduser()
        one_medical_fhir = Path(self.manifest.one_medical_fhir_json).expanduser()
        one_medical_ccd = Path(self.manifest.one_medical_ccd_xml).expanduser()
        vaccine_pdf = Path(self.manifest.vaccine_pdf).expanduser()
        apple_health_export = Path(self.manifest.apple_health_export_xml).expanduser()
        if not apple_vcf.exists():
            raise PhaseFailure(f"Apple contacts export not found: {apple_vcf}")
        if not photos_library.exists():
            raise PhaseFailure(f"Photos library not found: {photos_library}")
        if not one_medical_fhir.exists():
            raise PhaseFailure(f"One Medical FHIR export not found: {one_medical_fhir}")
        if not one_medical_ccd.exists():
            raise PhaseFailure(f"One Medical CCD export not found: {one_medical_ccd}")
        if not vaccine_pdf.exists():
            raise PhaseFailure(f"Vaccine PDF not found: {vaccine_pdf}")
        if not apple_health_export.exists():
            raise PhaseFailure(f"Apple Health export not found: {apple_health_export}")
        for path in (self.log_dir, self.checkpoint_dir):
            self._ensure_writable_dir(path)
        for path in (
            self.log_dir,
            self.checkpoint_dir,
            self.vault.parent,
            Path(self.manifest.imessage_snapshot_dir).expanduser().parent,
        ):
            free_gb = self._disk_free_gb(path)
            if free_gb < self.config.min_free_gb:
                raise PhaseFailure(
                    f"Insufficient free space at {path}: {free_gb:.2f} GB < {self.config.min_free_gb:.2f} GB"
                )
        if (
            _vault_has_content(self.vault)
            and not _is_vault_initialized(self.vault)
            and not self.config.allow_nonempty_vault
        ):
            raise PhaseFailure("Vault has unexpected content but is not initialized for a resumable run")
        return {
            "python_bin": str(python_bin),
            "free_gb_log_dir": round(self._disk_free_gb(self.log_dir), 2),
            "free_gb_checkpoint_dir": round(self._disk_free_gb(self.checkpoint_dir), 2),
        }

    def _phase_prep_readonly(self) -> dict[str, Any]:
        snapshot_dir = Path(self.manifest.imessage_snapshot_dir).expanduser()
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self._run_command(
            [
                self.manifest.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-imessage-snapshot.py"),
                "--output-dir",
                str(snapshot_dir),
                "--source-label",
                self.manifest.imessage_source_label,
            ],
            retryable=True,
        )
        expected = [
            snapshot_dir / "snapshot-meta.json",
            snapshot_dir / "inspection.json",
            snapshot_dir / "attachments-manifest.json",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        if missing:
            raise PhaseFailure(f"Snapshot bundle missing expected files: {', '.join(missing)}")
        contacts_metrics = self._ingest(
            "google-auth-warmup.contacts",
            ContactsAdapter(),
            dry_run=True,
            sources=["google"],
        )
        gmail_metrics = self._ingest(
            "google-auth-warmup.gmail-correspondents",
            GmailCorrespondentsAdapter(),
            dry_run=True,
            account_email=self.manifest.google_account,
            max_messages=1,
        )
        calendar_metrics = self._ingest(
            "google-auth-warmup.calendar-events",
            CalendarEventsAdapter(),
            dry_run=True,
            account_email=self.manifest.google_account,
            calendar_id=self.manifest.google_calendar_id,
            max_events=1,
            quick_update=True,
        )
        inspection = _read_json(snapshot_dir / "inspection.json", {})
        return {
            "snapshot_dir": str(snapshot_dir),
            "snapshot_messages": _coerce_int(inspection.get("message_count")),
            "snapshot_latest_rowid": _coerce_int(inspection.get("latest_message_rowid")),
            "google_contacts": contacts_metrics,
            "gmail": gmail_metrics,
            "calendar": calendar_metrics,
        }

    def _phase_init_canonical(self) -> dict[str, Any]:
        if (
            _vault_has_content(self.vault)
            and not self.config.allow_nonempty_vault
            and not _is_vault_initialized(self.vault)
        ):
            raise PhaseFailure("Refusing to initialize a non-empty vault without --allow-nonempty-vault")
        self._run_command(
            ["bash", str(REPO_ROOT / "scripts" / "ppa-init-vault.sh"), str(self.vault)],
            env={"PYTHON": self.manifest.python_bin},
        )
        if not _is_vault_initialized(self.vault):
            raise PhaseFailure("Vault init did not create expected metadata files")
        metrics = {"vault": str(self.vault), **_vault_metrics(self.vault)}
        self._checkpoint(
            "init-canonical",
            metrics=metrics,
            full="init-canonical" in self.manifest.full_checkpoint_phases,
        )
        return metrics

    def _phase_seed_apple_contacts(self) -> dict[str, Any]:
        before = _people_count(self.vault)
        metrics = self._ingest(
            "contacts.apple",
            ContactsAdapter(),
            sources=["apple"],
            vcf_paths=[self.manifest.apple_contacts_vcf],
        )
        after = _people_count(self.vault)
        if after <= 0:
            raise PhaseFailure("Apple contacts seed produced zero people")
        return {**metrics, "people_before": before, "people_after": after}

    def _phase_verify_apple_contacts(self) -> dict[str, Any]:
        cursor = _sync_cursor(self.vault, "contacts.apple")
        if not cursor:
            raise PhaseFailure("Missing contacts.apple sync-state entry after Apple contacts seed")
        validate = self._doctor_validate()
        stats_output = self._doctor_stats()
        metrics = {
            "cursor_present": True,
            "people_count": _people_count(self.vault),
            "dedup_candidates": _dedup_candidate_count(self.vault),
            "validate_total": (validate["report"].get("total_cards", 0) if isinstance(validate["report"], dict) else 0),
            "stats_preview": stats_output.splitlines()[:10],
        }
        self._checkpoint(
            "verify-apple-contacts",
            metrics=metrics,
            full="verify-apple-contacts" in self.manifest.full_checkpoint_phases,
        )
        return metrics

    def _phase_seed_google_contacts(self) -> dict[str, Any]:
        before = _people_count(self.vault)
        metrics = self._ingest("contacts.google", ContactsAdapter(), sources=["google"])
        after = _people_count(self.vault)
        return {**metrics, "people_before": before, "people_after": after}

    def _phase_verify_google_contacts(self) -> dict[str, Any]:
        cursor = _sync_cursor(self.vault, "contacts.google")
        if not cursor:
            raise PhaseFailure("Missing contacts.google sync-state entry after Google contacts seed")
        validate = self._doctor_validate()
        stats_output = self._doctor_stats()
        metrics = {
            "cursor_present": True,
            "people_count": _people_count(self.vault),
            "dedup_candidates": _dedup_candidate_count(self.vault),
            "validate_total": (validate["report"].get("total_cards", 0) if isinstance(validate["report"], dict) else 0),
            "stats_preview": stats_output.splitlines()[:10],
        }
        self._checkpoint(
            "verify-google-contacts",
            metrics=metrics,
            full="verify-google-contacts" in self.manifest.full_checkpoint_phases,
        )
        return metrics

    def _phase_seed_medical_records(self) -> dict[str, Any]:
        before_medical = _card_count(self.vault, "Medical")
        before_vaccinations = _card_count(self.vault, "Vaccinations")
        metrics = self._ingest(
            "medical-records",
            MedicalRecordsAdapter(),
            fhir_json_path=self.manifest.one_medical_fhir_json,
            ccd_xml_path=self.manifest.one_medical_ccd_xml,
            vaccine_pdf_path=self.manifest.vaccine_pdf,
            person_wikilink=self.manifest.person_wikilink,
        )
        after_medical = _card_count(self.vault, "Medical")
        after_vaccinations = _card_count(self.vault, "Vaccinations")
        if after_medical + after_vaccinations <= 0:
            raise PhaseFailure("Medical seed produced zero medical cards")
        return {
            **metrics,
            "medical_before": before_medical,
            "medical_after": after_medical,
            "vaccinations_before": before_vaccinations,
            "vaccinations_after": after_vaccinations,
        }

    def _phase_verify_medical_records(self) -> dict[str, Any]:
        cursor = _sync_cursor(self.vault, "medical-records:onemedical")
        if not cursor:
            raise PhaseFailure("Missing medical-records:onemedical sync-state entry after One Medical import")
        medical_count = _card_count(self.vault, "Medical")
        vaccination_count = _card_count(self.vault, "Vaccinations")
        medical_people = _cards_with_exact_people_count(self.vault, "Medical", 1)
        vaccination_people = _cards_with_exact_people_count(self.vault, "Vaccinations", 1)
        if medical_count and medical_people != medical_count:
            raise PhaseFailure("Not all Medical cards have exactly one person link")
        if vaccination_count and vaccination_people != vaccination_count:
            raise PhaseFailure("Not all Vaccination cards have exactly one person link")
        metrics = {
            "cursor_present": True,
            "medical_records": medical_count,
            "vaccinations": vaccination_count,
            "medical_with_exact_person_link": medical_people,
            "vaccinations_with_exact_person_link": vaccination_people,
        }
        return metrics

    def _phase_seed_apple_health(self) -> dict[str, Any]:
        before = _card_count(self.vault, "Medical")
        metrics = self._ingest(
            "apple-health",
            AppleHealthAdapter(),
            export_xml_path=self.manifest.apple_health_export_xml,
            person_wikilink=self.manifest.person_wikilink,
        )
        after = _card_count(self.vault, "Medical")
        return {**metrics, "medical_before": before, "medical_after": after}

    def _phase_verify_apple_health(self) -> dict[str, Any]:
        cursor = _sync_cursor(self.vault, "apple-health")
        if not cursor:
            raise PhaseFailure("Missing apple-health sync-state entry after Apple Health import")
        medical_count = _card_count(self.vault, "Medical")
        medical_people = _cards_with_exact_people_count(self.vault, "Medical", 1)
        if medical_count and medical_people != medical_count:
            raise PhaseFailure("Not all Medical cards have exactly one person link after Apple Health import")
        metrics = {
            "cursor_present": True,
            "medical_records": medical_count,
            "medical_with_exact_person_link": medical_people,
            "emitted_records": _coerce_int(cursor.get("emitted_records")),
        }
        return metrics

    def _phase_seed_imessage(self) -> dict[str, Any]:
        self._run_command(
            [
                self.manifest.python_bin,
                str(REPO_ROOT / "scripts" / "ppa-imessage-import-all.py"),
                "--vault",
                str(self.vault),
                "--snapshot-dir",
                self.manifest.imessage_snapshot_dir,
                "--source-label",
                self.manifest.imessage_source_label,
                "--batch-size",
                str(self.config.imessage_batch_size),
                "--workers",
                str(self.config.imessage_workers),
            ],
            retryable=True,
        )
        cursor = _sync_cursor(
            self.vault,
            f"imessage:{self.manifest.imessage_source_label.strip().lower()}",
        )
        if not _imessage_complete(cursor):
            raise PhaseFailure(
                "iMessage import did not reach snapshot rowid completion",
                retryable=True,
            )
        return {
            "cursor": cursor,
            "imessage_threads": _card_count(self.vault, "IMessageThreads"),
            "imessage_messages": _card_count(self.vault, "IMessage"),
        }

    def _phase_verify_imessage(self) -> dict[str, Any]:
        cursor = _sync_cursor(
            self.vault,
            f"imessage:{self.manifest.imessage_source_label.strip().lower()}",
        )
        if not _imessage_complete(cursor):
            raise PhaseFailure("iMessage sync-state is not complete")
        metrics = {
            "last_completed_message_rowid": _coerce_int(cursor.get("last_completed_message_rowid")),
            "snapshot_max_message_rowid": _coerce_int(cursor.get("snapshot_max_message_rowid")),
            "imessage_threads": _card_count(self.vault, "IMessageThreads"),
            "imessage_messages": _card_count(self.vault, "IMessage"),
            "threads_with_people_links": _cards_with_field(self.vault, "IMessageThreads", "people"),
            "messages_with_attachments": _cards_with_field(self.vault, "IMessage", "attachments"),
        }
        self._checkpoint(
            "verify-imessage",
            metrics=metrics,
            full="verify-imessage" in self.manifest.full_checkpoint_phases,
        )
        return metrics

    def _phase_seed_photos(self) -> dict[str, Any]:
        before = _card_count(self.vault, "Photos")
        metrics = self._ingest(
            "photos",
            PhotosAdapter(),
            source_label=self.manifest.photos_source_label,
            library_path=self.manifest.photos_library_path,
            quick_update=True,
            include_private_people=self.config.photos_include_private_people,
            include_private_labels=self.config.photos_include_private_labels,
        )
        after = _card_count(self.vault, "Photos")
        return {**metrics, "photos_before": before, "photos_after": after}

    def _phase_verify_photos(self) -> dict[str, Any]:
        cursor = _sync_cursor(self.vault, f"photos:{self.manifest.photos_source_label.strip().lower()}")
        if not cursor:
            raise PhaseFailure("Missing photos sync-state entry after Photos import")
        metrics = {
            "scanned_assets": _coerce_int(cursor.get("scanned_assets")),
            "emitted_assets": _coerce_int(cursor.get("emitted_assets")),
            "skipped_unchanged_assets": _coerce_int(cursor.get("skipped_unchanged_assets")),
            "photos_cards": _card_count(self.vault, "Photos"),
            "photos_with_people_links": _cards_with_field(self.vault, "Photos", "people"),
        }
        self._checkpoint("verify-photos", metrics=metrics, full=False)
        return metrics

    def _phase_seed_gmail_messages(self) -> dict[str, Any]:
        key = f"gmail-messages:{self.manifest.google_account.strip().lower()}"
        tracked_fields = [
            "page_token",
            "page_index",
            "scanned_threads",
            "emitted_threads",
            "emitted_messages",
            "emitted_attachments",
            "skipped_unchanged_threads",
            "skipped_unchanged_messages",
            "skipped_unchanged_attachments",
        ]
        iterations = 0
        while True:
            iterations += 1
            before = _sync_cursor(self.vault, key)
            metrics = self._ingest(
                f"gmail-messages.loop.{iterations}",
                GmailMessagesAdapter(),
                account_email=self.manifest.google_account,
                max_threads=self.config.gmail_max_threads,
                max_messages=self.config.gmail_max_messages,
                max_attachments=self.config.gmail_max_attachments,
                page_size=self.config.gmail_page_size,
                workers=self.config.gmail_workers,
                quick_update=True,
            )
            after = _sync_cursor(self.vault, key)
            self._record_event(
                "gmail-loop",
                "Completed Gmail batch",
                phase_id=self.current_phase,
                data={"iteration": iterations, **metrics},
            )
            if _gmail_complete(after):
                return {"iterations": iterations, "cursor": after, **metrics}
            if not _progress_changed(before, after, tracked_fields):
                raise PhaseFailure("Gmail messages cursor did not advance", retryable=True)

    def _phase_verify_gmail_messages(self) -> dict[str, Any]:
        key = f"gmail-messages:{self.manifest.google_account.strip().lower()}"
        cursor = _sync_cursor(self.vault, key)
        if not _gmail_complete(cursor):
            raise PhaseFailure("Gmail messages cursor is not exhausted")
        metrics = {
            "scanned_threads": _coerce_int(cursor.get("scanned_threads")),
            "emitted_threads": _coerce_int(cursor.get("emitted_threads")),
            "emitted_messages": _coerce_int(cursor.get("emitted_messages")),
            "emitted_attachments": _coerce_int(cursor.get("emitted_attachments")),
            "email_threads": _card_count(self.vault, "EmailThreads"),
            "email_messages": _card_count(self.vault, "Email"),
            "threads_with_calendar_links": _cards_with_field(self.vault, "EmailThreads", "calendar_events"),
            "messages_with_people_links": _cards_with_field(self.vault, "Email", "people"),
        }
        self._checkpoint("verify-gmail-messages", metrics=metrics, full=False)
        return metrics

    def _phase_seed_calendar_events(self) -> dict[str, Any]:
        key = f"calendar-events:{self.manifest.google_account.strip().lower()}:{self.manifest.google_calendar_id.strip().lower()}"
        tracked_fields = ["page_token", "emitted_events", "skipped_unchanged_events"]
        iterations = 0
        while True:
            iterations += 1
            before = _sync_cursor(self.vault, key)
            metrics = self._ingest(
                f"calendar-events.loop.{iterations}",
                CalendarEventsAdapter(),
                account_email=self.manifest.google_account,
                calendar_id=self.manifest.google_calendar_id,
                max_events=self.config.calendar_max_events,
                quick_update=True,
            )
            after = _sync_cursor(self.vault, key)
            self._record_event(
                "calendar-loop",
                "Completed Calendar batch",
                phase_id=self.current_phase,
                data={"iteration": iterations, **metrics},
            )
            if _calendar_complete(after):
                return {"iterations": iterations, "cursor": after, **metrics}
            if not _progress_changed(before, after, tracked_fields):
                raise PhaseFailure("Calendar cursor did not advance", retryable=True)

    def _phase_verify_calendar_events(self) -> dict[str, Any]:
        key = f"calendar-events:{self.manifest.google_account.strip().lower()}:{self.manifest.google_calendar_id.strip().lower()}"
        cursor = _sync_cursor(self.vault, key)
        if not _calendar_complete(cursor):
            raise PhaseFailure("Calendar cursor is not exhausted")
        metrics = {
            "emitted_events": _coerce_int(cursor.get("emitted_events")),
            "calendar_events": _card_count(self.vault, "Calendar"),
            "events_with_source_messages": _cards_with_field(self.vault, "Calendar", "source_messages"),
            "events_with_people_links": _cards_with_field(self.vault, "Calendar", "people"),
        }
        self._checkpoint("verify-calendar-events", metrics=metrics, full=False)
        return metrics

    def _phase_seed_gmail_correspondents(self) -> dict[str, Any]:
        key = f"gmail-correspondents:{self.manifest.google_account.strip().lower()}"
        tracked_fields = [
            "page_token",
            "scanned_messages",
            "processed",
            "created",
            "merged",
        ]
        iterations = 0
        while True:
            iterations += 1
            before = _sync_cursor(self.vault, key)
            metrics = self._ingest(
                f"gmail-correspondents.loop.{iterations}",
                GmailCorrespondentsAdapter(),
                account_email=self.manifest.google_account,
                max_messages=self.config.correspondents_max_messages,
            )
            after = _sync_cursor(self.vault, key)
            self._record_event(
                "gmail-correspondents-loop",
                "Completed Gmail correspondents batch",
                phase_id=self.current_phase,
                data={"iteration": iterations, **metrics},
            )
            if _correspondents_complete(after):
                return {"iterations": iterations, "cursor": after, **metrics}
            if not _progress_changed(before, after, tracked_fields):
                raise PhaseFailure("Gmail correspondents cursor did not advance", retryable=True)

    def _phase_seed_file_libraries(self) -> dict[str, Any]:
        before = _card_count(self.vault, "Documents")
        metrics = self._ingest(
            "file-libraries",
            FileLibrariesAdapter(),
            quick_update=True,
        )
        after = _card_count(self.vault, "Documents")
        return {**metrics, "documents_before": before, "documents_after": after}

    def _phase_verify_file_libraries(self) -> dict[str, Any]:
        cursor = _sync_cursor(self.vault, "file-libraries")
        if not cursor:
            raise PhaseFailure("Missing file-libraries sync-state entry after document import")
        metrics = {
            "scanned_candidates": _coerce_int(cursor.get("scanned_candidates")),
            "emitted_documents": _coerce_int(cursor.get("emitted_documents")),
            "documents": _card_count(self.vault, "Documents"),
            "documents_with_people_links": _cards_with_field(self.vault, "Documents", "people"),
            "documents_with_orgs": _cards_with_field(self.vault, "Documents", "orgs"),
        }
        self._checkpoint("verify-file-libraries", metrics=metrics, full=False)
        return metrics

    def _phase_canonical_quality_gate(self) -> dict[str, Any]:
        self._run_command(
            ["bash", str(REPO_ROOT / "scripts" / "ppa-post-import.sh")],
            env={"PPA_PATH": str(self.vault), "PYTHON": self.manifest.python_bin},
        )
        validation_errors = _validation_error_count(self.vault)
        if validation_errors > 0:
            raise PhaseFailure("Canonical quality gate failed validation")
        metrics = {
            "dedup_candidates": _dedup_candidate_count(self.vault),
            "validation_errors": validation_errors,
            **_vault_metrics(self.vault),
        }
        self._checkpoint(
            "canonical-quality-gate",
            metrics=metrics,
            full="canonical-quality-gate" in self.manifest.full_checkpoint_phases,
        )
        return metrics

    def _archive_modules(self):
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        os.environ["PPA_PATH"] = str(self.vault)
        os.environ["ARCHIVE_INDEX_DSN"] = self.manifest.archive_index_dsn
        os.environ["ARCHIVE_EMBEDDING_PROVIDER"] = self.manifest.embedding_provider
        os.environ["ARCHIVE_EMBEDDING_MODEL"] = self.manifest.embedding_model
        os.environ["ARCHIVE_EMBEDDING_VERSION"] = str(self.manifest.embedding_version)
        import importlib

        archive_server = importlib.import_module("archive_cli.server")
        archive_index_store = importlib.import_module("archive_cli.index_store")
        archive_embedding = importlib.import_module("archive_cli.embedding_provider")
        archive_seed_links = importlib.import_module("archive_cli.seed_links")
        return (
            archive_server,
            archive_index_store,
            archive_embedding,
            archive_seed_links,
        )

    def _phase_rebuild_derived_index(self) -> dict[str, Any]:
        _, archive_index_store, _, _ = self._archive_modules()
        index = archive_index_store.PostgresArchiveIndex(self.vault, dsn=self.manifest.archive_index_dsn)
        bootstrap = index.bootstrap()
        counts = index.rebuild()
        status = index.status()
        metrics = {"bootstrap": bootstrap, "counts": counts, "status": status}
        if _coerce_int(status.get("duplicate_uid_count")) != 0:
            raise PhaseFailure("Derived index rebuild reported duplicate UIDs")
        if _coerce_int(status.get("medical_record_count")) <= 0:
            raise PhaseFailure("Derived index rebuild did not materialize medical_records rows")
        if _coerce_int(status.get("vaccination_count")) <= 0:
            raise PhaseFailure("Derived index rebuild did not materialize vaccinations rows")
        return metrics

    def _phase_seed_link_review(self) -> dict[str, Any]:
        _, archive_index_store, _, archive_seed_links = self._archive_modules()
        index = archive_index_store.PostgresArchiveIndex(self.vault, dsn=self.manifest.archive_index_dsn)
        metrics = archive_seed_links.run_seed_link_backfill(
            index,
            max_workers=max(4, os.cpu_count() or 4),
            include_llm=True,
            apply_promotions=True,
        )
        if _coerce_int(metrics.get("jobs_failed")) > 0:
            raise PhaseFailure("Seed link review reported failed jobs", retryable=True)
        return metrics

    def _phase_link_quality_gate(self) -> dict[str, Any]:
        _, archive_index_store, _, archive_seed_links = self._archive_modules()
        index = archive_index_store.PostgresArchiveIndex(self.vault, dsn=self.manifest.archive_index_dsn)
        metrics = archive_seed_links.compute_link_quality_gate(index)
        if not bool(metrics.get("passes")):
            raise PhaseFailure("Seed link quality gate did not pass")
        return metrics

    def _phase_graph_quality_gate(self) -> dict[str, Any]:
        archive_server, archive_index_store, _, _ = self._archive_modules()
        index = archive_index_store.get_archive_index(self.vault)
        status = index.status()
        duplicate_uid_count = _coerce_int(status.get("duplicate_uid_count"))
        edge_count = _coerce_int(status.get("edge_count"))
        card_count = _coerce_int(status.get("card_count"))
        chunk_count = _coerce_int(status.get("chunk_count"))
        if duplicate_uid_count != 0:
            raise PhaseFailure("Graph gate blocked by duplicate UIDs in the derived index")
        if edge_count <= 0 or card_count <= 0 or chunk_count <= 0:
            raise PhaseFailure("Derived index looks incomplete for graph verification")
        people_paths = _sample_people_paths(self.vault, limit=3)
        if not people_paths:
            raise PhaseFailure("No people cards available for graph verification")
        graph_hits = 0
        graph_samples: dict[str, str] = {}
        for path in people_paths:
            rel_path = path.relative_to(self.vault).as_posix()
            result = archive_server.archive_graph(rel_path, hops=1)
            graph_samples[rel_path] = result
            if "->" in result:
                graph_hits += 1
        if graph_hits == 0:
            raise PhaseFailure("Graph verification found no linked notes for sampled people")
        summaries = _sample_summaries(self.vault, limit=1)
        query_term = summaries[0] if summaries else "archive"
        query_output = archive_server.archive_query(type_filter="person", limit=5)
        search_output = archive_server.archive_search(query_term, limit=5)
        metrics = {
            "edge_count": edge_count,
            "card_count": card_count,
            "chunk_count": chunk_count,
            "edge_density": round(edge_count / max(card_count, 1), 4),
            "query_preview": query_output.splitlines()[:5],
            "search_preview": search_output.splitlines()[:5],
            "graph_samples": graph_samples,
        }
        return metrics

    def _phase_embed_final_archive(self) -> dict[str, Any]:
        archive_server, archive_index_store, archive_embedding, _ = self._archive_modules()
        index = archive_index_store.get_archive_index(self.vault)
        provider = archive_embedding.get_embedding_provider(model=self.manifest.embedding_model)
        iterations = 0
        while True:
            status = index.embedding_status(
                embedding_model=self.manifest.embedding_model,
                embedding_version=self.manifest.embedding_version,
            )
            pending = _coerce_int(status.get("pending_chunk_count"))
            self._record_event(
                "embedding-status",
                "Embedding status polled",
                phase_id=self.current_phase,
                data=status,
            )
            if pending == 0:
                break
            iterations += 1
            result = index.embed_pending(
                provider=provider,
                embedding_model=self.manifest.embedding_model,
                embedding_version=self.manifest.embedding_version,
                limit=max(20, pending),
            )
            if _coerce_int(result.get("failed")) > 0:
                raise PhaseFailure("Embedding run reported failed chunks", retryable=True)
            if _coerce_int(result.get("embedded")) <= 0:
                raise PhaseFailure("Embedding run did not make progress", retryable=True)
        summaries = _sample_summaries(self.vault, limit=1)
        query_term = summaries[0] if summaries else "archive"
        vector_output = archive_server.archive_vector_search(
            query_term,
            limit=5,
            embedding_model=self.manifest.embedding_model,
            embedding_version=self.manifest.embedding_version,
        )
        hybrid_output = archive_server.archive_hybrid_search(
            query_term,
            limit=5,
            embedding_model=self.manifest.embedding_model,
            embedding_version=self.manifest.embedding_version,
        )
        final_status = index.embedding_status(
            embedding_model=self.manifest.embedding_model,
            embedding_version=self.manifest.embedding_version,
        )
        if _coerce_int(final_status.get("pending_chunk_count")) != 0:
            raise PhaseFailure("Embedding backlog is not empty after embed loop", retryable=True)
        return {
            "iterations": iterations,
            "status": final_status,
            "vector_preview": vector_output.splitlines()[:5],
            "hybrid_preview": hybrid_output.splitlines()[:5],
        }

    def _phase_final_acceptance(self) -> dict[str, Any]:
        archive_server, archive_index_store, _, _ = self._archive_modules()
        index = archive_index_store.get_archive_index(self.vault)
        index_status = index.status()
        embedding_status = index.embedding_status(
            embedding_model=self.manifest.embedding_model,
            embedding_version=self.manifest.embedding_version,
        )
        metrics = {
            "vault_metrics": _vault_metrics(self.vault),
            "dedup_candidates": _dedup_candidate_count(self.vault),
            "validation_errors": _validation_error_count(self.vault),
            "index_status": index_status,
            "embedding_status": embedding_status,
            "archive_stats": archive_server.archive_stats().splitlines()[:15],
            "connectivity": {
                "photos_with_people_links": _cards_with_field(self.vault, "Photos", "people"),
                "imessage_threads_with_people_links": _cards_with_field(self.vault, "IMessageThreads", "people"),
                "gmail_messages_with_people_links": _cards_with_field(self.vault, "Email", "people"),
                "documents_with_people_links": _cards_with_field(self.vault, "Documents", "people"),
                "documents_with_orgs": _cards_with_field(self.vault, "Documents", "orgs"),
                "medical_with_people_links": _cards_with_field(self.vault, "Medical", "people"),
                "vaccinations_with_people_links": _cards_with_field(self.vault, "Vaccinations", "people"),
                "email_threads_with_calendar_links": _cards_with_field(self.vault, "EmailThreads", "calendar_events"),
                "calendar_events_with_source_messages": _cards_with_field(self.vault, "Calendar", "source_messages"),
                "calendar_events_with_people_links": _cards_with_field(self.vault, "Calendar", "people"),
            },
        }
        if metrics["validation_errors"] != 0:
            raise PhaseFailure("Final acceptance blocked by validation errors")
        if _coerce_int(index_status.get("duplicate_uid_count")) != 0:
            raise PhaseFailure("Final acceptance blocked by duplicate UIDs")
        if _coerce_int(embedding_status.get("pending_chunk_count")) != 0:
            raise PhaseFailure("Final acceptance blocked by pending embeddings")
        self._checkpoint(
            "final-acceptance",
            metrics=metrics,
            full="final-acceptance" in self.manifest.full_checkpoint_phases,
        )
        return metrics


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Supervise the initial PPA seeding runbook")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_shared_arguments(target: argparse.ArgumentParser) -> None:
        target.add_argument(
            "--run-id",
            default=os.environ.get("RUN_ID", f"ppa-seed-{datetime.now():%Y%m%d-%H%M%S}"),
        )
        target.add_argument(
            "--vault",
            default=os.environ.get("PPA_PATH", str(Path.home() / "Archive" / "production" / "hf-archives")),
        )
        target.add_argument(
            "--log-dir",
            default=os.environ.get(
                "LOG_DIR",
                str(Path.home() / "Archive" / "ops" / "ppa-seed-logs" / os.environ.get("RUN_ID", "latest")),
            ),
        )
        target.add_argument(
            "--checkpoint-dir",
            default=os.environ.get(
                "CHECKPOINT_DIR",
                str(Path.home() / "Archive" / "ops" / "ppa-seed-checkpoints" / os.environ.get("RUN_ID", "latest")),
            ),
        )
        target.add_argument("--apple-contacts-vcf", default=os.environ.get("APPLE_CONTACTS_VCF", ""))
        target.add_argument(
            "--google-account",
            default=os.environ.get("GOOGLE_ACCOUNT", "rheeger@gmail.com"),
        )
        target.add_argument(
            "--google-calendar-id",
            default=os.environ.get("GOOGLE_CALENDAR_ID", "primary"),
        )
        target.add_argument(
            "--imessage-snapshot-dir",
            default=os.environ.get(
                "IMESSAGE_SNAPSHOT_DIR",
                str(Path.home() / "Archive" / "ppa-imessage-snapshot"),
            ),
        )
        target.add_argument(
            "--imessage-source-label",
            default=os.environ.get("IMESSAGE_SOURCE_LABEL", "local-messages"),
        )
        target.add_argument(
            "--photos-library-path",
            default=os.environ.get(
                "PHOTOS_LIBRARY_PATH",
                str(Path.home() / "Pictures" / "Photos Library.photoslibrary"),
            ),
        )
        target.add_argument(
            "--photos-source-label",
            default=os.environ.get("PHOTOS_SOURCE_LABEL", "apple-photos"),
        )
        target.add_argument(
            "--one-medical-fhir-json",
            default=os.environ.get("ONE_MEDICAL_FHIR_JSON", ""),
        )
        target.add_argument("--one-medical-ccd-xml", default=os.environ.get("ONE_MEDICAL_CCD_XML", ""))
        target.add_argument("--vaccine-pdf", default=os.environ.get("VACCINE_PDF", ""))
        target.add_argument(
            "--apple-health-export-xml",
            default=os.environ.get("APPLE_HEALTH_EXPORT_XML", ""),
        )
        target.add_argument("--person-wikilink", default=os.environ.get("PERSON_WIKILINK", ""))
        target.add_argument("--archive-index-dsn", default=os.environ.get("ARCHIVE_INDEX_DSN", ""))
        target.add_argument(
            "--embedding-provider",
            default=os.environ.get("ARCHIVE_EMBEDDING_PROVIDER", "hash"),
        )
        target.add_argument(
            "--embedding-model",
            default=os.environ.get("ARCHIVE_EMBEDDING_MODEL", "archive-hash-dev"),
        )
        target.add_argument(
            "--embedding-version",
            type=int,
            default=int(os.environ.get("ARCHIVE_EMBEDDING_VERSION", "1")),
        )
        target.add_argument("--python-bin", default=os.environ.get("PYTHON", sys.executable))

    run = sub.add_parser("run")
    add_shared_arguments(run)
    run.add_argument("--retry-limit", type=int, default=3)
    run.add_argument("--min-free-gb", type=float, default=5.0)
    run.add_argument("--allow-nonempty-vault", action="store_true")
    run.add_argument("--imessage-batch-size", type=int, default=10000)
    run.add_argument("--imessage-workers", type=int, default=4)
    run.add_argument("--gmail-max-threads", type=int, default=250)
    run.add_argument("--gmail-max-messages", type=int, default=2500)
    run.add_argument("--gmail-max-attachments", type=int, default=2500)
    run.add_argument("--gmail-page-size", type=int, default=50)
    run.add_argument("--gmail-workers", type=int, default=32)
    run.add_argument("--calendar-max-events", type=int, default=500)
    run.add_argument("--correspondents-max-messages", type=int, default=5000)
    run.add_argument("--no-photos-private-people", action="store_true")
    run.add_argument("--no-photos-private-labels", action="store_true")
    run.add_argument(
        "--full-checkpoint-phases",
        default=",".join(sorted(FULL_CHECKPOINT_PHASES)),
        help="Comma-separated phase IDs that should store full vault tar checkpoints",
    )

    status = sub.add_parser("status")
    add_shared_arguments(status)

    return parser


def _manifest_from_args(args: argparse.Namespace) -> RunManifest:
    full_checkpoint_phases = [
        item.strip() for item in getattr(args, "full_checkpoint_phases", "").split(",") if item.strip()
    ]
    return RunManifest(
        run_id=args.run_id,
        vault_path=args.vault,
        log_dir=args.log_dir,
        checkpoint_dir=args.checkpoint_dir,
        apple_contacts_vcf=args.apple_contacts_vcf,
        google_account=args.google_account,
        google_calendar_id=args.google_calendar_id,
        imessage_snapshot_dir=args.imessage_snapshot_dir,
        imessage_source_label=args.imessage_source_label,
        photos_library_path=args.photos_library_path,
        photos_source_label=args.photos_source_label,
        one_medical_fhir_json=args.one_medical_fhir_json,
        one_medical_ccd_xml=args.one_medical_ccd_xml,
        vaccine_pdf=args.vaccine_pdf,
        apple_health_export_xml=args.apple_health_export_xml,
        person_wikilink=args.person_wikilink,
        archive_index_dsn=args.archive_index_dsn,
        embedding_provider=args.embedding_provider,
        embedding_model=args.embedding_model,
        embedding_version=args.embedding_version,
        python_bin=args.python_bin,
        full_checkpoint_phases=full_checkpoint_phases,
    )


def _print_status(manifest: RunManifest) -> int:
    state_path = Path(manifest.log_dir).expanduser().resolve() / "run-state.json"
    state = _read_json(state_path, {})
    if not state:
        print(f"No run state found at {state_path}")
        return 1
    print(f"run_id: {state.get('run_id', '')}")
    print(f"status: {state.get('status', '')}")
    print(f"current_phase: {state.get('current_phase', '')}")
    for phase_id in PHASE_ORDER:
        phase = state.get("phases", {}).get(phase_id, {})
        print(f"- {phase_id}: {phase.get('status', 'unknown')} attempts={phase.get('attempts', 0)}")
    vault = Path(manifest.vault_path).expanduser().resolve()
    if vault.exists():
        for key, value in _vault_metrics(vault).items():
            print(f"vault_{key}: {value}")
        state_payload = load_sync_state(vault)
        for key in sorted(state_payload):
            cursor = state_payload[key]
            if not isinstance(cursor, dict):
                continue
            print(f"[{key}]")
            for field in sorted(cursor):
                if field in {
                    "last_completed_message_rowid",
                    "snapshot_max_message_rowid",
                    "scanned_assets",
                    "emitted_assets",
                    "page_token",
                    "page_index",
                    "scanned_threads",
                    "emitted_threads",
                    "emitted_messages",
                    "emitted_attachments",
                    "emitted_events",
                    "skipped_unchanged_assets",
                    "skipped_unchanged_threads",
                    "skipped_unchanged_messages",
                    "skipped_unchanged_attachments",
                    "skipped_unchanged_events",
                    "processed",
                    "created",
                    "merged",
                    "errors",
                }:
                    print(f"  {field}: {cursor[field]}")
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    manifest = _manifest_from_args(args)
    if args.command == "status":
        return _print_status(manifest)
    config = RunConfig(
        manifest=manifest,
        retry_limit=args.retry_limit,
        min_free_gb=args.min_free_gb,
        allow_nonempty_vault=bool(args.allow_nonempty_vault),
        imessage_batch_size=args.imessage_batch_size,
        imessage_workers=args.imessage_workers,
        gmail_max_threads=args.gmail_max_threads,
        gmail_max_messages=args.gmail_max_messages,
        gmail_max_attachments=args.gmail_max_attachments,
        gmail_page_size=args.gmail_page_size,
        gmail_workers=args.gmail_workers,
        calendar_max_events=args.calendar_max_events,
        correspondents_max_messages=args.correspondents_max_messages,
        photos_include_private_people=not args.no_photos_private_people,
        photos_include_private_labels=not args.no_photos_private_labels,
    )
    runbook = SeedRunbook(config)
    try:
        runbook.run()
    except PhaseFailure as exc:
        print(f"Runbook halted: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
