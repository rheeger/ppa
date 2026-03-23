"""Runbook supervisor tests."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "scripts" / "ppa-seed-runbook.py"
    spec = importlib.util.spec_from_file_location("ppa_seed_runbook", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _manifest(module, tmp_path: Path) -> object:
    log_dir = tmp_path / "logs"
    checkpoint_dir = tmp_path / "checkpoints"
    vault = tmp_path / "hf-archives"
    (vault / "_meta").mkdir(parents=True)
    (vault / "People").mkdir()
    apple_vcf = tmp_path / "apple.vcf"
    apple_vcf.write_text("BEGIN:VCARD\nEND:VCARD\n", encoding="utf-8")
    photos_library = tmp_path / "Photos Library.photoslibrary"
    photos_library.mkdir()
    one_medical_fhir = tmp_path / "onemedical.json"
    one_medical_fhir.write_text('{"entry":[]}', encoding="utf-8")
    one_medical_ccd = tmp_path / "onemedical.xml"
    one_medical_ccd.write_text("<ClinicalDocument/>", encoding="utf-8")
    vaccine_pdf = tmp_path / "vaccines.pdf"
    vaccine_pdf.write_bytes(b"%PDF-1.4\n% test pdf")
    apple_health_xml = tmp_path / "apple-health.xml"
    apple_health_xml.write_text("<HealthData/>", encoding="utf-8")
    return module.RunManifest(
        run_id="test-run",
        vault_path=str(vault),
        log_dir=str(log_dir),
        checkpoint_dir=str(checkpoint_dir),
        apple_contacts_vcf=str(apple_vcf),
        google_account="rheeger@gmail.com",
        google_calendar_id="primary",
        imessage_snapshot_dir=str(tmp_path / "snapshot"),
        imessage_source_label="local-messages",
        photos_library_path=str(photos_library),
        photos_source_label="apple-photos",
        one_medical_fhir_json=str(one_medical_fhir),
        one_medical_ccd_xml=str(one_medical_ccd),
        vaccine_pdf=str(vaccine_pdf),
        apple_health_export_xml=str(apple_health_xml),
        person_wikilink="[[robert-heeger]]",
        archive_index_dsn="postgresql://archive:archive@127.0.0.1:5432/archive",
        embedding_provider="hash",
        embedding_model="archive-hash-dev",
        embedding_version=1,
        python_bin=sys.executable,
        full_checkpoint_phases=["init-canonical"],
    )


def _config(module, manifest: object) -> object:
    return module.RunConfig(
        manifest=manifest,
        retry_limit=1,
        min_free_gb=0.0,
        allow_nonempty_vault=True,
        imessage_batch_size=100,
        imessage_workers=1,
        gmail_max_threads=10,
        gmail_max_messages=10,
        gmail_max_attachments=10,
        gmail_page_size=10,
        gmail_workers=1,
        calendar_max_events=10,
        correspondents_max_messages=10,
        photos_include_private_people=True,
        photos_include_private_labels=True,
    )


def test_completion_helpers_detect_cursor_exhaustion():
    module = _load_module()
    assert module._gmail_complete({"page_token": None, "page_thread_ids": [], "page_index": 0}) is True
    assert module._gmail_complete({"page_token": "tok", "page_thread_ids": [], "page_index": 0}) is False
    assert module._calendar_complete({"page_token": None}) is True
    assert module._calendar_complete({"page_token": "tok"}) is False
    assert module._correspondents_complete({"page_token": None}) is True
    assert module._imessage_complete({"last_completed_message_rowid": 100, "snapshot_max_message_rowid": 100}) is True
    assert module._imessage_complete({"last_completed_message_rowid": 99, "snapshot_max_message_rowid": 100}) is False
    assert module._progress_changed({"a": 1, "b": 2}, {"a": 1, "b": 3}, ["a", "b"]) is True
    assert module._progress_changed({"a": 1}, {"a": 1}, ["a"]) is False


def test_phase_order_includes_medical_imports_before_imessage():
    module = _load_module()
    assert module.PHASE_ORDER.index("verify-google-contacts") < module.PHASE_ORDER.index("seed-medical-records")
    assert module.PHASE_ORDER.index("seed-medical-records") < module.PHASE_ORDER.index("seed-apple-health")
    assert module.PHASE_ORDER.index("seed-apple-health") < module.PHASE_ORDER.index("seed-imessage")


def test_checkpoint_writes_meta_metrics_and_archive(tmp_path):
    module = _load_module()
    manifest = _manifest(module, tmp_path)
    config = _config(module, manifest)
    vault = Path(manifest.vault_path)
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    runbook = module.SeedRunbook(config)
    checkpoint = runbook._checkpoint("init-canonical", metrics={"ok": True}, full=True)
    assert (checkpoint / "_meta" / "sync-state.json").exists()
    assert (checkpoint / "_meta" / "identity-map.json").exists()
    assert (checkpoint / "metrics.json").exists()
    assert (checkpoint / "vault.tar").exists()


def test_print_status_renders_phase_and_sync_state(tmp_path, capsys):
    module = _load_module()
    manifest = _manifest(module, tmp_path)
    log_dir = Path(manifest.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "run_id": manifest.run_id,
        "status": "running",
        "current_phase": "seed-imessage",
        "phases": {
            phase_id: {"status": "completed" if phase_id == "preflight-manifest" else "pending", "attempts": 1}
            for phase_id in module.PHASE_ORDER
        },
    }
    state["phases"]["seed-imessage"] = {"status": "in_progress", "attempts": 2}
    (log_dir / "run-state.json").write_text(json.dumps(state), encoding="utf-8")
    vault = Path(manifest.vault_path)
    (vault / "_meta" / "sync-state.json").write_text(
        json.dumps({"imessage:local-messages": {"last_completed_message_rowid": 42, "snapshot_max_message_rowid": 100}}),
        encoding="utf-8",
    )
    exit_code = module._print_status(manifest)
    output = capsys.readouterr().out
    assert exit_code == 0
    assert "run_id: test-run" in output
    assert "current_phase: seed-imessage" in output
    assert "[imessage:local-messages]" in output
    assert "last_completed_message_rowid: 42" in output


def test_runbook_run_completes_all_phases_with_stubbed_phase_methods(tmp_path, monkeypatch):
    module = _load_module()
    manifest = _manifest(module, tmp_path)
    config = _config(module, manifest)
    runbook = module.SeedRunbook(config)

    for phase_id in module.PHASE_ORDER:
        method_name = f"_phase_{phase_id.replace('-', '_')}"
        monkeypatch.setattr(runbook, method_name, lambda phase_id=phase_id: {"phase": phase_id})

    runbook.run()

    assert runbook.state["status"] == "completed"
    assert runbook.state["completed_at"]
    for phase_id in module.PHASE_ORDER:
        assert runbook.state["phases"][phase_id]["status"] == "completed"
        assert runbook.state["phases"][phase_id]["metrics"]["phase"] == phase_id


def test_runbook_retries_retryable_phase_failure_then_completes(tmp_path, monkeypatch):
    module = _load_module()
    manifest = _manifest(module, tmp_path)
    config = _config(module, manifest)
    runbook = module.SeedRunbook(config)
    attempts = {"prep-readonly": 0}

    for phase_id in module.PHASE_ORDER:
        method_name = f"_phase_{phase_id.replace('-', '_')}"
        if phase_id == "prep-readonly":
            def flaky_phase(phase_id=phase_id):
                attempts[phase_id] += 1
                if attempts[phase_id] == 1:
                    raise module.PhaseFailure("transient", retryable=True)
                return {"phase": phase_id, "attempt": attempts[phase_id]}

            monkeypatch.setattr(runbook, method_name, flaky_phase)
        else:
            monkeypatch.setattr(runbook, method_name, lambda phase_id=phase_id: {"phase": phase_id})

    runbook.run()

    assert runbook.state["status"] == "completed"
    assert runbook.state["phases"]["prep-readonly"]["attempts"] == 2
    assert runbook.state["phases"]["prep-readonly"]["metrics"]["attempt"] == 2


def test_runbook_marks_state_failed_on_non_retryable_phase_failure(tmp_path, monkeypatch):
    module = _load_module()
    manifest = _manifest(module, tmp_path)
    config = _config(module, manifest)
    runbook = module.SeedRunbook(config)

    for phase_id in module.PHASE_ORDER:
        method_name = f"_phase_{phase_id.replace('-', '_')}"
        if phase_id == "seed-google-contacts":
            monkeypatch.setattr(
                runbook,
                method_name,
                lambda: (_ for _ in ()).throw(module.PhaseFailure("hard stop", retryable=False)),
            )
        else:
            monkeypatch.setattr(runbook, method_name, lambda phase_id=phase_id: {"phase": phase_id})

    with pytest.raises(module.PhaseFailure):
        runbook.run()

    assert runbook.state["status"] == "failed"
    assert runbook.state["current_phase"] == "seed-google-contacts"
    assert runbook.state["phases"]["seed-google-contacts"]["attempts"] == 1
