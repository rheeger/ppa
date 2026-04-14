"""Tests for enrichment manifest, orchestrator wiring, cache cost_summary, checkpoints."""

from __future__ import annotations

import json
from pathlib import Path

from archive_sync.llm_enrichment.cache import InferenceCache, _estimate_cost_usd
from archive_sync.llm_enrichment.card_enrichment_runner import CardEnrichmentRunner
from archive_sync.llm_enrichment.enrichment_orchestrator import (
    STEP_ORDER,
    EnrichmentManifest,
    EnrichmentOrchestrator,
    _fresh_steps,
)


def test_manifest_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "manifest.json"
    m = EnrichmentManifest(
        run_id="enrich-test-1",
        vault_path="/tmp/vault",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        provider="gemini",
        model="gemini-2.5-flash-lite",
        enrich_emails_model="gemini-2.5-flash",
        dry_run=True,
        workers=8,
        enrich_emails_workers=4,
        checkpoint_every=500,
        steps=_fresh_steps(),
        cost_summary=None,
    )
    m.steps[0].status = "completed"
    m.save(p)
    m2 = EnrichmentManifest.load(p)
    assert m2.run_id == "enrich-test-1"
    assert m2.steps[0].key == "extract_document_text"
    assert m2.steps[0].status == "completed"
    assert len(m2.steps) == len(STEP_ORDER)


def test_orchestrator_load_resets_stale_running(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    manifest_path = run_dir / "manifest.json"
    raw = {
        "run_id": "enrich-x",
        "vault_path": str(tmp_path / "vault"),
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "provider": "gemini",
        "model": "gemini-2.5-flash-lite",
        "enrich_emails_model": "gemini-2.5-flash",
        "dry_run": True,
        "workers": 24,
        "enrich_emails_workers": 8,
        "checkpoint_every": 500,
        "steps": [
            {"key": "extract_document_text", "status": "completed"},
            {"key": "enrich_emails", "status": "running", "started_at": "2026-01-01T01:00:00Z"},
        ],
        "cost_summary": None,
    }
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")
    (tmp_path / "vault").mkdir()

    orch = EnrichmentOrchestrator(
        vault_path=tmp_path / "vault",
        run_id="enrich-x",
        run_dir=run_dir,
        provider="gemini",
        model="gemini-2.5-flash-lite",
        enrich_emails_model="gemini-2.5-flash",
        base_url="http://localhost:11434",
        dry_run=True,
        workers=2,
        enrich_emails_workers=1,
        checkpoint_every=0,
        cache_db=run_dir / "cache.db",
        enabled_steps=frozenset({"enrich_emails"}),
    )
    m = orch._load_or_create_manifest()
    email_step = next(s for s in m.steps if s.key == "enrich_emails")
    assert email_step.status == "pending"


def test_inference_cache_cost_summary(tmp_path: Path) -> None:
    db = tmp_path / "c.db"
    cache = InferenceCache(db)
    cache.put(
        "k1",
        stage="classify",
        model_id="gemini-2.5-flash-lite",
        prompt_version="v1",
        content_hash="a" * 64,
        response={"x": 1},
        tokens=(1_000_000, 1_000_000),
        latency_ms=10.0,
        run_id="r1",
    )
    cache.put(
        "k2",
        stage="enrich_email_thread",
        model_id="gemini-2.5-flash-lite",
        prompt_version="v1",
        content_hash="b" * 64,
        response={"y": 2},
        tokens=(500_000, 0),
        latency_ms=5.0,
        run_id="r1",
    )
    s = cache.cost_summary("r1")
    assert s["total_prompt_tokens"] == 1_500_000
    assert s["total_completion_tokens"] == 1_000_000
    assert s["estimated_cost_usd"] > 0
    assert "classify" in s["by_stage"]
    assert "enrich_email_thread" in s["by_stage"]
    cache.close()


def test_estimate_cost_usd_local_zero() -> None:
    assert _estimate_cost_usd(1_000_000, 1_000_000, "gemma4:31b") == 0.0


def test_card_runner_checkpoint_writes(tmp_path: Path) -> None:
    """Checkpoint file is written with metrics snapshot (no full run)."""

    # Exercise _write_checkpoint directly
    runner = CardEnrichmentRunner(
        vault_path=tmp_path,
        workflow="email_thread",
        provider_kind="gemini",
        model="gemini-2.5-flash-lite",
        base_url="http://localhost:11434",
        cache_db=None,
        run_id="rid",
        staging_dir=tmp_path / "st",
        dry_run=True,
        progress_every=1,
        vault_percent=None,
        limit=None,
        skip_populated=True,
        workers=1,
        checkpoint_every=2,
    )
    runner.metrics.enriched = 4
    runner.metrics.llm_calls = 2
    t0 = __import__("time").perf_counter()
    runner._write_checkpoint(t0, 10)
    cp = (tmp_path / "st") / "_metrics_checkpoint.json"
    assert cp.is_file()
    data = json.loads(cp.read_text(encoding="utf-8"))
    assert data["run_id"] == "rid"
    assert data["processed_eligible"] == 10
    assert "checkpoint_at" in data
