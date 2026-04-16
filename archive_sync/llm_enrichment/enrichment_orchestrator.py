"""Sequential full-vault enrichment orchestration (``ppa enrich``)."""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.cache import InferenceCache
from archive_sync.llm_enrichment.card_enrichment_runner import \
    CardEnrichmentRunner
from archive_sync.llm_enrichment.defaults import \
    DEFAULT_ENRICH_CARD_GEMINI_MODEL
from archive_sync.llm_enrichment.document_text_extractor import \
    run_document_text_extraction
from archive_sync.llm_enrichment.enrich_runner import LlmEnrichmentRunner

log = logging.getLogger("ppa.enrichment_orchestrator")

STEP_ORDER: tuple[str, ...] = (
    "extract_document_text",
    "enrich_emails",
    "enrich_email_thread",
    "enrich_imessage_thread",
    "enrich_finance",
    "enrich_document",
)

CARD_WORKFLOW_BY_STEP: dict[str, str] = {
    "enrich_email_thread": "email_thread",
    "enrich_imessage_thread": "imessage_thread",
    "enrich_finance": "finance",
    "enrich_document": "document",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_run_id() -> str:
    return f"enrich-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"


@dataclass
class ManifestStep:
    key: str
    status: str = "pending"
    started_at: str | None = None
    completed_at: str | None = None
    elapsed_s: float | None = None
    metrics_summary: dict[str, Any] | None = None
    log_file: str | None = None
    error: str | None = None


@dataclass
class EnrichmentManifest:
    run_id: str
    vault_path: str
    created_at: str
    updated_at: str
    provider: str
    model: str
    enrich_emails_model: str
    dry_run: bool
    workers: int
    enrich_emails_workers: int
    checkpoint_every: int
    steps: list[ManifestStep] = field(default_factory=list)
    cost_summary: dict[str, Any] | None = None

    def to_jsonable(self) -> dict[str, Any]:
        d = asdict(self)
        d["steps"] = [asdict(s) for s in self.steps]
        return d

    def save(self, path: Path) -> None:
        self.updated_at = _utc_now_iso()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_jsonable(), indent=2), encoding="utf-8")

    @staticmethod
    def load(path: Path) -> EnrichmentManifest:
        raw = json.loads(path.read_text(encoding="utf-8"))
        steps = [
            ManifestStep(
                key=str(s["key"]),
                status=str(s.get("status") or "pending"),
                started_at=s.get("started_at"),
                completed_at=s.get("completed_at"),
                elapsed_s=s.get("elapsed_s"),
                metrics_summary=s.get("metrics_summary"),
                log_file=s.get("log_file"),
                error=s.get("error"),
            )
            for s in raw.get("steps") or []
        ]
        return EnrichmentManifest(
            run_id=str(raw["run_id"]),
            vault_path=str(raw["vault_path"]),
            created_at=str(raw.get("created_at") or _utc_now_iso()),
            updated_at=str(raw.get("updated_at") or _utc_now_iso()),
            provider=str(raw.get("provider") or "gemini"),
            model=str(raw.get("model") or DEFAULT_ENRICH_CARD_GEMINI_MODEL),
            enrich_emails_model=str(raw.get("enrich_emails_model") or "gemini-2.5-flash"),
            dry_run=bool(raw.get("dry_run")),
            workers=int(raw.get("workers") or 24),
            enrich_emails_workers=int(raw.get("enrich_emails_workers") or 8),
            checkpoint_every=int(raw.get("checkpoint_every") if raw.get("checkpoint_every") is not None else 500),
            steps=steps,
            cost_summary=raw.get("cost_summary"),
        )


def _fresh_steps() -> list[ManifestStep]:
    return [ManifestStep(key=k, status="pending") for k in STEP_ORDER]


def _ensure_all_steps(manifest: EnrichmentManifest) -> None:
    """Merge manifest steps with ``STEP_ORDER`` (fill missing keys, stable order)."""

    by_key = {s.key: s for s in manifest.steps}
    manifest.steps = [by_key.get(k) or ManifestStep(key=k, status="pending") for k in STEP_ORDER]


class EnrichmentOrchestrator:
    def __init__(
        self,
        *,
        vault_path: Path,
        run_id: str,
        run_dir: Path,
        provider: str,
        model: str,
        enrich_emails_model: str,
        base_url: str,
        dry_run: bool,
        workers: int,
        enrich_emails_workers: int,
        checkpoint_every: int,
        cache_db: Path,
        enabled_steps: frozenset[str] | None,
        skip_populated: bool = False,
    ) -> None:
        self.vault_path = Path(vault_path).resolve()
        self.run_id = run_id.strip() or default_run_id()
        self.run_dir = Path(run_dir).resolve()
        self.provider = (provider or "gemini").strip().lower()
        self.model = (model or DEFAULT_ENRICH_CARD_GEMINI_MODEL).strip()
        self.enrich_emails_model = (enrich_emails_model or "gemini-2.5-flash-lite").strip()
        self.base_url = (base_url or "http://localhost:11434").rstrip("/")
        self.dry_run = dry_run
        self.workers = max(1, int(workers))
        self.enrich_emails_workers = max(1, int(enrich_emails_workers))
        self.checkpoint_every = max(0, int(checkpoint_every))
        self.cache_db = Path(cache_db)
        self.enabled_steps = enabled_steps
        self.skip_populated = bool(skip_populated)
        self.manifest_path = self.run_dir / "manifest.json"

    def _load_or_create_manifest(self) -> EnrichmentManifest:
        if self.manifest_path.is_file():
            m = EnrichmentManifest.load(self.manifest_path)
            if m.run_id != self.run_id:
                raise ValueError(
                    f"manifest run_id mismatch: file has {m.run_id!r}, expected {self.run_id!r}"
                )
            vp = Path(m.vault_path).resolve()
            if vp != self.vault_path:
                raise ValueError(
                    f"manifest vault_path mismatch: file has {vp}, this run uses {self.vault_path}"
                )
            for s in m.steps:
                if s.status == "running":
                    s.status = "pending"
                    s.started_at = None
                    s.error = None
            return m

        now = _utc_now_iso()
        return EnrichmentManifest(
            run_id=self.run_id,
            vault_path=str(self.vault_path),
            created_at=now,
            updated_at=now,
            provider=self.provider,
            model=self.model,
            enrich_emails_model=self.enrich_emails_model,
            dry_run=self.dry_run,
            workers=self.workers,
            enrich_emails_workers=self.enrich_emails_workers,
            checkpoint_every=self.checkpoint_every,
            steps=_fresh_steps(),
            cost_summary=None,
        )

    def _step_enabled(self, key: str) -> bool:
        if self.enabled_steps is None:
            return True
        return key in self.enabled_steps

    def _summarize_metrics(self, step_key: str, metrics: Any) -> dict[str, Any]:
        if hasattr(metrics, "to_dict"):
            return metrics.to_dict()
        if isinstance(metrics, dict):
            if step_key == "extract_document_text":
                return {
                    k: metrics[k]
                    for k in (
                        "vault",
                        "total_document_cards",
                        "processed",
                        "ok",
                        "skipped",
                        "errors",
                        "dry_run",
                    )
                    if k in metrics
                }
            return dict(metrics)
        return {"value": str(metrics)}

    def _execute_step(self, step_key: str) -> Any:
        classify_index_db = self.run_dir / "classify_index.db"
        if step_key == "extract_document_text":
            return run_document_text_extraction(self.vault_path, dry_run=self.dry_run, limit=None)

        if step_key == "enrich_emails":
            classify_model = "gemini-2.5-flash-lite" if self.provider == "gemini" else ""
            runner = LlmEnrichmentRunner(
                vault_path=self.vault_path,
                staging_dir=self.run_dir / "staging" / "enrich_emails",
                extract_model=self.enrich_emails_model,
                classify_model=classify_model,
                provider_kind=self.provider,
                base_url=self.base_url,
                cache_db=self.cache_db,
                run_id=self.run_id,
                progress_every=25,
                limit_threads=None,
                vault_percent=None,
                dry_run=self.dry_run,
                workers=self.enrich_emails_workers,
                no_gate=False,
                skip_classify=False,
                classify_index_db=classify_index_db,
            )
            return runner.run()

        wf = CARD_WORKFLOW_BY_STEP.get(step_key)
        if wf is None:
            raise ValueError(f"unknown step: {step_key!r}")

        cache_db: Path | None = self.cache_db if not self.dry_run else None
        runner = CardEnrichmentRunner(
            vault_path=self.vault_path,
            workflow=wf,
            provider_kind=self.provider,
            model=self.model,
            base_url=self.base_url,
            cache_db=cache_db,
            run_id=self.run_id,
            staging_dir=self.run_dir / "staging" / step_key,
            dry_run=self.dry_run,
            progress_every=1,
            vault_percent=None,
            limit=None,
            skip_populated=self.skip_populated,
            workers=self.workers,
            uid_filter_file=None,
            classify_index_db=classify_index_db if wf == "email_thread" else None,
            checkpoint_every=self.checkpoint_every,
        )
        return runner.run()

    def _refresh_vault_cache_fingerprint(self) -> None:
        """Recompute the vault fingerprint and stamp it into the existing cache.

        After each step writes vault cards, file mtimes change and the cached
        fingerprint no longer matches.  Rather than rebuilding the entire
        multi-GB scan cache (46 min on the full seed), we just update the
        stored fingerprint so the next step's ``build_or_load`` sees a hit.
        The cached frontmatter rows may be stale for fields we just wrote,
        but enrichment reads those fields from disk via ``read_note()``, not
        from the scan cache.
        """

        from archive_cli.vault_cache import VaultScanCache

        cache_path = VaultScanCache.cache_path_for_vault(self.vault_path)
        if not cache_path.exists():
            return

        import sqlite3

        from archive_cli.vault_cache import _compute_fingerprint_with_paths

        t0 = time.perf_counter()
        _, _, new_fp = _compute_fingerprint_with_paths(self.vault_path)

        conn = sqlite3.connect(str(cache_path), timeout=60.0)
        conn.execute("INSERT OR REPLACE INTO cache_meta (key, value) VALUES ('vault_fingerprint', ?)", (new_fp,))
        conn.commit()
        conn.close()
        log.info(
            "vault-cache fingerprint refreshed in %.1fs (skip full rebuild for next step)",
            time.perf_counter() - t0,
        )

    def run(self) -> EnrichmentManifest:
        manifest = self._load_or_create_manifest()
        if not manifest.steps:
            manifest.steps = _fresh_steps()
        _ensure_all_steps(manifest)

        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.cache_db.parent.mkdir(parents=True, exist_ok=True)

        log.info(
            "enrichment run_id=%s vault=%s run_dir=%s dry_run=%s skip_populated=%s",
            manifest.run_id,
            self.vault_path,
            self.run_dir,
            self.dry_run,
            self.skip_populated,
        )

        from archive_cli.log import attach_file_log

        for step in manifest.steps:
            if step.status == "completed":
                log.info("step %s already completed — skipping", step.key)
                continue

            if not self._step_enabled(step.key):
                step.status = "skipped"
                step.completed_at = _utc_now_iso()
                step.metrics_summary = {"reason": "not in --steps filter"}
                manifest.save(self.manifest_path)
                log.info("step %s skipped (filtered)", step.key)
                continue

            step.status = "running"
            step.started_at = _utc_now_iso()
            step.error = None
            manifest.save(self.manifest_path)

            log_path = self.run_dir / f"{step.key}.log"
            attach_file_log(log_path)
            step.log_file = f"{step.key}.log"
            log.info("— begin step %s —", step.key)

            t0 = time.perf_counter()
            try:
                metrics = self._execute_step(step.key)
            except Exception as exc:
                step.status = "failed"
                step.error = str(exc)
                step.completed_at = _utc_now_iso()
                step.elapsed_s = round(time.perf_counter() - t0, 3)
                log.exception("step %s failed", step.key)
                manifest.save(self.manifest_path)
                raise

            step.status = "completed"
            step.completed_at = _utc_now_iso()
            step.elapsed_s = round(time.perf_counter() - t0, 3)
            step.metrics_summary = self._summarize_metrics(step.key, metrics)
            step.error = None

            self._refresh_vault_cache_fingerprint()

            with InferenceCache(self.cache_db) as cache:
                manifest.cost_summary = cache.cost_summary(self.run_id)
            manifest.save(self.manifest_path)
            log.info("step %s completed in %.1fs", step.key, step.elapsed_s or 0)

        log.info("enrichment run %s finished", manifest.run_id)
        return manifest
