"""Email corpus hygiene apply orchestration."""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import GATE_LOCAL_SEED_STAGING_APPLY, GATE_RUN_STATUS_PASSED
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.report import GateRunReport, gate_artifact_dir, write_gate_report
from archive_sync.gmail_promotion.ledger import FilePromotionLedger, default_ledger_path
from archive_sync.llm_enrichment.email_promotion_policy import EMAIL_PROMOTION_POLICY_VERSION

from .constants import SECTION_B_APPLY_ARTIFACT_GATE, SECTION_B_APPLY_COMPLETION_STATE
from .decision_io import load_decision_records_jsonl, validate_decision_records
from .decisions import EmailCorpusDecisionRecord
from .report import render_apply_summary
from .state_store import (
    CORPUS_STATE_SUPPRESSED,
    ApplyCounts,
    all_card_uids_for_records,
    apply_decision_records,
    purge_card_uids,
    rel_paths_for_card_uids,
    removal_uids_for_records,
)

logger = logging.getLogger("ppa.corpus_hygiene")

ROLLBACK_KIT_LIMIT = 20
DEFAULT_DELETE_PROGRESS_EVERY = 100
HYGIENE_ROLLBACK_KIT_ROOT = "_artifacts/hygiene-rollback-kit"
ROLLBACK_JSON_FLUSH_EVERY = 4096


@dataclass
class ApplyResult:
    decision_run_id: str
    archive_instance: str
    vault_path: str
    index_schema: str
    engine_mode: str
    counts: ApplyCounts
    records: list[EmailCorpusDecisionRecord] = field(default_factory=list)
    total_elapsed_ms: int = 0
    artifact_paths: dict[str, str] = field(default_factory=dict)
    rollback_path: str = ""
    vault_markdown_deleted: bool = False
    rollback_kit_path: str = ""
    ccs_only: bool = False


def _format_mins_secs(seconds: float) -> str:
    total = int(round(max(seconds, 0.0)))
    minutes, secs = divmod(total, 60)
    return f"{minutes}:{secs:02d}"


def rollback_kit_dir(vault_path: str | Path, decision_run_id: str) -> Path:
    return Path(vault_path) / HYGIENE_ROLLBACK_KIT_ROOT / decision_run_id


def apply_rollback_json_path(repo_root: str | Path, decision_run_id: str) -> Path:
    return (
        gate_artifact_dir(
            repo_root,
            gate=SECTION_B_APPLY_ARTIFACT_GATE,
            run_id=f"{decision_run_id}-apply",
        )
        / "rollback.json"
    )


def _write_json_string_array(fh: Any, items: list[str]) -> None:
    """Stream a JSON string array (indent=2) and flush periodically."""

    fh.write("[\n")
    n = len(items)
    for i, item in enumerate(items):
        suffix = "," if i + 1 < n else ""
        fh.write(f"    {json.dumps(item)}{suffix}\n")
        if (i + 1) % ROLLBACK_JSON_FLUSH_EVERY == 0:
            fh.flush()
    fh.write("  ]")


def write_rollback_json(
    path: str | Path,
    *,
    decision_run_id: str,
    archive_instance: str,
    card_uids: Iterable[str],
    removed_card_uids: Iterable[str],
    vault_markdown_deleted: bool,
    rollback_kit_path: str = "",
    ccs_only: bool = False,
) -> Path:
    """Persist rollback.json via a streamed, atomically-replaced write.

    UID arrays are collected with sets by the caller. This writer never
    ``json.dumps`` the whole payload, so a large-N apply cannot die while
    materializing one giant string. A completed file is renamed into place;
    a kill mid-write leaves the previous complete ``rollback.json`` if any.
    """

    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    card_list = sorted({uid for uid in card_uids if uid})
    removed_list = [uid for uid in dict.fromkeys(removed_card_uids) if uid]
    logger.info(
        "hygiene rollback.json start card_uids=%d removed_card_uids=%d path=%s",
        len(card_list),
        len(removed_list),
        dest,
    )
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write("{\n")
        fh.write(f'  "archive_instance": {json.dumps(archive_instance)},\n')
        fh.write('  "card_uids": ')
        _write_json_string_array(fh, card_list)
        fh.write(",\n")
        fh.flush()
        fh.write(f'  "ccs_only": {json.dumps(ccs_only)},\n')
        fh.write(f'  "decision_run_id": {json.dumps(decision_run_id)},\n')
        fh.write('  "removed_card_uids": ')
        _write_json_string_array(fh, removed_list)
        fh.write(",\n")
        fh.flush()
        fh.write(f'  "rollback_kit_path": {json.dumps(rollback_kit_path)},\n')
        fh.write(f'  "vault_markdown_deleted": {json.dumps(vault_markdown_deleted)}\n')
        fh.write("}\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, dest)
    logger.info("hygiene rollback.json wrote path=%s", dest)
    return dest


def safe_vault_file(vault_path: Path, rel_path: str) -> Path | None:
    """Resolve *rel_path* under *vault_path*; reject path traversal."""

    rel = str(rel_path or "").strip()
    if not rel:
        return None
    vault_root = Path(vault_path).resolve()
    candidate = (vault_root / rel).resolve()
    try:
        candidate.relative_to(vault_root)
    except ValueError:
        logger.warning("hygiene vault-remove skipped path-escape rel_path=%s", rel)
        return None
    return candidate


def copy_rollback_kit(
    vault_path: Path,
    rel_paths: list[str],
    *,
    decision_run_id: str,
    limit: int = ROLLBACK_KIT_LIMIT,
) -> tuple[Path, list[str]]:
    """Copy ≤*limit* notes aside before delete. Restore uses the kit manifest only."""

    kit = rollback_kit_dir(vault_path, decision_run_id)
    kit.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for rel in rel_paths:
        if len(copied) >= limit:
            break
        src = safe_vault_file(vault_path, rel)
        if src is None or not src.is_file():
            continue
        dest = kit / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied.append(rel)
    manifest = {"decision_run_id": decision_run_id, "rel_paths": copied}
    (kit / "kit_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return kit, copied


def restore_rollback_kit(vault_path: Path, decision_run_id: str) -> int:
    """Restore only the small-N kit (copy files back). Does not restore the full pile."""

    kit = rollback_kit_dir(vault_path, decision_run_id)
    manifest_path = kit / "kit_manifest.json"
    if not manifest_path.is_file():
        return 0
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    restored = 0
    for rel in payload.get("rel_paths") or []:
        src = kit / str(rel)
        dest = safe_vault_file(vault_path, str(rel))
        if dest is None or not src.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        restored += 1
    logger.info(
        "hygiene rollback-kit restored files=%d decision_run_id=%s",
        restored,
        decision_run_id,
    )
    return restored


def delete_vault_markdown(
    vault_path: Path,
    rel_paths: list[str],
    *,
    progress_every: int = DEFAULT_DELETE_PROGRESS_EVERY,
) -> int:
    """Delete indexed markdown notes. Logs i/n, pct, elapsed — no silent loop."""

    targets: list[tuple[str, Path]] = []
    seen: set[str] = set()
    for rel in rel_paths:
        if rel in seen:
            continue
        seen.add(rel)
        path = safe_vault_file(vault_path, rel)
        if path is not None and path.is_file():
            targets.append((rel, path))
    n = len(targets)
    logger.info("hygiene vault-remove start files=%d", n)
    if n == 0:
        return 0
    t0 = time.perf_counter()
    deleted = 0
    for i, (rel, path) in enumerate(targets, start=1):
        try:
            path.unlink()
            deleted += 1
        except OSError as exc:
            logger.warning("hygiene vault-remove failed rel_path=%s error=%s", rel, exc)
        should_log = progress_every > 0 and (i % progress_every == 0 or i == n)
        if should_log:
            elapsed = time.perf_counter() - t0
            rate = i / elapsed if elapsed > 0 else 0.0
            remaining = (n - i) / rate if rate > 0 else 0.0
            pct = 100.0 * i / n
            logger.info(
                "hygiene vault-remove files=%d/%d (%.1f%%) elapsed=%s eta_remaining=%s rate_files_per_s=%.1f",
                i,
                n,
                pct,
                _format_mins_secs(elapsed),
                _format_mins_secs(remaining),
                rate,
            )
    return deleted


def append_promotion_ledger(vault_path: Path, records: list[EmailCorpusDecisionRecord]) -> int:
    """Append suppressed decisions so Gmail continue will not re-emit junk.

    Quarantine is not ledgered: those notes stay in the vault, and a later
    inbound classification change can still write or update them.
    """

    matching = [
        rec for rec in records if rec.corpus_decision == CORPUS_STATE_SUPPRESSED and rec.gmail_thread_id.strip()
    ]
    if not matching:
        return 0
    ledger = FilePromotionLedger(default_ledger_path(vault_path))
    for rec in matching:
        ledger.persist(rec)
    logger.info("hygiene promotion ledger appended=%d path=%s", len(matching), default_ledger_path(vault_path))
    return len(matching)


def apply_vault_remove(
    conn: Any,
    schema: str,
    records: list[EmailCorpusDecisionRecord],
    *,
    vault_path: str,
    decision_run_id: str,
    progress_every: int = DEFAULT_DELETE_PROGRESS_EVERY,
    kit_limit: int = ROLLBACK_KIT_LIMIT,
    remove_uids: list[str] | None = None,
) -> ApplyCounts:
    """Delete suppressed notes, purge those UIDs, append the promotion ledger.

    Quarantine stays on disk and in the index so search can still surface it.
    """

    counts = ApplyCounts()
    if remove_uids is None:
        remove_uids = removal_uids_for_records(records)
    vault = Path(vault_path) if vault_path else None
    rel_by_uid: dict[str, str] = {}
    if conn is not None and schema and remove_uids:
        rel_by_uid = rel_paths_for_card_uids(conn, schema, remove_uids)
    rel_paths = [rel_by_uid[uid] for uid in remove_uids if uid in rel_by_uid]

    if vault is not None:
        kit, kit_files = copy_rollback_kit(
            vault,
            rel_paths,
            decision_run_id=decision_run_id,
            limit=kit_limit,
        )
        counts.rollback_kit_files = len(kit_files)
        counts.files_deleted = delete_vault_markdown(vault, rel_paths, progress_every=progress_every)
        counts.ledger_records_appended = append_promotion_ledger(vault, records)

    if conn is not None and schema and remove_uids:
        counts.uids_purged = purge_card_uids(conn, schema, remove_uids)
        logger.info("hygiene index purge uids=%d", counts.uids_purged)
    return counts


def run_email_corpus_apply(
    conn: Any,
    schema: str,
    records: list[EmailCorpusDecisionRecord],
    *,
    decision_run_id: str,
    archive_instance: str,
    vault_path: str,
    engine_mode: str | None = None,
    repo_root: Path | None = None,
    registry: GateRegistry | None = None,
    ccs_only: bool = False,
    progress_every: int = DEFAULT_DELETE_PROGRESS_EVERY,
) -> ApplyResult:
    validate_decision_records(records, decision_run_id=decision_run_id)
    t0 = time.perf_counter()
    counts = apply_decision_records(conn, schema, records, decision_run_id=decision_run_id)
    card_uids = all_card_uids_for_records(records)
    removed_card_uids = removal_uids_for_records(records)
    vault_markdown_deleted = False
    rollback_kit_path = ""
    rollback_path = ""
    if repo_root is not None:
        rollback_dest = apply_rollback_json_path(repo_root, decision_run_id)
        write_rollback_json(
            rollback_dest,
            decision_run_id=decision_run_id,
            archive_instance=archive_instance,
            card_uids=card_uids,
            removed_card_uids=removed_card_uids,
            vault_markdown_deleted=False,
            rollback_kit_path="",
            ccs_only=ccs_only,
        )
        rollback_path = str(rollback_dest)
    if not ccs_only and vault_path:
        vr = apply_vault_remove(
            conn,
            schema,
            records,
            vault_path=vault_path,
            decision_run_id=decision_run_id,
            progress_every=progress_every,
            remove_uids=removed_card_uids,
        )
        counts.files_deleted = vr.files_deleted
        counts.uids_purged = vr.uids_purged
        counts.ledger_records_appended = vr.ledger_records_appended
        counts.rollback_kit_files = vr.rollback_kit_files
        vault_markdown_deleted = vr.files_deleted > 0
        if vr.rollback_kit_files:
            rollback_kit_path = str(rollback_kit_dir(vault_path, decision_run_id))
    elapsed = int((time.perf_counter() - t0) * 1000)
    result = ApplyResult(
        decision_run_id=decision_run_id,
        archive_instance=archive_instance,
        vault_path=vault_path,
        index_schema=schema,
        engine_mode=engine_mode or ppa_engine(),
        counts=counts,
        records=records,
        total_elapsed_ms=elapsed,
        vault_markdown_deleted=vault_markdown_deleted,
        rollback_kit_path=rollback_kit_path,
        ccs_only=ccs_only,
        rollback_path=rollback_path,
    )
    if repo_root is not None:
        result.artifact_paths = write_apply_artifacts(repo_root, result)
        rollback_dest = Path(result.artifact_paths["report"]).parent / "rollback.json"
        write_rollback_json(
            rollback_dest,
            decision_run_id=decision_run_id,
            archive_instance=archive_instance,
            card_uids=card_uids,
            removed_card_uids=removed_card_uids,
            vault_markdown_deleted=vault_markdown_deleted,
            rollback_kit_path=rollback_kit_path,
            ccs_only=ccs_only,
        )
        result.rollback_path = str(rollback_dest)
        result.artifact_paths["rollback"] = str(rollback_dest)
        if rollback_kit_path:
            result.artifact_paths["rollback_kit"] = rollback_kit_path

    if registry is not None and result.artifact_paths:
        apply_run = registry.create_run(
            gate=GATE_LOCAL_SEED_STAGING_APPLY,
            archive_instance=archive_instance,
            vault_path=vault_path,
            index_schema=schema,
            engine_mode=result.engine_mode,
            policy_version=EMAIL_PROMOTION_POLICY_VERSION,
            input_hash=decision_run_id,
        )
        registry.complete_run(
            apply_run.run_id,
            status=GATE_RUN_STATUS_PASSED,
            report_path=result.artifact_paths.get("report", ""),
            summary_path=result.artifact_paths.get("summary", ""),
            applied=True,
        )
    return result


def write_apply_artifacts(repo_root: Path, result: ApplyResult) -> dict[str, str]:
    run_id = f"{result.decision_run_id}-apply"
    report = GateRunReport(
        run_id=run_id,
        gate=SECTION_B_APPLY_ARTIFACT_GATE,
        ladder_gate="Local seed staging apply",
        archive_instance=result.archive_instance,
        vault_path=result.vault_path,
        index_schema=result.index_schema,
        engine_mode=result.engine_mode,
        policy_version=EMAIL_PROMOTION_POLICY_VERSION,
        decision_run_id=result.decision_run_id,
        overall_status="passed",
        total_elapsed_ms=result.total_elapsed_ms,
        corpus_counts=result.counts.by_corpus_state or {},
        next_recommended_gate="production_dry_run",
        completion_state=SECTION_B_APPLY_COMPLETION_STATE,
    )
    report.details = {
        "threads_applied": result.counts.threads_applied,
        "cards_updated": result.counts.cards_updated,
        "files_deleted": result.counts.files_deleted,
        "uids_purged": result.counts.uids_purged,
        "ledger_records_appended": result.counts.ledger_records_appended,
        "rollback_kit_files": result.counts.rollback_kit_files,
        "ccs_only": result.ccs_only,
        "safety": {
            "production_mutation": False,
            "vault_markdown_deleted": bool(result.vault_markdown_deleted),
            "rollback_available": True,
        },
    }
    paths = write_gate_report(repo_root, report)
    summary_path = Path(paths["summary"])
    summary_path.write_text(render_apply_summary(result, report), encoding="utf-8")
    paths["summary"] = str(summary_path)
    return paths


def apply_from_decisions_path(
    conn: Any,
    schema: str,
    decisions_path: Path,
    **kwargs: Any,
) -> ApplyResult:
    records = load_decision_records_jsonl(decisions_path)
    decision_run_id = kwargs.pop("decision_run_id", records[0].decision_run_id if records else "")
    return run_email_corpus_apply(conn, schema, records, decision_run_id=decision_run_id, **kwargs)
