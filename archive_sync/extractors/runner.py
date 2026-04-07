"""Batch extraction: scan vault, match extractors, extract in parallel, write staging/vault."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_sync.extractors.base import EmailExtractor
from archive_sync.extractors.field_metrics import compute_field_population
from archive_sync.extractors.field_validation import validate_provenance_round_trip
from archive_sync.extractors.preprocessing import clean_email_body
from archive_sync.extractors.registry import ExtractorRegistry
from hfa.card_contracts import get_card_type_spec
from hfa.provenance import strip_provenance
from hfa.schema import validate_card_strict
from hfa.vault import iter_parsed_notes, read_note, write_card

log = logging.getLogger("ppa.extractor.runner")


def _format_elapsed(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m:d}:{s:02d}"


def derive_output_rel_path(card_type: str, uid: str, sent_hint: str) -> str:
    """Vault-relative path under Phase-1 card family (YYYY-MM segment)."""
    from datetime import date

    spec = get_card_type_spec(card_type)
    if len(sent_hint) >= 7 and sent_hint[4] == "-":
        ym = sent_hint[:7]
    else:
        ym = date.today().strftime("%Y-%m")
    return f"{spec.rel_path_family}/{ym}/{uid}.md"


def _dedupe_preserve_order(items: list[EmailExtractor]) -> list[EmailExtractor]:
    seen: set[int] = set()
    out: list[EmailExtractor] = []
    for item in items:
        i = id(item)
        if i in seen:
            continue
        seen.add(i)
        out.append(item)
    return out


def _email_domain(from_email: str) -> str:
    from_email = (from_email or "").strip().lower()
    if "@" not in from_email:
        return ""
    return from_email.rsplit("@", 1)[-1]


def _uid_in_vault_percent_sample(uid: str, vault_percent: float) -> bool:
    """Deterministic inclusion: ~vault_percent% of UIDs (0–100), stable across runs."""
    if vault_percent <= 0:
        return True
    pct = min(100.0, max(0.0, float(vault_percent)))
    digest = hashlib.sha256(uid.encode("utf-8")).digest()
    h = int.from_bytes(digest[:4], "big")
    # million buckets for two decimal places
    threshold = int(round(pct * 10_000))
    return (h % 1_000_000) < threshold


def _finalize_metrics(metrics: ExtractionMetrics) -> None:
    """Populate yield_by_extractor, cards_per_second, rejected_emails from counters."""
    yld: dict[str, float] = {}
    rejected: dict[str, int] = {}
    for name, row in metrics.per_extractor.items():
        matched = max(0, int(row.get("matched", 0)))
        extracted = max(0, int(row.get("extracted", 0)))
        yld[name] = (extracted / matched) if matched else 0.0
        rj = int(row.get("rejected", 0))
        if rj:
            rejected[name] = rj
    metrics.yield_by_extractor = yld
    metrics.rejected_emails = rejected
    wall = max(metrics.wall_clock_seconds, 1e-9)
    metrics.cards_per_second = metrics.extracted_cards / wall


@dataclass
class _EmailWorkItem:
    rel_path: str
    from_email: str
    subject: str
    extractor: EmailExtractor


def _card_dump_for_idempotency(card: Any) -> dict[str, Any]:
    """Compare cards without extraction_confidence (optional metadata not always in older notes)."""
    d = card.model_dump(mode="python")
    d.pop("extraction_confidence", None)
    return d


def _note_content_matches(
    output_root: Path,
    rel_path: str,
    card: Any,
    body: str,
) -> bool:
    path = output_root / rel_path
    if not path.is_file():
        return False
    try:
        fm, old_body, _ = read_note(output_root, rel_path)
    except FileNotFoundError:
        return False
    if fm.get("uid") != card.uid:
        return False
    try:
        old_card = validate_card_strict(fm)
    except Exception:
        return False
    if _card_dump_for_idempotency(old_card) != _card_dump_for_idempotency(card):
        return False
    # Ignore provenance dates (today) when comparing idempotency.
    return strip_provenance(old_body).strip() == body.strip()


@dataclass
class ExtractionMetrics:
    """Per-extractor and aggregate metrics."""

    total_emails_scanned: int = 0
    matched_emails: int = 0
    extracted_cards: int = 0
    errors: int = 0
    skipped_existing: int = 0
    wall_clock_seconds: float = 0.0
    per_extractor: dict[str, dict[str, int]] = field(default_factory=dict)
    yield_by_extractor: dict[str, float] = field(default_factory=dict)
    cards_per_second: float = 0.0
    rejected_emails: dict[str, int] = field(default_factory=dict)
    field_population: dict[str, dict[str, float]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_emails_scanned": self.total_emails_scanned,
            "matched_emails": self.matched_emails,
            "extracted_cards": self.extracted_cards,
            "errors": self.errors,
            "skipped_existing": self.skipped_existing,
            "wall_clock_seconds": round(self.wall_clock_seconds, 3),
            "per_extractor": dict(self.per_extractor),
            "yield_by_extractor": dict(self.yield_by_extractor),
            "cards_per_second": round(self.cards_per_second, 4),
            "rejected_emails": dict(self.rejected_emails),
            "field_population": dict(self.field_population),
        }


class ExtractionRunner:
    def __init__(
        self,
        vault_path: str,
        registry: ExtractorRegistry,
        staging_dir: str | None = None,
        workers: int = 4,
        batch_size: int = 500,
        dry_run: bool = False,
        sender_filter: str | None = None,
        limit: int | None = None,
        progress_every: int = 5000,
        vault_percent: float | None = None,
    ) -> None:
        self.vault_path = str(vault_path)
        self.registry = registry
        self.staging_dir = staging_dir
        self.workers = max(1, workers)
        self.batch_size = max(1, batch_size)
        self.dry_run = dry_run
        self.sender_filter = (sender_filter or "").strip().lower() or None
        self.limit = limit
        self.progress_every = max(1, int(progress_every))
        self.vault_percent = float(vault_percent) if vault_percent is not None and vault_percent > 0 else None

    def _out_root(self) -> Path:
        return Path(self.staging_dir) if self.staging_dir else Path(self.vault_path)

    def _bump(self, lock: threading.Lock, metrics: ExtractionMetrics, extractor_id: str, key: str, n: int = 1) -> None:
        with lock:
            metrics.per_extractor.setdefault(
                extractor_id,
                {"matched": 0, "extracted": 0, "errors": 0, "skipped": 0, "rejected": 0},
            )
            metrics.per_extractor[extractor_id][key] = metrics.per_extractor[extractor_id].get(key, 0) + n

    def _process_item(self, item: _EmailWorkItem, lock: threading.Lock, metrics: ExtractionMetrics) -> None:
        extractor = item.extractor
        eid = extractor.extractor_id
        try:
            fm, body, _ = read_note(self.vault_path, item.rel_path)
        except Exception as exc:
            log.warning("read_note failed %s: %s", item.rel_path, exc)
            with lock:
                metrics.errors += 1
            self._bump(lock, metrics, eid, "errors", 1)
            return

        uid = str(fm.get("uid") or "")
        sent_hint = str(fm.get("sent_at") or fm.get("created") or "")[:10]
        raw_body = body
        body = clean_email_body(body)
        subject = str(fm.get("subject") or "")
        if not extractor.should_extract(subject, body):
            self._bump(lock, metrics, eid, "rejected", 1)
            return
        try:
            results = extractor.extract(fm, body, uid, item.rel_path, raw_body=raw_body)
        except Exception as exc:
            log.exception("extract failed %s: %s", item.rel_path, exc)
            with lock:
                metrics.errors += 1
            self._bump(lock, metrics, eid, "errors", 1)
            return

        out_root = self._out_root()
        for er in results:
            rt_warnings = validate_provenance_round_trip(
                card_data=er.card.model_dump(mode="python"),
                source_body=body,
                card_type=str(er.card.type),
            )
            if rt_warnings:
                for w in rt_warnings:
                    log.info("round-trip warning [%s]: %s", er.card.uid, w)
            rel_out = derive_output_rel_path(er.card.type, er.card.uid, sent_hint)
            if self.dry_run:
                with lock:
                    metrics.extracted_cards += 1
                self._bump(lock, metrics, eid, "extracted", 1)
                continue
            if _note_content_matches(out_root, rel_out, er.card, er.body):
                with lock:
                    metrics.skipped_existing += 1
                self._bump(lock, metrics, eid, "skipped", 1)
                continue
            # Staging runs: skip if an identical card already exists in the vault (post-promotion idempotency).
            if self.staging_dir and out_root != Path(self.vault_path):
                if _note_content_matches(Path(self.vault_path), rel_out, er.card, er.body):
                    with lock:
                        metrics.skipped_existing += 1
                    self._bump(lock, metrics, eid, "skipped", 1)
                    continue
            try:
                card_out = er.card.model_copy(update={"extraction_confidence": er.extraction_confidence})
                write_card(str(out_root), rel_out, card_out, er.body, er.provenance)
            except Exception as exc:
                log.warning("write_card failed %s: %s", rel_out, exc)
                with lock:
                    metrics.errors += 1
                self._bump(lock, metrics, eid, "errors", 1)
                continue
            with lock:
                metrics.extracted_cards += 1
            self._bump(lock, metrics, eid, "extracted", 1)

    def run(self) -> ExtractionMetrics:
        """Execute scan → match → extract (parallel) → write."""
        metrics = ExtractionMetrics()
        lock = threading.Lock()
        t0 = time.perf_counter()
        domain_idx = self.registry.domain_index()

        work_queue: list[_EmailWorkItem] = []
        scanned = 0
        for note in iter_parsed_notes(self.vault_path):
            fm = note.frontmatter
            if fm.get("type") != "email_message":
                continue
            scanned += 1
            uid = str(fm.get("uid") or "")
            if self.vault_percent is not None and uid and not _uid_in_vault_percent_sample(uid, self.vault_percent):
                if self.progress_every and scanned % self.progress_every == 0:
                    log.info(
                        "[extract] %s email_messages scanned, %s matched (queued) — %s",
                        f"{scanned:,}",
                        f"{len(work_queue):,}",
                        _format_elapsed(time.perf_counter() - t0),
                    )
                continue
            from_email = str(fm.get("from_email") or "")
            subject = str(fm.get("subject") or "")
            domain = _email_domain(from_email)
            candidates: list[EmailExtractor] = []
            candidates.extend(domain_idx.get(domain, []))
            candidates.extend(domain_idx.get("*", []))
            candidates = _dedupe_preserve_order(candidates)

            matched: EmailExtractor | None = None
            for ext in candidates:
                if self.sender_filter and ext.extractor_id != self.sender_filter:
                    continue
                if ext.matches(from_email, subject):
                    matched = ext
                    break

            if matched is not None:
                work_queue.append(
                    _EmailWorkItem(
                        rel_path=str(note.rel_path),
                        from_email=from_email,
                        subject=subject,
                        extractor=matched,
                    )
                )

            if self.progress_every and scanned % self.progress_every == 0:
                log.info(
                    "[extract] %s email_messages scanned, %s matched (queued) — %s",
                    f"{scanned:,}",
                    f"{len(work_queue):,}",
                    _format_elapsed(time.perf_counter() - t0),
                )

        metrics.total_emails_scanned = scanned

        if self.limit is not None and self.limit >= 0:
            work_queue = work_queue[: self.limit]

        for item in work_queue:
            eid = item.extractor.extractor_id
            self._bump(lock, metrics, eid, "matched", 1)
        metrics.matched_emails = len(work_queue)

        log.info(
            "extract-emails scan done: scanned=%s matched=%s dry_run=%s staging=%s",
            metrics.total_emails_scanned,
            metrics.matched_emails,
            self.dry_run,
            self.staging_dir or "",
        )

        if not work_queue:
            metrics.wall_clock_seconds = time.perf_counter() - t0
            if self.staging_dir:
                metrics.field_population = compute_field_population(Path(self.staging_dir))
            _finalize_metrics(metrics)
            self._write_metrics_json(metrics)
            log.info("extract-emails complete: %s", _format_elapsed(metrics.wall_clock_seconds))
            return metrics

        # Parallel extract+write in batches of futures
        processed = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = []
            for i in range(0, len(work_queue), self.batch_size):
                batch = work_queue[i : i + self.batch_size]
                for item in batch:
                    futures.append(pool.submit(self._process_item, item, lock, metrics))
            for fut in as_completed(futures):
                fut.result()
                processed += 1
                if self.progress_every and processed % self.progress_every == 0:
                    elapsed = time.perf_counter() - t0
                    cps = metrics.extracted_cards / max(elapsed, 1e-9)
                    log.info(
                        "[extract] processed %s/%s jobs — extracted=%s skipped=%s errors=%s (~%.0f cards/s) — %s",
                        f"{processed:,}",
                        f"{len(work_queue):,}",
                        f"{metrics.extracted_cards:,}",
                        f"{metrics.skipped_existing:,}",
                        f"{metrics.errors:,}",
                        cps,
                        _format_elapsed(elapsed),
                    )

        metrics.wall_clock_seconds = time.perf_counter() - t0
        if self.staging_dir:
            metrics.field_population = compute_field_population(Path(self.staging_dir))
        _finalize_metrics(metrics)
        self._write_metrics_json(metrics)
        log.info(
            "extract-emails complete: elapsed=%s cards=%s skipped=%s errors=%s cards/s=%.1f",
            _format_elapsed(metrics.wall_clock_seconds),
            metrics.extracted_cards,
            metrics.skipped_existing,
            metrics.errors,
            metrics.cards_per_second,
        )
        return metrics

    def _write_metrics_json(self, metrics: ExtractionMetrics) -> None:
        if not self.staging_dir:
            return
        root = Path(self.staging_dir)
        root.mkdir(parents=True, exist_ok=True)
        path = root / "_metrics.json"
        try:
            path.write_text(json.dumps(metrics.to_dict(), indent=2) + "\n", encoding="utf-8")
        except OSError as exc:
            log.warning("could not write %s: %s", path, exc)
