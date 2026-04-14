"""LLM enrichment pipeline: Stage 0 (domain gate) → Stage 1 (LLM classify) → Stage 2 (LLM extract).

Stage 0: Free deterministic filter — known noise/personal domains skipped, known transactional
         domains fast-tracked to Stage 2.
Stage 1: Lightweight LLM classify (~100 tokens) — classifies unknown threads as transactional/
         personal/marketing/noise. Only transactional proceeds.
Stage 2: Full LLM extraction (~1,700 tokens) — extracts typed card JSON from thread body.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_auth import INTERNAL_DOMAINS
from archive_cli.vault_cache import VaultScanCache
from archive_sync.extractors.registry import ExtractorRegistry, build_default_registry
from archive_sync.extractors.runner import derive_output_rel_path, note_content_matches, uid_in_vault_percent_sample
from archive_sync.llm_enrichment.cache import InferenceCache
from archive_sync.llm_enrichment.classify import CLASSIFY_PROMPT_VERSION, classify_thread, render_classify_input
from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.defaults import DEFAULT_ENRICH_EXTRACT_MODEL
from archive_sync.llm_enrichment.extract import ExtractedCard, extract_cards_for_thread
from archive_sync.llm_enrichment.known_senders import TRANSACTIONAL_DOMAINS, _email_domain, _is_marketing_subject
from archive_sync.llm_enrichment.threads import (
    ThreadDocument,
    ThreadStub,
    build_thread_index,
    hydrate_thread,
    load_email_stubs_for_vault,
)
from archive_vault.llm_provider import GeminiProvider, OllamaProvider
from archive_vault.schema import CARD_TYPES, BaseCard, validate_card_strict
from archive_vault.vault import write_card

log = logging.getLogger("ppa.llm_enrichment.enrich_runner")


def _default_run_id() -> str:
    return f"enrich-{time.strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"


def _ymd_from_thread(doc: ThreadDocument) -> str:
    if not doc.messages:
        return time.strftime("%Y-%m-%d")
    last = doc.messages[-1].sent_at or ""
    if len(last) >= 10 and last[4] == "-":
        return last[:10]
    return time.strftime("%Y-%m-%d")


def _stable_payload_for_uid(card_type: str, data: dict[str, Any]) -> str:
    skip = frozenset({"uid", "type", "created", "updated", "extraction_confidence"})
    pruned = {k: v for k, v in sorted(data.items()) if k not in skip and v not in (None, "", [], {})}
    return json.dumps({"type": card_type, "fields": pruned}, sort_keys=True, default=str)


def derive_llm_card_uid(thread_id: str, card_type: str, data: dict[str, Any]) -> str:
    payload = f"{thread_id}\n{_stable_payload_for_uid(card_type, data)}"
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
    slug = card_type.replace("_", "-")
    return f"hfa-{slug}-{h}"


_VALID_CARD_TYPES = frozenset({
    "meal_order", "grocery_order", "purchase", "ride", "flight",
    "accommodation", "car_rental", "shipment", "subscription",
    "event_ticket", "payroll",
})

_MINIMUM_FIELDS = frozenset({
    "uid", "type", "source", "source_id", "created", "updated",
    "extraction_confidence", "source_email", "summary",
})


def _coerce_items(raw: dict[str, Any]) -> None:
    """Fix common LLM output: items as dict → list of {name, price} dicts."""
    items = raw.get("items")
    if isinstance(items, dict):
        coerced = []
        for k, v in items.items():
            entry: dict[str, Any] = {"name": str(k)}
            if isinstance(v, (int, float)):
                entry["price"] = v
                entry["qty"] = 1
            elif isinstance(v, dict):
                entry.update(v)
                entry.setdefault("qty", 1)
            elif isinstance(v, str):
                entry["price"] = v
                entry["qty"] = 1
            coerced.append(entry)
        raw["items"] = coerced


def _card_has_meaningful_fields(ct: str, raw: dict[str, Any]) -> bool:
    """Return False if the card has no useful content beyond system fields."""
    for k, v in raw.items():
        if k in _MINIMUM_FIELDS:
            continue
        if v in (None, "", 0, 0.0, [], {}):
            continue
        return True
    return False


def _coerce_nulls(raw: dict[str, Any], model_cls: type) -> None:
    """Convert None values to field-type defaults so Pydantic doesn't reject them."""
    for field_name, field_info in model_cls.model_fields.items():
        if field_name not in raw:
            continue
        if raw[field_name] is not None:
            continue
        anno = field_info.annotation
        if anno is str or (hasattr(anno, "__origin__") and anno.__origin__ is str):
            raw[field_name] = ""
        elif anno is float or anno is int:
            raw[field_name] = 0.0
        elif anno is list or (hasattr(anno, "__origin__") and anno.__origin__ is list):
            raw[field_name] = []
        else:
            raw[field_name] = ""


def _fix_summary(raw: dict[str, Any], doc: ThreadDocument, ct: str) -> None:
    """Ensure summary is human-readable — never an llm: hash or empty."""
    s = str(raw.get("summary") or "").strip()
    if not s or s.startswith("llm:") or len(s) < 3:
        subj = doc.subject.strip() if doc.subject else ""
        label = ct.replace("_", " ").title()
        raw["summary"] = f"{label}: {subj}" if subj else label


def _decode_html_entities(raw: dict[str, Any]) -> None:
    """Fix &amp; → & in URL-like fields."""
    for k in ("barcode_url", "ticket_url", "url", "tracking_url"):
        v = raw.get(k)
        if isinstance(v, str) and "&amp;" in v:
            raw[k] = v.replace("&amp;", "&")


def _merge_to_card(
    ec: ExtractedCard,
    *,
    thread_id: str,
    doc: ThreadDocument,
    run_id: str,
    confidence: float,
) -> BaseCard | None:
    ct = ec.card_type
    if ct not in _VALID_CARD_TYPES:
        log.info("skipping invented card type %r", ct)
        return None
    model = CARD_TYPES.get(ct)
    if model is None:
        return None
    allowed = set(model.model_fields)
    raw = {k: v for k, v in ec.data.items() if k in allowed and k != "type"}
    _coerce_items(raw)
    _coerce_nulls(raw, model)
    _fix_summary(raw, doc, ct)
    _decode_html_entities(raw)
    if not _card_has_meaningful_fields(ct, raw):
        log.info("skipping empty card type=%s thread=%s", ct, thread_id)
        return None
    ymd = _ymd_from_thread(doc)
    msg_uid = doc.messages[-1].uid if doc.messages else ""
    uid = raw.get("uid") if isinstance(raw.get("uid"), str) else None
    if not uid or not str(uid).startswith("hfa-"):
        uid = derive_llm_card_uid(thread_id, ct, raw)
    sid_src = f"{run_id}|{thread_id}|{ct}|{_stable_payload_for_uid(ct, raw)}"
    source_id = f"llm:{hashlib.sha256(sid_src.encode()).hexdigest()[:16]}"
    ext_conf = max(0.0, min(1.0, confidence * 0.95))
    merged: dict[str, Any] = {
        **raw,
        "type": ct,
        "uid": uid,
        "source": ["llm_enrichment"],
        "source_id": source_id,
        "created": ymd,
        "updated": ymd,
        "extraction_confidence": round(ext_conf, 4),
    }
    if "source_email" in allowed and msg_uid:
        merged["source_email"] = f"[[{msg_uid}]]"
    try:
        return validate_card_strict(merged)
    except Exception as exc:
        log.warning("validate_card_strict failed %s: %s", ct, exc)
        return None


def _card_body_markdown(card: BaseCard, *, run_id: str) -> str:
    title = getattr(card, "summary", "") or card.type.replace("_", " ").title()
    return f"## {title}\n\n<!-- llm_enrichment run_id: {run_id} -->\n"


def _llm_provenance(
    card: BaseCard,
    *,
    model_id: str,
    run_id: str,
    content_hash: str = "",
) -> dict[str, Any]:
    """Provenance with LLM model + run_id populated (richer than deterministic_provenance)."""

    from archive_vault.provenance import PROVENANCE_EXEMPT_FIELDS, ProvenanceEntry
    from archive_vault.schema import card_to_frontmatter

    today = __import__("datetime").date.today().isoformat()
    fm = card_to_frontmatter(card)
    prov: dict[str, ProvenanceEntry] = {}
    for field_name, value in fm.items():
        if field_name in PROVENANCE_EXEMPT_FIELDS or value in ("", [], None, 0):
            continue
        prov[field_name] = ProvenanceEntry(
            source="llm_enrichment",
            date=today,
            method="deterministic",
            model=model_id,
            input_hash=content_hash[:16] if content_hash else "",
        )
    return prov


# ---------------------------------------------------------------------------
# Thread gating: extractor matches() + TRANSACTIONAL_DOMAINS (no LLM triage)
# ---------------------------------------------------------------------------

def _gate_thread(
    group: list[ThreadStub],
    registry: ExtractorRegistry,
) -> tuple[str, list[str]]:
    """Decide whether a thread should be extracted, classified, or skipped.

    Returns ``(decision, card_types)``:
    - ``("extract", [...])`` — known transactional sender, send to LLM extraction
    - ``("classify", [])`` — unknown sender, send to LLM classify first
    - ``("skip", [])`` — known noise, no LLM call
    """
    from archive_sync.llm_enrichment.known_senders import classify_thread_prefilter

    card_types: list[str] = []

    for stub in group:
        fe = stub.from_email
        subj = stub.subject

        ext = registry.match(fe, subj)
        if ext is not None and ext.output_card_type:
            ct = ext.output_card_type
            if ct not in card_types:
                card_types.append(ct)

        dom = _email_domain(fe)
        dom_types = TRANSACTIONAL_DOMAINS.get(dom)
        if dom_types:
            for ct in dom_types:
                if ct not in card_types:
                    card_types.append(ct)

    if card_types:
        subjects = [s.subject for s in group if s.subject]
        if subjects and all(_is_marketing_subject(s) for s in subjects):
            return "skip", []
        return "classify_with_types", card_types

    # No known transactional match — check if it's known noise or needs classify
    from_emails = [s.from_email for s in group if s.from_email]
    subjects = [s.subject for s in group if s.subject]
    pf_decision, _ = classify_thread_prefilter(
        from_emails, subjects, user_domains=frozenset(INTERNAL_DOMAINS)
    )
    if pf_decision == "skip":
        return "skip", []
    return "classify", []


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class EnrichmentMetrics:
    total_threads: int = 0
    total_emails: int = 0
    threads_processed: int = 0
    # Stage 0: domain gate
    stage0_fast_track: int = 0
    stage0_skip: int = 0
    stage0_classify: int = 0
    # Stage 1: LLM classify
    stage1_transactional: int = 0
    stage1_skip: int = 0
    stage1_cache_hits: int = 0
    # Stage 2: LLM extract
    extraction_cache_hits: int = 0
    cards_extracted: int = 0
    cards_written: int = 0
    skipped_existing: int = 0
    errors: int = 0
    schema_validation_failures: int = 0
    round_trip_warnings: int = 0
    wall_clock_seconds: float = 0.0
    per_card_type: dict[str, int] = field(default_factory=dict)

    @property
    def total_extract_candidates(self) -> int:
        return self.stage0_fast_track + self.stage1_transactional

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_threads": self.total_threads,
            "total_emails": self.total_emails,
            "threads_processed": self.threads_processed,
            "stage0_fast_track": self.stage0_fast_track,
            "stage0_skip": self.stage0_skip,
            "stage0_classify": self.stage0_classify,
            "stage1_transactional": self.stage1_transactional,
            "stage1_skip": self.stage1_skip,
            "stage1_cache_hits": self.stage1_cache_hits,
            "extraction_cache_hits": self.extraction_cache_hits,
            "cards_extracted": self.cards_extracted,
            "cards_written": self.cards_written,
            "skipped_existing": self.skipped_existing,
            "errors": self.errors,
            "schema_validation_failures": self.schema_validation_failures,
            "round_trip_warnings": self.round_trip_warnings,
            "wall_clock_seconds": round(self.wall_clock_seconds, 3),
            "per_card_type": dict(self.per_card_type),
        }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class LlmEnrichmentRunner:
    """Known-sender gate → LLM extraction → write derived cards to staging."""

    def __init__(
        self,
        vault_path: str | Path,
        *,
        staging_dir: str | Path,
        extract_model: str = "",
        classify_model: str = "",
        provider_kind: str = "ollama",
        base_url: str = "http://localhost:11434",
        cache_db: Path | str | None = None,
        run_id: str = "",
        progress_every: int = 100,
        limit_threads: int | None = None,
        vault_percent: float | None = None,
        dry_run: bool = False,
        workers: int = 1,
        classify_workers: int | None = None,
        no_gate: bool = False,
        skip_classify: bool = False,
        classify_index_db: Path | str | None = None,
        # Legacy — accepted but ignored (no triage stage)
        triage_model: str = "",
    ) -> None:
        self.vault_path = Path(vault_path).resolve()
        self.staging_dir = Path(staging_dir)
        self.extract_model = (extract_model.strip() or DEFAULT_ENRICH_EXTRACT_MODEL).strip()
        self.classify_model = (classify_model.strip() or self.extract_model).strip()
        self.provider_kind = provider_kind.strip().lower() or "ollama"
        self.base_url = base_url.rstrip("/")
        self.cache_db = Path(cache_db) if cache_db else None
        self.run_id = run_id.strip() or _default_run_id()
        self.progress_every = max(1, int(progress_every))
        self.limit_threads = int(limit_threads) if limit_threads and limit_threads > 0 else None
        self.vault_percent = float(vault_percent) if vault_percent and vault_percent > 0 else None
        self.dry_run = dry_run
        self.workers = max(1, int(workers))
        self.classify_workers = int(classify_workers) if classify_workers and classify_workers > 0 else self.workers * 3
        self.no_gate = bool(no_gate)
        self.skip_classify = bool(skip_classify)
        self.classify_index_db = Path(classify_index_db) if classify_index_db else None

    def run(self) -> EnrichmentMetrics:
        t0 = time.perf_counter()
        metrics = EnrichmentMetrics()

        if self.provider_kind == "gemini":
            provider: OllamaProvider | GeminiProvider = GeminiProvider(model=self.extract_model)
            if not provider.health_check():
                raise RuntimeError(
                    "Gemini health_check failed — is GEMINI_API_KEY set?"
                )
        else:
            provider = OllamaProvider(model=self.extract_model, base_url=self.base_url)
            if not provider.health_check():
                raise RuntimeError(
                    f"Ollama health_check failed for model {self.extract_model!r} — "
                    "is `ollama serve` running and is the model pulled?"
                )
        log.info(
            "enrich-emails provider=%s extract_model=%s classify_model=%s extract_workers=%d classify_workers=%d",
            self.provider_kind, self.extract_model, self.classify_model, self.workers, self.classify_workers,
        )

        registry = build_default_registry()

        main_scan_cache = VaultScanCache.build_or_load(self.vault_path, tier=2, progress_every=0)
        _scan_cache_path = VaultScanCache.cache_path_for_vault(self.vault_path)

        stubs = load_email_stubs_for_vault(self.vault_path)
        index = build_thread_index(stubs)
        metrics.total_emails = len(stubs)
        thread_items = list(index.items())

        def _thread_sort_key(item: tuple[str, list[ThreadStub]]) -> str:
            tid, group = item
            if not group:
                return tid
            return min((s.sent_at or "" for s in group), default=tid)

        thread_items.sort(key=_thread_sort_key)

        if self.vault_percent is not None:
            thread_items = [
                (tid, g) for tid, g in thread_items
                if uid_in_vault_percent_sample(tid or "none", float(self.vault_percent))
            ]
        if self.limit_threads is not None:
            thread_items = thread_items[: self.limit_threads]

        write_lock = threading.Lock()
        _tls = threading.local()

        def _get_thread_scan_cache() -> VaultScanCache:
            if self.workers <= 1:
                return main_scan_cache
            sc = getattr(_tls, "scan_cache", None)
            if sc is None:
                import sqlite3 as _sqlite3
                conn = _sqlite3.connect(str(_scan_cache_path), timeout=60.0, check_same_thread=False)
                conn.row_factory = _sqlite3.Row
                sc = VaultScanCache(conn, tier=2, vault_fingerprint="preloaded", cache_hit=True)
                _tls.scan_cache = sc
            return sc

        # Classify index for persistent storage
        _classify_idx: ClassifyIndex | None = None
        if self.classify_index_db is not None:
            _classify_idx = ClassifyIndex(self.classify_index_db)

        # Init inference cache schema once on main thread so workers don't race on PRAGMA
        _main_cache: InferenceCache | None = None
        if self.cache_db is not None:
            _main_cache = InferenceCache(self.cache_db)

        def _get_thread_inference_cache() -> InferenceCache | None:
            if self.cache_db is None:
                return None
            if self.workers <= 1:
                return _main_cache
            ic = getattr(_tls, "inference_cache", None)
            if ic is None:
                ic = InferenceCache(self.cache_db, _skip_init=True)
                _tls.inference_cache = ic
            return ic

        from archive_sync.llm_enrichment.schema_gen import all_extractable_card_types
        _all_types = [t for t in all_extractable_card_types() if t in _VALID_CARD_TYPES]

        metrics.total_threads = len(thread_items)

        # ── Stage 0: Domain gate (free, instant) ──────────────────────────
        fast_track: list[tuple[str, list[ThreadStub], list[str]]] = []
        needs_classify: list[tuple[str, list[ThreadStub]]] = []

        needs_classify_with_types: list[tuple[str, list[ThreadStub], list[str]]] = []

        if self.no_gate:
            for tid, group in thread_items:
                fast_track.append((tid, group, _all_types))
                metrics.stage0_fast_track += 1
        else:
            for tid, group in thread_items:
                decision, card_types = _gate_thread(group, registry)
                if decision == "classify_with_types":
                    if self.skip_classify:
                        fast_track.append((tid, group, card_types))
                        metrics.stage0_fast_track += 1
                    else:
                        needs_classify_with_types.append((tid, group, card_types))
                        metrics.stage0_classify += 1
                elif decision == "classify" and not self.skip_classify:
                    needs_classify.append((tid, group))
                    metrics.stage0_classify += 1
                else:
                    metrics.stage0_skip += 1

        all_classify = len(needs_classify) + len(needs_classify_with_types)
        log.info(
            "stage0 gate: %d fast-track, %d classify (%d known-sender + %d unknown), %d skip (of %d threads) — %.1fs",
            metrics.stage0_fast_track,
            all_classify,
            len(needs_classify_with_types),
            len(needs_classify),
            metrics.stage0_skip,
            metrics.total_threads,
            time.perf_counter() - t0,
        )

        # Merge both classify lists — known-sender items carry suggested types
        combined_classify: list[tuple[str, list[ThreadStub], list[str] | None]] = []
        for tid, group, types in needs_classify_with_types:
            combined_classify.append((tid, group, types))
        for tid, group in needs_classify:
            combined_classify.append((tid, group, None))

        # ── Stage 1: LLM classify (cheap, ~100 tokens per thread) ────────
        if combined_classify:
            t1 = time.perf_counter()
            classified_tx: list[tuple[str, list[ThreadStub], list[str]]] = []
            _classify_count = [0]
            _classify_total = len(combined_classify)

            _ClassifyOut = tuple[str, list[ThreadStub], bool, bool, list[str], str, float, list[str] | None]

            def _classify_one(item: tuple[str, list[ThreadStub], list[str] | None]) -> _ClassifyOut:
                tid, group, suggested_types = item
                first = group[0] if group else None
                ci = render_classify_input(
                    subject=first.subject if first else "",
                    from_email=first.from_email if first else "",
                    snippet=first.snippet if first else "",
                    message_count=len(group),
                )
                tl_cache = _get_thread_inference_cache()
                cr = classify_thread(
                    provider, ci,
                    model=self.classify_model, cache=tl_cache, run_id=self.run_id,
                )
                return tid, group, cr.is_transactional, cr.cache_hit, cr.card_types, cr.category, cr.confidence, suggested_types

            def _handle_classify_result(tid: str, group: list[ThreadStub], is_tx: bool, ch: bool, card_types: list[str], category: str = "", confidence: float = 0.0, suggested_types: list[str] | None = None) -> None:
                if ch:
                    metrics.stage1_cache_hits += 1
                if is_tx:
                    types = suggested_types or card_types or _all_types
                    classified_tx.append((tid, group, types))
                    metrics.stage1_transactional += 1
                else:
                    metrics.stage1_skip += 1
                if _classify_idx is not None:
                    first = group[0] if group else None
                    _classify_idx.put_classification(
                        thread_id=tid,
                        category=category or ("transactional" if is_tx else "skip"),
                        confidence=confidence,
                        card_types=card_types,
                        message_count=len(group),
                        first_subject=first.subject if first else "",
                        first_from_email=first.from_email if first else "",
                        classify_model=self.extract_model,
                        classify_prompt_version=CLASSIFY_PROMPT_VERSION,
                        run_id=self.run_id,
                    )
                _classify_count[0] += 1
                n = _classify_count[0]
                if n % 200 == 0 or n == _classify_total:
                    elapsed = time.perf_counter() - t1
                    rate = n / (elapsed / 60.0) if elapsed > 0 else 0
                    log.info(
                        "stage1 classify %d/%d (%.0f/min) tx=%d skip=%d — %.0fs",
                        n, _classify_total, rate,
                        metrics.stage1_transactional, metrics.stage1_skip, elapsed,
                    )

            log.info(
                "stage1 starting: %d threads, %d classify_workers",
                len(combined_classify),
                self.classify_workers,
            )

            if self.classify_workers <= 1:
                for item in combined_classify:
                    tid, group, is_tx, ch, ctypes, cat, conf, sugg = _classify_one(item)
                    _handle_classify_result(tid, group, is_tx, ch, ctypes, cat, conf, sugg)
            else:
                with ThreadPoolExecutor(max_workers=self.classify_workers) as pool:
                    futures = {pool.submit(_classify_one, item): item for item in combined_classify}
                    for future in as_completed(futures):
                        tid, group, is_tx, ch, ctypes, cat, conf, sugg = future.result()
                        _handle_classify_result(tid, group, is_tx, ch, ctypes, cat, conf, sugg)

            log.info(
                "stage1 done: %d transactional, %d skip, %d cache_hits — %.1fs",
                metrics.stage1_transactional,
                metrics.stage1_skip,
                metrics.stage1_cache_hits,
                time.perf_counter() - t1,
            )

            for tid, group, types in classified_tx:
                fast_track.append((tid, group, types))

        # ── Stage 2 queue ─────────────────────────────────────────────────
        extract_queue = fast_track
        log.info(
            "stage2 extract queue: %d threads (fast-track %d + classify %d)",
            len(extract_queue),
            metrics.stage0_fast_track,
            metrics.stage1_transactional,
        )

        def _process_thread(item: tuple[str, list[ThreadStub], list[str]]) -> dict[str, Any]:
            tid, group, card_types = item
            result: dict[str, Any] = {
                "thread_id": tid,
                "cards_extracted": 0,
                "cards_written": 0,
                "errors": 0,
                "schema_failures": 0,
                "rt_warnings": 0,
                "cache_hit": False,
                "per_type": {},
                "skipped_existing": 0,
            }
            try:
                subj = group[0].subject[:60] if group else "?"
                log.info("→ extract thread=%s types=%s subj=%r msgs=%d", tid, card_types, subj, len(group))
                t_start = time.perf_counter()
                tl_scan = _get_thread_scan_cache()
                tl_cache = _get_thread_inference_cache()
                doc = hydrate_thread(group, self.vault_path, scan_cache=tl_scan)
                ex = extract_cards_for_thread(
                    provider, doc, card_types,
                    model=self.extract_model, cache=tl_cache, run_id=self.run_id,
                )
                t_dur = time.perf_counter() - t_start
                if ex.cache_hit:
                    result["cache_hit"] = True
                if ex.reasoning == "_llm_error":
                    result["errors"] += 1
                    log.warning("extract LLM error thread=%s", tid)

                result["cards_extracted"] = len(ex.cards)
                log.info(
                    "← thread=%s cards=%d cache=%s %.1fs",
                    tid, len(ex.cards), ex.cache_hit, t_dur,
                )
                for c in ex.cards:
                    result["rt_warnings"] += len(c.round_trip_warnings)

                for ec in ex.cards:
                    ct = ec.card_type
                    result["per_type"][ct] = result["per_type"].get(ct, 0) + 1
                    card = _merge_to_card(
                        ec, thread_id=tid, doc=doc,
                        run_id=self.run_id, confidence=1.0,
                    )
                    if card is None:
                        result["schema_failures"] += 1
                        continue
                    sent_hint = _ymd_from_thread(doc)
                    rel_out = derive_output_rel_path(card.type, card.uid, sent_hint)
                    body = _card_body_markdown(card, run_id=self.run_id)
                    prov = _llm_provenance(
                        card,
                        model_id=self.extract_model,
                        run_id=self.run_id,
                        content_hash=doc.content_hash,
                    )
                    if self.dry_run:
                        continue
                    with write_lock:
                        if note_content_matches(self.staging_dir, rel_out, card, body):
                            result["skipped_existing"] += 1
                            continue
                        if self.staging_dir != self.vault_path:
                            if note_content_matches(self.vault_path, rel_out, card, body):
                                result["skipped_existing"] += 1
                                continue
                        try:
                            write_card(str(self.staging_dir), rel_out, card, body, prov)
                            result["cards_written"] += 1
                        except Exception as exc:
                            log.warning("write_card failed %s: %s", rel_out, exc)
                            result["errors"] += 1
            except Exception as exc:
                log.exception("thread failed %s: %s", tid, exc)
                result["errors"] += 1
            return result

        try:
            processed = 0
            if self.workers <= 1:
                for item in extract_queue:
                    r = _process_thread(item)
                    processed += 1
                    self._apply_result(metrics, r)
                    if r.get("cards_written", 0) > 0 and _classify_idx is not None:
                        _classify_idx.mark_extracted(r["thread_id"], self.run_id)
                    metrics.threads_processed = metrics.stage0_skip + metrics.stage1_skip + processed
                    self._progress(processed, metrics, t0)
            else:
                with ThreadPoolExecutor(max_workers=self.workers) as pool:
                    futures = {pool.submit(_process_thread, item): item for item in extract_queue}
                    for future in as_completed(futures):
                        r = future.result()
                        processed += 1
                        self._apply_result(metrics, r)
                        if r.get("cards_written", 0) > 0 and _classify_idx is not None:
                            _classify_idx.mark_extracted(r["thread_id"], self.run_id)
                        metrics.threads_processed = metrics.stage0_skip + metrics.stage1_skip + processed
                        self._progress(processed, metrics, t0)
        finally:
            pass

        metrics.wall_clock_seconds = time.perf_counter() - t0
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = self.staging_dir / "_metrics.json"
        metrics_path.write_text(json.dumps(metrics.to_dict(), indent=2), encoding="utf-8")
        log.info("enrich-emails done — wrote %s", metrics_path)
        return metrics

    @staticmethod
    def _apply_result(metrics: EnrichmentMetrics, r: dict[str, Any]) -> None:
        metrics.cards_extracted += r.get("cards_extracted", 0)
        metrics.cards_written += r.get("cards_written", 0)
        metrics.errors += r.get("errors", 0)
        metrics.schema_validation_failures += r.get("schema_failures", 0)
        metrics.round_trip_warnings += r.get("rt_warnings", 0)
        metrics.skipped_existing += r.get("skipped_existing", 0)
        if r.get("cache_hit"):
            metrics.extraction_cache_hits += 1
        for ct, n in r.get("per_type", {}).items():
            metrics.per_card_type[ct] = metrics.per_card_type.get(ct, 0) + n

    def _progress(self, processed: int, metrics: EnrichmentMetrics, t0: float) -> None:
        if self.progress_every and processed % self.progress_every == 0:
            elapsed = time.perf_counter() - t0
            rate = processed / (elapsed / 60.0) if elapsed > 0 else 0.0
            log.info(
                "enrich-emails %d/%d extracted (%.1f thr/min) — "
                "cards=%d written=%d errors=%d elapsed=%.0fs",
                processed,
                metrics.total_extract_candidates,
                rate,
                metrics.cards_extracted,
                metrics.cards_written,
                metrics.errors,
                elapsed,
            )


def run_enrich_emails(**kwargs: Any) -> EnrichmentMetrics:
    """Convenience wrapper."""
    return LlmEnrichmentRunner(**kwargs).run()
