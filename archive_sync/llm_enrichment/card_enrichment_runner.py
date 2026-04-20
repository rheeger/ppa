"""Phase 2.875 — gate → LLM enrich → vault writes + JSONL staging."""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_auth import INTERNAL_DOMAINS
from archive_cli.vault_cache import VaultScanCache
from archive_sync.extractors.runner import uid_in_vault_percent_sample
from archive_sync.llm_enrichment.cache import (InferenceCache,
                                               build_inference_cache_key)
from archive_sync.llm_enrichment.classify_index import ClassifyIndex
from archive_sync.llm_enrichment.threads import (
    MessageStubIndex, ParticipantNameResolver, build_message_stub_index,
    build_participant_name_resolver, build_thread_index, chunk_thread_messages,
    hydrate_imessage_thread, hydrate_thread, imessage_thread_content_hash,
    load_email_stubs_for_vault, render_imessage_chunk_for_llm)
from archive_sync.llm_enrichment.workflows import calendar_event as wf_calendar
from archive_sync.llm_enrichment.workflows import document as wf_document
from archive_sync.llm_enrichment.workflows import email_thread as wf_email
from archive_sync.llm_enrichment.workflows import finance as wf_finance
from archive_sync.llm_enrichment.workflows import \
    imessage_thread as wf_imessage
from archive_vault.llm_provider import (GeminiProvider, LLMResponse,
                                        OllamaProvider)
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import validate_card_strict
from archive_vault.vault import read_note, write_card

log = logging.getLogger("ppa.card_enrichment")


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _default_run_id() -> str:
    return f"enrich-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"


def _load_uid_filter_file(path: Path) -> set[str]:
    """One uid per line; # starts a comment line."""

    out: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line)
    return out


@dataclass
class CardEnrichmentMetrics:
    workflow: str = ""
    total_cards: int = 0
    gated: int = 0
    skipped_prefilter: int = 0
    skipped_populated: int = 0
    skipped_no_thread: int = 0
    skipped_sample: int = 0
    skipped_uid_filter: int = 0
    llm_calls: int = 0
    cache_hits: int = 0
    llm_nonempty_summary: int = 0
    vault_writes: int = 0
    dry_run_writes: int = 0
    errors: int = 0
    entity_mentions_staged: int = 0
    match_candidates_staged: int = 0
    enriched: int = 0
    enriched_card_uids: list[str] = field(default_factory=list)
    prefilter_breakdown: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        if self.llm_calls > 0:
            base["llm_yield_rate"] = round(self.llm_nonempty_summary / self.llm_calls, 4)
        else:
            base["llm_yield_rate"] = None
        return base


@dataclass
class CardEnrichmentRunner:
    vault_path: Path
    workflow: str
    provider_kind: str
    model: str
    base_url: str
    cache_db: Path | None
    run_id: str
    staging_dir: Path
    dry_run: bool
    progress_every: int
    vault_percent: float | None
    limit: int | None
    skip_populated: bool
    workers: int = 24
    uid_filter_file: Path | None = None
    classify_index_db: Path | str | None = None
    checkpoint_every: int = 500

    metrics: CardEnrichmentMetrics = field(default_factory=CardEnrichmentMetrics)

    def __post_init__(self) -> None:
        self.vault_path = Path(self.vault_path).resolve()
        self.staging_dir = Path(self.staging_dir)
        if self.classify_index_db is not None:
            self.classify_index_db = Path(self.classify_index_db)
        self.run_id = (self.run_id or "").strip() or _default_run_id()
        self.metrics.workflow = self.workflow
        self.checkpoint_every = max(0, int(self.checkpoint_every))

    def _checkpoint_payload(self, t0: float, processed_eligible: int) -> dict[str, Any]:
        d = self.metrics.to_dict()
        d.pop("enriched_card_uids", None)
        return {
            **d,
            "checkpoint_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "run_id": self.run_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "processed_eligible": processed_eligible,
        }

    def _write_checkpoint(self, t0: float, processed_eligible: int) -> None:
        if not self.checkpoint_every:
            return
        path = self.staging_dir / "_metrics_checkpoint.json"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self._checkpoint_payload(t0, processed_eligible), indent=2),
            encoding="utf-8",
        )

    def _provider(self) -> OllamaProvider | GeminiProvider:
        if self.provider_kind == "gemini":
            return GeminiProvider(model=self.model)
        return OllamaProvider(model=self.model, base_url=self.base_url)

    def _apply_thread_summary_enrichment(
        self,
        rel_path: str,
        field_updates: dict[str, Any],
        content_hash: str,
    ) -> None:
        fm, body, existing_prov = read_note(self.vault_path, rel_path)
        merged = {**fm, **field_updates}
        card = validate_card_strict(merged)
        incoming: dict[str, ProvenanceEntry] = {}
        if "thread_summary" in field_updates:
            incoming["thread_summary"] = ProvenanceEntry(
                source="card_enrichment",
                date=_utc_today(),
                method="llm",
                model=self.model,
                input_hash=content_hash[:16] if content_hash else "",
            )
        prov = merge_provenance(existing_prov, incoming)
        write_card(self.vault_path, rel_path, card, body, prov)
        self.metrics.vault_writes += 1

    def _apply_finance_enrichment(
        self,
        rel_path: str,
        field_updates: dict[str, Any],
        content_hash: str,
    ) -> None:
        fm, body, existing_prov = read_note(self.vault_path, rel_path)
        merged = {**fm, **field_updates}
        card = validate_card_strict(merged)
        incoming: dict[str, ProvenanceEntry] = {}
        if "provider_tags" in field_updates:
            incoming["provider_tags"] = ProvenanceEntry(
                source="card_enrichment",
                date=_utc_today(),
                method="llm",
                model=self.model,
                input_hash=content_hash[:16] if content_hash else "",
            )
        prov = merge_provenance(existing_prov, incoming)
        write_card(self.vault_path, rel_path, card, body, prov)
        self.metrics.vault_writes += 1

    def _apply_document_enrichment(
        self,
        rel_path: str,
        field_updates: dict[str, Any],
        content_hash: str,
    ) -> None:
        from archive_vault.schema import DETERMINISTIC_ONLY

        fm, body, existing_prov = read_note(self.vault_path, rel_path)
        merged = {**fm, **field_updates}
        card = validate_card_strict(merged)
        incoming: dict[str, ProvenanceEntry] = {}
        ih = content_hash[:16] if content_hash else ""
        for key in field_updates:
            method = "deterministic" if key in DETERMINISTIC_ONLY else "llm"
            incoming[key] = ProvenanceEntry(
                source="card_enrichment",
                date=_utc_today(),
                method=method,
                model=self.model,
                input_hash=ih,
            )
        prov = merge_provenance(existing_prov, incoming)
        write_card(self.vault_path, rel_path, card, body, prov)
        self.metrics.vault_writes += 1

    def _process_one_email_thread(
        self,
        rel_path: str,
        *,
        scan_cache: VaultScanCache,
        thread_index: dict[str, list],
        cache: InferenceCache | None,
        prov: OllamaProvider | GeminiProvider,
        entity_path: Path,
        match_path: Path,
        lock: threading.Lock | None,
    ) -> str:
        """Returns 'enriched' | 'skipped' | 'error'."""

        def _bump(metric: str, n: int = 1) -> None:
            if lock:
                with lock:
                    setattr(self.metrics, metric, getattr(self.metrics, metric) + n)
            else:
                setattr(self.metrics, metric, getattr(self.metrics, metric) + n)

        fm = scan_cache.frontmatter_for_rel_path(rel_path)
        uid = str(fm.get("uid") or "")
        tid = str(fm.get("gmail_thread_id") or "").strip()
        group = thread_index.get(tid, [])
        if not group:
            log.warning("no email_message stubs for thread_id=%s uid=%s", tid, uid)
            _bump("errors")
            return "error"

        doc = hydrate_thread(group, self.vault_path, scan_cache=scan_cache)
        account_email = str(fm.get("account_email") or "").strip()
        system_prompt = wf_email.load_system_prompt(account_email)
        user_msg = wf_email.render_user_message(doc, account_email=account_email)

        content_hash = doc.content_hash
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=self.model,
            prompt_version=wf_email.ENRICH_EMAIL_THREAD_PROMPT_VERSION,
            schema_version="email_thread_v1",
            temperature=0.0,
            seed=42,
        )

        parsed: dict[str, Any] | None = None
        llm_invoked = False
        if cache is not None:
            if lock:
                with lock:
                    hit = cache.get(cache_key)
            else:
                hit = cache.get(cache_key)
            if hit and isinstance(hit, dict) and "_error" not in hit:
                parsed = hit
                _bump("cache_hits")

        if parsed is None:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ]
            resp: LLMResponse = prov.chat_json(
                messages,
                model=self.model,
                temperature=0.0,
                seed=42,
                max_tokens=4096,
            )
            parsed = resp.parsed_json
            _bump("llm_calls")
            llm_invoked = True
            if cache is not None and parsed is not None and not self.dry_run:
                if lock:
                    with lock:
                        cache.put(
                            cache_key,
                            stage="enrich_email_thread",
                            model_id=self.model,
                            prompt_version=wf_email.ENRICH_EMAIL_THREAD_PROMPT_VERSION,
                            content_hash=content_hash,
                            response=wf_email.response_to_cache_payload(parsed),
                            tokens=(resp.prompt_tokens, resp.completion_tokens),
                            latency_ms=resp.latency_ms,
                            run_id=self.run_id,
                        )
                else:
                    cache.put(
                        cache_key,
                        stage="enrich_email_thread",
                        model_id=self.model,
                        prompt_version=wf_email.ENRICH_EMAIL_THREAD_PROMPT_VERSION,
                        content_hash=content_hash,
                        response=wf_email.response_to_cache_payload(parsed),
                        tokens=(resp.prompt_tokens, resp.completion_tokens),
                        latency_ms=resp.latency_ms,
                        run_id=self.run_id,
                    )

        if not parsed:
            log.warning("no JSON from LLM rel_path=%s thread_id=%s", rel_path, tid)
            _bump("errors")
            return "error"

        field_updates, entities, matches = wf_email.parse_email_thread_response(
            parsed,
            source_uid=uid,
            run_id=self.run_id,
        )

        if llm_invoked and str(field_updates.get("thread_summary") or "").strip():
            _bump("llm_nonempty_summary")

        if lock:
            with lock:
                self.metrics.enriched += 1
                if uid:
                    self.metrics.enriched_card_uids.append(uid)
        else:
            self.metrics.enriched += 1
            if uid:
                self.metrics.enriched_card_uids.append(uid)

        if self.dry_run:
            preview_line = (
                json.dumps(
                    {
                        "uid": uid,
                        "rel_path": rel_path,
                        "subject": str(fm.get("subject") or ""),
                        "parsed": parsed,
                        "field_updates": field_updates,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"

            def _append_preview() -> None:
                with preview_path.open("a", encoding="utf-8") as fh:
                    fh.write(preview_line)

            if lock:
                with lock:
                    _append_preview()
            else:
                _append_preview()

        if not self.dry_run:
            for e in entities:
                line = e.to_json_line() + "\n"
                if lock:
                    with lock:
                        with entity_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.entity_mentions_staged += 1
                else:
                    with entity_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.entity_mentions_staged += 1
            for m in matches:
                line = m.to_json_line() + "\n"
                if lock:
                    with lock:
                        with match_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.match_candidates_staged += 1
                else:
                    with match_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.match_candidates_staged += 1

        if field_updates:
            if self.dry_run:
                _bump("dry_run_writes")
            else:
                if lock:
                    with lock:
                        self._apply_thread_summary_enrichment(rel_path, field_updates, doc.content_hash)
                else:
                    self._apply_thread_summary_enrichment(rel_path, field_updates, doc.content_hash)

        return "enriched"

    def _process_one_imessage_thread(
        self,
        rel_path: str,
        *,
        card_type: str,
        scan_cache: VaultScanCache,
        msg_index: MessageStubIndex,
        name_resolver: ParticipantNameResolver,
        cache: InferenceCache | None,
        prov: OllamaProvider | GeminiProvider,
        entity_path: Path,
        lock: threading.Lock | None,
    ) -> str:
        """``card_type`` is ``imessage_thread`` or ``beeper_thread``."""

        def _bump(metric: str, n: int = 1) -> None:
            if lock:
                with lock:
                    setattr(self.metrics, metric, getattr(self.metrics, metric) + n)
            else:
                setattr(self.metrics, metric, getattr(self.metrics, metric) + n)

        fm = scan_cache.frontmatter_for_rel_path(rel_path)
        uid = str(fm.get("uid") or "")
        handle_names = name_resolver.names_for_thread(fm)
        messages = hydrate_imessage_thread(
            fm,
            self.vault_path,
            scan_cache=scan_cache,
            msg_index=msg_index,
            handle_names=handle_names,
        )
        if not messages:
            log.warning("no imessage/beeper messages resolved rel_path=%s uid=%s", rel_path, uid)
            _bump("errors")
            return "error"

        display_label = wf_imessage.thread_display_label(
            fm, card_type=card_type, handle_names=handle_names
        )
        ctx = wf_imessage.thread_context_header(fm, card_type=card_type)
        chunks = chunk_thread_messages(
            messages,
            chunk_size=wf_imessage.CHUNK_SIZE,
            overlap=wf_imessage.CHUNK_OVERLAP,
        )
        system_prompt = wf_imessage.load_system_prompt()
        base_hash = imessage_thread_content_hash(messages)

        all_conversations: list[dict[str, Any]] = []
        chunk_parsed_list: list[dict[str, Any] | None] = []
        chunk_llm_invoked = 0

        for idx, chunk in enumerate(chunks):
            user_msg = render_imessage_chunk_for_llm(
                chunk,
                display_label=display_label,
                context_header=ctx,
            )
            seg_hash = wf_imessage.chunk_cache_segment_hash(chunk, display_label, ctx)
            chunk_id = f"{idx}:{len(chunks)}:{seg_hash}"
            cache_key = build_inference_cache_key(
                content_hash=chunk_id,
                model_id=self.model,
                prompt_version=wf_imessage.ENRICH_IMESSAGE_THREAD_PROMPT_VERSION,
                schema_version="imessage_thread_chunk_v1",
                temperature=0.0,
                seed=42,
            )

            parsed: dict[str, Any] | None = None
            if cache is not None:
                if lock:
                    with lock:
                        hit = cache.get(cache_key)
                else:
                    hit = cache.get(cache_key)
                if hit and isinstance(hit, dict) and "_error" not in hit:
                    parsed = hit
                    _bump("cache_hits")

            if parsed is None:
                llm_messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ]
                resp: LLMResponse = prov.chat_json(
                    llm_messages,
                    model=self.model,
                    temperature=0.0,
                    seed=42,
                    max_tokens=8192,
                )
                parsed = resp.parsed_json
                _bump("llm_calls")
                chunk_llm_invoked += 1
                if cache is not None and parsed is not None and not self.dry_run:
                    if lock:
                        with lock:
                            cache.put(
                                cache_key,
                                stage="enrich_imessage_thread",
                                model_id=self.model,
                                prompt_version=wf_imessage.ENRICH_IMESSAGE_THREAD_PROMPT_VERSION,
                                content_hash=base_hash,
                                response=wf_imessage.response_to_cache_payload(parsed),
                                tokens=(resp.prompt_tokens, resp.completion_tokens),
                                latency_ms=resp.latency_ms,
                                run_id=self.run_id,
                            )
                    else:
                        cache.put(
                            cache_key,
                            stage="enrich_imessage_thread",
                            model_id=self.model,
                            prompt_version=wf_imessage.ENRICH_IMESSAGE_THREAD_PROMPT_VERSION,
                            content_hash=base_hash,
                            response=wf_imessage.response_to_cache_payload(parsed),
                            tokens=(resp.prompt_tokens, resp.completion_tokens),
                            latency_ms=resp.latency_ms,
                            run_id=self.run_id,
                        )

            if not parsed:
                log.warning(
                    "no JSON from LLM imessage chunk rel_path=%s chunk=%d/%d uid=%s",
                    rel_path,
                    idx + 1,
                    len(chunks),
                    uid,
                )
                _bump("errors")
                return "error"

            chunk_parsed_list.append(parsed)
            all_conversations.extend(wf_imessage.parse_chunk_conversations(parsed))

        merged = wf_imessage.dedupe_conversations(all_conversations)
        field_updates, entities = wf_imessage.build_outputs_from_conversations(
            merged,
            display_label=display_label,
            source_uid=uid,
            source_card_type=card_type,
            run_id=self.run_id,
        )

        if chunk_llm_invoked > 0 and str(field_updates.get("thread_summary") or "").strip():
            _bump("llm_nonempty_summary")

        if lock:
            with lock:
                self.metrics.enriched += 1
                if uid:
                    self.metrics.enriched_card_uids.append(uid)
        else:
            self.metrics.enriched += 1
            if uid:
                self.metrics.enriched_card_uids.append(uid)

        if self.dry_run:
            preview_line = (
                json.dumps(
                    {
                        "uid": uid,
                        "rel_path": rel_path,
                        "parsed": {
                            "chunks": chunk_parsed_list,
                            "conversations_merged": merged,
                        },
                        "field_updates": field_updates,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"

            def _append_preview() -> None:
                with preview_path.open("a", encoding="utf-8") as fh:
                    fh.write(preview_line)

            if lock:
                with lock:
                    _append_preview()
            else:
                _append_preview()

        if not self.dry_run:
            for e in entities:
                line = e.to_json_line() + "\n"
                if lock:
                    with lock:
                        with entity_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.entity_mentions_staged += 1
                else:
                    with entity_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.entity_mentions_staged += 1

        if field_updates:
            if self.dry_run:
                _bump("dry_run_writes")
            else:
                if lock:
                    with lock:
                        self._apply_thread_summary_enrichment(rel_path, field_updates, base_hash)
                else:
                    self._apply_thread_summary_enrichment(rel_path, field_updates, base_hash)

        return "enriched"

    def _process_one_finance(
        self,
        rel_path: str,
        *,
        scan_cache: VaultScanCache,
        cache: InferenceCache | None,
        prov: OllamaProvider | GeminiProvider,
        entity_path: Path,
        match_path: Path,
        lock: threading.Lock | None,
    ) -> str:
        def _bump(metric: str, n: int = 1) -> None:
            if lock:
                with lock:
                    setattr(self.metrics, metric, getattr(self.metrics, metric) + n)
            else:
                setattr(self.metrics, metric, getattr(self.metrics, metric) + n)

        fm = scan_cache.frontmatter_for_rel_path(rel_path)
        uid = str(fm.get("uid") or "")
        system_prompt = wf_finance.load_system_prompt()
        user_msg = wf_finance.render_user_message(fm)
        content_hash = wf_finance.finance_content_hash(fm)
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=self.model,
            prompt_version=wf_finance.ENRICH_FINANCE_PROMPT_VERSION,
            schema_version="finance_v1",
            temperature=0.0,
            seed=42,
        )

        parsed: dict[str, Any] | None = None
        llm_invoked = False
        if cache is not None:
            if lock:
                with lock:
                    hit = cache.get(cache_key)
            else:
                hit = cache.get(cache_key)
            if hit and isinstance(hit, dict) and "_error" not in hit:
                parsed = hit
                _bump("cache_hits")

        if parsed is None:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ]
            resp: LLMResponse = prov.chat_json(
                messages,
                model=self.model,
                temperature=0.0,
                seed=42,
                max_tokens=2048,
            )
            parsed = resp.parsed_json
            _bump("llm_calls")
            llm_invoked = True
            if cache is not None and parsed is not None and not self.dry_run:
                if lock:
                    with lock:
                        cache.put(
                            cache_key,
                            stage="enrich_finance",
                            model_id=self.model,
                            prompt_version=wf_finance.ENRICH_FINANCE_PROMPT_VERSION,
                            content_hash=content_hash,
                            response=wf_finance.response_to_cache_payload(parsed),
                            tokens=(resp.prompt_tokens, resp.completion_tokens),
                            latency_ms=resp.latency_ms,
                            run_id=self.run_id,
                        )
                else:
                    cache.put(
                        cache_key,
                        stage="enrich_finance",
                        model_id=self.model,
                        prompt_version=wf_finance.ENRICH_FINANCE_PROMPT_VERSION,
                        content_hash=content_hash,
                        response=wf_finance.response_to_cache_payload(parsed),
                        tokens=(resp.prompt_tokens, resp.completion_tokens),
                        latency_ms=resp.latency_ms,
                        run_id=self.run_id,
                    )

        if not parsed:
            log.warning("no JSON from LLM finance rel_path=%s uid=%s", rel_path, uid)
            _bump("errors")
            return "error"

        existing_tags = fm.get("provider_tags")
        ex_list = existing_tags if isinstance(existing_tags, list) else []
        field_updates, entities, matches = wf_finance.parse_finance_response(
            parsed,
            source_uid=uid,
            run_id=self.run_id,
            existing_provider_tags=[str(t) for t in ex_list],
            finance_frontmatter=fm,
        )

        if llm_invoked and str(parsed.get("counterparty_type") or "").strip():
            _bump("llm_nonempty_summary")

        if lock:
            with lock:
                self.metrics.enriched += 1
                if uid:
                    self.metrics.enriched_card_uids.append(uid)
        else:
            self.metrics.enriched += 1
            if uid:
                self.metrics.enriched_card_uids.append(uid)

        if self.dry_run:
            preview_line = (
                json.dumps(
                    {
                        "uid": uid,
                        "rel_path": rel_path,
                        "card_type": "finance",
                        "subject": str(fm.get("counterparty") or ""),
                        "parsed": parsed,
                        "field_updates": field_updates,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"

            def _append_preview() -> None:
                with preview_path.open("a", encoding="utf-8") as fh:
                    fh.write(preview_line)

            if lock:
                with lock:
                    _append_preview()
            else:
                _append_preview()

        if not self.dry_run:
            for e in entities:
                line = e.to_json_line() + "\n"
                if lock:
                    with lock:
                        with entity_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.entity_mentions_staged += 1
                else:
                    with entity_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.entity_mentions_staged += 1
            for m in matches:
                line = m.to_json_line() + "\n"
                if lock:
                    with lock:
                        with match_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.match_candidates_staged += 1
                else:
                    with match_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.match_candidates_staged += 1

        if field_updates:
            if self.dry_run:
                _bump("dry_run_writes")
            else:
                if lock:
                    with lock:
                        self._apply_finance_enrichment(rel_path, field_updates, content_hash)
                else:
                    self._apply_finance_enrichment(rel_path, field_updates, content_hash)

        return "enriched"

    def _process_one_calendar_event(
        self,
        rel_path: str,
        *,
        scan_cache: VaultScanCache,
        cache: InferenceCache | None,
        prov: OllamaProvider | GeminiProvider,
        entity_path: Path,
        lock: threading.Lock | None,
    ) -> str:
        """Place extraction only — no vault field writes."""

        def _bump(metric: str, n: int = 1) -> None:
            if lock:
                with lock:
                    setattr(self.metrics, metric, getattr(self.metrics, metric) + n)
            else:
                setattr(self.metrics, metric, getattr(self.metrics, metric) + n)

        fm = scan_cache.frontmatter_for_rel_path(rel_path)
        uid = str(fm.get("uid") or "")
        system_prompt = wf_calendar.load_system_prompt()
        user_msg = wf_calendar.render_user_message(fm)
        content_hash = wf_calendar.calendar_content_hash(fm)
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=self.model,
            prompt_version=wf_calendar.ENRICH_CALENDAR_EVENT_PROMPT_VERSION,
            schema_version="calendar_event_v2",
            temperature=0.0,
            seed=42,
        )

        parsed: dict[str, Any] | None = None
        llm_invoked = False
        if cache is not None:
            if lock:
                with lock:
                    hit = cache.get(cache_key)
            else:
                hit = cache.get(cache_key)
            if hit and isinstance(hit, dict) and "_error" not in hit:
                parsed = hit
                _bump("cache_hits")

        if parsed is None:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ]
            resp: LLMResponse = prov.chat_json(
                messages,
                model=self.model,
                temperature=0.0,
                seed=42,
                max_tokens=1024,
            )
            parsed = resp.parsed_json
            _bump("llm_calls")
            llm_invoked = True
            if cache is not None and parsed is not None and not self.dry_run:
                if lock:
                    with lock:
                        cache.put(
                            cache_key,
                            stage="enrich_calendar_event",
                            model_id=self.model,
                            prompt_version=wf_calendar.ENRICH_CALENDAR_EVENT_PROMPT_VERSION,
                            content_hash=content_hash,
                            response=wf_calendar.response_to_cache_payload(parsed),
                            tokens=(resp.prompt_tokens, resp.completion_tokens),
                            latency_ms=resp.latency_ms,
                            run_id=self.run_id,
                        )
                else:
                    cache.put(
                        cache_key,
                        stage="enrich_calendar_event",
                        model_id=self.model,
                        prompt_version=wf_calendar.ENRICH_CALENDAR_EVENT_PROMPT_VERSION,
                        content_hash=content_hash,
                        response=wf_calendar.response_to_cache_payload(parsed),
                        tokens=(resp.prompt_tokens, resp.completion_tokens),
                        latency_ms=resp.latency_ms,
                        run_id=self.run_id,
                    )

        if not parsed:
            log.warning("no JSON from LLM calendar_event rel_path=%s uid=%s", rel_path, uid)
            _bump("errors")
            return "error"

        entities = wf_calendar.parse_calendar_response(
            parsed,
            source_uid=uid,
            run_id=self.run_id,
        )

        if llm_invoked and entities:
            _bump("llm_nonempty_summary")

        if lock:
            with lock:
                self.metrics.enriched += 1
                if uid:
                    self.metrics.enriched_card_uids.append(uid)
        else:
            self.metrics.enriched += 1
            if uid:
                self.metrics.enriched_card_uids.append(uid)

        if self.dry_run:
            preview_line = (
                json.dumps(
                    {
                        "uid": uid,
                        "rel_path": rel_path,
                        "card_type": "calendar_event",
                        "subject": str(fm.get("title") or ""),
                        "parsed": parsed,
                        "field_updates": {},
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"

            def _append_preview() -> None:
                with preview_path.open("a", encoding="utf-8") as fh:
                    fh.write(preview_line)

            if lock:
                with lock:
                    _append_preview()
            else:
                _append_preview()

        if not self.dry_run:
            for e in entities:
                line = e.to_json_line() + "\n"
                if lock:
                    with lock:
                        with entity_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.entity_mentions_staged += 1
                else:
                    with entity_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.entity_mentions_staged += 1

        return "enriched"

    def _process_one_document(
        self,
        rel_path: str,
        *,
        cache: InferenceCache | None,
        prov: OllamaProvider | GeminiProvider,
        entity_path: Path,
        lock: threading.Lock | None,
    ) -> str:
        def _bump(metric: str, n: int = 1) -> None:
            if lock:
                with lock:
                    setattr(self.metrics, metric, getattr(self.metrics, metric) + n)
            else:
                setattr(self.metrics, metric, getattr(self.metrics, metric) + n)

        fm, body, _ = read_note(self.vault_path, rel_path)
        uid = str(fm.get("uid") or "")
        system_prompt = wf_document.load_system_prompt()
        user_msg = wf_document.render_user_message(fm, body)
        content_hash = wf_document.document_content_hash(fm, body)
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=self.model,
            prompt_version=wf_document.ENRICH_DOCUMENT_PROMPT_VERSION,
            schema_version="document_v3",
            temperature=0.0,
            seed=42,
        )

        parsed: dict[str, Any] | None = None
        llm_invoked = False
        if cache is not None:
            if lock:
                with lock:
                    hit = cache.get(cache_key)
            else:
                hit = cache.get(cache_key)
            if hit and isinstance(hit, dict) and "_error" not in hit:
                parsed = hit
                _bump("cache_hits")

        if parsed is None:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ]
            resp: LLMResponse = prov.chat_json(
                messages,
                model=self.model,
                temperature=0.0,
                seed=42,
                max_tokens=2048,
            )
            parsed = resp.parsed_json
            _bump("llm_calls")
            llm_invoked = True
            if cache is not None and parsed is not None and not self.dry_run:
                if lock:
                    with lock:
                        cache.put(
                            cache_key,
                            stage="enrich_document",
                            model_id=self.model,
                            prompt_version=wf_document.ENRICH_DOCUMENT_PROMPT_VERSION,
                            content_hash=content_hash,
                            response=wf_document.response_to_cache_payload(parsed),
                            tokens=(resp.prompt_tokens, resp.completion_tokens),
                            latency_ms=resp.latency_ms,
                            run_id=self.run_id,
                        )
                else:
                    cache.put(
                        cache_key,
                        stage="enrich_document",
                        model_id=self.model,
                        prompt_version=wf_document.ENRICH_DOCUMENT_PROMPT_VERSION,
                        content_hash=content_hash,
                        response=wf_document.response_to_cache_payload(parsed),
                        tokens=(resp.prompt_tokens, resp.completion_tokens),
                        latency_ms=resp.latency_ms,
                        run_id=self.run_id,
                    )

        if not parsed:
            log.warning("no JSON from LLM document rel_path=%s uid=%s", rel_path, uid)
            _bump("errors")
            return "error"

        field_updates, entities = wf_document.parse_document_response(
            parsed,
            fm=fm,
            body=body,
            source_uid=uid,
            run_id=self.run_id,
        )

        if llm_invoked and (
            field_updates
            or entities
            or str(parsed.get("description") or "").strip()
            or str(parsed.get("title") or "").strip()
        ):
            _bump("llm_nonempty_summary")

        if lock:
            with lock:
                self.metrics.enriched += 1
                if uid:
                    self.metrics.enriched_card_uids.append(uid)
        else:
            self.metrics.enriched += 1
            if uid:
                self.metrics.enriched_card_uids.append(uid)

        if self.dry_run:
            preview_line = (
                json.dumps(
                    {
                        "uid": uid,
                        "rel_path": rel_path,
                        "card_type": "document",
                        "subject": str(fm.get("title") or fm.get("filename") or ""),
                        "parsed": parsed,
                        "field_updates": field_updates,
                        "entity_mentions_count": len(entities),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"

            def _append_preview() -> None:
                with preview_path.open("a", encoding="utf-8") as fh:
                    fh.write(preview_line)

            if lock:
                with lock:
                    _append_preview()
            else:
                _append_preview()

        if not self.dry_run:
            for e in entities:
                line = e.to_json_line() + "\n"
                if lock:
                    with lock:
                        with entity_path.open("a", encoding="utf-8") as fh:
                            fh.write(line)
                        self.metrics.entity_mentions_staged += 1
                else:
                    with entity_path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    self.metrics.entity_mentions_staged += 1

        if field_updates:
            if self.dry_run:
                _bump("dry_run_writes")
            else:
                if lock:
                    with lock:
                        self._apply_document_enrichment(rel_path, field_updates, content_hash)
                else:
                    self._apply_document_enrichment(rel_path, field_updates, content_hash)

        return "enriched"

    def run(self) -> CardEnrichmentMetrics:
        if self.workflow == "email_thread":
            return self._run_email_thread()
        if self.workflow in ("imessage_thread", "beeper_thread"):
            return self._run_imessage_thread()
        if self.workflow == "finance":
            return self._run_finance()
        if self.workflow == "calendar_event":
            return self._run_calendar_event()
        if self.workflow == "document":
            return self._run_document()
        raise ValueError(
            f"unsupported workflow: {self.workflow!r} (expected email_thread, imessage_thread, "
            "beeper_thread, finance, calendar_event, document)"
        )

    def _run_email_thread(self) -> CardEnrichmentMetrics:
        prov = self._provider()
        if not prov.health_check():
            raise RuntimeError(
                "LLM health_check failed — for gemini set GEMINI_API_KEY; for ollama ensure model is pulled"
            )

        uid_filter: set[str] | None = None
        if self.uid_filter_file is not None:
            p = Path(self.uid_filter_file)
            if not p.is_file():
                raise FileNotFoundError(f"uid filter file not found: {p}")
            uid_filter = _load_uid_filter_file(p)

        cache: InferenceCache | None = None
        if self.cache_db is not None:
            cache = InferenceCache(self.cache_db)

        classify_idx: ClassifyIndex | None = None
        if self.classify_index_db is not None:
            cip = Path(self.classify_index_db)
            if cip.is_file():
                classify_idx = ClassifyIndex(cip)

        scan_cache = VaultScanCache.build_or_load(self.vault_path, tier=2, progress_every=0)
        stubs = load_email_stubs_for_vault(self.vault_path)
        thread_index = build_thread_index(stubs)

        by_type = scan_cache.rel_paths_by_type()
        paths = list(by_type.get("email_thread") or [])
        paths.sort(
            key=lambda p: str(
                scan_cache.frontmatter_for_rel_path(p).get("last_message_at")
                or scan_cache.frontmatter_for_rel_path(p).get("first_message_at")
                or ""
            ),
            reverse=True,
        )

        self.metrics.total_cards = len(paths)

        entity_path = self.staging_dir / "entity_mentions.jsonl"
        match_path = self.staging_dir / "match_candidates.jsonl"
        metrics_path = self.staging_dir / "_metrics.json"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"
            if preview_path.exists():
                preview_path.unlink()

        t0 = time.perf_counter()
        use_parallel = self.workers > 1 and self.limit is None
        if self.limit is not None and self.workers > 1:
            log.info(
                "enrich-cards: --limit=%s set — using sequential processing (ignore --workers=%s for stable preview)",
                self.limit,
                self.workers,
            )

        if use_parallel:
            log.info(
                "enrich-cards: parallel workers=%d (no --limit). "
                "Slowness before was mostly sequential Gemini HTTP; this overlaps requests.",
                self.workers,
            )

        eligible: list[str] = []
        for rel_path in paths:
            if self.vault_percent is not None:
                uid_s = str(scan_cache.frontmatter_for_rel_path(rel_path).get("uid") or "")
                if uid_s and not uid_in_vault_percent_sample(uid_s, float(self.vault_percent)):
                    self.metrics.skipped_sample += 1
                    continue

            fm = scan_cache.frontmatter_for_rel_path(rel_path)
            uid = str(fm.get("uid") or "")

            if uid_filter is not None and uid not in uid_filter:
                self.metrics.skipped_uid_filter += 1
                continue

            if not wf_email.gate_email_thread_card(fm):
                continue
            self.metrics.gated += 1

            if self.skip_populated:
                existing = str(fm.get("thread_summary") or "").strip()
                if existing:
                    self.metrics.skipped_populated += 1
                    continue

            tid = str(fm.get("gmail_thread_id") or "").strip()
            group = thread_index.get(tid, [])
            if not group:
                self.metrics.skipped_no_thread += 1
                log.warning("no email_message stubs for thread_id=%s uid=%s", tid, uid)
                continue

            raw_labels = fm.get("label_ids")
            thread_label_ids = (
                [str(x) for x in raw_labels] if isinstance(raw_labels, list) else None
            )
            ok, reason = wf_email.prefilter_email_thread(
                group,
                classify_idx,
                thread_label_ids=thread_label_ids,
                user_domains=frozenset(INTERNAL_DOMAINS),
            )
            if not ok:
                self.metrics.skipped_prefilter += 1
                self.metrics.prefilter_breakdown[reason] = (
                    self.metrics.prefilter_breakdown.get(reason, 0) + 1
                )
                continue

            eligible.append(rel_path)

        log.info(
            "enrich-cards: eligible_paths=%d (recent-first). limit=%s workers=%s parallel=%s",
            len(eligible),
            self.limit,
            self.workers,
            use_parallel,
        )

        lock: threading.Lock | None = threading.Lock() if use_parallel else None
        processed = 0

        def _run_one(rel: str) -> None:
            nonlocal processed
            self._process_one_email_thread(
                rel,
                scan_cache=scan_cache,
                thread_index=thread_index,
                cache=cache,
                prov=prov,
                entity_path=entity_path,
                match_path=match_path,
                lock=lock,
            )
            if lock:
                with lock:
                    processed += 1
                    pe = processed
            else:
                processed += 1
                pe = processed
            if self.progress_every > 0 and pe % self.progress_every == 0:
                log.info(
                    "enrich-cards progress enriched=%d writes=%d elapsed=%.1fs",
                    self.metrics.enriched,
                    self.metrics.vault_writes,
                    time.perf_counter() - t0,
                )
            if self.checkpoint_every and pe % self.checkpoint_every == 0:
                self._write_checkpoint(t0, pe)

        if use_parallel:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = [ex.submit(_run_one, rel) for rel in eligible]
                for fut in as_completed(futures):
                    fut.result()
        else:
            for rel_path in eligible:
                if self.limit is not None and self.metrics.enriched >= self.limit:
                    break
                _run_one(rel_path)

        if cache is not None:
            cache.close()
        if classify_idx is not None:
            classify_idx.close()

        self.metrics.workflow = self.workflow
        payload = {
            **self.metrics.to_dict(),
            "run_id": self.run_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "workers_effective": self.workers if use_parallel else 1,
            "dry_run": self.dry_run,
        }
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if self.dry_run:
            if self.metrics.enriched_card_uids:
                _write_dry_run_preview_review_md(self.staging_dir, self.metrics.enriched_card_uids)
        elif self.workflow == "email_thread" and self.metrics.enriched_card_uids:
            _write_step2b_review_md(
                self.vault_path,
                self.staging_dir,
                scan_cache,
                self.metrics.enriched_card_uids,
                self.metrics.to_dict(),
            )

        return self.metrics

    def _run_finance(self) -> CardEnrichmentMetrics:
        prov = self._provider()
        if not prov.health_check():
            raise RuntimeError(
                "LLM health_check failed — for gemini set GEMINI_API_KEY; for ollama ensure model is pulled"
            )

        uid_filter: set[str] | None = None
        if self.uid_filter_file is not None:
            p = Path(self.uid_filter_file)
            if not p.is_file():
                raise FileNotFoundError(f"uid filter file not found: {p}")
            uid_filter = _load_uid_filter_file(p)

        cache: InferenceCache | None = None
        if self.cache_db is not None:
            cache = InferenceCache(self.cache_db)

        scan_cache = VaultScanCache.build_or_load(self.vault_path, tier=2, progress_every=0)
        by_type = scan_cache.rel_paths_by_type()
        paths = list(by_type.get("finance") or [])
        paths.sort(
            key=lambda p: str(scan_cache.frontmatter_for_rel_path(p).get("created") or ""),
            reverse=True,
        )

        self.metrics.total_cards = len(paths)

        entity_path = self.staging_dir / "entity_mentions.jsonl"
        match_path = self.staging_dir / "match_candidates.jsonl"
        metrics_path = self.staging_dir / "_metrics.json"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"
            if preview_path.exists():
                preview_path.unlink()

        t0 = time.perf_counter()
        use_parallel = self.workers > 1 and self.limit is None
        if self.limit is not None and self.workers > 1:
            log.info(
                "enrich-cards: --limit=%s set — using sequential processing (ignore --workers=%s for stable preview)",
                self.limit,
                self.workers,
            )

        if use_parallel:
            log.info(
                "enrich-cards: parallel workers=%d (no --limit). "
                "Slowness before was mostly sequential Gemini HTTP; this overlaps requests.",
                self.workers,
            )

        eligible: list[str] = []
        for rel_path in paths:
            if self.vault_percent is not None:
                uid_s = str(scan_cache.frontmatter_for_rel_path(rel_path).get("uid") or "")
                if uid_s and not uid_in_vault_percent_sample(uid_s, float(self.vault_percent)):
                    self.metrics.skipped_sample += 1
                    continue

            fm = scan_cache.frontmatter_for_rel_path(rel_path)
            uid = str(fm.get("uid") or "")

            if uid_filter is not None and uid not in uid_filter:
                self.metrics.skipped_uid_filter += 1
                continue

            if not wf_finance.gate_finance_card(fm):
                continue
            self.metrics.gated += 1

            if self.skip_populated and wf_finance.has_counterparty_type_tag(fm):
                self.metrics.skipped_populated += 1
                continue

            ok, reason = wf_finance.prefilter_finance(fm)
            if not ok:
                self.metrics.skipped_prefilter += 1
                self.metrics.prefilter_breakdown[reason] = (
                    self.metrics.prefilter_breakdown.get(reason, 0) + 1
                )
                continue

            eligible.append(rel_path)

        log.info(
            "enrich-cards workflow=finance eligible_paths=%d (recent-first). limit=%s workers=%s parallel=%s",
            len(eligible),
            self.limit,
            self.workers,
            use_parallel,
        )

        lock: threading.Lock | None = threading.Lock() if use_parallel else None
        processed = 0

        def _run_one_fin(rel: str) -> None:
            nonlocal processed
            self._process_one_finance(
                rel,
                scan_cache=scan_cache,
                cache=cache,
                prov=prov,
                entity_path=entity_path,
                match_path=match_path,
                lock=lock,
            )
            if lock:
                with lock:
                    processed += 1
                    pe = processed
            else:
                processed += 1
                pe = processed
            if self.progress_every > 0 and pe % self.progress_every == 0:
                log.info(
                    "enrich-cards progress enriched=%d writes=%d elapsed=%.1fs",
                    self.metrics.enriched,
                    self.metrics.vault_writes,
                    time.perf_counter() - t0,
                )
            if self.checkpoint_every and pe % self.checkpoint_every == 0:
                self._write_checkpoint(t0, pe)

        if use_parallel:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = [ex.submit(_run_one_fin, rel) for rel in eligible]
                for fut in as_completed(futures):
                    fut.result()
        else:
            for rel_path in eligible:
                if self.limit is not None and self.metrics.enriched >= self.limit:
                    break
                _run_one_fin(rel_path)

        if cache is not None:
            cache.close()

        self.metrics.workflow = self.workflow
        payload = {
            **self.metrics.to_dict(),
            "run_id": self.run_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "workers_effective": self.workers if use_parallel else 1,
            "dry_run": self.dry_run,
        }
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if self.dry_run:
            if self.metrics.enriched_card_uids:
                _write_dry_run_preview_review_md(self.staging_dir, self.metrics.enriched_card_uids)

        return self.metrics

    def _run_calendar_event(self) -> CardEnrichmentMetrics:
        """Place extraction only — no vault field writes, no match candidates."""

        prov = self._provider()
        if not prov.health_check():
            raise RuntimeError(
                "LLM health_check failed — for gemini set GEMINI_API_KEY; for ollama ensure model is pulled"
            )

        uid_filter: set[str] | None = None
        if self.uid_filter_file is not None:
            p = Path(self.uid_filter_file)
            if not p.is_file():
                raise FileNotFoundError(f"uid filter file not found: {p}")
            uid_filter = _load_uid_filter_file(p)

        cache: InferenceCache | None = None
        if self.cache_db is not None:
            cache = InferenceCache(self.cache_db)

        scan_cache = VaultScanCache.build_or_load(self.vault_path, tier=2, progress_every=0)
        by_type = scan_cache.rel_paths_by_type()
        paths = list(by_type.get("calendar_event") or [])
        paths.sort(
            key=lambda p: str(scan_cache.frontmatter_for_rel_path(p).get("start_at") or ""),
            reverse=True,
        )

        self.metrics.total_cards = len(paths)

        entity_path = self.staging_dir / "entity_mentions.jsonl"
        metrics_path = self.staging_dir / "_metrics.json"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"
            if preview_path.exists():
                preview_path.unlink()

        t0 = time.perf_counter()
        use_parallel = self.workers > 1 and self.limit is None
        if self.limit is not None and self.workers > 1:
            log.info(
                "enrich-cards: --limit=%s set — using sequential processing (ignore --workers=%s for stable preview)",
                self.limit,
                self.workers,
            )

        if use_parallel:
            log.info(
                "enrich-cards: parallel workers=%d (no --limit). "
                "Slowness before was mostly sequential Gemini HTTP; this overlaps requests.",
                self.workers,
            )

        eligible: list[str] = []
        for rel_path in paths:
            if self.vault_percent is not None:
                uid_s = str(scan_cache.frontmatter_for_rel_path(rel_path).get("uid") or "")
                if uid_s and not uid_in_vault_percent_sample(uid_s, float(self.vault_percent)):
                    self.metrics.skipped_sample += 1
                    continue

            fm = scan_cache.frontmatter_for_rel_path(rel_path)
            uid = str(fm.get("uid") or "")

            if uid_filter is not None and uid not in uid_filter:
                self.metrics.skipped_uid_filter += 1
                continue

            if not wf_calendar.gate_calendar_event(fm):
                continue
            self.metrics.gated += 1

            ok, reason = wf_calendar.prefilter_calendar_event(fm)
            if not ok:
                self.metrics.skipped_prefilter += 1
                self.metrics.prefilter_breakdown[reason] = (
                    self.metrics.prefilter_breakdown.get(reason, 0) + 1
                )
                continue

            eligible.append(rel_path)

        log.info(
            "enrich-cards workflow=calendar_event eligible_paths=%d (recent-first). limit=%s workers=%s parallel=%s",
            len(eligible),
            self.limit,
            self.workers,
            use_parallel,
        )

        lock: threading.Lock | None = threading.Lock() if use_parallel else None
        processed = 0

        def _run_one_cal(rel: str) -> None:
            nonlocal processed
            self._process_one_calendar_event(
                rel,
                scan_cache=scan_cache,
                cache=cache,
                prov=prov,
                entity_path=entity_path,
                lock=lock,
            )
            if lock:
                with lock:
                    processed += 1
                    pe = processed
            else:
                processed += 1
                pe = processed
            if self.progress_every > 0 and pe % self.progress_every == 0:
                log.info(
                    "enrich-cards progress enriched=%d writes=%d elapsed=%.1fs",
                    self.metrics.enriched,
                    self.metrics.vault_writes,
                    time.perf_counter() - t0,
                )
            if self.checkpoint_every and pe % self.checkpoint_every == 0:
                self._write_checkpoint(t0, pe)

        if use_parallel:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = [ex.submit(_run_one_cal, rel) for rel in eligible]
                for fut in as_completed(futures):
                    fut.result()
        else:
            for rel_path in eligible:
                if self.limit is not None and self.metrics.enriched >= self.limit:
                    break
                _run_one_cal(rel_path)

        if cache is not None:
            cache.close()

        self.metrics.workflow = self.workflow
        payload = {
            **self.metrics.to_dict(),
            "run_id": self.run_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "workers_effective": self.workers if use_parallel else 1,
            "dry_run": self.dry_run,
        }
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if self.dry_run:
            if self.metrics.enriched_card_uids:
                _write_dry_run_preview_review_md(self.staging_dir, self.metrics.enriched_card_uids)

        return self.metrics

    def _run_document(self) -> CardEnrichmentMetrics:
        prov = self._provider()
        if not prov.health_check():
            raise RuntimeError(
                "LLM health_check failed — for gemini set GEMINI_API_KEY; for ollama ensure model is pulled"
            )

        uid_filter: set[str] | None = None
        if self.uid_filter_file is not None:
            p = Path(self.uid_filter_file)
            if not p.is_file():
                raise FileNotFoundError(f"uid filter file not found: {p}")
            uid_filter = _load_uid_filter_file(p)

        cache: InferenceCache | None = None
        if self.cache_db is not None:
            cache = InferenceCache(self.cache_db)

        scan_cache = VaultScanCache.build_or_load(self.vault_path, tier=2, progress_every=0)
        by_type = scan_cache.rel_paths_by_type()
        paths = list(by_type.get("document") or [])
        paths.sort(
            key=lambda p: str(scan_cache.frontmatter_for_rel_path(p).get("file_modified_at") or ""),
            reverse=True,
        )

        self.metrics.total_cards = len(paths)

        entity_path = self.staging_dir / "entity_mentions.jsonl"
        metrics_path = self.staging_dir / "_metrics.json"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"
            if preview_path.exists():
                preview_path.unlink()

        t0 = time.perf_counter()
        use_parallel = self.workers > 1 and self.limit is None
        if self.limit is not None and self.workers > 1:
            log.info(
                "enrich-cards: --limit=%s set — using sequential processing (ignore --workers=%s for stable preview)",
                self.limit,
                self.workers,
            )

        if use_parallel:
            log.info(
                "enrich-cards: parallel workers=%d (no --limit). "
                "Slowness before was mostly sequential Gemini HTTP; this overlaps requests.",
                self.workers,
            )

        eligible: list[str] = []
        for rel_path in paths:
            if self.vault_percent is not None:
                uid_s = str(scan_cache.frontmatter_for_rel_path(rel_path).get("uid") or "")
                if uid_s and not uid_in_vault_percent_sample(uid_s, float(self.vault_percent)):
                    self.metrics.skipped_sample += 1
                    continue

            fm = scan_cache.frontmatter_for_rel_path(rel_path)
            uid = str(fm.get("uid") or "")

            if uid_filter is not None and uid not in uid_filter:
                self.metrics.skipped_uid_filter += 1
                continue

            if not wf_document.gate_document(fm):
                continue
            self.metrics.gated += 1

            if self.skip_populated and wf_document.should_skip_populated_document(fm):
                self.metrics.skipped_populated += 1
                continue

            ok, reason = wf_document.prefilter_document(fm)
            if not ok:
                self.metrics.skipped_prefilter += 1
                self.metrics.prefilter_breakdown[reason] = (
                    self.metrics.prefilter_breakdown.get(reason, 0) + 1
                )
                continue

            eligible.append(rel_path)

        log.info(
            "enrich-cards workflow=document eligible_paths=%d (recent-first). limit=%s workers=%s parallel=%s",
            len(eligible),
            self.limit,
            self.workers,
            use_parallel,
        )

        lock: threading.Lock | None = threading.Lock() if use_parallel else None
        processed = 0

        def _run_one_doc(rel: str) -> None:
            nonlocal processed
            self._process_one_document(
                rel,
                cache=cache,
                prov=prov,
                entity_path=entity_path,
                lock=lock,
            )
            if lock:
                with lock:
                    processed += 1
                    pe = processed
            else:
                processed += 1
                pe = processed
            if self.progress_every > 0 and pe % self.progress_every == 0:
                log.info(
                    "enrich-cards progress enriched=%d writes=%d elapsed=%.1fs",
                    self.metrics.enriched,
                    self.metrics.vault_writes,
                    time.perf_counter() - t0,
                )
            if self.checkpoint_every and pe % self.checkpoint_every == 0:
                self._write_checkpoint(t0, pe)

        if use_parallel:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = [ex.submit(_run_one_doc, rel) for rel in eligible]
                for fut in as_completed(futures):
                    fut.result()
        else:
            for rel_path in eligible:
                if self.limit is not None and self.metrics.enriched >= self.limit:
                    break
                _run_one_doc(rel_path)

        if cache is not None:
            cache.close()

        self.metrics.workflow = self.workflow
        payload = {
            **self.metrics.to_dict(),
            "run_id": self.run_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "workers_effective": self.workers if use_parallel else 1,
            "dry_run": self.dry_run,
        }
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if self.dry_run:
            if self.metrics.enriched_card_uids:
                _write_dry_run_preview_review_md(self.staging_dir, self.metrics.enriched_card_uids)

        return self.metrics

    def _run_imessage_thread(self) -> CardEnrichmentMetrics:
        prov = self._provider()
        if not prov.health_check():
            raise RuntimeError(
                "LLM health_check failed — for gemini set GEMINI_API_KEY; for ollama ensure model is pulled"
            )

        uid_filter: set[str] | None = None
        if self.uid_filter_file is not None:
            p = Path(self.uid_filter_file)
            if not p.is_file():
                raise FileNotFoundError(f"uid filter file not found: {p}")
            uid_filter = _load_uid_filter_file(p)

        cache: InferenceCache | None = None
        if self.cache_db is not None:
            cache = InferenceCache(self.cache_db)

        scan_cache = VaultScanCache.build_or_load(self.vault_path, tier=2, progress_every=0)
        card_type = self.workflow

        msg_index = build_message_stub_index(scan_cache)
        name_resolver = build_participant_name_resolver(scan_cache)

        by_type = scan_cache.rel_paths_by_type()
        raw_paths = list(by_type.get(card_type) or [])
        thread_rows: list[tuple[str, dict[str, Any], str]] = []
        for rp in raw_paths:
            fm0 = scan_cache.frontmatter_for_rel_path(rp)
            sk = str(
                fm0.get("last_message_at")
                or fm0.get("first_message_at")
                or ""
            )
            thread_rows.append((sk, fm0, rp))
        thread_rows.sort(key=lambda t: t[0], reverse=True)

        self.metrics.total_cards = len(thread_rows)

        entity_path = self.staging_dir / "entity_mentions.jsonl"
        metrics_path = self.staging_dir / "_metrics.json"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            preview_path = self.staging_dir / "llm_enrichment_preview.jsonl"
            if preview_path.exists():
                preview_path.unlink()

        t0 = time.perf_counter()
        use_parallel = self.workers > 1 and self.limit is None
        if self.limit is not None and self.workers > 1:
            log.info(
                "enrich-cards: --limit=%s set — using sequential processing (ignore --workers=%s for stable preview)",
                self.limit,
                self.workers,
            )

        if use_parallel:
            log.info(
                "enrich-cards: parallel workers=%d (no --limit). "
                "Slowness before was mostly sequential Gemini HTTP; this overlaps requests.",
                self.workers,
            )

        log.info(
            "enrich-cards: scanning %d %s cards for eligibility (in-memory index)",
            len(thread_rows),
            card_type,
        )
        t_scan0 = time.perf_counter()
        eligible: list[str] = []
        for _sk, fm, rel_path in thread_rows:
            uid = str(fm.get("uid") or "")

            if self.vault_percent is not None:
                if uid and not uid_in_vault_percent_sample(uid, float(self.vault_percent)):
                    self.metrics.skipped_sample += 1
                    continue

            if uid_filter is not None and uid not in uid_filter:
                self.metrics.skipped_uid_filter += 1
                continue

            if not wf_imessage.gate_imessage_thread_card(fm):
                continue
            self.metrics.gated += 1

            if self.skip_populated:
                existing = str(fm.get("thread_summary") or "").strip()
                if existing:
                    self.metrics.skipped_populated += 1
                    continue

            stubs = wf_imessage.load_message_stub_frontmatters_for_thread(
                fm, msg_index=msg_index
            )
            if not stubs:
                self.metrics.skipped_no_thread += 1
                log.warning("no resolvable message cards for thread uid=%s rel=%s", uid, rel_path)
                continue

            ok, reason = wf_imessage.prefilter_imessage_thread(stubs)
            if not ok:
                self.metrics.skipped_prefilter += 1
                self.metrics.prefilter_breakdown[reason] = (
                    self.metrics.prefilter_breakdown.get(reason, 0) + 1
                )
                continue

            eligible.append(rel_path)

        log.info(
            "enrich-cards: eligibility scan done cards=%d elapsed=%.1fs",
            len(thread_rows),
            time.perf_counter() - t_scan0,
        )
        log.info(
            "enrich-cards workflow=%s eligible_paths=%d (recent-first). limit=%s workers=%s parallel=%s",
            card_type,
            len(eligible),
            self.limit,
            self.workers,
            use_parallel,
        )

        lock: threading.Lock | None = threading.Lock() if use_parallel else None
        processed = 0

        def _run_one_ims(rel: str) -> None:
            nonlocal processed
            self._process_one_imessage_thread(
                rel,
                card_type=card_type,
                scan_cache=scan_cache,
                msg_index=msg_index,
                name_resolver=name_resolver,
                cache=cache,
                prov=prov,
                entity_path=entity_path,
                lock=lock,
            )
            if lock:
                with lock:
                    processed += 1
                    pe = processed
            else:
                processed += 1
                pe = processed
            if self.progress_every > 0 and pe % self.progress_every == 0:
                log.info(
                    "enrich-cards progress enriched=%d writes=%d elapsed=%.1fs",
                    self.metrics.enriched,
                    self.metrics.vault_writes,
                    time.perf_counter() - t0,
                )
            if self.checkpoint_every and pe % self.checkpoint_every == 0:
                self._write_checkpoint(t0, pe)

        if use_parallel:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = [ex.submit(_run_one_ims, rel) for rel in eligible]
                for fut in as_completed(futures):
                    fut.result()
        else:
            for rel_path in eligible:
                if self.limit is not None and self.metrics.enriched >= self.limit:
                    break
                _run_one_ims(rel_path)

        if cache is not None:
            cache.close()

        self.metrics.workflow = self.workflow
        payload = {
            **self.metrics.to_dict(),
            "run_id": self.run_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "workers_effective": self.workers if use_parallel else 1,
            "dry_run": self.dry_run,
        }
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if self.dry_run:
            if self.metrics.enriched_card_uids:
                _write_dry_run_preview_review_md(self.staging_dir, self.metrics.enriched_card_uids)

        return self.metrics


def _write_dry_run_preview_review_md(staging_dir: Path, uids: list[str]) -> None:
    """Human-readable review from llm_enrichment_preview.jsonl (no vault reads)."""

    prev = staging_dir / "llm_enrichment_preview.jsonl"
    if not prev.exists():
        return
    by_uid: dict[str, dict[str, Any]] = {}
    for line in prev.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        u = str(row.get("uid") or "")
        if u:
            by_uid[u] = row

    lines: list[str] = [
        "# Phase 2.875 — dry run (LLM preview only)",
        "",
        "**No vault writes.** Source: `llm_enrichment_preview.jsonl`. Re-run without `--dry-run` to apply.",
        "",
        f"Cards in this run: **{len(uids)}**",
        "",
    ]
    vault_hint = "(dry run — paths are vault-relative)"
    for uid in uids:
        row = by_uid.get(uid)
        if not row:
            lines.extend([f"## {uid}", "", "_No preview row for this uid._", ""])
            continue
        subj = str(row.get("subject") or "").strip()
        rel = str(row.get("rel_path") or "")
        fu = row.get("field_updates") if isinstance(row.get("field_updates"), dict) else {}
        parsed = row.get("parsed") if isinstance(row.get("parsed"), dict) else {}
        card_type = str(row.get("card_type") or "")

        if card_type == "finance":
            cpt = str(parsed.get("counterparty_type") or "").strip()
            ptags = (fu or {}).get("provider_tags")
            tag_line = ""
            if isinstance(ptags, list) and ptags:
                tag_line = ", ".join(str(t) for t in ptags)
            lines.extend(
                [
                    f"## {subj or uid}",
                    "",
                    f"- **uid:** `{uid}`",
                    f"- **path:** `{rel}` {vault_hint}",
                    "- **card_type:** finance",
                    "",
                    "### counterparty_type (LLM)",
                    "",
                    cpt or "_(empty)_",
                    "",
                ]
            )
            if tag_line:
                lines.extend(
                    [
                        "### provider_tags (would be written)",
                        "",
                        tag_line,
                        "",
                    ]
                )
        elif card_type == "calendar_event":
            pl = parsed.get("place_extraction")
            lines.extend(
                [
                    f"## {subj or uid}",
                    "",
                    f"- **uid:** `{uid}`",
                    f"- **path:** `{rel}` {vault_hint}",
                    "- **card_type:** calendar_event",
                    f"- **location:** `{parsed.get('_input_location', '')}`",
                    "",
                    "### place_extraction",
                    "",
                    f"```json\n{json.dumps(pl, indent=2)}\n```" if isinstance(pl, dict) and pl else "null",
                    "",
                ]
            )
        elif card_type == "document":
            lines.extend(
                [
                    f"## {subj or uid}",
                    "",
                    f"- **uid:** `{uid}`",
                    f"- **path:** `{rel}` {vault_hint}",
                    "- **card_type:** document",
                    "",
                    "### field_updates (would be written)",
                    "",
                    f"```json\n{json.dumps(fu, indent=2)}\n```" if isinstance(fu, dict) and fu else "_(none)_",
                    "",
                ]
            )
        else:
            summary = str((fu or {}).get("thread_summary") or "").strip()
            lines.extend(
                [
                    f"## {subj or uid}",
                    "",
                    f"- **uid:** `{uid}`",
                    f"- **path:** `{rel}` {vault_hint}",
                    "",
                    "### thread_summary (would be written)",
                    "",
                    summary or "_(empty)_",
                    "",
                ]
            )
        ents = parsed.get("entity_mentions") or []
        if isinstance(ents, list) and ents:
            lines.append("### entity_mentions (from LLM)")
            lines.append("")
            for e in ents[:30]:
                if isinstance(e, dict):
                    lines.append(f"- **{e.get('type')}** {e.get('name')!r}")
            if len(ents) > 30:
                lines.append(f"- _… {len(ents) - 30} more_")
            lines.append("")
        cal = parsed.get("calendar_match")
        if cal:
            lines.extend(["### calendar_match", "", f"```json\n{json.dumps(cal, indent=2)}\n```", ""])
        ematch = parsed.get("email_match")
        if ematch:
            lines.extend(["### email_match", "", f"```json\n{json.dumps(ematch, indent=2)}\n```", ""])
        lines.extend(["---", ""])

    out = staging_dir / "phase_2.875_dry_run_preview.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("wrote dry-run review path=%s", out)


def _write_step2b_review_md(
    vault: Path,
    staging_dir: Path,
    scan_cache: VaultScanCache,
    uids: list[str],
    metrics_snapshot: dict[str, Any] | None = None,
) -> None:
    """Human review packet for Step 2b (email thread enrichment)."""

    uid_to_rel = scan_cache.uid_to_rel_path()
    entities_by_uid: dict[str, list[dict[str, Any]]] = {}
    em_path = staging_dir / "entity_mentions.jsonl"
    if em_path.exists():
        for line in em_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            u = str(row.get("source_card_uid") or "")
            if u:
                entities_by_uid.setdefault(u, []).append(row)

    matches_by_uid: dict[str, list[dict[str, Any]]] = {}
    mc_path = staging_dir / "match_candidates.jsonl"
    if mc_path.exists():
        for line in mc_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            u = str(row.get("source_card_uid") or "")
            if u:
                matches_by_uid.setdefault(u, []).append(row)

    lines: list[str] = [
        "# Phase 2.875 — Step 2b email thread enrichment (review)",
        "",
        "Open each **path** in your vault editor to compare `thread_summary` to the original thread bodies.",
        "",
        f"Cards enriched in this run: **{len(uids)}**",
        "",
    ]
    if metrics_snapshot:
        lines.extend(
            [
                "## Prefilter metrics (this run)",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| total_cards | {metrics_snapshot.get('total_cards')} |",
                f"| gated | {metrics_snapshot.get('gated')} |",
                f"| skipped_prefilter | {metrics_snapshot.get('skipped_prefilter')} |",
                f"| skipped_no_thread | {metrics_snapshot.get('skipped_no_thread')} |",
                f"| llm_calls | {metrics_snapshot.get('llm_calls')} |",
                f"| cache_hits | {metrics_snapshot.get('cache_hits')} |",
                f"| llm_nonempty_summary | {metrics_snapshot.get('llm_nonempty_summary')} |",
                f"| llm_yield_rate | {metrics_snapshot.get('llm_yield_rate')} |",
                "",
            ]
        )
        br = metrics_snapshot.get("prefilter_breakdown") or {}
        if isinstance(br, dict) and br:
            lines.append("**prefilter_breakdown:**")
            lines.append("")
            for k, v in sorted(br.items()):
                lines.append(f"- `{k}`: {v}")
            lines.append("")

    for uid in uids:
        rel = uid_to_rel.get(uid)
        if not rel:
            lines.extend([f"## {uid}", "", "_Card not found in scan cache._", ""])
            continue
        fm = scan_cache.frontmatter_for_rel_path(rel)
        subj = str(fm.get("subject") or "")
        parts = fm.get("participants") or []
        if not isinstance(parts, list):
            parts = []
        mc = int(fm.get("message_count") or 0)
        summary = str(fm.get("thread_summary") or "").strip()
        abs_path = vault / rel
        lines.extend(
            [
                f"## {subj or uid}",
                "",
                f"- **uid:** `{uid}`",
                f"- **path:** `{rel}`",
                f"- **open:** `{abs_path}`",
                f"- **participants:** {', '.join(str(p) for p in parts[:20])}",
                f"- **message_count:** {mc}",
                "",
                "### thread_summary",
                "",
                summary or "_(empty)_",
                "",
            ]
        )
        ents = entities_by_uid.get(uid, [])
        if ents:
            lines.append("### entity_mentions (staged)")
            lines.append("")
            for e in ents:
                lines.append(f"- **{e.get('entity_type')}** {e.get('raw_text')!r} — `{e.get('context')}`")
            lines.append("")
        mats = matches_by_uid.get(uid, [])
        if mats:
            lines.append("### calendar_match (staged)")
            lines.append("")
            for m in mats:
                lines.append(f"- `{m.get('match_signals')}`")
            lines.append("")
        lines.append("---")
        lines.append("")

    out = staging_dir / "phase_2.875_step2b_review.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("wrote review packet path=%s", out)
