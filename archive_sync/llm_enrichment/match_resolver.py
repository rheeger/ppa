"""Phase 3 Step 1 — cross-card match resolution (email_thread→calendar_event, finance→email_message)."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from archive_cli.vault_cache import (VaultScanCache,
                                     refresh_stored_vault_fingerprint)
from archive_sync.llm_enrichment.cache import (InferenceCache,
                                               build_inference_cache_key)
from archive_sync.llm_enrichment.staging_types import MatchCandidate
from archive_vault.llm_provider import (GeminiProvider, LLMResponse,
                                        OllamaProvider)
from archive_vault.provenance import (ProvenanceEntry, compute_input_hash,
                                      merge_provenance)
from archive_vault.schema import DETERMINISTIC_ONLY, validate_card_strict
from archive_vault.vault import read_note, write_card

try:
    from rapidfuzz import fuzz
except Exception:
    from difflib import SequenceMatcher

    class _FallbackFuzz:
        @staticmethod
        def token_sort_ratio(a: str, b: str) -> float:
            left = " ".join(sorted(a.split()))
            right = " ".join(sorted(b.split()))
            return SequenceMatcher(None, left, right).ratio() * 100

    fuzz = _FallbackFuzz()  # type: ignore[assignment]


log = logging.getLogger("ppa.match_resolver")

_PROMPT_FILE = Path(__file__).resolve().parent / "prompts" / "match_resolution.txt"
_MATCH_PROMPT_VERSION = "v1"


def _utc_today() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).date().isoformat()


def _parse_iso_day(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except (ValueError, TypeError):
        return None


def _load_candidates(paths: list[Path]) -> list[MatchCandidate]:
    out: list[MatchCandidate] = []
    for p in paths:
        if not p.is_file():
            log.warning("match_candidates file missing: %s", p)
            continue
        with p.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError as exc:
                    log.warning("skip bad JSONL line in %s: %s", p, exc)
                    continue
                if not isinstance(d, dict):
                    continue
                out.append(
                    MatchCandidate(
                        source_card_uid=d.get("source_card_uid", ""),
                        source_card_type=d.get("source_card_type", ""),
                        workflow=d.get("workflow", ""),
                        target_card_type=d.get("target_card_type", ""),
                        match_signals=d.get("match_signals", {}),
                        field_to_write=d.get("field_to_write", ""),
                        confidence=float(d.get("confidence", 0.5)),
                        run_id=d.get("run_id", ""),
                    )
                )
    return out


def _date_proximity_score(d1: date | None, d2: date | None) -> float:
    """0.0–1.0 score based on day distance (0 days → 1.0, >30 days → 0.0)."""
    if d1 is None or d2 is None:
        return 0.0
    diff = abs((d1 - d2).days)
    if diff == 0:
        return 1.0
    if diff > 30:
        return 0.0
    return max(0.0, 1.0 - diff / 30.0)


def _email_jaccard(a: list[str], b: list[str]) -> float:
    sa = {e.lower().strip() for e in a if e}
    sb = {e.lower().strip() for e in b if e}
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _keyword_score(keywords: list[str], target_text: str) -> float:
    if not keywords or not target_text:
        return 0.0
    target_lower = target_text.lower()
    hits = sum(1 for kw in keywords if kw.lower() in target_lower)
    return hits / len(keywords)


def _build_date_index(
    fm_map: dict[str, dict[str, Any]],
    date_field: str,
) -> dict[str, list[str]]:
    """Map YYYY-MM-DD → [uid, ...] for fast date-window lookups."""
    idx: dict[str, list[str]] = {}
    for uid, fm in fm_map.items():
        raw = str(fm.get(date_field) or "")[:10]
        d = _parse_iso_day(raw)
        if d is not None:
            key = d.isoformat()
            idx.setdefault(key, []).append(uid)
    return idx


def _date_window_uids(
    date_index: dict[str, list[str]],
    center: date | None,
    days: int = 14,
) -> set[str]:
    """Return UIDs within ±days of center date."""
    if center is None:
        return set()
    out: set[str] = set()
    for delta in range(-days, days + 1):
        key = (center + timedelta(days=delta)).isoformat()
        for uid in date_index.get(key, []):
            out.add(uid)
    return out


def _build_keyword_index(
    fm_map: dict[str, dict[str, Any]],
    text_fields: list[str],
) -> dict[str, set[str]]:
    """Map lowercase token → {uid, ...} for keyword pre-filtering."""
    idx: dict[str, set[str]] = {}
    for uid, fm in fm_map.items():
        for field in text_fields:
            text = str(fm.get(field) or "").lower()
            for token in text.split():
                token = token.strip(".,;:!?\"'()[]{}").lower()
                if len(token) >= 3:
                    idx.setdefault(token, set()).add(uid)
    return idx


def _keyword_filter_uids(
    keyword_index: dict[str, set[str]],
    keywords: list[str],
) -> set[str]:
    """Return UIDs that match any keyword token."""
    out: set[str] = set()
    for kw in keywords:
        for token in kw.lower().split():
            token = token.strip(".,;:!?\"'()[]{}").lower()
            if token in keyword_index:
                out |= keyword_index[token]
    return out


class MatchResolver:
    def __init__(
        self,
        vault_path: str,
        provider: OllamaProvider | GeminiProvider,
        cache: InferenceCache | None,
        *,
        match_threshold: float = 0.55,
        finance_match_threshold: float | None = None,
        ambiguity_margin: float = 0.12,
        dry_run: bool = False,
        run_id: str = "phase3-match",
        model: str = "",
    ):
        self.vault_path = vault_path
        self.provider = provider
        self.cache = cache
        self.match_threshold = match_threshold
        self.finance_match_threshold = (
            float(finance_match_threshold)
            if finance_match_threshold is not None
            else 0.47
        )
        self.ambiguity_margin = ambiguity_margin
        self.dry_run = dry_run
        self.run_id = run_id
        self.model = model
        self._prompt: str | None = None

    def _load_prompt(self) -> str:
        if self._prompt is None:
            self._prompt = _PROMPT_FILE.read_text(encoding="utf-8")
        return self._prompt

    def _score_thread_calendar(
        self,
        mc: MatchCandidate,
        target_fm: dict[str, Any],
    ) -> float:
        """Score email_thread → calendar_event match."""
        signals = mc.match_signals
        kws = signals.get("title_keywords") or []
        approx_date = _parse_iso_day(signals.get("approximate_date"))
        attendee_emails = signals.get("attendee_emails") or []

        title = str(target_fm.get("title") or target_fm.get("subject") or "")
        kw_score = _keyword_score(kws, title)
        title_fuzz = fuzz.token_sort_ratio(" ".join(kws), title) / 100.0 if kws else 0.0

        start_date = _parse_iso_day(str(target_fm.get("start_at") or ""))
        date_score = _date_proximity_score(approx_date, start_date)

        target_attendees = target_fm.get("attendee_emails") or []
        email_score = _email_jaccard(attendee_emails, target_attendees)

        return (
            0.20 * kw_score
            + 0.15 * title_fuzz
            + 0.35 * date_score
            + 0.30 * email_score
        )

    def _score_finance_email(
        self,
        mc: MatchCandidate,
        target_fm: dict[str, Any],
        *,
        signals: dict[str, Any] | None = None,
    ) -> float:
        """Score finance → email_message match."""
        sig = signals if signals is not None else mc.match_signals
        kws = sig.get("counterparty_keywords") or []
        amount = sig.get("amount")
        date_range = sig.get("date_range") or []

        subject = str(target_fm.get("subject") or "")
        kw_score = _keyword_score(kws, subject)
        body_snippet = str(target_fm.get("snippet") or "")
        snippet_score = _keyword_score(kws, body_snippet)

        sent_date = _parse_iso_day(str(target_fm.get("sent_at") or ""))
        if date_range and len(date_range) >= 1:
            range_mid = _parse_iso_day(date_range[0])
        else:
            range_mid = None
        date_score = _date_proximity_score(range_mid, sent_date)

        amount_score = 0.0
        if amount is not None:
            body_text = f"{subject} {body_snippet}".lower()
            amt_str = f"{abs(float(amount)):.2f}"
            if amt_str in body_text or f"${amt_str}" in body_text:
                amount_score = 1.0

        return (
            0.25 * max(kw_score, snippet_score)
            + 0.35 * date_score
            + 0.10 * (fuzz.token_sort_ratio(" ".join(kws), subject) / 100.0 if kws else 0.0)
            + 0.30 * amount_score
        )

    def _disambiguate_llm(
        self,
        mc: MatchCandidate,
        top_targets: list[tuple[str, float, dict[str, Any]]],
    ) -> tuple[str | None, float, str]:
        """LLM disambiguation for ambiguous matches. Returns (uid, confidence, reason)."""
        prompt = self._load_prompt()
        source_desc = f"Source: {mc.source_card_type} uid={mc.source_card_uid}\nSignals: {json.dumps(mc.match_signals, default=str)}"
        candidates_desc = "\n".join(
            f"  Candidate uid={uid} score={score:.3f}: title={fm.get('title') or fm.get('subject', '')} "
            f"date={fm.get('start_at') or fm.get('sent_at', '')} "
            f"attendees={fm.get('attendee_emails', [])}"
            for uid, score, fm in top_targets[:5]
        )
        user_msg = f"{source_desc}\n\nCandidates:\n{candidates_desc}"

        content_hash = compute_input_hash({"source": mc.source_card_uid, "targets": [t[0] for t in top_targets[:5]]})
        cache_key = build_inference_cache_key(
            content_hash=content_hash,
            model_id=self.model,
            prompt_version=_MATCH_PROMPT_VERSION,
            schema_version="match_resolution_v1",
            temperature=0.0,
            seed=42,
        )

        parsed: dict[str, Any] | None = None
        if self.cache is not None:
            hit = self.cache.get(cache_key)
            if hit and isinstance(hit, dict) and "_error" not in hit:
                parsed = hit

        if parsed is None:
            messages = [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_msg},
            ]
            resp: LLMResponse = self.provider.chat_json(
                messages, model=self.model, temperature=0.0, seed=42, max_tokens=512,
            )
            parsed = resp.parsed_json
            if self.cache is not None and parsed is not None:
                self.cache.put(
                    cache_key,
                    stage="match_resolution",
                    model_id=self.model,
                    prompt_version=_MATCH_PROMPT_VERSION,
                    content_hash=content_hash,
                    response=parsed,
                    tokens=(resp.prompt_tokens, resp.completion_tokens),
                    latency_ms=resp.latency_ms,
                    run_id=self.run_id,
                )

        if not parsed:
            return None, 0.0, "llm_no_response"
        uid = parsed.get("choice_uid")
        conf = float(parsed.get("confidence", 0.0))
        reason = str(parsed.get("reason", ""))
        if uid and isinstance(uid, str):
            valid_uids = {t[0] for t in top_targets}
            if uid not in valid_uids:
                return None, 0.0, f"llm_chose_invalid_uid: {uid}"
        return uid or None, conf, reason

    def _write_wikilink(
        self,
        source_uid: str,
        source_rel: str,
        field: str,
        target_slug: str,
        resolution_method: str,
    ) -> bool:
        """Append a [[wikilink]] to a field on a source card."""
        fm, body, existing_prov = read_note(self.vault_path, source_rel)
        wikilink = f"[[{target_slug}]]"

        _LIST_FIELDS = {"calendar_events", "source_threads", "source_messages"}
        existing = fm.get(field)

        if field in _LIST_FIELDS:
            if isinstance(existing, list):
                if wikilink in existing:
                    return False
                existing.append(wikilink)
                fm[field] = existing
            else:
                fm[field] = [wikilink]
        else:
            if isinstance(existing, str) and existing == wikilink:
                return False
            fm[field] = wikilink

        card = validate_card_strict(fm)

        from archive_vault.provenance import PROVENANCE_EXEMPT_FIELDS
        if field not in PROVENANCE_EXEMPT_FIELDS:
            method = "deterministic" if field in DETERMINISTIC_ONLY else "llm"
            ih = compute_input_hash({"run_id": self.run_id, "resolution_method": resolution_method, "target": target_slug})
            incoming = {
                field: ProvenanceEntry(
                    source="match_resolution",
                    date=_utc_today(),
                    method=method,
                    model=self.model if resolution_method == "llm" else "",
                    input_hash=ih[:16],
                ),
            }
            prov = merge_provenance(existing_prov, incoming)
        else:
            prov = existing_prov

        write_card(self.vault_path, source_rel, card, body, prov)
        return True

    def resolve_all(
        self,
        candidates_jsonl_paths: list[Path],
        *,
        progress_every: int = 50,
        vault_cache_progress_every: int = 5000,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        candidates = _load_candidates(candidates_jsonl_paths)
        log.info("match_resolver loaded %d candidates from %d files", len(candidates), len(candidates_jsonl_paths))

        scan_cache = VaultScanCache.build_or_load(
            self.vault_path, tier=2, progress_every=vault_cache_progress_every,
        )
        uid_to_rel = scan_cache.uid_to_rel_path()
        note_count = scan_cache.note_count()
        log.info("match_resolver vault cache loaded: %d notes, %d uid mappings", note_count, len(uid_to_rel))

        by_type = scan_cache.rel_paths_by_type()
        target_fm_cache: dict[str, dict[str, dict[str, Any]]] = {}
        for ttype in ("calendar_event", "email_message"):
            paths = by_type.get(ttype) or []
            fm_map: dict[str, dict[str, Any]] = {}
            for rp in paths:
                fm = scan_cache.frontmatter_for_rel_path(rp)
                uid = str(fm.get("uid") or "")
                if uid:
                    fm_map[uid] = fm
            target_fm_cache[ttype] = fm_map
            log.info("match_resolver loaded %d %s target cards", len(fm_map), ttype)

        t_idx = time.perf_counter()
        cal_date_idx = _build_date_index(target_fm_cache.get("calendar_event", {}), "start_at")
        email_date_idx = _build_date_index(target_fm_cache.get("email_message", {}), "sent_at")
        email_kw_idx = _build_keyword_index(
            target_fm_cache.get("email_message", {}), ["subject", "snippet"],
        )
        log.info(
            "match_resolver built pre-filter indexes in %.1fs (cal_dates=%d email_dates=%d email_kw_tokens=%d)",
            time.perf_counter() - t_idx, len(cal_date_idx), len(email_date_idx), len(email_kw_idx),
        )

        stats: dict[str, int] = {
            "total": len(candidates),
            "confident": 0,
            "llm": 0,
            "no_match": 0,
            "written": 0,
            "already_linked": 0,
            "llm_calls": 0,
            "errors": 0,
        }
        errors: list[str] = []

        # ── Phase A: deterministic scoring (no LLM, no vault writes yet) ──
        AmbiguousItem = tuple  # (mc, scored_top5, source_rel)
        ambiguous_queue: list[AmbiguousItem] = []
        write_queue: list[tuple[MatchCandidate, str, str, str]] = []  # (mc, source_rel, chosen_uid, method)

        log.info("match_resolver Phase A: deterministic scoring of %d candidates", len(candidates))
        for i, mc in enumerate(candidates):
            if progress_every > 0 and (i + 1) % progress_every == 0:
                log.info(
                    "match_resolver Phase A %d/%d confident=%d ambiguous=%d no_match=%d errors=%d",
                    i + 1, len(candidates),
                    stats["confident"], len(ambiguous_queue), stats["no_match"], stats["errors"],
                )

            source_rel = uid_to_rel.get(mc.source_card_uid, "")
            if not source_rel:
                stats["errors"] += 1
                errors.append(f"source_uid_not_found: {mc.source_card_uid}")
                continue

            target_fms = target_fm_cache.get(mc.target_card_type, {})
            if not target_fms:
                stats["errors"] += 1
                errors.append(f"no_targets_for_type: {mc.target_card_type}")
                continue

            signals = mc.match_signals
            eff_finance_signals: dict[str, Any] | None = None
            if mc.target_card_type == "calendar_event":
                approx = _parse_iso_day(signals.get("approximate_date"))
                candidate_uids = _date_window_uids(cal_date_idx, approx, days=14)
                if not candidate_uids:
                    candidate_uids = _date_window_uids(cal_date_idx, approx, days=30)
            elif mc.target_card_type == "email_message":
                eff_signals = dict(mc.match_signals)
                dr = list(mc.match_signals.get("date_range") or [])
                if not dr:
                    src_fm = scan_cache.frontmatter_for_rel_path(source_rel)
                    for key in ("created", "activity_at"):
                        raw = str(src_fm.get(key) or "").strip()
                        day = _parse_iso_day(raw)
                        if day:
                            dr = [day.isoformat(), day.isoformat()]
                            eff_signals = {**mc.match_signals, "date_range": dr}
                            break
                date_range = eff_signals.get("date_range") or []
                range_center = _parse_iso_day(date_range[0]) if date_range else None
                date_uids = _date_window_uids(email_date_idx, range_center, days=21)
                kw_uids = _keyword_filter_uids(
                    email_kw_idx, eff_signals.get("counterparty_keywords") or [],
                )
                candidate_uids = date_uids | kw_uids
                if not candidate_uids:
                    candidate_uids = _date_window_uids(email_date_idx, range_center, days=45)
                eff_finance_signals = eff_signals
            else:
                candidate_uids = set(target_fms.keys())

            scored: list[tuple[str, float, dict[str, Any]]] = []
            for target_uid in candidate_uids:
                target_fm = target_fms.get(target_uid)
                if target_fm is None:
                    continue
                if mc.target_card_type == "calendar_event":
                    score = self._score_thread_calendar(mc, target_fm)
                elif mc.target_card_type == "email_message":
                    score = self._score_finance_email(
                        mc, target_fm, signals=eff_finance_signals,
                    )
                else:
                    continue
                if score > 0.05:
                    scored.append((target_uid, score, target_fm))

            scored.sort(key=lambda x: x[1], reverse=True)

            thresh = (
                self.finance_match_threshold
                if mc.target_card_type == "email_message"
                else self.match_threshold
            )
            if not scored or scored[0][1] < thresh:
                stats["no_match"] += 1
                continue

            best_uid, best_score, best_fm = scored[0]
            second_score = scored[1][1] if len(scored) > 1 else 0.0
            gap = best_score - second_score

            if gap >= self.ambiguity_margin:
                stats["confident"] += 1
                write_queue.append((mc, source_rel, best_uid, "deterministic"))
            else:
                ambiguous_queue.append((mc, scored[:5], source_rel))

        log.info(
            "match_resolver Phase A complete: confident=%d ambiguous=%d no_match=%d errors=%d elapsed=%.1fs",
            stats["confident"], len(ambiguous_queue), stats["no_match"], stats["errors"],
            time.perf_counter() - t0,
        )

        # ── Phase B: LLM disambiguation (parallel workers) ──
        if ambiguous_queue and not self.dry_run:
            import threading
            from concurrent.futures import ThreadPoolExecutor, as_completed

            llm_lock = threading.Lock()
            log.info("match_resolver Phase B: LLM disambiguation of %d ambiguous candidates with parallel workers", len(ambiguous_queue))

            def _resolve_one_llm(item: AmbiguousItem) -> tuple[MatchCandidate, str, str | None, str]:
                mc_item, scored_top5, src_rel = item
                chosen_uid_llm, conf, reason = self._disambiguate_llm(mc_item, scored_top5)
                with llm_lock:
                    stats["llm_calls"] += 1
                if chosen_uid_llm and conf >= 0.4:
                    return mc_item, src_rel, chosen_uid_llm, "llm"
                return mc_item, src_rel, None, "llm_no_match"

            llm_results: list[tuple[MatchCandidate, str, str | None, str]] = []
            workers = min(16, len(ambiguous_queue))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(_resolve_one_llm, item) for item in ambiguous_queue]
                done_count = 0
                for fut in as_completed(futures):
                    result_item = fut.result()
                    llm_results.append(result_item)
                    done_count += 1
                    if progress_every > 0 and done_count % progress_every == 0:
                        log.info(
                            "match_resolver Phase B LLM progress %d/%d",
                            done_count, len(ambiguous_queue),
                        )

            for mc_item, src_rel, chosen_uid, method in llm_results:
                stats["llm"] += 1
                if chosen_uid:
                    write_queue.append((mc_item, src_rel, chosen_uid, method))
                else:
                    stats["no_match"] += 1

            log.info(
                "match_resolver Phase B complete: llm_resolved=%d llm_no_match=%d llm_calls=%d elapsed=%.1fs",
                sum(1 for _, _, uid, _ in llm_results if uid),
                sum(1 for _, _, uid, _ in llm_results if not uid),
                stats["llm_calls"],
                time.perf_counter() - t0,
            )
        elif ambiguous_queue and self.dry_run:
            stats["llm"] = len(ambiguous_queue)
            stats["llm_calls"] = len(ambiguous_queue)
            log.info("match_resolver Phase B (dry-run): %d ambiguous candidates would need LLM", len(ambiguous_queue))

        # ── Phase C: write all resolved links to vault ──
        log.info("match_resolver Phase C: writing %d links to vault (dry_run=%s)", len(write_queue), self.dry_run)
        for wi, (mc_w, source_rel_w, chosen_uid_w, method_w) in enumerate(write_queue):
            if progress_every > 0 and (wi + 1) % progress_every == 0:
                log.info("match_resolver Phase C write progress %d/%d", wi + 1, len(write_queue))

            target_rel = uid_to_rel.get(chosen_uid_w, "")
            if not target_rel:
                stats["errors"] += 1
                errors.append(f"target_uid_not_found: {chosen_uid_w}")
                continue

            target_slug = Path(target_rel).stem

            if self.dry_run:
                stats["written"] += 1
                continue

            try:
                wrote = self._write_wikilink(
                    mc_w.source_card_uid, source_rel_w,
                    mc_w.field_to_write, target_slug, method_w,
                )
                if wrote:
                    stats["written"] += 1
                else:
                    stats["already_linked"] += 1

                if mc_w.target_card_type == "calendar_event" and mc_w.field_to_write == "calendar_events":
                    source_slug = Path(source_rel_w).stem
                    self._write_wikilink(
                        chosen_uid_w, target_rel,
                        "source_threads", source_slug, method_w,
                    )
            except Exception as exc:
                stats["errors"] += 1
                errors.append(f"write_error: {mc_w.source_card_uid} → {chosen_uid_w}: {exc}")

        elapsed = time.perf_counter() - t0
        log.info(
            "match_resolver complete total=%d confident=%d llm=%d no_match=%d "
            "written=%d already_linked=%d errors=%d elapsed=%.1fs",
            stats["total"], stats["confident"], stats["llm"], stats["no_match"],
            stats["written"], stats["already_linked"], stats["errors"], elapsed,
        )

        return {
            **stats,
            "elapsed_s": round(elapsed, 3),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "errors_detail": errors[:100],
        }


def run_match_resolution(
    vault_path: str,
    staging_root: Path,
    *,
    provider_kind: str = "gemini",
    model: str = "gemini-2.5-flash-lite",
    base_url: str = "http://localhost:11434",
    cache_db: Path | None = None,
    dry_run: bool = False,
    run_id: str = "phase3-match",
    progress_every: int = 50,
    vault_cache_progress_every: int = 5000,
) -> dict[str, Any]:
    """Entry point called by CLI handler."""
    if provider_kind == "gemini":
        provider: OllamaProvider | GeminiProvider = GeminiProvider()
    else:
        provider = OllamaProvider(base_url=base_url)

    if not provider.health_check():
        raise RuntimeError(
            "LLM health_check failed — for gemini set GEMINI_API_KEY; for ollama ensure model is pulled"
        )

    cache: InferenceCache | None = None
    if cache_db is not None:
        cache = InferenceCache(cache_db)

    resolver = MatchResolver(
        vault_path,
        provider,
        cache,
        dry_run=dry_run,
        run_id=run_id,
        model=model,
    )

    candidates_paths: list[Path] = []
    staging_root = Path(staging_root)
    for subdir in sorted(staging_root.iterdir()) if staging_root.is_dir() else []:
        mc_file = subdir / "match_candidates.jsonl"
        if mc_file.is_file():
            candidates_paths.append(mc_file)

    if not candidates_paths:
        log.warning("match_resolver: no match_candidates.jsonl files found in %s", staging_root)
        return {"total": 0, "error": "no_candidates_found"}

    result = resolver.resolve_all(
        candidates_paths,
        progress_every=progress_every,
        vault_cache_progress_every=vault_cache_progress_every,
    )

    if cache is not None:
        cache.close()

    if not dry_run and result.get("written", 0) > 0:
        refresh_stored_vault_fingerprint(vault_path)
        log.info("match_resolver refreshed vault fingerprint after %d writes", result["written"])

    return result
