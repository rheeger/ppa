#!/usr/bin/env python3
"""Phase 2.75 Step 9 — run triage + extraction against ground truth; write scores + reports.

Reads JSON from ``build_enrichment_benchmark.py`` (``benchmark_set`` / ``negative_benchmark_set``,
``meta.vault``). For each Ollama model, runs the same pipeline as enrichment (triage → extract),
scores triage + field overlap vs expected cards, and writes::

  <output>/<model_slug>/scores.json
  <output>/<model_slug>/per_example_results.json
  <output>/<model_slug>/failures.json
  <output>/<model_slug>/improvements.json
  <output>/<model_slug>/summary.md

Gemma 4 sizes on Ollama map to ``e2b``, ``e4b``, ``26b``, ``31b`` — see ``docs/gemma4-local-models.md``.

Run from ``ppa/`` with Ollama up::

  .venv/bin/python scripts/run_enrichment_benchmark.py \\
    --ground-truth _benchmark/enrichment_ground_truth_10pct.json \\
    --models gemma4:31b \\
    --output _benchmark/results/

Use ``--limit-positives`` / ``--limit-negatives`` for a short smoke run.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from archive_mcp.vault_cache import VaultScanCache
from archive_sync.llm_enrichment.runner import run_thread_pipeline
from hfa.llm_provider import OllamaProvider


def _slug(model: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", model.strip()).strip("_") or "model"


def _norm_scalar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        try:
            return round(float(v), 4)
        except (TypeError, ValueError):
            return v
    if isinstance(v, str):
        t = v.strip().lower()
        return t
    if isinstance(v, list):
        return json.dumps(v, sort_keys=True, default=str)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, default=str)
    return v


def _field_score(expected: dict[str, Any], extracted: dict[str, Any]) -> tuple[float, int, int]:
    """Return (f1-like 0-1, matched_keys, total_expected_keys) for overlapping keys."""

    keys = [k for k in expected if k not in ("type", "uid") and expected[k] not in (None, "", [], {})]
    if not keys:
        return 1.0, 0, 0
    matched = 0
    for k in keys:
        if k not in extracted:
            continue
        if _norm_scalar(expected.get(k)) == _norm_scalar(extracted.get(k)):
            matched += 1
    precision = matched / max(len(keys), 1)
    return precision, matched, len(keys)


def _best_match_expected(
    expected_cards: list[dict[str, Any]],
    extracted_payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Greedy match by type + field overlap; returns one result dict per expected card."""

    unused = list(extracted_payloads)
    out: list[dict[str, Any]] = []
    for exp in expected_cards:
        et = str(exp.get("type") or "")
        best_i = -1
        best_s = -1.0
        for i, got in enumerate(unused):
            if str(got.get("type") or "") != et:
                continue
            s, _, _ = _field_score(exp, got)
            if s > best_s:
                best_s = s
                best_i = i
        if best_i >= 0:
            got = unused.pop(best_i)
            sc, mk, tk = _field_score(exp, got)
            out.append(
                {
                    "expected_type": et,
                    "matched": True,
                    "field_match_rate": sc,
                    "matched_fields": mk,
                    "expected_fields": tk,
                }
            )
        else:
            out.append(
                {
                    "expected_type": et,
                    "matched": False,
                    "field_match_rate": 0.0,
                    "matched_fields": 0,
                    "expected_fields": len(
                        [k for k in exp if exp[k] not in (None, "", [], {}) and k != "type"]
                    ),
                }
            )
    return out


def _flatten_bodies(source_emails: list[dict[str, Any]]) -> str:
    return "\n".join(str(e.get("body") or "") for e in source_emails)


def run_benchmark_for_model(
    *,
    model_id: str,
    vault: Path,
    positives: list[dict[str, Any]],
    negatives: list[dict[str, Any]],
    scan_cache: VaultScanCache,
    triage_model: str | None,
    extract_model: str | None,
    run_id: str,
) -> dict[str, Any]:
    provider = OllamaProvider(model=model_id)
    t_mod = triage_model or model_id
    x_mod = extract_model or model_id

    per_example: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    improvements: list[dict[str, Any]] = []

    t0 = time.perf_counter()
    pos_fn = 0  # triage skipped but should extract
    pos_ok = 0
    neg_fp = 0  # triage not skipped on negative thread

    ext_schema_ok = 0
    ext_with_cards = 0
    field_rates: list[float] = []

    for row in positives:
        tid = str(row.get("source_thread_id") or "")
        exp_cards = list(row.get("expected_cards") or [])
        ex: dict[str, Any] = {
            "thread_id": tid,
            "kind": "positive",
            "triage": None,
            "extraction": None,
            "match": None,
        }
        try:
            tri, er, doc = run_thread_pipeline(
                vault,
                tid,
                provider,
                scan_cache=scan_cache,
                inference_cache=None,
                triage_model=t_mod,
                extract_model=x_mod,
                run_id=run_id,
            )
        except Exception as exc:
            ex["error"] = str(exc)
            per_example.append(ex)
            continue

        ex["triage"] = {
            "skip": tri.skip,
            "classification": tri.classification,
            "card_types": tri.card_types,
            "confidence": tri.confidence,
            "reasoning": tri.reasoning[:500] if tri.reasoning else "",
        }
        if tri.reasoning == "_llm_error":
            pos_fn += 1
        elif tri.skip:
            pos_fn += 1
            failures.append(
                {
                    "thread_id": tid,
                    "reason": "triage_skipped_positive",
                    "expected_types": [c.get("type") for c in exp_cards],
                }
            )
        else:
            pos_ok += 1

        extracted_payloads: list[dict[str, Any]] = []
        if er is not None:
            for c in er.cards:
                d = dict(c.data)
                d["type"] = c.card_type
                extracted_payloads.append(d)
                if c.validated is not None:
                    ext_schema_ok += 1
            if er.cards:
                ext_with_cards += len(er.cards)

        ex["extraction"] = {
            "n_cards": len(extracted_payloads),
            "schema_validated": sum(1 for c in (er.cards if er else []) if c.validated is not None),
        }
        blob = _flatten_bodies(row.get("source_emails") or [])
        match_rows = _best_match_expected(exp_cards, extracted_payloads)
        ex["match"] = match_rows
        for m in match_rows:
            if m.get("matched"):
                field_rates.append(float(m.get("field_match_rate") or 0.0))
                if m["field_match_rate"] >= 0.85:
                    improvements.append({"thread_id": tid, "match": m})
                elif m["field_match_rate"] < 0.5:
                    failures.append({"thread_id": tid, "match": m, "kind": "low_field_overlap"})
        # crude hallucination proxy: values in extracted not substring of blob
        for pay in extracted_payloads:
            for k, v in pay.items():
                if k in ("type", "uid") or v in (None, "", [], {}):
                    continue
                s = str(v).strip()
                if len(s) > 3 and s.lower() not in blob.lower():
                    ex.setdefault("possible_hallucinations", []).append(f"{k}={s[:80]}")

        per_example.append(ex)

    for row in negatives:
        tid = str(row.get("source_thread_id") or "")
        exn: dict[str, Any] = {"thread_id": tid, "kind": "negative", "triage": None}
        try:
            tri, er, _doc = run_thread_pipeline(
                vault,
                tid,
                provider,
                scan_cache=scan_cache,
                inference_cache=None,
                triage_model=t_mod,
                extract_model=x_mod,
                run_id=run_id,
            )
        except Exception as exc:
            exn["error"] = str(exc)
            per_example.append(exn)
            continue
        exn["triage"] = {
            "skip": tri.skip,
            "classification": tri.classification,
            "card_types": tri.card_types,
        }
        n_cards = len(er.cards) if er else 0
        exn["extraction_n_cards"] = n_cards
        if not tri.skip:
            neg_fp += 1
            failures.append({"thread_id": tid, "reason": "triage_false_positive_on_negative"})
        if n_cards > 0:
            failures.append({"thread_id": tid, "reason": "cards_on_negative", "n_cards": n_cards})
        per_example.append(exn)

    elapsed = time.perf_counter() - t0
    n_pos = len(positives)
    n_neg = len(negatives)
    total_threads = n_pos + n_neg

    scores = {
        "model": model_id,
        "triage_model": t_mod,
        "extract_model": x_mod,
        "vault": str(vault),
        "threads": {
            "positives": n_pos,
            "negatives": n_neg,
            "total": total_threads,
            "wall_clock_seconds": round(elapsed, 3),
            "threads_per_minute": round(total_threads / (elapsed / 60.0), 4) if elapsed > 0 else 0.0,
        },
        "triage": {
            "positive_not_skipped_rate": round(pos_ok / n_pos, 4) if n_pos else None,
            "positive_false_negative_count": pos_fn,
            "negative_triage_false_positive_rate": round(neg_fp / n_neg, 4) if n_neg else None,
        },
        "extraction": {
            "extracted_cards_total": ext_with_cards,
            "schema_validated_cards": ext_schema_ok,
            "mean_field_match_on_matched_positives": round(sum(field_rates) / len(field_rates), 4)
            if field_rates
            else None,
        },
    }

    return {
        "scores": scores,
        "per_example_results": per_example,
        "failures": failures,
        "improvements": improvements,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ground-truth", type=Path, default=Path("_benchmark/enrichment_ground_truth_10pct.json"))
    ap.add_argument("--vault", type=Path, default=None, help="Override meta.vault in JSON")
    ap.add_argument("--output", type=Path, default=Path("_benchmark/results"))
    ap.add_argument(
        "--models",
        type=str,
        required=True,
        help="Comma-separated Ollama model names (Gemma 4 family), e.g. gemma4:e4b,gemma4:26b,gemma4:31b",
    )
    ap.add_argument("--triage-model", type=str, default=None, help="Override triage model for all runs")
    ap.add_argument("--extract-model", type=str, default=None, help="Override extraction model for all runs")
    ap.add_argument("--limit-positives", type=int, default=0, help="Cap positive threads (0 = all)")
    ap.add_argument("--limit-negatives", type=int, default=0, help="Cap negative threads (0 = all)")
    ap.add_argument("--run-id", type=str, default="benchmark-1")
    args = ap.parse_args()

    gt_path = args.ground_truth.resolve()
    if not gt_path.is_file():
        raise SystemExit(f"ground truth not found: {gt_path}")

    data = json.loads(gt_path.read_text(encoding="utf-8"))
    meta = data.get("meta") or {}
    vault = Path(args.vault or meta.get("vault") or "").resolve()
    if not vault.is_dir():
        raise SystemExit(f"vault not found: {vault} (pass --vault)")

    positives = list(data.get("benchmark_set") or [])
    negatives = list(data.get("negative_benchmark_set") or [])
    if args.limit_positives > 0:
        positives = positives[: args.limit_positives]
    if args.limit_negatives > 0:
        negatives = negatives[: args.limit_negatives]

    scan_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)

    model_ids = [m.strip() for m in args.models.split(",") if m.strip()]
    if not model_ids:
        raise SystemExit("no models")

    out_root = args.output.resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    for model_id in model_ids:
        probe = OllamaProvider(model=model_id)
        if not probe.health_check():
            print(f"skip: model not available in Ollama: {model_id!r}", flush=True)
            continue

        bundle = run_benchmark_for_model(
            model_id=model_id,
            vault=vault,
            positives=positives,
            negatives=negatives,
            scan_cache=scan_cache,
            triage_model=args.triage_model,
            extract_model=args.extract_model,
            run_id=args.run_id,
        )
        slug = _slug(model_id)
        ddir = out_root / slug
        ddir.mkdir(parents=True, exist_ok=True)
        (ddir / "scores.json").write_text(
            json.dumps(bundle["scores"], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        (ddir / "per_example_results.json").write_text(
            json.dumps(bundle["per_example_results"], indent=2, ensure_ascii=False, default=str) + "\n",
            encoding="utf-8",
        )
        (ddir / "failures.json").write_text(
            json.dumps(bundle["failures"], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        (ddir / "improvements.json").write_text(
            json.dumps(bundle["improvements"], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        s = bundle["scores"]
        lines = [
            f"# Benchmark — `{model_id}`",
            "",
            f"- Ground truth: `{gt_path}`",
            f"- Vault: `{vault}`",
            f"- Positives / negatives evaluated: {s['threads']['positives']} / {s['threads']['negatives']}",
            f"- Wall clock: {s['threads']['wall_clock_seconds']} s",
            f"- Throughput: {s['threads']['threads_per_minute']} threads/min",
            "",
            "## Triage",
            "",
            f"- Positive not skipped rate (want high): {s['triage']['positive_not_skipped_rate']}",
            f"- Positive triage false negatives (count): {s['triage']['positive_false_negative_count']}",
            f"- Negative triage FP rate (want low): {s['triage']['negative_triage_false_positive_rate']}",
            "",
            "## Extraction",
            "",
            f"- Schema-validated cards: {s['extraction']['schema_validated_cards']}",
            f"- Mean field match (matched positives): {s['extraction']['mean_field_match_on_matched_positives']}",
            "",
        ]
        (ddir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
        print(f"wrote {ddir}", flush=True)


if __name__ == "__main__":
    main()
