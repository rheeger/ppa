"""Multi-model judge spot-check.

Picks N candidates from a precomputed cache, re-judges each with multiple LLMs,
and reports verdict agreement. Use to validate whether a cheaper model agrees
with a gold-standard model often enough to be the production default.

Usage:
    OPENAI_API_KEY=... \\
    .venv/bin/python archive_scripts/phase6_multimodel_spot.py \\
        --cache _artifacts/_phase6-iterations/cache-1020.json \\
        --sample 300 \\
        --models gpt-4o-mini,gpt-4o[,gemini-2.0-flash-lite,gemini-2.5-flash-lite]

If GEMINI_API_KEY is unset, Gemini models in --models are silently skipped.
Output: _artifacts/_phase6-iterations/multimodel-spot-{date}.{json,md}
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import random
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from archive_cli.seed_links import (
    LINK_TYPE_SEMANTICALLY_RELATED,
    MODULE_SEMANTIC,
    LinkEvidence,
    SeedCardSketch,
    SeedLinkCandidate,
    _parse_llm_json,
)
from archive_vault.llm_provider import GeminiProvider, OpenAIProvider
from archive_vault.provenance import compute_input_hash

REPO_ROOT = Path(__file__).resolve().parents[1]
ITER_DIR = REPO_ROOT / "_artifacts" / "_phase6-iterations"


def _make_provider(model: str):
    """Return a provider instance for the named model, or None if unsupported."""
    m = model.lower()
    if m.startswith("gpt-") or m.startswith("o1-") or m.startswith("o3-") or m.startswith("o4-"):
        if not os.environ.get("OPENAI_API_KEY"):
            return None
        return OpenAIProvider(model=model)
    if m.startswith("gemini"):
        if not os.environ.get("GEMINI_API_KEY"):
            return None
        return GeminiProvider(model=model)
    return None


def _build_prompt(src: dict[str, Any], tgt: dict[str, Any], emb: float) -> str:
    """Same prompt shape as archive_cli.seed_links._llm_prompt."""
    cand = SeedLinkCandidate(
        module_name=MODULE_SEMANTIC,
        source_card_uid=src["card_uid"], source_rel_path=src["rel_path"],
        target_card_uid=tgt["card_uid"], target_rel_path=tgt["rel_path"],
        target_kind="card", proposed_link_type=LINK_TYPE_SEMANTICALLY_RELATED,
        candidate_group="",
        input_hash=compute_input_hash({"s": src["card_uid"], "t": tgt["card_uid"]}),
        evidence_hash="spot",
        features={"embedding_similarity": round(emb, 6), "deterministic_hits": [], "ambiguous_target_count": 0},
        evidences=[LinkEvidence("embedding_similarity", "pgvector_knn", "cosine_similarity",
                                 f"{emb:.6f}", emb, {})],
        surface="derived_only", promotion_target="derived_edge",
    )
    src_sk = SeedCardSketch(uid=src["card_uid"], rel_path=src["rel_path"], slug="",
                            card_type=src["type"], summary=src.get("summary", ""),
                            frontmatter={}, body="", content_hash="",
                            activity_at="", wikilinks=[])
    tgt_sk = SeedCardSketch(uid=tgt["card_uid"], rel_path=tgt["rel_path"], slug="",
                            card_type=tgt["type"], summary=tgt.get("summary", ""),
                            frontmatter={}, body="", content_hash="",
                            activity_at="", wikilinks=[])
    from archive_cli.seed_links import _llm_prompt
    return _llm_prompt(cand, src_sk, tgt_sk)


def _judge_with(provider, prompt: str) -> tuple[str, float]:
    """Return (verdict, score) from a single provider call. ('', 0.0) on failure."""
    response = provider.complete(prompt, max_tokens=128)
    if not response:
        return "", 0.0
    payload = _parse_llm_json(response) or {}
    verdict = str(payload.get("link", "")).strip().upper()
    try:
        score = float(payload.get("confidence", 0.0))
    except (TypeError, ValueError):
        score = 0.0
    return verdict, max(0.0, min(score, 1.0))


def _hydrate_card_meta(dsn: str, schema: str, uids: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        for chunk in [uids[i:i+500] for i in range(0, len(uids), 500)]:
            rows = conn.execute(
                f"SELECT uid, rel_path, type, summary FROM {schema}.cards WHERE uid = ANY(%s)",
                (chunk,),
            ).fetchall()
            for r in rows:
                out[str(r["uid"])] = {
                    "card_uid": str(r["uid"]), "rel_path": str(r["rel_path"]),
                    "type": str(r["type"]), "summary": str(r.get("summary") or ""),
                }
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--cache", required=True, type=Path)
    p.add_argument("--sample", type=int, default=300)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--models", default="gpt-4o-mini,gpt-4o")
    p.add_argument("--schema", default=os.environ.get("PPA_INDEX_SCHEMA", "ppa"))
    p.add_argument("--dsn", default=os.environ.get("PPA_INDEX_DSN", ""))
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()
    if not args.dsn:
        raise SystemExit("--dsn or PPA_INDEX_DSN required")

    cache = json.loads(args.cache.read_text())
    keys = sorted(cache.keys())
    rng = random.Random(args.seed)
    sample_keys = rng.sample(keys, min(args.sample, len(keys)))
    print(f"[spot] cache_size={len(cache)} sample={len(sample_keys)} models={args.models}")

    needed_uids: set[str] = set()
    for k in sample_keys:
        e = cache[k]
        needed_uids.add(e["source_uid"])
        needed_uids.add(e["target_uid"])
    print(f"[spot] hydrating {len(needed_uids)} card rows from {args.schema}.cards")
    meta = _hydrate_card_meta(args.dsn, args.schema, sorted(needed_uids))

    requested = [m.strip() for m in args.models.split(",") if m.strip()]
    providers: dict[str, Any] = {}
    for m in requested:
        prov = _make_provider(m)
        if prov is None:
            print(f"[spot] skip model {m} (no API key configured)")
            continue
        providers[m] = prov
    if not providers:
        raise SystemExit("no usable model providers — set OPENAI_API_KEY and/or GEMINI_API_KEY")
    print(f"[spot] active models: {list(providers.keys())}")

    rows: list[dict[str, Any]] = []

    def _judge_one(idx: int, key: str) -> dict[str, Any]:
        e = cache[key]
        src = meta.get(e["source_uid"])
        tgt = meta.get(e["target_uid"])
        if src is None or tgt is None:
            return {"key": key, "skipped": True}
        prompt = _build_prompt(src, tgt, e["embedding_similarity"])
        per_model: dict[str, dict[str, Any]] = {}
        for name, prov in providers.items():
            verdict, score = _judge_with(prov, prompt)
            per_model[name] = {"verdict": verdict, "score": score}
        return {
            "key": key,
            "source_uid": e["source_uid"], "source_type": e["source_type"],
            "source_rel_path": e["source_rel_path"],
            "target_uid": e["target_uid"], "target_type": e["target_type"],
            "target_rel_path": e["target_rel_path"],
            "embedding_similarity": e["embedding_similarity"],
            "cached_verdict": e.get("llm_verdict", ""),
            "cached_score": e.get("llm_score", 0.0),
            "cached_model": e.get("llm_model", ""),
            "judged": per_model,
        }

    print(f"[spot] judging {len(sample_keys)} pairs across {len(providers)} models with {args.workers} workers")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_judge_one, i, k): k for i, k in enumerate(sample_keys)}
        for j, fut in enumerate(as_completed(futures)):
            try:
                rows.append(fut.result())
            except Exception as exc:
                rows.append({"key": futures[fut], "error": str(exc)})
            if (j + 1) % 50 == 0:
                print(f"[spot] {j+1}/{len(sample_keys)} done", flush=True)

    valid = [r for r in rows if "judged" in r]
    print(f"[spot] {len(valid)} valid / {len(rows)} total")

    # Agreement matrix: model_a vs model_b: % of pairs where verdicts match.
    model_names = list(providers.keys())
    agree: dict[tuple[str, str], int] = {}
    totals: dict[tuple[str, str], int] = {}
    for r in valid:
        for i, a in enumerate(model_names):
            for b in model_names[i:]:
                va = r["judged"][a]["verdict"]
                vb = r["judged"][b]["verdict"]
                if not va or not vb:
                    continue
                key = (a, b)
                totals[key] = totals.get(key, 0) + 1
                if va == vb:
                    agree[key] = agree.get(key, 0) + 1

    date = _dt.date.today().strftime("%Y%m%d")
    out_json = ITER_DIR / f"multimodel-spot-{date}.json"
    out_json.write_text(json.dumps({"sample_size": len(valid), "rows": rows,
                                     "agreement": {f"{a}|{b}": {"agree": agree.get((a, b), 0),
                                                                  "total": totals.get((a, b), 0)}
                                                    for (a, b) in totals.keys()}}, indent=2))
    print(f"[spot] wrote {out_json}")

    # Markdown summary
    md = [f"# Multi-model judge spot-check — {date}", "",
          f"- cache: `{args.cache}`",
          f"- sample size: **{len(valid)}**",
          f"- models judged: {', '.join(model_names)}",
          "", "## Per-model verdict distribution", "",
          "| model | YES | UNSURE | NO | empty |",
          "|---|---|---|---|---|"]
    for m in model_names:
        c = Counter(r["judged"][m]["verdict"] for r in valid)
        md.append(f"| {m} | {c.get('YES', 0)} | {c.get('UNSURE', 0)} | {c.get('NO', 0)} | {c.get('', 0)} |")
    md.extend(["", "## Pairwise agreement (% verdicts that match)", "",
               "| model_a | model_b | agreement | sample |",
               "|---|---|---|---|"])
    for (a, b), tot in sorted(totals.items()):
        ag = agree.get((a, b), 0)
        pct = (ag / tot * 100.0) if tot else 0.0
        md.append(f"| {a} | {b} | {pct:.1f}% | {tot} |")
    out_md = ITER_DIR / f"multimodel-spot-{date}.md"
    out_md.write_text("\n".join(md) + "\n")
    print(f"[spot] wrote {out_md}")


if __name__ == "__main__":
    main()
