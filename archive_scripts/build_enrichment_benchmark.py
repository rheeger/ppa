#!/usr/bin/env python3
"""Build LLM enrichment benchmark ground truth from regex Phase 3 staging + vault.

Selection (Phase 2.75 plan Step 8):
  - Card types: seven transaction families (excludes ``purchase`` / subscription / etc.).
  - ``extraction_confidence >=`` threshold (default 0.8).
  - No quality flags: critical_fail / critical_err / heuristic (same bar as extraction-quality report).

Outputs ``_artifacts/_benchmark/enrichment_ground_truth.json`` (or a slice-specific name) with
``benchmark_set`` (positives) and ``negative_benchmark_set`` (threads not used as
positives — review in Step 8b).

**Vault must match the tree you extracted against** — ``source_email`` wikilinks resolve
under that vault's ``Email/``. Same pairing as ``make extraction-quality-reports``.

**Verification order (always start small — full seed is huge and slow):**

1. **1pct** — ``make build-enrichment-benchmark-smoke`` (``_artifacts/_staging-1pct/`` + ``.slices/1pct``).
2. **10pct** (and/or **5pct**) — only for slices where you actually ran
   ``make extract-emails-{10,5}pct-slice``. Each ``_artifacts/_staging-Npct/`` must match that extract;
   if you skip the 5pct extractor, skip ``build-enrichment-benchmark-5pct`` (stale dir → 0 positives).
3. **Full** — ``build-enrichment-benchmark`` with ``PPA_PATH`` after slices look good.

**Make:** ``make build-enrichment-benchmark-slices`` runs **1pct + 10pct** (common case).
Use ``make build-enrichment-benchmark-slices-all`` for 1+5+10 only when all three extract runs exist.

Run from ``ppa/``::

  make build-enrichment-benchmark-smoke
  make build-enrichment-benchmark-slices
"""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from pathlib import Path
from typing import Any

from archive_sync.extractors.quality_flags import (
    card_quality_flags,
    duplicate_uids,
    email_uid_index,
    is_ground_truth_eligible,
    load_staging_cards,
    source_uids_needed,
    wikilink_uid,
)
from archive_sync.llm_enrichment.schema_gen import LLM_OMIT_FIELDS
from archive_sync.llm_enrichment.threads import (
    ThreadDocument,
    ThreadMessage,
    build_thread_index,
    hydrate_thread,
    load_email_stubs_for_vault,
    thread_stub_from_frontmatter,
)
from archive_vault.vault import read_note_file

# Plan: seven regex-backed transaction types for enrichment benchmarking.
BENCHMARK_CARD_TYPES: frozenset[str] = frozenset(
    {
        "meal_order",
        "ride",
        "flight",
        "accommodation",
        "shipment",
        "grocery_order",
        "car_rental",
    }
)

# Strip metadata; keep ``type`` for expected card (LLM schema omits type from model output).
_EXPECTED_STRIP: frozenset[str] = (
    frozenset(LLM_OMIT_FIELDS)
    | {
        "people",
        "orgs",
        "tags",
        "summary",
        "source",
        "source_id",
        "created",
        "updated",
        "extraction_confidence",
    }
) - {"type"}


def _confidence(fm: dict[str, Any]) -> float:
    c = fm.get("extraction_confidence")
    if c is None:
        return 0.0
    try:
        return float(c)
    except (TypeError, ValueError):
        return 0.0


def expected_card_fields(fm: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fm.items():
        if k in _EXPECTED_STRIP:
            continue
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            out[k] = v
        elif isinstance(v, list):
            out[k] = v
        elif isinstance(v, dict):
            out[k] = v
    return out


def _thread_id_for_email(path: Path, fm: dict[str, Any], vault: Path, uid: str) -> str:
    rel = path.relative_to(vault).as_posix()
    stub = thread_stub_from_frontmatter(rel, fm)
    if stub:
        return stub.gmail_thread_id
    return f"_singleton:{uid}"


def _email_dict_from_message(m: ThreadMessage) -> dict[str, str]:
    return {
        "uid": m.uid,
        "subject": m.subject,
        "from_email": m.from_email,
        "sent_at": m.sent_at,
        "body": m.body,
    }


def _document_to_source_emails(doc: ThreadDocument) -> list[dict[str, str]]:
    return [_email_dict_from_message(m) for m in doc.messages]


def _marketing_score(subject: str, snippet: str) -> int:
    """Rough score for negative-thread sampling (higher = more newsletter-like)."""

    s = f"{subject} {snippet}".lower()
    hits = 0
    for needle in (
        "unsubscribe",
        "newsletter",
        "% off",
        "limited time",
        "sale",
        "promotion",
        "view in browser",
        "you are receiving this",
        "marketing",
        "no-reply",
        "noreply",
    ):
        if needle in s:
            hits += 1
    return hits


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--staging-dir",
        type=Path,
        default=Path("_artifacts/_staging"),
        help=(
            "Phase 3 regex staging dir under cwd (e.g. _artifacts/_staging). "
            "Must match the vault used for extract-emails that produced it."
        ),
    )
    ap.add_argument("--vault", type=Path, required=True, help="Source vault (Email/ tree)")
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("_artifacts/_benchmark/enrichment_ground_truth.json"),
        help="Output JSON path",
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-confidence", type=float, default=0.8)
    ap.add_argument("--min-positive-cards", type=int, default=200, help="Target minimum card count")
    ap.add_argument("--max-per-type", type=int, default=40, help="Soft cap per card type")
    ap.add_argument("--negative-samples", type=int, default=50, help="Negative threads to sample")
    args = ap.parse_args()

    random.seed(args.seed)
    staging = args.staging_dir.resolve()
    vault = args.vault.resolve()
    if not staging.is_dir():
        raise SystemExit(f"staging-dir not a directory: {staging}")
    if not vault.is_dir():
        raise SystemExit(f"vault not a directory: {vault}")

    by_type = load_staging_cards(staging)
    dup_uids = duplicate_uids(by_type)
    need = source_uids_needed(by_type)
    uid_to_path = email_uid_index(vault, need)

    # thread_id -> list of (card_type, fm)
    by_thread: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)

    for ct, rows in by_type.items():
        if ct not in BENCHMARK_CARD_TYPES:
            continue
        for _path, fm in rows:
            if _confidence(fm) < args.min_confidence:
                continue
            src = str(fm.get("source_email") or "")
            uid = wikilink_uid(src)
            if not uid or uid not in uid_to_path:
                continue
            ep = uid_to_path[uid]
            try:
                em = read_note_file(ep, vault_root=vault).frontmatter
            except OSError:
                continue
            fl = card_quality_flags(ct, fm, vault=vault, uid_to_path=uid_to_path, dup_uids=dup_uids)
            if not is_ground_truth_eligible(fl):
                continue
            tid = _thread_id_for_email(ep, em, vault, uid)
            by_thread[tid].append((ct, fm))

    stubs = load_email_stubs_for_vault(vault)
    thread_index = build_thread_index(stubs)
    positive_thread_ids = frozenset(by_thread.keys())

    # Greedy thread selection: add whole threads until caps / min cards met.
    thread_ids = [t for t in by_thread if t in thread_index and by_thread[t]]
    random.shuffle(thread_ids)
    per_type: dict[str, int] = defaultdict(int)
    selected: list[str] = []
    total_cards = 0

    def thread_fits_caps(tid: str) -> bool:
        delta: dict[str, int] = defaultdict(int)
        for ct, _fm in by_thread[tid]:
            delta[ct] += 1
        for ct, n in delta.items():
            if per_type[ct] + n > args.max_per_type:
                return False
        return True

    def accept_thread(tid: str) -> None:
        nonlocal total_cards
        selected.append(tid)
        for ct, _fm in by_thread[tid]:
            per_type[ct] += 1
            total_cards += 1

    for tid in thread_ids:
        if total_cards >= args.min_positive_cards and len(selected) > 10:
            break
        if not thread_fits_caps(tid):
            continue
        accept_thread(tid)

    benchmark_set: list[dict[str, Any]] = []
    for tid in selected:
        group = by_thread[tid]
        stubs_for_t = thread_index.get(tid)
        if not stubs_for_t:
            continue
        doc = hydrate_thread(stubs_for_t, vault, scan_cache=None)
        expected_cards: list[dict[str, Any]] = []
        for ct, fm in group:
            card = expected_card_fields(fm)
            card["type"] = ct
            expected_cards.append(card)
        benchmark_set.append(
            {
                "source_thread_id": tid,
                "source_emails": _document_to_source_emails(doc),
                "expected_cards": expected_cards,
            }
        )

    # Negatives: threads not used as positive sources; stratify toward newsletter-like subjects.
    negative_candidates = [
        tid for tid in thread_index if tid not in positive_thread_ids and thread_index.get(tid)
    ]
    scored = [
        (
            _marketing_score(
                thread_index[tid][0].subject,
                thread_index[tid][0].snippet,
            ),
            tid,
        )
        for tid in negative_candidates
    ]
    scored.sort(key=lambda x: (-x[0], x[1]))
    top_m = [t for _, t in scored[: max(args.negative_samples * 2, args.negative_samples)]]
    random.shuffle(top_m)
    neg_pick = top_m[: args.negative_samples]

    negative_benchmark_set: list[dict[str, Any]] = []
    for tid in neg_pick:
        stubs_for_t = thread_index[tid]
        doc = hydrate_thread(stubs_for_t, vault, scan_cache=None)
        negative_benchmark_set.append(
            {
                "source_thread_id": tid,
                "source_emails": _document_to_source_emails(doc),
                "expected_cards": [],
                "auto_candidate": True,
            }
        )

    out_path = args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "staging_dir": str(staging),
            "vault": str(vault),
            "seed": args.seed,
            "min_confidence": args.min_confidence,
            "benchmark_card_types": sorted(BENCHMARK_CARD_TYPES),
            "positive_threads": len(benchmark_set),
            "positive_cards": sum(len(x["expected_cards"]) for x in benchmark_set),
            "negative_threads": len(negative_benchmark_set),
            "per_type_selected": dict(sorted(per_type.items())),
        },
        "benchmark_set": benchmark_set,
        "negative_benchmark_set": negative_benchmark_set,
    }
    out_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n",
        encoding="utf-8",
    )

    m = payload["meta"]
    print(json.dumps(m, indent=2))
    if m["positive_cards"] < args.min_positive_cards:
        print(
            f"warning: only {m['positive_cards']} positive cards "
            f"(target {args.min_positive_cards}); widen staging or lower --min-confidence",
            flush=True,
        )
    if m["negative_threads"] < args.negative_samples:
        print(
            f"warning: only {m['negative_threads']} negative threads "
            f"(requested {args.negative_samples})",
            flush=True,
        )


if __name__ == "__main__":
    main()
