"""Apply the classification filter to an existing precomputed cache offline.

Reads cache (with source_uid + target_uid + already-judged LLM verdicts) and the
{schema}.card_classifications table; produces a filtered cache that drops any
candidate where source OR target is in the skip set.

Use to retroactively test what the filter would do to the existing cache-1020.json
without re-running kNN or LLM judge.

Usage:
    PPA_INDEX_DSN=... .venv/bin/python archive_scripts/phase6_apply_filter_offline.py \\
        --cache _artifacts/_phase6-iterations/cache-1020.json \\
        --schema ppa \\
        [--skip marketing,automated,noise,personal] \\
        [--out _artifacts/_phase6-iterations/cache-1020-filtered.json]
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DEFAULT_SKIP = "marketing,automated,automated_notification,noise,personal,person_to_person"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--cache", required=True, type=Path)
    p.add_argument("--schema", default=os.environ.get("PPA_INDEX_SCHEMA", "ppa"))
    p.add_argument("--dsn", default=os.environ.get("PPA_INDEX_DSN", ""))
    p.add_argument("--skip", default=DEFAULT_SKIP)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    if not args.dsn:
        raise SystemExit("--dsn or PPA_INDEX_DSN required")

    skip_set = {s.strip().lower() for s in args.skip.split(",") if s.strip()}
    print(f"[filter] skip set: {sorted(skip_set)}")

    cache = json.loads(args.cache.read_text())
    uids: set[str] = set()
    for v in cache.values():
        uids.add(v["source_uid"])
        uids.add(v["target_uid"])
    print(f"[filter] cache size: {len(cache)} pairs / {len(uids)} unique cards")

    # Pull classifications for every unique uid
    classifications: dict[str, str] = {}
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        for chunk in [list(uids)[i:i+500] for i in range(0, len(uids), 500)]:
            rows = conn.execute(
                f"SELECT card_uid, classification FROM {args.schema}.card_classifications WHERE card_uid = ANY(%s)",
                (chunk,),
            ).fetchall()
            for r in rows:
                classifications[str(r["card_uid"])] = str(r["classification"]).strip().lower()

    print(f"[filter] {len(classifications)} of {len(uids)} cards have a classification "
          f"({len(classifications) / max(len(uids), 1) * 100:.1f}%)")
    cat_dist = Counter(classifications.values())
    for cat, n in cat_dist.most_common():
        marker = "  SKIP" if cat in skip_set else "  KEEP"
        print(f"  {cat}: {n}{marker}")

    # Filter
    kept: dict[str, dict] = {}
    drop_reasons: Counter = Counter()
    for k, v in cache.items():
        src_class = classifications.get(v["source_uid"], "")
        tgt_class = classifications.get(v["target_uid"], "")
        if src_class in skip_set:
            drop_reasons[f"source:{src_class}"] += 1
            continue
        if tgt_class in skip_set:
            drop_reasons[f"target:{tgt_class}"] += 1
            continue
        kept[k] = v

    print()
    print(f"[filter] kept: {len(kept)} ({len(kept) / max(len(cache), 1) * 100:.1f}%)")
    print(f"[filter] dropped: {len(cache) - len(kept)}")
    for reason, n in drop_reasons.most_common(10):
        print(f"  {reason}: {n}")

    out_path = args.out or args.cache.with_name(args.cache.stem + "-filtered.json")
    out_path.write_text(json.dumps(kept, indent=2))
    print(f"[filter] wrote {out_path}")


if __name__ == "__main__":
    main()
