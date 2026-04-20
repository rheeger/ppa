"""Phase 6.5 Step 16.1 — timezone consistency audit (vault scan)."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from archive_cli.vault_cache import VaultScanCache
from archive_sync.adapters.datetime_canon import (AUDITED_TIMESTAMP_FIELDS,
                                                  classify_timestamp)

AUDITED_FIELDS = AUDITED_TIMESTAMP_FIELDS


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--vault", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, default=Path("_artifacts/_phase6_5-timezone-audit"))
    p.add_argument("--tier", type=int, default=1)
    args = p.parse_args()

    cache = VaultScanCache.build_or_load(Path(args.vault).resolve(), tier=args.tier)
    counts: dict[tuple[str, str, str, str], int] = defaultdict(int)
    samples: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)

    for rel_path, fm in cache.all_frontmatters():
        card_type = str(fm.get("type") or "")
        if card_type not in AUDITED_FIELDS:
            continue
        sources = fm.get("source") or []
        if isinstance(sources, str):
            sources = [sources]
        source_label = ",".join(str(s) for s in sources) or "unknown"
        for field in AUDITED_FIELDS[card_type]:
            value = fm.get(field)
            if value is None:
                continue
            sval = value if isinstance(value, str) else str(value)
            category, sample = classify_timestamp(sval)
            key = (card_type, field, source_label, category)
            counts[key] += 1
            if sample is not None and len(samples[key]) < 5:
                samples[key].append(sample)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date().isoformat()

    lines = [
        f"# Phase 6.5 Step 16.1 timezone audit ({today})",
        f"\n**Vault:** `{args.vault.resolve()}`\n",
        "| card_type | field | source | category | count | samples |",
        "|---|---|---|---:|---:|---|",
    ]
    for (ct, f, src, cat), count in sorted(counts.items()):
        sample_str = "; ".join(samples[(ct, f, src, cat)][:3]) or ""
        lines.append(f"| {ct} | {f} | {src} | {cat} | {count} | {sample_str} |")
    (args.output_dir / f"report-{today}.md").write_text("\n".join(lines), encoding="utf-8")

    json_payload = [
        {
            "card_type": ct,
            "field": f,
            "source": src,
            "category": cat,
            "count": count,
            "samples": samples[(ct, f, src, cat)],
        }
        for (ct, f, src, cat), count in sorted(counts.items())
    ]
    (args.output_dir / f"report-{today}.json").write_text(
        json.dumps(json_payload, indent=2),
        encoding="utf-8",
    )
    print(json.dumps({"written_md": str(args.output_dir / f"report-{today}.md")}, indent=2))


if __name__ == "__main__":
    main()
