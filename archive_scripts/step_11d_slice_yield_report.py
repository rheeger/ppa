#!/usr/bin/env python3
"""Step 11d: per-extractor yield on a vault using one dry-run pass (no card writes).

Default vault: ``ppa/.slices/10pct``. Uses the same matching/extraction path as production;
``dry_run`` skips writing derived cards while counting extracted vs matched.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from archive_sync.extractors.registry import build_default_registry
    from archive_sync.extractors.runner import ExtractionRunner

    ap = argparse.ArgumentParser(description="Step 11d: per-extractor yield (dry-run, single vault scan)")
    ap.add_argument(
        "--vault",
        default=str(root / ".slices" / "10pct"),
        help="Vault root (default: .slices/10pct)",
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--json", action="store_true", help="Print JSON only")
    args = ap.parse_args()
    vault = str(Path(args.vault).resolve())
    if not Path(vault).is_dir():
        print(f"Missing vault directory: {vault}", file=sys.stderr)
        sys.exit(1)

    reg = build_default_registry()
    runner = ExtractionRunner(
        vault_path=vault,
        registry=reg,
        staging_dir=None,
        dry_run=True,
        workers=max(1, args.workers),
        sender_filter=None,
    )
    m = runner.run()
    rows: list[dict[str, object]] = []
    for eid in sorted(m.per_extractor.keys()):
        row = m.per_extractor[eid]
        matched = int(row.get("matched", 0))
        extracted = int(row.get("extracted", 0))
        yld = (extracted / matched) if matched else 0.0
        rows.append(
            {
                "extractor": eid,
                "matched": matched,
                "extracted": extracted,
                "yield": round(yld, 6),
                "rejected": int(row.get("rejected", 0)),
                "errors": int(row.get("errors", 0)),
            }
        )

    payload = {
        "vault": vault,
        "total_emails_scanned": m.total_emails_scanned,
        "matched_emails": m.matched_emails,
        "extracted_cards": m.extracted_cards,
        "wall_clock_seconds": round(m.wall_clock_seconds, 3),
        "by_extractor": rows,
    }
    if args.json:
        print(json.dumps(payload, indent=2))
        return

    print("# Step 11d — per-extractor yield (dry-run)\n")
    print(f"**Vault:** `{vault}`  \n")
    print("| Extractor | Matched | Extracted | Yield | Rejected | Errors |")
    print("|-----------|--------:|----------:|------:|---------:|-------:|")
    for r in rows:
        y = float(r["yield"])  # type: ignore[arg-type]
        print(
            f"| {r['extractor']} | {r['matched']} | {r['extracted']} | {y * 100:.1f}% | "
            f"{r['rejected']} | {r['errors']} |"
        )
    print()
    print(
        f"_Scanned **{m.total_emails_scanned:,}** `email_message` notes in **{m.wall_clock_seconds:.1f}s**._\n"
        "Receipt-type extractors: iterate parsers against `specs/samples_seed/` until yield is acceptable."
    )


if __name__ == "__main__":
    main()
