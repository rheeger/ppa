"""Phase 6.5 Step 0/1c — finance ``source_email`` vault coverage (no Postgres)."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path


def _has_source_email_line(text: str) -> bool:
    return bool(re.search(r"(?m)^source_email:\s*\S", text))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--vault", type=Path, required=True)
    p.add_argument("--finance-glob", default="**/Finance/**/*.md")
    p.add_argument("--output", type=Path, default=None)
    args = p.parse_args()
    vault = Path(args.vault).resolve()
    paths = sorted(vault.glob(args.finance_glob))
    total = len(paths)
    populated = 0
    for pth in paths:
        try:
            raw = pth.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if _has_source_email_line(raw):
            populated += 1
    pct = round(100.0 * populated / total, 2) if total else 0.0
    payload = {
        "vault": str(vault),
        "finance_md_files": total,
        "with_source_email_line": populated,
        "coverage_percent": pct,
        "date": datetime.now().date().isoformat(),
        "gate_step1a": ">= 50% on full seed (re-run after enrich_finance + match_resolver)",
    }
    out_txt = json.dumps(payload, indent=2)
    print(out_txt)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(out_txt + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
