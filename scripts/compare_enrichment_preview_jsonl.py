#!/usr/bin/env python3
"""Build a markdown report comparing two or three ``llm_enrichment_preview.jsonl`` runs.

Escapes cell text so markdown tables render reliably (pipes, newlines).

Two-way example (from ``ppa/``)::

  python scripts/compare_enrichment_preview_jsonl.py \\
    --baseline _artifacts/_staging-enrichment-1pct-full/llm_enrichment_preview.jsonl \\
    --compare _artifacts/_staging-enrichment-1pct-2p5-flash-lite/llm_enrichment_preview.jsonl \\
    --out _artifacts/_compare_1pct_gemini_3p1_vs_2p5.md

Three-way: pass ``--tertiary`` and ``--tertiary-label`` (intersection of uids only).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_jsonl(path: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        out[o["uid"]] = o
    return out


def _summary(row: dict[str, Any]) -> str:
    """Prefer ``field_updates`` (post-parse, e.g. boilerplate-stripped) over raw ``parsed``."""

    fu = row.get("field_updates")
    if isinstance(fu, dict):
        ts = fu.get("thread_summary")
        if ts is not None and str(ts).strip():
            return str(ts).strip()
    return (row.get("parsed") or {}).get("thread_summary") or ""


def _nz(s: str) -> bool:
    return bool((s or "").strip())


def _cell_unlimited(text: str) -> str:
    """Full cell text for tables: escape pipes; preserve line breaks as HTML ``<br>``."""

    t = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    t = t.replace("|", "&#124;")
    return t.replace("\n", "<br>\n")


def _cell(text: str, max_chars: int | None) -> str:
    """Table cell safe for GFM. If ``max_chars`` is None or <= 0, no truncation (see ``_cell_unlimited``)."""

    if max_chars is None or max_chars <= 0:
        return _cell_unlimited(text)
    t = " ".join((text or "").split())
    t = t.replace("|", "&#124;")
    if len(t) > max_chars:
        t = t[: max_chars - 3].rstrip() + "..."
    return t


def _metrics(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _max_pairwise_delta_lens(lens: list[int]) -> int:
    if len(lens) < 2:
        return 0
    out = 0
    for i in range(len(lens)):
        for j in range(i + 1, len(lens)):
            out = max(out, abs(lens[i] - lens[j]))
    return out


def _report_three_way(args: argparse.Namespace) -> None:
    base_path: Path = args.baseline
    cmp_path: Path = args.compare
    ter_path: Path = args.tertiary
    out_path: Path = args.out

    A = _load_jsonl(base_path)
    B = _load_jsonl(cmp_path)
    C = _load_jsonl(ter_path)
    l1, l2, l3 = args.baseline_label, args.compare_label, args.tertiary_label

    ma = _metrics(args.baseline_metrics or base_path.with_name("_metrics.json"))
    mb = _metrics(args.compare_metrics or cmp_path.with_name("_metrics.json"))
    mc = _metrics(args.tertiary_metrics or ter_path.with_name("_metrics.json"))

    uids_all_three = sorted(set(A) & set(B) & set(C))
    only_ab = sorted(set(A) & set(B) - set(C))
    only_ac = sorted(set(A) & set(C) - set(B))
    only_bc = sorted(set(B) & set(C) - set(A))

    lines: list[str] = []
    lines.append(f"# {args.title}\n")
    lines.append(f"- **{l1}:** `{base_path}`\n")
    lines.append(f"- **{l2}:** `{cmp_path}`\n")
    lines.append(f"- **{l3}:** `{ter_path}`\n")
    if args.note.strip():
        lines.append("\n" + args.note.strip() + "\n")

    lines.append("\n## Coverage\n\n")
    lines.append(f"- UIDs in **all three** JSONLs: **{len(uids_all_three)}**\n")
    if only_ab:
        lines.append(f"- Only in {l1} ∩ {l2} (not {l3}): **{len(only_ab)}** uids\n")
    if only_ac:
        lines.append(f"- Only in {l1} ∩ {l3} (not {l2}): **{len(only_ac)}** uids\n")
    if only_bc:
        lines.append(f"- Only in {l2} ∩ {l3} (not {l1}): **{len(only_bc)}** uids\n")

    lines.append("\n## Metrics (`_metrics.json`)\n\n")
    if ma and mb and mc:
        keys = [
            ("llm_calls", "LLM calls"),
            ("llm_nonempty_summary", "Non-empty `thread_summary`"),
            ("llm_yield_rate", "Yield rate"),
            ("errors", "Errors"),
            ("enriched", "Enriched rows"),
            ("dry_run_writes", "Dry-run preview writes"),
            ("elapsed_s", "Elapsed (s)"),
        ]
        lines.append(f"| | {l1} | {l2} | {l3} |\n")
        lines.append("| --- | --- | --- | --- |\n")
        for k, label in keys:
            lines.append(f"| {label} | {ma.get(k)} | {mb.get(k)} | {mc.get(k)} |\n")
    else:
        lines.append("_Some metrics files missing; skipped._\n")

    triple_identical = 0
    triple_nonempty_identical: list[tuple[str, str]] = []
    triple_all_empty_subjects: list[str] = []
    for u in uids_all_three:
        sa, sb, sc = _summary(A[u]), _summary(B[u]), _summary(C[u])
        if sa.strip() == sb.strip() == sc.strip():
            triple_identical += 1
            if not sa.strip():
                triple_all_empty_subjects.append(A[u].get("subject") or "")
            else:
                triple_nonempty_identical.append((A[u].get("subject") or "", sa))

    lines.append("\n## Summary strings (uids in all three runs)\n\n")
    lines.append(f"- Identical `thread_summary` on all three: **{triple_identical}**\n")
    lines.append(
        f"- Of those, all empty: **{len(triple_all_empty_subjects)}** | "
        f"same non-empty text: **{len(triple_nonempty_identical)}**\n"
    )

    divergent: list[tuple[int, int, str]] = []
    for u in uids_all_three:
        sa, sb, sc = _summary(A[u]), _summary(B[u]), _summary(C[u])
        if sa.strip() == sb.strip() == sc.strip():
            continue
        lens = [len(sa), len(sb), len(sc)]
        divergent.append((_max_pairwise_delta_lens(lens), sum(lens), u))
    divergent.sort(reverse=True)

    no_trunc = getattr(args, "no_truncate", False)
    include_all = getattr(args, "include_all_rows", False) or no_trunc
    cc: int | None = None if no_trunc else args.cell_chars
    max_rows = args.max_side_by_side

    if include_all:
        ordered_uids = sorted(
            uids_all_three,
            key=lambda u: ((A[u].get("subject") or "").lower(), u),
        )
        lines.append(
            "\n## Full side-by-side (every uid in intersection; summaries not truncated)\n\n"
            if no_trunc
            else f"\n## Side-by-side — all threads in intersection (up to length sort; {len(ordered_uids)} rows)\n\n"
        )
        row_uids = ordered_uids
        rest = 0
    else:
        lines.append(
            f"\n## Side-by-side — largest max pairwise length delta (up to {max_rows} rows)\n\n"
        )
        row_uids = []
        shown = 0
        for _, __, u in divergent:
            if shown >= max_rows:
                break
            row_uids.append(u)
            shown += 1
        rest = len(divergent) - shown

    lines.append(f"| Subject | {l1} | {l2} | {l3} |\n")
    lines.append("| --- | --- | --- | --- |\n")
    for u in row_uids:
        subj = (
            _cell_unlimited(A[u].get("subject") or "")
            if no_trunc
            else _cell(A[u].get("subject") or "", 72)
        )
        lines.append(
            f"| {subj} | {_cell(_summary(A[u]), cc)} | {_cell(_summary(B[u]), cc)} | "
            f"{_cell(_summary(C[u]), cc)} |\n"
        )
    if not include_all and rest > 0:
        lines.append(
            f"\n_{rest} more threads differ with smaller max pairwise length deltas "
            f"(not shown; raise `--max-side-by-side` or use `--include-all-rows`)._\n"
        )

    lines.append(f"\n## Same non-empty text on all three ({len(triple_nonempty_identical)} threads)\n\n")
    if triple_nonempty_identical:
        lines.append("| Subject | Shared summary |\n")
        lines.append("| --- | --- |\n")
        lim = len(triple_nonempty_identical) if no_trunc else min(40, len(triple_nonempty_identical))
        for subj, sa in sorted(triple_nonempty_identical, key=lambda x: x[0].lower())[:lim]:
            if no_trunc:
                lines.append(f"| {_cell_unlimited(subj)} | {_cell_unlimited(sa)} |\n")
            else:
                lines.append(f"| {_cell(subj, 72)} | {_cell(sa, cc)} |\n")
        if not no_trunc and len(triple_nonempty_identical) > 40:
            lines.append(
                f"\n_…and {len(triple_nonempty_identical) - 40} more (trimmed for size)._\n"
            )
    else:
        lines.append("_None._\n")

    if triple_all_empty_subjects:
        lines.append(
            f"\n## All three empty `thread_summary` ({len(triple_all_empty_subjects)} threads)\n\n"
        )
        lines.append("| Subject |\n")
        lines.append("| --- |\n")
        for subj in sorted(triple_all_empty_subjects, key=lambda x: x.lower()):
            lines.append(f"| {_cell(subj, 120)} |\n")

    text = "".join(lines)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote {out_path.resolve()} ({len(text)} bytes, {text.count(chr(10)) + 1} lines)")


def _report_two_way(args: argparse.Namespace) -> None:
    base_path: Path = args.baseline
    cmp_path: Path = args.compare
    out_path: Path = args.out

    A = _load_jsonl(base_path)
    B = _load_jsonl(cmp_path)
    uids = sorted(set(A) & set(B))

    ma = _metrics(args.baseline_metrics or base_path.with_name("_metrics.json"))
    mb = _metrics(args.compare_metrics or cmp_path.with_name("_metrics.json"))

    both_nz = both_z = a_only_nz = b_only_nz = 0
    identical = 0
    for u in uids:
        sa, sb = _summary(A[u]), _summary(B[u])
        na, nb = _nz(sa), _nz(sb)
        if na and nb:
            both_nz += 1
        elif not na and not nb:
            both_z += 1
        elif na and not nb:
            a_only_nz += 1
        elif nb and not na:
            b_only_nz += 1
        if sa.strip() == sb.strip():
            identical += 1

    divergent: list[tuple[int, int, str]] = []
    for u in uids:
        sa, sb = _summary(A[u]), _summary(B[u])
        if sa.strip() == sb.strip():
            continue
        divergent.append((abs(len(sa) - len(sb)), len(sa) + len(sb), u))
    divergent.sort(reverse=True)

    only_b: list[tuple[str, str, str]] = []
    for u in uids:
        sa, sb = _summary(A[u]), _summary(B[u])
        if _nz(sb) and not _nz(sa):
            only_b.append((u, A[u].get("subject") or "", sb))

    identical_nonempty: list[tuple[str, str]] = []
    both_empty_subjects: list[str] = []
    for u in uids:
        sa, sb = _summary(A[u]), _summary(B[u])
        if sa.strip() != sb.strip():
            continue
        if not sa.strip():
            both_empty_subjects.append(A[u].get("subject") or "")
        else:
            identical_nonempty.append((A[u].get("subject") or "", sa))

    lines: list[str] = []
    lines.append(f"# {args.title}\n")
    lines.append(f"- Baseline: `{base_path}` ({args.baseline_label})\n")
    lines.append(f"- Compare: `{cmp_path}` ({args.compare_label})\n")
    if args.note.strip():
        lines.append("\n" + args.note.strip() + "\n")

    lines.append("\n## Metrics (`_metrics.json`)\n\n")
    if ma and mb:
        keys = [
            ("llm_calls", "LLM calls"),
            ("llm_nonempty_summary", "Non-empty `thread_summary`"),
            ("llm_yield_rate", "Yield rate"),
            ("errors", "Errors"),
            ("enriched", "Enriched rows"),
            ("dry_run_writes", "Dry-run preview writes"),
            ("elapsed_s", "Elapsed (s)"),
        ]
        lines.append("| | " + args.baseline_label + " | " + args.compare_label + " |\n")
        lines.append("| --- | --- | --- |\n")
        for k, label in keys:
            lines.append(f"| {label} | {ma.get(k)} | {mb.get(k)} |\n")
    else:
        lines.append("_Metrics files not found or unreadable; skipped._\n")

    lines.append("\n## Summary overlap (uids in both JSONLs)\n\n")
    lines.append(f"- Threads in both: **{len(uids)}**\n")
    lines.append(f"- Identical `thread_summary` string: **{identical}**\n")
    lines.append(
        f"- Both non-empty: **{both_nz}** | both empty: **{both_z}** | "
        f"only baseline non-empty: **{a_only_nz}** | only compare non-empty: **{b_only_nz}**\n"
    )

    cc = args.cell_chars
    lines.append(
        f"\n## Side-by-side — largest length deltas (up to {args.max_side_by_side} rows)\n\n"
    )
    lines.append("| Subject | " + args.baseline_label + " | " + args.compare_label + " |\n")
    lines.append("| --- | --- | --- |\n")
    shown = 0
    for _, __, u in divergent:
        if shown >= args.max_side_by_side:
            break
        subj = _cell(A[u].get("subject") or "", 80)
        sa = _cell(_summary(A[u]), cc)
        sb = _cell(_summary(B[u]), cc)
        lines.append(f"| {subj} | {sa} | {sb} |\n")
        shown += 1
    rest = len(divergent) - shown
    if rest > 0:
        lines.append(
            f"\n_{rest} more threads differ with smaller absolute length deltas "
            f"(not shown; raise `--max-side-by-side`)._\n"
        )

    if only_b:
        lines.append(
            f"\n## Compare-only non-empty summaries (baseline empty) ({len(only_b)} threads)\n\n"
        )
        lines.append("| Subject | " + args.compare_label + " |\n")
        lines.append("| --- | --- |\n")
        for _uid, subj, sb in sorted(only_b, key=lambda x: x[1].lower()):
            lines.append(f"| {_cell(subj, 80)} | {_cell(sb, cc)} |\n")

    lines.append(f"\n## Identical non-empty summaries ({len(identical_nonempty)} threads)\n\n")
    if identical_nonempty:
        lines.append("| Subject | Shared summary |\n")
        lines.append("| --- | --- |\n")
        for subj, sa in sorted(identical_nonempty, key=lambda x: x[0].lower()):
            lines.append(f"| {_cell(subj, 80)} | {_cell(sa, cc)} |\n")
    else:
        lines.append(
            "_None — all matching strings in this run were empty on both sides (see below)._\n"
        )

    if both_empty_subjects:
        lines.append(
            f"\n## Both empty `thread_summary` ({len(both_empty_subjects)} threads)\n\n"
        )
        lines.append(
            "_Same empty summary on baseline and compare (often filtered/noise threads)._\n\n"
        )
        lines.append("| Subject |\n")
        lines.append("| --- |\n")
        for subj in sorted(both_empty_subjects, key=lambda x: x.lower()):
            lines.append(f"| {_cell(subj, 120)} |\n")

    text = "".join(lines)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote {out_path.resolve()} ({len(text)} bytes, {text.count(chr(10)) + 1} lines)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", type=Path, required=True, help="Older / reference JSONL")
    ap.add_argument("--compare", type=Path, required=True, help="Second JSONL")
    ap.add_argument("--out", type=Path, required=True, help="Output .md path")
    ap.add_argument(
        "--tertiary",
        type=Path,
        default=None,
        help="Optional third JSONL (enables 3-column summary comparison on uid intersection)",
    )
    ap.add_argument(
        "--tertiary-label",
        default="Run 3",
        help="Label for --tertiary column",
    )
    ap.add_argument(
        "--baseline-metrics",
        type=Path,
        default=None,
        help="Optional _metrics.json next to baseline",
    )
    ap.add_argument(
        "--compare-metrics",
        type=Path,
        default=None,
        help="Optional _metrics.json next to compare",
    )
    ap.add_argument(
        "--tertiary-metrics",
        type=Path,
        default=None,
        help="Optional _metrics.json next to tertiary",
    )
    ap.add_argument(
        "--title",
        default="Enrichment preview comparison",
        help="H1 title",
    )
    ap.add_argument(
        "--baseline-label",
        default="Baseline",
        help="Column label for baseline summaries",
    )
    ap.add_argument(
        "--compare-label",
        default="Compare",
        help="Column label for compare summaries",
    )
    ap.add_argument(
        "--max-side-by-side",
        type=int,
        default=250,
        help="Max rows in the main length-delta table (default 250)",
    )
    ap.add_argument(
        "--cell-chars",
        type=int,
        default=420,
        help="Max characters per summary cell after escaping (default 420; use ~280–320 for 3-way)",
    )
    ap.add_argument(
        "--note",
        default="",
        help="Optional markdown paragraph inserted after the title",
    )
    ap.add_argument(
        "--no-truncate",
        action="store_true",
        help="Emit full thread_summary text (newlines as <br>); implies all intersection rows in the main table",
    )
    ap.add_argument(
        "--include-all-rows",
        action="store_true",
        help="Main side-by-side includes every uid in intersection (not only pairwise-divergent rows)",
    )
    args = ap.parse_args()

    if args.tertiary is not None:
        _report_three_way(args)
    else:
        _report_two_way(args)


if __name__ == "__main__":
    main()
