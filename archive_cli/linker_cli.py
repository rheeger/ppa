"""``ppa linker`` CLI subcommands for Phase 6.5.

Sub-commands:
  list              Dump the ALL_LINKERS registry as a text table.
  info --module X   Show a single LinkerSpec's fields.
  calibrate         `--mode vault` (default): run generator over vault, write jsonl +
                    report under _artifacts/_linkers/{module}/calibration/.
                    `--mode index`: seed-link-enqueue + seed-link-worker on Postgres.
  replay            Re-apply scoring thresholds against a cached candidates.jsonl.
  health            Per-module last-sweep + promoted-count + pending-review table.
  impact            Behavioral manifest checks + graph-neighbor diff (all vs emits-only).
  scaffold          Write a stub ``linker_modules/*.py`` module (add import to ``__init__.py``).
  deprecate/revive/retire --module X
                    Write a lifecycle override to
                    _artifacts/_linkers/_lifecycle_overrides.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from archive_cli import linker_framework as lf
from archive_cli import seed_links as s


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in str(s or "").split(",") if p.strip()]


def _to_snake(name: str) -> str:
    out: list[str] = []
    for i, c in enumerate(name):
        if c.isupper() and i:
            out.append("_")
        out.append(c.lower())
    return "".join(out)


def _tuple_literal(items: tuple[str, ...]) -> str:
    if not items:
        return "()"
    parts = ", ".join(repr(x) for x in items)
    if len(items) == 1:
        return f"({parts},)"
    return f"({parts})"


def _impact_anchor_uids(
    index: Any,
    schema: str,
    manifest: dict[str, Any],
    emits: frozenset[str],
) -> list[str]:
    """Pick graph manifest anchors whose expected edge types overlap this linker."""
    out: list[str] = []
    seen: set[str] = set()
    for entry in manifest.get("graph_queries", []) or []:
        if emits:
            inc = frozenset(entry.get("expect_edge_types_to_include", []) or [])
            if not (inc & emits):
                continue
        uid = str(entry.get("start_uid", "")).strip()
        path = str(entry.get("anchor_rel_path", "")).strip()
        if path.upper().startswith("PLACEHOLDER"):
            continue
        if not uid and path:
            with index._connect() as conn:
                row = conn.execute(
                    f"SELECT uid FROM {schema}.cards WHERE rel_path = %s LIMIT 1",
                    (path,),
                ).fetchone()
            uid = str(row["uid"]).strip() if row else ""
        if uid and uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def _linker_module_template(module_name: str, source_types: tuple[str, ...], emits: tuple[str, ...]) -> str:
    sn = _to_snake(module_name)
    const = sn.upper()
    st_lit = _tuple_literal(source_types)
    em_lit = _tuple_literal(emits)
    return f'''"""Scaffolded linker module ({module_name}) — replace stubs before enabling."""
from __future__ import annotations

from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.seed_links import SeedCardSketch, SeedLinkCandidate, SeedLinkCatalog

MODULE_{const} = "{module_name}"


def _generate_{sn}_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    _ = catalog
    _ = source
    return []


def _score_{sn}_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    det = float(features.get("deterministic_score", 0.0))
    risk = float(features.get("risk_penalty", 0.0))
    return det, 0.0, 0.0, 0.0, risk


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_{const},
    source_card_types={st_lit},
    emits_link_types={em_lit},
    generator=_generate_{sn}_candidates,
    scoring_fn=_score_{sn}_features,
    scoring_mode="deterministic",
    policies=(),
    requires_llm_judge=False,
    lifecycle_state="active",
    phase_owner="scaffold",
    post_promotion_action="edges_only",
    description="Scaffolded linker — implement generator and policies.",
))
'''


def _format_source_types(spec: lf.LinkerSpec) -> str:
    if not spec.source_card_types:
        return "(any)"
    types = list(spec.source_card_types)
    if len(types) > 3:
        return f"{', '.join(types[:3])} (+{len(types)-3} more)"
    return ", ".join(types)


def _format_emits(spec: lf.LinkerSpec) -> str:
    if not spec.emits_link_types:
        return "-"
    types = list(spec.emits_link_types)
    if len(types) > 2:
        return f"{', '.join(types[:2])} (+{len(types)-2})"
    return ", ".join(types)


def cmd_list(args: argparse.Namespace) -> int:
    specs = lf.list_linkers(
        lifecycle=args.lifecycle if args.lifecycle != "all" else None,
    )
    if args.json:
        out = [
            {
                "module_name": sp.module_name,
                "lifecycle_state": sp.lifecycle_state,
                "scoring_mode": sp.scoring_mode,
                "phase_owner": sp.phase_owner,
                "source_card_types": list(sp.source_card_types),
                "emits_link_types": list(sp.emits_link_types),
                "requires_llm_judge": sp.requires_llm_judge,
                "post_promotion_action": sp.post_promotion_action,
                "description": sp.description,
            }
            for sp in specs
        ]
        print(json.dumps(out, indent=2))
        return 0
    # Plain text table; no rich dependency so the base CLI stays minimal.
    rows = [
        (sp.module_name, sp.lifecycle_state, sp.scoring_mode,
         _format_source_types(sp), _format_emits(sp),
         str(s.SEED_LINK_POLICY_VERSION))
        for sp in specs
    ]
    header = ("MODULE", "STATE", "SCORING", "SOURCE TYPES", "EMITS", "POLICY")
    widths = [
        max(len(r[i]) for r in [header, *rows])
        for i in range(len(header))
    ]
    line = "  ".join(h.ljust(w) for h, w in zip(header, widths))
    print(line)
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(c.ljust(w) for c, w in zip(row, widths)))
    return 0


def cmd_info(args: argparse.Namespace) -> int:
    spec = lf.get_linker(args.module)
    if spec is None:
        print(f"No linker registered as {args.module!r}", file=sys.stderr)
        return 2
    out = {
        "module_name": spec.module_name,
        "lifecycle_state": spec.lifecycle_state,
        "scoring_mode": spec.scoring_mode,
        "phase_owner": spec.phase_owner,
        "source_card_types": list(spec.source_card_types),
        "emits_link_types": list(spec.emits_link_types),
        "requires_llm_judge": spec.requires_llm_judge,
        "post_promotion_action": spec.post_promotion_action,
        "description": spec.description,
        "catalog_indexes": [
            {"name": idx.name, "source_card_types": list(idx.source_card_types)}
            for idx in spec.catalog_indexes
        ],
        "policies": [
            {
                "link_type": p.link_type,
                "surface": p.surface,
                "auto_promote_floor": p.auto_promote_floor,
                "auto_review_floor": p.auto_review_floor,
            }
            for p in spec.policies
        ],
        "has_post_build_hook": spec.post_build_hook is not None,
        "has_bespoke_evaluator": spec.bespoke_evaluator is not None,
    }
    print(json.dumps(out, indent=2))
    return 0


def cmd_calibrate(args: argparse.Namespace) -> int:
    """Run calibration for one linker.

    * ``--mode vault`` (default): generator-only over a vault — writes
      ``_artifacts/_linkers/{module}/calibration/candidates-{date}.jsonl`` and a
      markdown report. No Postgres or ``link_decisions`` writes.

    * ``--mode index``: sets ``PPA_INDEX_SCHEMA`` to ``--scope``, then runs
      ``seed-link-enqueue`` + ``seed-link-worker`` (requires
      ``PPA_SEED_LINKS_ENABLED=1``). Persists candidates to the DB like the
      legacy CLI.
    """
    spec = lf.get_linker(args.module)
    if spec is None:
        print(f"No linker registered as {args.module!r}", file=sys.stderr)
        return 2
    if spec.lifecycle_state == "retired":
        print(
            f"{args.module} is retired; skipping (lifecycle_state=retired).",
            file=sys.stderr,
        )
        return 2

    if args.mode == "vault":
        from archive_cli.commands._resolve import resolve_vault
        from archive_cli.errors import VaultNotFoundError
        from archive_cli.linker_calibration import run_vault_calibration

        try:
            vault_path = Path(args.vault).resolve() if args.vault else resolve_vault()
        except VaultNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        if not vault_path.is_dir():
            print(f"Vault not found: {vault_path}", file=sys.stderr)
            return 2
        root = Path(args.artifact_root) if args.artifact_root else Path("_artifacts/_linkers")
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        summary = run_vault_calibration(
            vault_path,
            (args.module,),
            artifact_root=root,
            write_phase_summary=False,
        )
        print(json.dumps(summary, indent=2))
        return 0

    # index mode
    from archive_cli.commands import seed_links as seed_cmd
    from archive_cli.commands._resolve import resolve_index
    from archive_cli.errors import (IndexUnavailableError,
                                    SeedLinksDisabledError, VaultNotFoundError)

    log = logging.getLogger("ppa.cli.linker.calibrate")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    prev_schema = os.environ.get("PPA_INDEX_SCHEMA")
    try:
        os.environ["PPA_INDEX_SCHEMA"] = args.scope
        try:
            index = resolve_index()
        except VaultNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        except (IndexUnavailableError, RuntimeError) as exc:
            print(f"Index unavailable: {exc}", file=sys.stderr)
            return 2
        try:
            enq = seed_cmd.seed_link_enqueue(
                index=index,
                logger=log,
                modules=args.module,
            )
            work = seed_cmd.seed_link_worker(
                index=index,
                logger=log,
                modules=args.module,
                limit=args.limit,
                workers=args.workers,
                include_llm=bool(args.include_llm),
            )
        except SeedLinksDisabledError as exc:
            print(str(exc), file=sys.stderr)
            print("Set PPA_SEED_LINKS_ENABLED=1 for index-mode calibration.", file=sys.stderr)
            return 2
        print(json.dumps({"enqueue": enq, "worker": work}, indent=2, default=str))
        return 0
    finally:
        if prev_schema is None:
            os.environ.pop("PPA_INDEX_SCHEMA", None)
        else:
            os.environ["PPA_INDEX_SCHEMA"] = prev_schema


def cmd_replay(args: argparse.Namespace) -> int:
    """Offline iteration against a calibration-cache JSONL."""
    cache_path = Path(args.cache)
    if not cache_path.exists():
        print(f"Cache not found: {cache_path}", file=sys.stderr)
        return 2
    thresholds_path = Path(args.thresholds) if args.thresholds else None
    thresholds: dict[str, float] = {}
    if thresholds_path and thresholds_path.exists():
        thresholds = json.loads(thresholds_path.read_text())
    # Count candidates per tier.
    tiers: dict[str, int] = {}
    promoted: dict[str, int] = {}
    reviewed: dict[str, int] = {}
    with cache_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            features = row.get("features") or {}
            tier = features.get("tier") or "UNTIERED"
            tiers[tier] = tiers.get(tier, 0) + 1
            det = float(features.get("deterministic_score") or row.get("deterministic_score") or 0.0)
            risk = float(features.get("risk_penalty") or row.get("risk_penalty") or 0.0)
            final = max(0.0, min(1.0, det - risk))
            tier_promote = thresholds.get(f"{tier}_auto_promote_floor", 0.80)
            tier_review = thresholds.get(f"{tier}_auto_review_floor", 0.40)
            if final >= tier_promote:
                promoted[tier] = promoted.get(tier, 0) + 1
            elif final >= tier_review:
                reviewed[tier] = reviewed.get(tier, 0) + 1
    print(f"{'TIER':40s}  {'CANDIDATES':>10s}  {'AUTO-PROMOTE':>12s}  {'REVIEW':>8s}")
    for tier in sorted(tiers.keys()):
        print(f"{tier:40s}  {tiers[tier]:>10d}  "
              f"{promoted.get(tier, 0):>12d}  {reviewed.get(tier, 0):>8d}")
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    """Minimal health report. A richer version queries link_decisions + edges
    tables; this baseline uses registry metadata only."""
    specs = lf.list_linkers()
    print(f"{'MODULE':40s}  {'STATE':12s}  {'PHASE':12s}  DESCRIPTION")
    print("-" * 120)
    for sp in specs:
        desc = sp.description[:60] + ("..." if len(sp.description) > 60 else "")
        print(f"{sp.module_name:40s}  {sp.lifecycle_state:12s}  "
              f"{sp.phase_owner:12s}  {desc}")
    return 0


def cmd_impact(args: argparse.Namespace) -> int:
    """Slice manifest behavioral checks plus 1-hop neighbor trust: all edges vs linker emits only."""
    from archive_cli.commands import health_check as health_check_cmd
    from archive_cli.index_config import get_index_dsn
    from archive_cli.index_store import PostgresArchiveIndex

    spec = lf.get_linker(args.module)
    if spec is None:
        print(f"No linker registered as {args.module!r}", file=sys.stderr)
        return 2
    dsn = str(args.dsn or get_index_dsn() or "").strip()
    if not dsn:
        print("PPA_INDEX_DSN is required", file=sys.stderr)
        return 2
    vault = Path(os.environ.get("PPA_PATH", ".")).resolve()
    schema = str(args.scope or os.environ.get("PPA_INDEX_SCHEMA") or "ppa").strip()
    manifest_path = Path(args.manifest)
    if not manifest_path.is_file():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        return 2
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    index = PostgresArchiveIndex(vault, dsn=dsn)
    index.schema = schema

    emits = frozenset(spec.emits_link_types)
    anchors = _impact_anchor_uids(index, schema, manifest, emits)
    seen_anchors = set(anchors)
    for uid in _split_csv(args.extra_anchors):
        if uid not in seen_anchors:
            anchors.append(uid)
            seen_anchors.add(uid)
    if not anchors:
        print(
            "No anchor UIDs resolved (no manifest graph_queries overlap emits_link_types); "
            "pass --anchors uid1,uid2",
            file=sys.stderr,
        )
        return 2

    unf = index.fetch_graph_neighbors_for_uids(anchors)
    filt_types = list(spec.emits_link_types)
    filtered = (
        index.fetch_graph_neighbors_for_uids(anchors, edge_type_filter=filt_types)
        if filt_types
        else dict(unf)
    )
    only_unf = sorted(set(unf.keys()) - set(filtered.keys()))

    behavioral = health_check_cmd.run_behavioral_checks(index, manifest)
    payload: dict[str, Any] = {
        "module": spec.module_name,
        "schema": schema,
        "emits_link_types": list(spec.emits_link_types),
        "anchor_uids": anchors,
        "graph_neighbor_unfiltered": unf,
        "graph_neighbor_filtered_to_emits": filtered,
        "uids_only_in_unfiltered": only_unf,
        "behavioral": {
            "ok": behavioral.ok,
            "fts": [asdict(r) for r in behavioral.fts_results],
            "graph": behavioral.graph_results,
            "temporal": behavioral.temporal_results,
        },
    }
    out_path = (
        Path(args.output)
        if args.output
        else Path("_artifacts/_linkers") / spec.module_name / "impact-report.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    summary = {"written": str(out_path), "behavioral_ok": behavioral.ok}
    print(json.dumps(summary, indent=2))
    return 0


def cmd_scaffold(args: argparse.Namespace) -> int:
    """Emit a minimal ``register_linker`` module under ``linker_modules/``."""
    module_name = str(args.module).strip()
    if not module_name:
        print("--module required", file=sys.stderr)
        return 2
    source_types = tuple(_split_csv(args.source_types))
    emits = tuple(_split_csv(args.emits))
    stem = _to_snake(module_name)
    out = Path(args.out) if args.out else Path(__file__).resolve().parent / "linker_modules" / f"{stem}.py"
    if out.exists() and not args.force:
        print(f"Refusing to overwrite {out} (use --force)", file=sys.stderr)
        return 2
    text = _linker_module_template(module_name, source_types, emits)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    init_hint = f"from . import {stem}  # noqa: F401  — add to linker_modules/__init__.py"
    print(json.dumps({"written": str(out), "next": init_hint}, indent=2))
    return 0


def _cmd_lifecycle_override(state: str, args: argparse.Namespace) -> int:
    if args.module not in lf.ALL_LINKERS:
        print(f"No linker registered as {args.module!r}", file=sys.stderr)
        return 2
    lf.set_lifecycle_override(args.module, state)  # type: ignore[arg-type]
    print(f"Wrote lifecycle override: {args.module} -> {state}")
    print(f"Takes effect on next process start.")
    return 0


def cmd_deprecate(args: argparse.Namespace) -> int:
    return _cmd_lifecycle_override("deprecated", args)


def cmd_retire(args: argparse.Namespace) -> int:
    return _cmd_lifecycle_override("retired", args)


def cmd_revive(args: argparse.Namespace) -> int:
    return _cmd_lifecycle_override("active", args)


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("linker", help="Phase 6.5 linker framework commands")
    sub = p.add_subparsers(dest="linker_command", required=True)

    p_list = sub.add_parser("list", help="List registered linkers")
    p_list.add_argument("--lifecycle", choices=["all", "active", "deprecated", "retired"],
                        default="all")
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    p_info = sub.add_parser("info", help="Show one linker's full spec")
    p_info.add_argument("--module", required=True)
    p_info.set_defaults(func=cmd_info)

    p_cal = sub.add_parser(
        "calibrate",
        help="Calibrate one linker (vault generators or index enqueue+worker)",
    )
    p_cal.add_argument("--module", required=True)
    p_cal.add_argument(
        "--mode",
        choices=("vault", "index"),
        default="vault",
        help="vault=generator-only+jsonl (default); index=Postgres enqueue+worker",
    )
    p_cal.add_argument(
        "--vault",
        default="",
        help="Vault path for vault mode (default: PPA_PATH)",
    )
    p_cal.add_argument(
        "--artifact-root",
        default="",
        help="Under _artifacts; default _artifacts/_linkers",
    )
    p_cal.add_argument(
        "--scope",
        default="ppa_1pct",
        help="PPA_INDEX_SCHEMA for index mode (default: ppa_1pct)",
    )
    p_cal.add_argument("--limit", type=int, default=0)
    p_cal.add_argument("--workers", type=int, default=4)
    p_cal.add_argument(
        "--include-llm",
        action="store_true",
        help="Pass include_llm to seed-link-worker (index mode)",
    )
    p_cal.set_defaults(func=cmd_calibrate)

    p_rep = sub.add_parser("replay", help="Offline-iterate a cached candidates.jsonl")
    p_rep.add_argument("--cache", required=True)
    p_rep.add_argument("--thresholds", default="")
    p_rep.set_defaults(func=cmd_replay)

    p_hea = sub.add_parser("health", help="Per-module health summary")
    p_hea.set_defaults(func=cmd_health)

    p_imp = sub.add_parser(
        "impact",
        help="Behavioral manifest checks + graph neighbor diff (all vs linker emits)",
    )
    p_imp.add_argument("--module", required=True)
    p_imp.add_argument(
        "--manifest",
        default="archive_tests/slice_manifest.json",
        help="Path to slice_manifest.json",
    )
    p_imp.add_argument(
        "--output",
        default="",
        help="Report JSON path (default _artifacts/_linkers/{module}/impact-report.json)",
    )
    p_imp.add_argument("--dsn", default="", help="Postgres DSN (default PPA_INDEX_DSN)")
    p_imp.add_argument("--scope", default="", help="Index schema (default PPA_INDEX_SCHEMA or ppa)")
    p_imp.add_argument(
        "--anchors",
        dest="extra_anchors",
        default="",
        help="Additional anchor card UIDs (comma-separated)",
    )
    p_imp.set_defaults(func=cmd_impact)

    p_sca = sub.add_parser("scaffold", help="Write a stub linker module file")
    p_sca.add_argument("--module", required=True, help="Registry name e.g. myFeatureLinker")
    p_sca.add_argument(
        "--source-types",
        dest="source_types",
        default="",
        help="Comma-separated source card types",
    )
    p_sca.add_argument("--emits", default="", help="Comma-separated proposed_link_type strings")
    p_sca.add_argument(
        "--out",
        default="",
        help="Output path (default archive_cli/linker_modules/<snake>.py)",
    )
    p_sca.add_argument("--force", action="store_true", help="Overwrite existing file")
    p_sca.set_defaults(func=cmd_scaffold)

    for cmd, fn in (
        ("deprecate", cmd_deprecate),
        ("retire", cmd_retire),
        ("revive", cmd_revive),
    ):
        p_lc = sub.add_parser(cmd, help=f"Set lifecycle_state override -> {cmd}")
        p_lc.add_argument("--module", required=True)
        p_lc.set_defaults(func=fn)


def dispatch(args: argparse.Namespace) -> int:
    """Entry point invoked from __main__.py when args.command == 'linker'."""
    func = getattr(args, "func", None)
    if func is None:
        print("Usage: ppa linker {list|info|calibrate|replay|health|impact|scaffold|"
              "deprecate|retire|revive}", file=sys.stderr)
        return 2
    return int(func(args))
