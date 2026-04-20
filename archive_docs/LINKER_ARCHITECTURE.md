# Linker Architecture (Phase 6.5)

Phase 6.5 introduced a declarative `LinkerSpec` + `register_linker` framework
that replaces the bespoke dispatch-by-string patterns that had accumulated in
`archive_cli/seed_links.py`. Every linker is one `LinkerSpec` registered at
module import time; dispatch, scoring, lifecycle gating, CLI integration,
and the `ppa status` Linker panel all read off the registry.

## Package layout

```
archive_cli/
├── linker_framework.py        # LinkerSpec, CatalogIndexSpec, register_linker,
│                              # ALL_LINKERS, private-index helpers, lifecycle
├── linker_cli.py              # `ppa linker {list|info|calibrate|replay|
│                              # health|deprecate|retire|revive}`
├── linker_modules/            # Phase 6.5 new modules (one file each)
│   ├── meeting_artifact.py
│   ├── trip_cluster.py
│   └── finance_reconcile.py
├── merchant_normalizer.py     # Step 8.0 deliverable
├── iata.py + data/iata_cities.csv  # Step 7.0 deliverable
└── seed_links.py              # legacy + the 7 pre-existing linkers;
                               # binds wiring tables to the framework;
                               # registers legacy linkers via register_linker
```

## The `LinkerSpec` contract

```python
@dataclass(frozen=True)
class LinkerSpec:
    module_name: str                          # stable registry key
    source_card_types: tuple[str, ...]        # worker enqueue filter
    emits_link_types: tuple[str, ...]         # impact/filter targets
    generator: Callable[[catalog, source], list[SeedLinkCandidate]]
    scoring_fn: Callable[[features], 5-tuple]
    scoring_mode: Literal["deterministic", "weighted", "semantic", "bespoke"]
    catalog_indexes: tuple[CatalogIndexSpec, ...] = ()
    policies: tuple[LinkSurfacePolicy, ...] = ()
    requires_llm_judge: bool = False
    lifecycle_state: Literal["active", "deprecated", "retired"] = "active"
    phase_owner: str = ""
    post_promotion_action: Literal["edges_only", "frontmatter_delta", "new_cards"]
    description: str = ""
    bespoke_evaluator: BespokeEvaluator | None = None
    post_build_hook: PostBuildHook | None = None
```

`register_linker(spec)` wires:

1. `ALL_LINKERS[module_name]` — always.
2. `LINK_SURFACE_BY_TYPE` — always, for every `policy` in `spec.policies`.
3. `PROPOSED_LINK_TYPES` — always, for every `emit_link_type` + policy.
4. `CARD_TYPE_MODULES` — only if `lifecycle_state == "active"`.
5. `LLM_REVIEW_MODULES` — only if `requires_llm_judge` and not retired.

## Scoring modes

| mode            | final_confidence formula                                                 |
| --------------- | ------------------------------------------------------------------------ |
| `deterministic` | `det - risk` (Phase 6.5 new linkers)                                     |
| `weighted`      | `0.45 det + 0.12 lex + 0.13 graph + 0.18 llm + 0.12 emb - risk` (legacy) |
| `semantic`      | dual-tier gate `llm * emb - risk` (MODULE_SEMANTIC, retired)             |
| `bespoke`       | delegates to `bespoke_evaluator` (reserve escape hatch)                  |

## Catalog indexes

Every linker declares the catalog indexes it needs via `CatalogIndexSpec`.
`build_seed_link_catalog` runs each registered spec's `post_build_hook` at
the end of catalog construction; hooks attach linker-owned indexes via
`linker_framework.set_private_index(catalog, name, data)`. Consumers read
with `linker_framework.get_private_index(catalog, name)`.

Framework-core indexes (`cards_by_uid`, `calendar_events_by_ical_uid`, etc.)
live as named fields on `SeedLinkCatalog` and don't need redeclaration.

## Re-embed decision protocol

`post_promotion_action` tells the post-promotion command what to run:

| action              | follow-up                                                  |
| ------------------- | ---------------------------------------------------------- |
| `edges_only`        | `ppa incremental-rebuild` (touches only the `edges` table) |
| `frontmatter_delta` | `ppa incremental-rebuild` (delta-embeds touched cards)     |
| `new_cards`         | `ppa incremental-rebuild && ppa embed-pending`             |

All Phase 6.5 new linkers are `edges_only`.

## Lifecycle

| state        | enqueued?     | visible in `ppa linker list`? |
| ------------ | ------------- | ----------------------------- |
| `active`     | yes           | yes                           |
| `deprecated` | yes + warning | yes                           |
| `retired`    | no            | yes (with state tag)          |

Ops can override a spec's state without code edits by writing
`_artifacts/_linkers/_lifecycle_overrides.json`:

```json
{ "overrides": { "moduleCamelLinker": "retired" } }
```

Overrides take effect on next process start. Use
`ppa linker retire/revive/deprecate --module X` to write the file atomically.

## Adding a new linker

1. `ppa linker scaffold --name X --source-types Y --emits Z` _(future; see
   `.cursor/plans/_templates/linker.plan.md` for the manual layout today)_.
2. Implement generator + scoring_fn + catalog indexes in
   `archive_cli/linker_modules/{x}.py`.
3. Call `register_linker(LinkerSpec(...))` at module bottom.
4. Import the module from `archive_cli/linker_modules/__init__.py`.
5. Write unit tests covering every tier + negative controls.
6. Run `ppa linker calibrate --module X --scope ppa_1pct` and review the
   emitted calibration cache + report.

See `archive_docs/CONTRIBUTING_LINKERS.md` for the full contribution flow.

## Quality gates (precision-first)

See `archive_docs/runbooks/linker-quality-gates.md`. Every linker tier
that auto-promotes against `PPA_INDEX_SCHEMA=ppa` must hit ≥95% precision
on a stratified ≥30-candidate sample, recorded at
`_artifacts/_linkers/{module}/calibration/report-{date}.md`. Tiers below
the gate either tighten predicates, drop below `auto_promote_floor`, or
get retired via the retirement protocol.

Coverage (% of cards a linker writes onto) is reported as context only
and is NOT a pass/fail metric. The 2026-04-26 Step 1a run is the
canonical example: a coverage-gate-driven `finance.source_email`
campaign raised coverage from 2.67% → 58.36% but the review packet
showed roughly 95% of newly-added links were `high` or `medium` risk and
categorically wrong. Coverage rose, quality fell. We pruned back. The
runbook codifies the rule we should have been following.

## Retirement

See `archive_docs/runbooks/linker-retirement-protocol.md`. Every retirement
becomes a new entry in that runbook's appendix. MODULE_SEMANTIC (Phase 6
Tier 3) is the first example.

## v3 integration

The `ALL_LINKERS` registry is the basis for:

- **v3 Phase 11 (setup wizard):** renders a per-linker opt-in screen during
  `ppa setup`, dynamically disabling linkers whose source card types have no
  connected data.
- **v3 Phase 15 (contribution framework):** `archive_docs/CONTRIBUTING_LINKERS.md`
  - a future `ppa-dev/ppa-linker-template` repo enable community linker
    contributions with the same acceptance flow as connector contributions.
- **v4 Rust engine rewrite:** each `linker_modules/{X}.py` maps 1:1 to a
  future `ppa_engine/linkers/modules/{X}.rs`. `LinkerSpec` is a cross-language
  ABI.

## Compatibility notes

`archive_cli/seed_links.py` still exists and holds the core dataclasses
(`SeedLinkCatalog`, `SeedLinkCandidate`, etc.) + the legacy generator
functions. The framework augments rather than replaces; external callers
that import `from archive_cli.seed_links import ...` continue to work
unchanged. A follow-up cleanup phase (tentatively Phase 6.75) can migrate
the seven legacy generators into per-module files under `linker_modules/`
and turn `seed_links.py` into a pure shim. That refactor does not change
the `LinkerSpec` contract.
