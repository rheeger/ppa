# Linker architecture

The same event often leaves records in several services. A linker can connect a booking to related travel records or a meeting to its transcript. It proposes a relationship and records the evidence for it. Once accepted, that relationship is available to later questions and other agents.

Every module registers a `LinkerSpec`. Dispatch, enqueue filters, CLI inspection, and lifecycle controls use the registry. See [contributing a linker](CONTRIBUTING_LINKERS.md) for the workflow and [quality gates](runbooks/linker-quality-gates.md) for the required evidence.

## Package layout

| Path | Responsibility |
| --- | --- |
| `archive_cli/linker_framework.py` | `LinkerSpec`, `CatalogIndexSpec`, registration, private indexes, and lifecycle |
| `archive_cli/linker_cli.py` | List, inspect, scaffold, calibrate, replay, impact, and lifecycle commands |
| `archive_cli/linker_modules/` | Registered linker modules, including meeting artifacts, trips, finance, and identity |
| `archive_cli/seed_links.py` | Shared candidate and catalog records plus compatibility functions |
| `archive_cli/merchant_normalizer.py` | Merchant normalization used by structural matches |
| `archive_cli/iata.py` | Airport and city helpers for travel matches |

## The registration contract

`LinkerSpec` declares the module name, source card types, emitted link types, generator, scoring function and mode, catalog indexes, policies, and lifecycle state. It also carries model-judge requirements, post-promotion action, description, and optional evaluator and post-build hooks. Use the dataclass in `linker_framework.py` as the current field definition.

`register_linker(spec)` adds the module to `ALL_LINKERS`, records policies and proposed link types, registers active source-type enqueue rules, and includes eligible modules in model review. A retired module stays discoverable without being enqueued.

## Catalog indexes

A linker declares its own indexes through `CatalogIndexSpec`. Catalog construction runs registered post-build hooks; hooks attach private data with `set_private_index`, and consumers read it with `get_private_index`.

Shared indexes such as `cards_by_uid` and `calendar_events_by_ical_uid` remain named fields on `SeedLinkCatalog`. Reuse them instead of scanning the vault for every candidate.

## Scoring and evidence

The scoring function returns deterministic, lexical, graph, embedding, and risk components. The registered scoring mode combines those components and any model review:

| Mode | Final-confidence rule |
| --- | --- |
| `deterministic` | `det - risk` |
| `weighted` | `0.45 det + 0.12 lex + 0.13 graph + 0.18 llm + 0.12 emb - risk` |
| `semantic` | Legacy dual-tier `llm * emb - risk`; the semantic module is retired |
| `bespoke` | Delegates to `bespoke_evaluator` |

A numerical score does not replace evidence. Promotion rules must satisfy the [quality gates](runbooks/linker-quality-gates.md). Preserve candidate tiers, features, and review results so a contributor can explain why a link was accepted.

Warehouse `edges` do not contain `method`, `confidence`, or `evidence_uids` columns. Promoted inferred links have their own decision records. The [retrieval fidelity contract](RETRIEVAL_FIDELITY_CONTRACT.md) defines how these become serving records without inventing missing evidence.

## Publication after promotion

`post_promotion_action` distinguishes `edges_only`, `frontmatter_delta`, and `new_cards`. Edge-only changes do not require re-embedding unchanged text. Card changes must flow through the relevant materialization and publication path before clients can use them.

The archive keeps source facts, inferred links, and corrections distinguishable. A promoted link should not silently rewrite deterministic source fields.

## Lifecycle

| State | Scheduling | Inspection |
| --- | --- | --- |
| `active` | Enqueued | Visible |
| `deprecated` | Enqueued with warning | Visible |
| `retired` | Not enqueued | Visible with state |

Operators can override lifecycle state in `_artifacts/_linkers/_lifecycle_overrides.json`. `ppa linker retire`, `revive`, and `deprecate` write that file atomically; overrides apply on the next process start.

Retirement is a supported outcome when a relationship cannot be established reliably. The [retirement protocol](runbooks/linker-retirement-protocol.md) preserves the reason and evidence for later contributors.

## Compatibility

Keep shared candidate and catalog records importable from `archive_cli.seed_links` while existing callers use them. Register new modules so dispatch, inspection, and lifecycle commands all discover the same behavior.
