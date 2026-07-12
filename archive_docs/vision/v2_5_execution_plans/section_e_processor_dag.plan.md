# Section E Execution Plan - Processor DAG

## Objective

Define how v2.5 turns extraction, enrichment, embeddings, and linkers into an incremental processor DAG, then **execute** that DAG on dirty inputs from Section D.

The source updater contract in Section D produces dirty inputs. The processor DAG decides which downstream work is stale, runs only what is necessary, and records enough status for Section F observability.

## Implementation Phases

### Phase 1 — Contract (LANDED)

Commit: `v2.5 section E: processor dag` (`b24a4114` on branch `v2.5`).

Delivered:

- Processor declarations for promotion, typed extraction, thread enrichment, materialization, embeddings, linkers, entity resolution.
- Staleness / input-hash helpers and plan builder.
- Run report shapes and state store.
- CLI: `ppa processors` (declarations, status, plan).
- `ppa maintain --record-processor-status` seeds status only — **does not run processors**.

Phase 1 is **not** incremental refresh. Do not treat it as Section E complete for v3 readiness.

### Phase 2 — Execution (NEXT, AFTER D PHASE 2)

Objective: consume dirty UIDs from source updater runs, plan processors, execute only stale/pending work via existing extract/enrich/embed/link entrypoints, wire into `ppa maintain`.

Non-goals for Phase 2:

- Do not invent a generic workflow engine.
- Do not rewrite extractors/embedders/linkers.
- Do not run full embeddings, all linkers, or broad LLM jobs by default.
- Do not process suppressed/quarantine inputs on active-only processors.

#### Phase 2 Agent Handoff Checklist

Before implementation:

- Confirm D Phase 2 commit present and dirty UID artifact path known.
- Confirm tree clean on `v2.5`.
- Read this plan, Section D Phase 2, Section H, `maintain.py`, extract/enrich/embed/link entrypoints.
- Start by executing **one** cheap processor path end-to-end (e.g. materialization or typed extraction on a fixture dirty set) before wiring all processors.

Likely files:

- `archive_sync/processors/runner.py` (new) — execute a plan item via existing entrypoints.
- `archive_cli/processors/cli.py` — implement `run` with `--apply` / dry-run defaults.
- `archive_cli/commands/maintain.py` — after source updaters, call processor runner on dirty UIDs.
- Thin adapters into `extractors/runner.py`, enrich orchestrator, embedder, seed_links — not rewrites.

Required CLI shape:

```bash
# Plan only
ppa processors plan --dirty-uids PATH --format json

# Execute planned work for one processor (dry-run default)
ppa processors run --processor email_typed_extraction --dirty-uids PATH --dry-run --format json
ppa processors run --processor email_typed_extraction --dirty-uids PATH --apply --run-id <id> --format json

# Maintain
ppa maintain --run-source-updaters --run-processors
```

Expensive work still requires Section G opt-in guards (`guard_expensive_work_opt_in`).

#### Phase 2 Required Behavior

1. Load dirty UIDs from D Phase 2 artifact or maintain in-memory handoff.
2. Build processor plan (staleness, active-only skips, dependency order).
3. Execute pending items by calling existing runners; record `processor_runs`.
4. Idempotent upserts by output identity; skip already-current outputs.
5. Isolate LLM failures from deterministic processors.
6. Write reports with counts, skip/stale reasons, engine mode, next action.

#### Phase 2 Definition of Done

- Dirty UIDs from a source updater run trigger only expected processors.
- `ppa maintain --run-processors` executes the plan after updaters (or from provided dirty path).
- Suppressed inputs skip active-only processors.
- No default full embedding / all-linker / broad LLM rerun.
- Tests cover execution idempotency, failure isolation, and suppression skips.
- Commit subject: `v2.5 section E: processor dag execution`

#### Phase 2 Completion Artifacts

- Runner + CLI `run`.
- Maintain flag wiring.
- Plan+run reports from fixture dirty set.
- Focused tests; tree clean after one commit.

## Non-Goals (whole section)

- Do not implement a generic workflow engine.
- Do not rewrite all extractors.
- Do not make every processor event-driven on day one.
- Do not rerun classification for already-classified email unless content hash or policy requires it.
- Do not run processors on suppressed/quarantine inputs by default.
- Do not change derived card schemas unless a later implementation plan proves it is necessary.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_d_source_updater_contract.plan.md`
- `ppa/archive_sync/processors/` (Phase 1)
- `ppa/archive_sync/extractors/runner.py`
- `ppa/archive_sync/llm_enrichment/enrichment_orchestrator.py`
- `ppa/archive_cli/embedder.py`
- `ppa/archive_cli/seed_links.py`
- `ppa/archive_cli/commands/maintain.py`

## Core Concept (Phase 1 + Phase 2)

A processor is any deterministic or LLM-assisted unit of work that consumes active cards or source decision records and produces cards, rows, chunks, embeddings, links, summaries, or status.

Examples:

- email promotion classifier.
- deterministic email extractors.
- LLM typed email extraction.
- email thread enrichment.
- calendar event enrichment.
- iMessage thread enrichment.
- entity resolution.
- materialization.
- embedding.
- deterministic linkers.
- semantic/linker judge flows, if active.

## Processor Declaration

Every processor should declare:

| Field               | Meaning                                                |
| ------------------- | ------------------------------------------------------ |
| `processor_key`     | Stable name, e.g. `email_typed_extraction`             |
| `processor_version` | Version of logic/prompt/schema affecting output        |
| `input_card_types`  | Card types consumed                                    |
| `input_filters`     | Required corpus state, classification, labels, etc.    |
| `output_kinds`      | Cards, embeddings, links, entity mentions, status rows |
| `output_identity`   | Deterministic identity rule for outputs                |
| `input_hash_fields` | Fields/body content that affect output                 |
| `active_only`       | Whether suppressed/quarantine inputs are ignored       |
| `depends_on`        | Prior processors that must complete                    |
| `idempotent`        | Whether repeated runs produce the same outputs         |
| `llm_dependent`     | Whether provider/model availability matters            |
| `rollback_strategy` | How outputs can be reverted or superseded              |

Initial implementation should define processor declarations without running processors. Prefer a registry that can be inspected by tests, `ppa status`, and Section F.

Recommended first declarations:

| Processor               | Key                       | Active only                | LLM dependent     | Output kind                |
| ----------------------- | ------------------------- | -------------------------- | ----------------- | -------------------------- |
| Email promotion policy  | `email_promotion_policy`  | No                         | No by default     | `email_corpus_decisions`   |
| Email typed extraction  | `email_typed_extraction`  | Yes                        | Sometimes         | derived cards              |
| Email thread enrichment | `email_thread_enrichment` | Yes                        | Yes               | summaries/entities/matches |
| Materialization         | `materialization`         | No, but corpus-state aware | No                | cards/chunks/projections   |
| Embedding               | `embedding`               | Yes                        | External provider | embeddings                 |
| Linkers                 | `linkers`                 | Yes                        | Sometimes         | graph edges/link decisions |
| Entity resolution       | `entity_resolution`       | Yes                        | Sometimes         | person/place/org links     |

## Processor Run Record

Every processor run should record:

| Field                        | Meaning                                                        |
| ---------------------------- | -------------------------------------------------------------- |
| `run_id`                     | Processor run ID                                               |
| `processor_key`              | Processor name                                                 |
| `processor_version`          | Version used                                                   |
| `input_uid`                  | Source card/decision/input ID                                  |
| `input_hash`                 | Hash used for staleness                                        |
| `input_corpus_state`         | Active/suppressed/quarantine                                   |
| `status`                     | `pending`, `running`, `complete`, `skipped`, `failed`, `stale` |
| `skip_reason`                | Why skipped                                                    |
| `output_uids`                | Derived cards or output rows                                   |
| `error`                      | Failure summary                                                |
| `started_at`, `completed_at` | Timing                                                         |

This can be implemented through an existing `enrichment_queue` evolution, a new processor table, or a lightweight sidecar store. The execution preference is Postgres once the design is proven, because Section F needs production status.

Preferred first implementation:

- Add a durable `processor_runs` / `processor_state` store when an index connection exists.
- Allow in-memory or fixture-only stores for tests.
- Do not treat report files as the primary state source.
- Link `processor_runs.run_id` to Section G `gate_runs.run_id` or mirror it exactly.

Recommended `processor_state` fields:

| Field               | Meaning                                       |
| ------------------- | --------------------------------------------- |
| `processor_key`     | Primary processor identity                    |
| `processor_version` | Current version                               |
| `enabled`           | Whether processor participates in maintenance |
| `last_success_at`   | Last successful run                           |
| `last_attempt_at`   | Last attempted run                            |
| `last_error`        | Error payload                                 |
| `pending_count`     | Current pending count                         |
| `stale_count`       | Current stale count                           |
| `failed_count`      | Current failed count                          |
| `last_run_id`       | Last processor run                            |

Recommended `processor_runs` fields:

| Field                                                                        | Meaning                                              |
| ---------------------------------------------------------------------------- | ---------------------------------------------------- |
| `run_id`                                                                     | Gate-linked run ID                                   |
| `processor_key`                                                              | Processor identity                                   |
| `processor_version`                                                          | Version used                                         |
| `archive_instance`                                                           | Section G archive instance                           |
| `status`                                                                     | `success`, `partial`, `failed`, `blocked`, `skipped` |
| `input_count`, `dirty_count`, `stale_count`, `skipped_count`, `output_count` | Counts                                               |
| `skip_reasons`                                                               | JSON counts by reason                                |
| `stale_reasons`                                                              | JSON counts by reason                                |
| `engine_mode`                                                                | `rust`, `python`, `n/a`, or `mixed`                  |
| `started_at`, `completed_at`                                                 | Timing                                               |

## Staleness Rules

A processor output is stale when any of these change:

- input card UID exists but input hash changed.
- source updater marks the input dirty.
- upstream processor output changed.
- `processor_version` changed.
- input corpus state changed from active to suppressed/quarantine or back.
- output is missing or failed.

A processor output is not stale merely because wall-clock time passed, unless the processor explicitly declares a time-sensitive dependency.

## Rerun Rules

| Trigger                            | Required behavior                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------ |
| Dirty input from source updater    | Evaluate processors that consume that input                                          |
| Processor version bump             | Re-evaluate matching prior inputs                                                    |
| Corpus state changed to suppressed | Skip active-only processors and deactivate/filter outputs as needed                  |
| Corpus state changed to active     | Queue active processors                                                              |
| Upstream output changed            | Re-evaluate dependent processors                                                     |
| LLM provider unavailable           | Skip LLM-dependent processors with visible status; continue deterministic processors |

## Email Processor Flow

Email should use these processors:

### 1. `email_promotion_policy`

Input:

- Gmail source record metadata.
- existing classification.

Output:

- `email_corpus_decisions` record.
- corpus decision.
- processor decision.

Active-only:

- No. This processor decides active/suppressed/quarantine.

Version:

- `EMAIL_PROMOTION_POLICY_VERSION`.

### 2. `email_typed_extraction`

Input:

- active email threads with `processor_decision = typed_extraction`.

Output:

- derived typed cards like `meal_order`, `ride`, `flight`, `purchase`, etc.

Active-only:

- Yes.

Version:

- deterministic extractor versions and/or LLM extraction prompt/model schema version.

### 3. `email_thread_enrichment`

Input:

- active personal/transactional email threads eligible for summaries/entities/matches.

Output:

- thread summaries.
- entity mentions.
- match candidates.

Active-only:

- Yes.

### 4. `email_embedding`

Input:

- active email cards/chunks only.

Output:

- embeddings.

Active-only:

- Yes.

### 5. `email_linkers`

Input:

- active email and derived cards.

Output:

- graph edges.

Active-only:

- Yes. Suppressed cards cannot be source or target candidates.

## Non-Email Processor Mapping

### Calendar

- materialize changed events.
- embed active event chunks.
- run event/linker processors that connect calendar to email threads, meeting transcripts, places, people.

### iMessage

- materialize new/changed messages.
- enrich active threads if configured.
- embed active chunks.
- link to people, calendar, places if applicable.

### Photos

- materialize active metadata cards.
- embed text metadata only if meaningful.
- link media to temporal/location context only when active.

### Health / Structured Sources

- materialize structured records.
- embed summaries if active.
- link to organizations, providers, places, time windows.

## Processor DAG Ordering

Recommended order during `ppa maintain`:

```mermaid
flowchart LR
  source[Source Updaters] --> promotion[Promotion Decisions]
  promotion --> materialize[Materialization]
  materialize --> extract[Typed Extraction]
  extract --> entities[Entity Resolution]
  materialize --> enrich[Card Enrichment]
  entities --> embed[Embeddings]
  enrich --> embed
  embed --> linkers[Linkers]
  linkers --> status[Status Report]
```

The implementation can run independent processors in parallel later, but the first version should favor clarity and idempotency.

Section E Phase 1 delivered declarations/staleness/reporting only. Phase 2 must:

1. Keep the declaration registry and staleness helpers.
2. Add an execution runner that calls existing extract/enrich/embed/link entrypoints.
3. Wire `ppa maintain --run-processors` after `--run-source-updaters`.
4. Keep dry-run/status CLI working without executing processors.
5. Never execute broad processor work automatically without opt-in flags.

## Versioning

Processor versions should be explicit constants or metadata values.

Examples:

- `EMAIL_PROMOTION_POLICY_VERSION`
- `CLASSIFY_PROMPT_VERSION`
- `extractor_registry_version`
- `LLM_EMAIL_EXTRACTION_PROMPT_VERSION`
- `EMAIL_THREAD_ENRICHMENT_PROMPT_VERSION`
- `EMBEDDING_MODEL_ID`
- `LINKER_VERSION`

Version bumps should be intentional and report their blast radius:

- number of inputs now stale.
- expected processor queue size.
- expected LLM-dependent work.
- whether old outputs are superseded or deleted.

## Output Identity

Idempotency depends on deterministic output identity.

Required standards:

- Derived card UIDs remain deterministic from source UID/external ID and meaningful payload fields.
- Embedding chunk keys remain stable for unchanged active chunks.
- Linker outputs use stable source/target/relation identity.
- Processor output records can be upserted by `(processor_key, input_uid, output_identity)`.

## Suppression-Aware Behavior

When an input becomes suppressed:

- active-only processors should skip it.
- existing embeddings for its chunks should be excluded from default vector search.
- linkers should remove or deactivate edges where suppressed cards are source/target, unless the edge connects an active derived card to suppressed provenance.
- enrichment queues should drop pending work for suppressed inputs.
- status should count skipped work by suppression reason.

When an input returns to active:

- queue active processors using current input hash and processor versions.

## Tests and Validation

Future implementation should include:

- Unit tests for processor declaration validation.
- Unit tests for input hash calculation.
- Unit tests for staleness detection.
- Unit tests for processor version bump behavior.
- Unit tests for active-only suppression skips.
- Integration tests where a source update triggers only expected processors.
- Integration tests proving repeated runs are idempotent.
- Integration tests proving suppressed email is not embedded or linked.
- Integration tests proving re-promoted email queues processors.
- Failure tests proving LLM-dependent processor failure does not block deterministic processors.
- CLI test proving processor status/dry-run works without executing processors.
- CLI test proving missing provider/config-like blocked states return structured output and exit `4`, not traceback.
- Test proving full embedding/all-linker/broad LLM work requires explicit opt-in.

## Validation Ladder and Rust Standard

The processor DAG must prove it prevents broad reruns before production use.

Required gates:

1. **Synthetic fixtures:** staleness, version bumps, active-only skips, and idempotent output identity pass.
2. **Small slice:** dirty inputs trigger only expected processors.
3. **Larger slice:** queue size, runtime, and skipped suppressed inputs are measured.
4. **Local seed dry-run:** expected processor blast radius is reported without running expensive jobs by default.
5. **Local seed staging apply:** affected processors run on staging outputs and rollback works by run ID/output identity.
6. **Arnold dry-run:** report processor work that would run after cleanup/future sync.
7. **Arnold reviewed execution:** only reviewed processor runs execute on production.

Rust standard:

- Use Rust materialization/chunking for active changed cards wherever supported.
- Use Rust cache/type-filtered scans to compute input sets.
- Do not run full embeddings or all linkers by default; process dirty active inputs only.
- Processor reports must include engine mode, elapsed runtime, throughput, skipped suppressed count, and stale reason counts.
- Rust/Python divergence in chunk keys, materialized rows, or active/suppressed filtering blocks Arnold processor execution.

## Operational Reporting

Processor status should report:

- pending count by processor.
- running count by processor.
- failed count by processor.
- skipped count by reason.
- stale count by version/input/corpus-state reason.
- completed count in last maintenance run.
- LLM-dependent skipped count due to provider unavailability.
- top errors and retryability.

Recommended CLI surface:

```bash
ppa processors status --format json
ppa processors plan --dirty-uids PATH --format json
ppa processors run --processor <key> --run-id <run_id> --apply
```

Exact names can change, but Section F must have a programmatic way to read processor status and stale-work estimates without scraping logs.

Report fields should include:

| Field                                                                        | Meaning                                              |
| ---------------------------------------------------------------------------- | ---------------------------------------------------- |
| `run_id`                                                                     | Gate-linked run ID                                   |
| `processor_key`                                                              | Processor identity                                   |
| `processor_version`                                                          | Version used                                         |
| `archive_instance`                                                           | Section G archive instance                           |
| `status`                                                                     | `success`, `partial`, `failed`, `blocked`, `skipped` |
| `input_count`, `dirty_count`, `stale_count`, `skipped_count`, `output_count` | Counts                                               |
| `skip_reasons`, `stale_reasons`                                              | JSON reason maps                                     |
| `artifact_paths`                                                             | JSON report / summary paths                          |
| `engine_mode`                                                                | `rust`, `python`, `n/a`, or `mixed`                  |

## Rollback / Recovery

Processor rollback strategies:

- Deterministic derived cards: supersede or delete by output identity.
- LLM-derived cards: preserve provenance and output run ID; rollback by run ID.
- Embeddings: delete or mark inactive by chunk key/model.
- Linkers: delete/deactivate by linker version and source/target/relation.
- Enrichment field updates: restore prior field values from provenance/run history where available; otherwise rerun current processor after rollback.

Rollback should be scoped by processor run ID when possible.

## Definition of Done

**Phase 1 (landed):** declarations, staleness/plan, status CLI, maintain snapshot hooks, contract tests.

**Phase 2 (required for H / v3):**

- Dirty inputs from source updaters map to executable processor plans.
- Runner executes only stale/pending active work via existing entrypoints.
- Active-only processors skip suppressed/quarantine inputs.
- `ppa maintain --run-processors` participates in the live cycle.
- Reports prove no default full embedding, full linker, or broad LLM rerun.
- Missing config/provider blocked states return structured exit `4`.
- Section H seed/Arnold processor gates can pass.

## Completion Artifacts

**Phase 1:** declaration registry, staleness tests, plan/status samples.

**Phase 2:** runner + CLI `run`, maintain wiring, fixture dirty-set run report, suppression skip proof, commit `v2.5 section E: processor dag execution`.

## Commit Instructions

Phase 1 already committed as `v2.5 section E: processor dag`.

Phase 2:

- Start only from a clean tree after D Phase 2.
- Stage only Phase 2 execution files, tests, and artifacts.
- Commit subject: `v2.5 section E: processor dag execution`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before Section H (or F readiness hardening) starts.
