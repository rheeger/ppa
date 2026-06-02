# Section E Execution Plan - Processor DAG

## Objective

Define how v2.5 turns extraction, enrichment, embeddings, and linkers into an incremental processor DAG.

The source updater contract in Section D produces dirty inputs. The processor DAG decides which downstream work is stale, runs only what is necessary, and records enough status for Section F observability.

## Non-Goals

- Do not implement a generic workflow engine in this planning pass.
- Do not rewrite all extractors.
- Do not make every processor event-driven on day one.
- Do not rerun classification for already-classified email unless content hash or policy requires it.
- Do not run processors on suppressed/quarantine inputs by default.
- Do not change derived card schemas unless a later implementation plan proves it is necessary.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_a_email_corpus_semantics.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_c_future_gmail_sync_promotion.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_d_source_updater_contract.plan.md`
- `ppa/archive_sync/extractors/runner.py`
- `ppa/archive_sync/extractors/base.py`
- `ppa/archive_sync/extractors/registry.py`
- `ppa/archive_sync/llm_enrichment/enrich_runner.py`
- `ppa/archive_sync/llm_enrichment/enrichment_orchestrator.py`
- `ppa/archive_sync/llm_enrichment/card_enrichment_runner.py`
- `ppa/archive_sync/llm_enrichment/workflows/`
- `ppa/archive_cli/embedder.py`
- `ppa/archive_cli/seed_links.py`
- `ppa/archive_cli/commands/maintain.py`

## Agent Handoff Checklist

Before implementation:

- Read `README.md`, `v2.5vision.md`, Sections D, F, G, and this plan.
- Confirm Section A, Section B dry-run, Section B apply/rollback, Section C, Section D, and Section G commits are present.
- Confirm Section D leaves a source state/run surface that Section E can consume without running live source sync.
- Run `git status --short --branch` and stop if the tree is not clean.
- Start with processor declarations and stale-output detection before scheduling broad work.
- Represent existing extractors/enrichment/embedding/linkers in the DAG before adding new abstractions.
- Do not run full embeddings or all linkers by default.
- Implement report/status surfaces before wiring broad `ppa maintain` behavior.

Likely implementation files:

- processor declaration/status module, likely under `archive_cli/processors/`.
- processor state/run store or migration.
- `archive_cli/commands/maintain.py`
- `archive_cli/__main__.py` only for a small processor status/report CLI if needed.
- `archive_sync/extractors/runner.py`
- `archive_sync/llm_enrichment/enrichment_orchestrator.py`
- `archive_cli/embedder.py`
- `archive_cli/seed_links.py`

Required first tests:

- input hash changes mark output stale.
- processor version bump marks expected inputs stale.
- suppressed inputs skip active-only processors.
- dirty input triggers only expected processors.
- processor status can be read without running processors.
- missing vault/config/provider-like blocked states return structured output and documented exit code, not traceback.

Stop conditions:

- queue expansion implies full corpus processor rerun by default.
- suppressed cards can still be embedded or linked.
- processor output identity is not deterministic.
- rollback cannot identify outputs by run ID or output identity.
- implementation starts rewriting extractors or embedding/linker internals before declarations/staleness/reporting are in place.
- CLI/status paths produce tracebacks for normal missing config/vault/provider misuse.
- implementation runs full embeddings, all linkers, or broad LLM jobs by default.

## Core Concept

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

| Field | Meaning |
| ----- | ------- |
| `processor_key` | Stable name, e.g. `email_typed_extraction` |
| `processor_version` | Version of logic/prompt/schema affecting output |
| `input_card_types` | Card types consumed |
| `input_filters` | Required corpus state, classification, labels, etc. |
| `output_kinds` | Cards, embeddings, links, entity mentions, status rows |
| `output_identity` | Deterministic identity rule for outputs |
| `input_hash_fields` | Fields/body content that affect output |
| `active_only` | Whether suppressed/quarantine inputs are ignored |
| `depends_on` | Prior processors that must complete |
| `idempotent` | Whether repeated runs produce the same outputs |
| `llm_dependent` | Whether provider/model availability matters |
| `rollback_strategy` | How outputs can be reverted or superseded |

Initial implementation should define processor declarations without running processors. Prefer a registry that can be inspected by tests, `ppa status`, and Section F.

Recommended first declarations:

| Processor | Key | Active only | LLM dependent | Output kind |
| --------- | --- | ----------- | ------------- | ----------- |
| Email promotion policy | `email_promotion_policy` | No | No by default | `email_corpus_decisions` |
| Email typed extraction | `email_typed_extraction` | Yes | Sometimes | derived cards |
| Email thread enrichment | `email_thread_enrichment` | Yes | Yes | summaries/entities/matches |
| Materialization | `materialization` | No, but corpus-state aware | No | cards/chunks/projections |
| Embedding | `embedding` | Yes | External provider | embeddings |
| Linkers | `linkers` | Yes | Sometimes | graph edges/link decisions |
| Entity resolution | `entity_resolution` | Yes | Sometimes | person/place/org links |

## Processor Run Record

Every processor run should record:

| Field | Meaning |
| ----- | ------- |
| `run_id` | Processor run ID |
| `processor_key` | Processor name |
| `processor_version` | Version used |
| `input_uid` | Source card/decision/input ID |
| `input_hash` | Hash used for staleness |
| `input_corpus_state` | Active/suppressed/quarantine |
| `status` | `pending`, `running`, `complete`, `skipped`, `failed`, `stale` |
| `skip_reason` | Why skipped |
| `output_uids` | Derived cards or output rows |
| `error` | Failure summary |
| `started_at`, `completed_at` | Timing |

This can be implemented through an existing `enrichment_queue` evolution, a new processor table, or a lightweight sidecar store. The execution preference is Postgres once the design is proven, because Section F needs production status.

Preferred first implementation:

- Add a durable `processor_runs` / `processor_state` store when an index connection exists.
- Allow in-memory or fixture-only stores for tests.
- Do not treat report files as the primary state source.
- Link `processor_runs.run_id` to Section G `gate_runs.run_id` or mirror it exactly.

Recommended `processor_state` fields:

| Field | Meaning |
| ----- | ------- |
| `processor_key` | Primary processor identity |
| `processor_version` | Current version |
| `enabled` | Whether processor participates in maintenance |
| `last_success_at` | Last successful run |
| `last_attempt_at` | Last attempted run |
| `last_error` | Error payload |
| `pending_count` | Current pending count |
| `stale_count` | Current stale count |
| `failed_count` | Current failed count |
| `last_run_id` | Last processor run |

Recommended `processor_runs` fields:

| Field | Meaning |
| ----- | ------- |
| `run_id` | Gate-linked run ID |
| `processor_key` | Processor identity |
| `processor_version` | Version used |
| `archive_instance` | Section G archive instance |
| `status` | `success`, `partial`, `failed`, `blocked`, `skipped` |
| `input_count`, `dirty_count`, `stale_count`, `skipped_count`, `output_count` | Counts |
| `skip_reasons` | JSON counts by reason |
| `stale_reasons` | JSON counts by reason |
| `engine_mode` | `rust`, `python`, `n/a`, or `mixed` |
| `started_at`, `completed_at` | Timing |

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

| Trigger | Required behavior |
| ------- | ----------------- |
| Dirty input from source updater | Evaluate processors that consume that input |
| Processor version bump | Re-evaluate matching prior inputs |
| Corpus state changed to suppressed | Skip active-only processors and deactivate/filter outputs as needed |
| Corpus state changed to active | Queue active processors |
| Upstream output changed | Re-evaluate dependent processors |
| LLM provider unavailable | Skip LLM-dependent processors with visible status; continue deterministic processors |

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

Section E first implementation should not fully rewrite `ppa maintain`. Preferred first slice:

1. Add processor declaration registry.
2. Add staleness evaluation helpers.
3. Add processor state/run reports.
4. Add a dry-run/status CLI that reports pending/stale/skipped work.
5. Wire only a minimal `ppa maintain` reporting step if it is low-risk.
6. Do not execute broad processor work automatically.

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

| Field | Meaning |
| ----- | ------- |
| `run_id` | Gate-linked run ID |
| `processor_key` | Processor identity |
| `processor_version` | Version used |
| `archive_instance` | Section G archive instance |
| `status` | `success`, `partial`, `failed`, `blocked`, `skipped` |
| `input_count`, `dirty_count`, `stale_count`, `skipped_count`, `output_count` | Counts |
| `skip_reasons`, `stale_reasons` | JSON reason maps |
| `artifact_paths` | JSON report / summary paths |
| `engine_mode` | `rust`, `python`, `n/a`, or `mixed` |

## Rollback / Recovery

Processor rollback strategies:

- Deterministic derived cards: supersede or delete by output identity.
- LLM-derived cards: preserve provenance and output run ID; rollback by run ID.
- Embeddings: delete or mark inactive by chunk key/model.
- Linkers: delete/deactivate by linker version and source/target/relation.
- Enrichment field updates: restore prior field values from provenance/run history where available; otherwise rerun current processor after rollback.

Rollback should be scoped by processor run ID when possible.

## Definition of Done

Section E implementation is ready when:

- Processor declarations exist for the major v2.5 processor types.
- Dirty inputs from source updaters can be mapped to processor checks.
- Staleness can be computed from input hashes and processor versions.
- Active-only processors skip suppressed/quarantine inputs.
- Email typed extraction, thread enrichment, embeddings, and linkers are represented in the DAG.
- Processor status feeds Section F observability.
- Tests cover staleness, idempotency, version bumps, and suppression-aware downstream behavior.
- Section G gates pass through processor slice/staging validation before Arnold processor execution.
- Processor reports prove no default full embedding, full linker, or broad LLM rerun occurs.
- Processor declaration/status read paths work without running processors.
- Missing config/provider-like blocked states return structured output and documented exit code, not tracebacks.
- Full embeddings, all linkers, and broad LLM jobs require explicit opt-in flags and report blast radius before running.

## Completion Artifacts

The implementation agent must leave:

- processor declaration registry or equivalent.
- stale-output detection test report.
- dirty-input scheduling report for slice/staging.
- active-only suppression skip report.
- processor run report with engine mode, throughput, stale reasons, skipped reasons, and output identities.
- rollback evidence for generated processor outputs.
- blocked/failure report examples for missing provider/config.
- processor status sample consumed by Section F.
- blast-radius report proving no default full embedding/linker/LLM work.

## Commit Instructions

Commit this section by itself.

- Start only from a clean tree.
- Stage only Section E implementation, tests, and artifacts.
- Commit subject: `v2.5 section E: processor dag`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before Section F work starts.
