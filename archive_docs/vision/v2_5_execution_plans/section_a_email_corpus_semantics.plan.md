# Section A Execution Plan - Email Corpus Semantics

**Status (Aug 2026, HEAD `5980464`):** Policy implemented and fixture-tested. **Product fork locked:** quarantine is labeled vault cards (`retrieval_weight=0.35`, inbound `emit_cards=True`), not a compact review record with no cards. Suppressed marketing is deleted from vault + purged from index + Gmail ledger; suppressed inbound does not emit. Written C3 (“quarantine = compact review, no cards”) is superseded. This seed is the living corpus; Arnold is not the home.

## Objective

Define the shared email corpus model for v2.5. This plan specifies the states, policy inputs, policy outputs, classification reuse rules, examples, and acceptance criteria for `EmailPromotionPolicy`.

This section is conceptual but binding. Sections B and C must use this policy rather than inventing separate cleanup and future-sync logic.

## Non-Goals

- Do not implement the policy in this planning pass.
- Do not modify Gmail adapters, materializers, CLI commands, schemas, or enrichment code in this planning pass.
- Do not define a new marketing-only classifier.
- Do not decide physical vault pruning in this section. Product fork (locked this campaign, owned by B/C): suppressed marketing is deleted; quarantine stays as labeled cards.
- Do not change derived card schemas.
- Do not change the meaning of existing typed extraction categories except where this plan separates corpus membership from typed extraction.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2vision.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_sync/llm_enrichment/classify.py`
- `ppa/archive_sync/llm_enrichment/classify_index.py`
- `ppa/archive_sync/llm_enrichment/enrich_runner.py`
- `ppa/archive_sync/llm_enrichment/known_senders.py`
- `ppa/archive_sync/llm_enrichment/workflows/email_thread.py`
- `ppa/archive_vault/schema.py`
- `ppa/archive_cli/seed_links.py`
- `ppa/archive_cli/schema_ddl.py`

## Agent Handoff Checklist

Before implementation:

- Read `README.md`, `v2.5vision.md`, and this plan.
- Confirm Section G reporting/ladder conventions exist or implement them first.
- Do not touch Gmail adapters, materializers, schema, or cleanup commands in this section.
- Implement policy logic behind fixture tests before any real vault evaluation.

Likely implementation files:

- new policy module under `archive_sync/llm_enrichment/` or `archive_cli/corpus_hygiene/`.
- tests under `archive_tests/` for policy examples and rule ordering.

Required first tests:

- marketing newsletter -> suppressed.
- transactional receipt -> active + typed extraction.
- personal reply thread -> active + thread enrichment.
- starred promotional thread -> quarantine.
- low-confidence attachment thread -> quarantine.

Stop conditions:

- policy requires a new classifier instead of reusing existing classification.
- personal mail is treated as non-corpus solely because it is not typed-extractable.
- rule ordering produces nondeterministic decisions for fixed inputs.

## Core Concepts

### Corpus State

Every Gmail thread receives one corpus state:

| State | Meaning | Included in active vault cards | Included in Postgres default retrieval | Embedded by default | Eligible for enrichment/linking |
| ----- | ------- | ------------------------------ | -------------------------------------- | ------------------- | ------------------------------- |
| `active` | First-class archive artifact | Yes | Yes | Yes | Yes |
| `suppressed` | Intentionally excluded from active corpus, ledger only | No for future sync; inactive for historical cleanup | No | No | No |
| `quarantine` | Ambiguous or conflicting signals; review needed. **Locked fork:** labeled cards stay (`retrieval_weight=0.35`); inbound writes cards (`emit_cards=True`). Compact-review-only is superseded. | Yes — labeled cards, not compact-only | Downweighted (`retrieval_weight=0.35`); not default-equal to active | No | No by default |

### Processor Decision

`processor_decision` is separate from `corpus_decision`.

| Processor decision | Meaning |
| ------------------ | ------- |
| `typed_extraction` | Run deterministic/LLM typed extraction for transaction-like facts |
| `thread_enrichment` | Keep as active correspondence and allow summaries/entities/matches |
| `no_downstream_processing` | Keep active if policy requires, but do not spend extraction/enrichment effort |
| `suppressed_no_processing` | Suppressed records never enter downstream processors |
| `quarantine_review` | Hold for review before downstream work |

This separation is mandatory. Existing classifiers may skip `personal` email for typed extraction, but v2.5 must not interpret that as "remove personal correspondence from the active corpus."

## Policy Inputs

`EmailPromotionPolicy` should consume a normalized input object. The implementation plan can name the concrete class later, but the semantics should be:

| Field | Source | Purpose |
| ----- | ------ | ------- |
| `source_key` | Source updater | Stable source/account identity, e.g. `gmail-messages:account@example.com` |
| `gmail_thread_id` | Gmail | Durable external thread identity |
| `gmail_history_id` | Gmail | Change detection and cursor safety |
| `thread_body_sha` | Gmail adapter / hydration | Determines whether classification is stale |
| `subject` | Gmail | Marketing and transactional signals |
| `from_emails` | Gmail | Known sender/domain classification |
| `participant_emails` | Gmail | Owner participation and correspondence signals |
| `label_ids` | Gmail | `CATEGORY_PROMOTIONS`, `CATEGORY_PERSONAL`, `IMPORTANT`, `STARRED`, sent/outbound labels |
| `message_count` | Gmail | Thread depth and back-and-forth signal |
| `first_message_at`, `last_message_at` | Gmail | Reporting and review context |
| `has_attachments` | Gmail | Review bucket and future attachment policy |
| `calendar_event_hints` | Gmail adapter | Active override signal |
| `classification` | Existing classifier/classification store | Core semantic category |
| `confidence` | Existing classifier/classification store | Suppression/quarantine threshold input |
| `card_types` | Existing classifier/classification store | Typed extraction routing |
| `classification_source` | Reuse layer | Auditability and rerun minimization |
| `classify_prompt_version` | Classifier | Staleness/audit context |
| `classify_model` | Classifier | Staleness/audit context |
| `policy_version` | v2.5 policy | Reinterpretation and rollback |
| `manual_overrides` | Operator config/review | Allow/block/keep/suppress policy input |

## Classification Categories

The policy must normalize existing category variants into canonical categories:

| Canonical category | Known existing variants |
| ------------------ | ----------------------- |
| `transactional` | `transactional`, `transactional_receipt`, `booking_confirmation`, `shipping_notification`, `subscription_event`, `purchase_receipt`, `payroll_notification` |
| `personal` | `personal`, `person_to_person` |
| `marketing` | `marketing`, `promotion` |
| `automated` | `automated`, `automated_notification` |
| `noise` | `noise`, `skip`, empty classifier error responses when confidence is low |
| `unknown` | missing classification |

Implementation should keep the raw classification for audit while making decisions against the canonical category.

## Corpus Decision Rules

Rules are ordered. Earlier rules win.

### 1. Manual Overrides

Manual overrides are policy inputs, not ad hoc edits.

| Override | Effect |
| -------- | ------ |
| `always_active_thread` | Force `active` unless source record is unavailable/deleted |
| `always_suppress_thread` | Force `suppressed` unless already tied to preserved derived cards requiring review |
| `always_active_sender` | Force `active` for sender/domain |
| `always_suppress_sender` | Force `suppressed` for sender/domain |
| `always_keep_starred` | Treat `STARRED` as active override |
| `always_keep_important` | Treat `IMPORTANT` as active override |

Forced decisions should still record the pre-override recommendation and reason.

### 2. Owner Action Overrides

Promote `active` when any are true:

- owner sent a message in the thread.
- owner replied to the thread.
- thread has clear back-and-forth human participation.
- thread has `STARRED` or `IMPORTANT`, unless high-confidence `noise`.
- thread has calendar event hints or invite links tied to real activity.

### 3. Transactional Promotion

Promote `active` and set `processor_decision = typed_extraction` when:

- canonical category is `transactional`; or
- known transactional domain/extractor matched and classification did not reject; or
- classifier returned non-empty allowed `card_types` with sufficient confidence.

### 4. Personal Promotion

Promote `active` and set `processor_decision = thread_enrichment` when:

- canonical category is `personal`; and
- there is owner participation, `CATEGORY_PERSONAL`, a non-bulk sender, or a meaningful multi-message thread.

Personal single-message passive mail with no owner interaction can be `quarantine` if confidence or signals are weak.

### 5. Marketing / Automated / Noise Suppression

Suppress when all are true:

- canonical category is `marketing`, `automated`, or `noise`; and
- confidence is high enough for suppression; and
- no manual, owner-action, transactional, starred, important, or calendar override applies.

Suppress when Gmail labels include `CATEGORY_PROMOTIONS` and classification or deterministic gates agree it is marketing/bulk.

### 6. Quarantine

Quarantine when:

- classification confidence is below threshold.
- classification is missing and deterministic gates are inconclusive.
- `CATEGORY_PROMOTIONS` conflicts with `STARRED` or `IMPORTANT`.
- classifier says marketing/noise but owner replied.
- thread has attachments and would otherwise be suppressed.
- thread has existing derived cards but raw email would otherwise be suppressed.

Quarantine is a review path, not a permanent state. Cards stay in the vault as labeled records (`retrieval_weight=0.35`). They are not compact-review-only and they are not deleted with suppressed marketing.

## Confidence Thresholds

The execution standard should define thresholds as policy constants, not hidden prompt behavior:

| Threshold | Initial recommendation | Meaning |
| --------- | ---------------------- | ------- |
| `SUPPRESS_CONFIDENCE_MIN` | `0.75` | Minimum confidence for automatic suppression without overrides |
| `PROMOTE_TRANSACTIONAL_MIN` | `0.50` | Minimum confidence for typed extraction eligibility when card types are present |
| `QUARANTINE_BELOW` | `0.50` | Below this, unknown or conflicting classifications go to quarantine |

These can be tuned later, but implementation should surface them in policy metadata and reports.

## Decision Record

Each policy evaluation should produce a decision record:

| Field | Meaning |
| ----- | ------- |
| `source_key` | Source/account identity |
| `external_id` | Gmail thread ID |
| `external_history_id` | Gmail history ID |
| `content_hash` | Thread body hash or metadata hash used for staleness |
| `policy_version` | `EMAIL_PROMOTION_POLICY_VERSION` |
| `classification` | Raw classifier output |
| `canonical_classification` | Normalized category |
| `confidence` | Classifier confidence |
| `card_types` | Suggested extraction card types |
| `classification_source` | `card_classifications`, `classify_index`, `frontmatter`, `stage0`, `new_llm`, etc. |
| `corpus_decision` | `active`, `suppressed`, or `quarantine` |
| `processor_decision` | Typed extraction/enrichment/no processing |
| `decision_reason` | Primary reason key |
| `decision_signals` | Secondary signals and overrides |
| `evaluated_at` | Evaluation timestamp |

The record should preserve enough information for dry-run diffs, rollback, and future policy reinterpretation without rerunning the LLM.

## Example Decisions

### Marketing Newsletter

Signals:

- Label: `CATEGORY_PROMOTIONS`
- Classification: `marketing`
- Confidence: `0.91`
- No owner reply, no starred/important, no transactional card types

Decision:

- `corpus_decision = suppressed`
- `processor_decision = suppressed_no_processing`
- `decision_reason = marketing_classification_with_promotions_label`

### Transactional Receipt

Signals:

- Sender: `doordash.com`
- Classification: `transactional`
- Card types: `["meal_order"]`
- Confidence: `0.84`

Decision:

- `corpus_decision = active`
- `processor_decision = typed_extraction`
- `decision_reason = transactional_extractable`

### Personal Reply Thread

Signals:

- Classification: `personal`
- Owner sent one or more messages
- Multiple participants and multi-message thread

Decision:

- `corpus_decision = active`
- `processor_decision = thread_enrichment`
- `decision_reason = owner_participation`

### Starred Promotional Thread

Signals:

- Label: `CATEGORY_PROMOTIONS`
- Classification: `marketing`
- Starred by owner
- No transactional card types

Decision:

- `corpus_decision = quarantine`
- `processor_decision = quarantine_review`
- `decision_reason = starred_marketing_conflict`

### Low-Confidence Ambiguous Thread

Signals:

- Classification: `automated`
- Confidence: `0.42`
- Has attachment
- No owner reply

Decision:

- `corpus_decision = quarantine`
- `processor_decision = quarantine_review`
- `decision_reason = low_confidence_with_attachment`

## CLI/API Behavior Decisions

Section A does not require a CLI by itself, but later commands should expose these policy concepts:

- Dry-run reports should group by `corpus_decision`, `processor_decision`, `decision_reason`, and `classification_source`.
- Status should report counts by corpus state.
- Review tools should allow manual overrides as policy inputs.
- Future source updaters should return promoted/suppressed/quarantined counts.

## Migration / Rollout Notes

- Section B applies this policy to existing seed cards (filename “Arnold cleanup” is historical).
- Section C applies this policy to future Gmail sync before card promotion.
- The first implementation should keep policy behavior deterministic and auditable before optimizing performance.
- Physical deletion of **suppressed** marketing is the locked B/C fork (this seed already applied). Quarantine is not deleted.

## Tests and Validation

Future implementation should include:

- Unit tests for every example above.
- Unit tests for canonical category normalization.
- Unit tests for rule ordering.
- Unit tests proving `personal` can be active while not typed-extractable.
- Unit tests proving high-confidence `marketing` with no overrides is suppressed.
- Unit tests proving owner reply overrides marketing suppression into active or quarantine as specified.
- Golden fixture tests for dry-run decision stability.

## Validation Ladder and Rust Standard

Section A is validated first because every later section depends on the policy semantics.

Required gates:

1. **Synthetic fixtures:** all example decisions in this plan pass without vault access or LLM calls.
2. **Small slice:** policy decisions are generated for real marketing, transactional, personal, starred promotional, and ambiguous Gmail threads.
3. **Larger slice:** decision counts remain stable and reports remain bounded.
4. **Local seed dry-run:** policy evaluates seed records using existing classifications first.
5. **Arnold dry-run:** historical. **Not a v2.5 closer.** This seed already received apply.

Rust standard:

- Use Rust-backed cache/type-filtered scans where real vault data is needed.
- Record `PPA_ENGINE` / engine mode in any policy dry-run report.
- Treat Rust/Python scan or hash divergence as blocking before Section B apply work.

## Rollback / Recovery

Policy decisions must be stored with `policy_version`. Rollback means:

- Reinterpret the same classification with an older policy version; or
- restore prior decision records from decision history.

Rollback must not require new LLM classification.

## Operational Reporting

Reports should include:

- total evaluated threads.
- counts by corpus state.
- counts by processor decision.
- counts by canonical classification.
- counts by classification source.
- counts by decision reason.
- override counts.
- quarantine counts and review samples.

## Definition of Done

Section A implementation is ready when:

- `EmailPromotionPolicy` semantics are implemented as documented.
- Existing and new classifiers can feed the same policy input shape.
- Policy decisions are deterministic for fixed inputs.
- Decision records preserve raw and canonical classification.
- Tests cover the example decisions and rule ordering.
- Section B and Section C can consume the policy without redefining behavior.
- Section G fixture and small-slice gates pass for policy decisions.
- A policy decision report fixture exists and includes corpus decision, processor decision, reason, signals, policy version, classification source, and engine mode when real vault data is used.

## Completion Artifacts

The implementation agent must leave:

- policy fixture test output.
- small-slice policy dry-run report.
- sample decisions for the five required examples.
- documented policy version constant.
- no production mutations.

## Commit Instructions

Commit this section by itself.

- Start only from a clean tree.
- Stage only Section A implementation, tests, and artifacts.
- Commit subject: `v2.5 section A: email corpus semantics`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before any Section B work starts.
