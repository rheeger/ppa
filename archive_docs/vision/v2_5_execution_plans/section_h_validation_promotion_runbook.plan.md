# Section H Execution Plan - v2.5 Validation and Promotion Runbook

**Status (Aug 2026, HEAD `5980464`):** **v2.5-local is complete.** Local gates 0–5 / earlier 5b and the scale ladder are landed. Hygiene + live updaters + soak already ran **on the canonical seed** `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` schema `ppa` — not a staging copy. The written sequence (slice vault-remove → 1pct/10pct updaters → Gate 5b on a staging copy → Arnold) was **skipped** and is **not** the remaining path.

**Completion note:** `local_seed_living_corpus`. Formal `ready: false` leftover from missing `validation_gates` / `corpus_cleanup` review rows is an **accepted local exception**. Do not restage a fake ladder. Do not invent fake gate artifacts. Do not copy the seed. Do not deploy Arnold.

“Never mutate the canonical seed” was a written constraint for an Arnold promotion path. This seed **is** the living corpus. Arnold is **down** and is **not** the long-term home.

**Product fork (locked):** suppressed marketing deleted (~502,622 files; no `rollback.json`). Quarantine stays as labeled cards (`retrieval_weight=0.35`). Inbound uncertain Gmail writes cards (`emit_cards=True`); suppressed inbound does not.

**Live updaters on this seed:** SUCCESS calendar, contacts, otter, file-libraries, beeper, imessage, gmail-messages, gmail-correspondents (vault-first + `after:last_sync` + HTTP/batch). GitHub campaign fixes landed (`5980464`). Photos not run (parked). Dirty rematerialize allowlist incremental; index ~1,392,108 cards / 3,962,176 embeddings / pending 0. Soak ran. Otter MCP auth persists. F freshness uses live keys. No full rebuild, no IVFFlat, no `--catch-up`.

**Parked (not v2.5 closers):** Photos, Apple Health, `--catch-up`, full rebuild, Arnold gates 6+. Do not re-run vault-remove. Do not full-mailbox-walk correspondents.

## Objective

Prove that v2.5 makes **this local seed** a living, high-signal archive — not that Phase-1 contracts exist, and **not** that Arnold is promoted.

Prerequisite: **D Phase 2** and **E Phase 2** commits are on `v2.5` (`source updater execution`, `processor dag execution`). Those commits are landed. Do not run this runbook against Phase-1-only code and call freshness proven.

```text
focused tests
  → synthetic gate
  → smoke / larger slice (corpus + updater fixtures)
  → local seed applied in place (hygiene + live updaters)   ← done on this machine
  → local soak                                             ← done
  → v2.5-local complete (local_seed_living_corpus)

historical / not required for v2.5:
  Arnold code deploy → Arnold updater proof → Arnold corpus apply → Arnold soak
```

Section H is not a new feature section. It is the runbook that recorded how v2.5 became a living seed. Gates 6+ below are **historical text**. Do not execute them to close v2.5.

## Relationship to Other Sections

| Section   | Role in H                                                       |
| --------- | --------------------------------------------------------------- |
| G         | Safety rails, gate registry, refusal codes, engine mode         |
| A–C       | Policy + corpus hygiene + Gmail promotion                       |
| D Phase 2 | Real source updater runs                                        |
| E Phase 2 | Real processor runs on dirty UIDs                               |
| F         | Status/readiness; must fail closed until this runbook completes |

- **Section G:** builds the rails.
- **Section H:** drives the train — including proving the train actually moves data.

## Non-Goals

- Do not introduce new corpus semantics or a second promotion policy.
- Do not treat `--record-source-status` / `--record-processor-status` as updater/processor proof.
- Do not apply corpus hygiene to Arnold. Arnold is down and is not the home.
- Do not run full Phase 9 `ppa-deploy-v2` (rebuild + embedding restore) for routine code promotion unless schema/index requires it.
- Do not re-run vault-remove on this seed (already applied; quarantine kept). Do not physically prune Arnold. Do not copy the seed first.
- Do not start v3 packaging from this runbook.
- Do not restage a fake validation ladder to flip `ready: true`.

## Existing Code and Docs to Inspect Before Running

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `section_d_source_updater_contract.plan.md` (Phase 2)
- `section_e_processor_dag.plan.md` (Phase 2)
- `section_f_arnold_observability_v3_gate.plan.md`
- `section_g_validation_ladder_rust_standard.plan.md`
- `hey-arnold/Makefile` (`ppa-sync`, `ppa-install`, `ppa-mcp-enable`, `ppa-deploy-v2`)
- `ppa/archive_cli/commands/deploy.py` (Arnold-side `ppa deploy` — migrate/rebuild path; use only if needed)
- `ppa/Makefile` (slice/seed targets)
- `ppa/archive_cli/validation_gates/`, `corpus_hygiene/`, `source_updaters/`, `processors/`, `status/`

## Agent Handoff Checklist

Before running validation:

- Confirm branch is `v2.5` and `git status --short --branch` is clean.
- Confirm commits include Phase 1 A–G **and**:
  - `v2.5 section D: source updater execution`
  - `v2.5 section E: processor dag execution`
- Confirm branch is pushed to `ppa/v2.5`.
- Confirm `PPA_ENGINE=rust` unless explicitly running Python parity.
- Do **not** require SSH to Arnold. Arnold is down and is not the v2.5 target.

Stop immediately if:

- D/E Phase 2 commits are missing.
- the branch is dirty.
- focused tests fail.
- any command would copy this seed, deploy Arnold, or invent `validation_gates` / `corpus_cleanup` review rows.
- updater “proof” is only a status snapshot with no cursor before/after from a real run.

## Standard Environment Matrix

Export these explicitly for every gate. Do not rely on ambient shell state.

| Gate class         | `PPA_PATH`                        | `PPA_INDEX_SCHEMA`                           | `PPA_ARCHIVE_INSTANCE_ROLE` | Notes                                     |
| ------------------ | --------------------------------- | -------------------------------------------- | --------------------------- | ----------------------------------------- |
| Synthetic / unit   | n/a or fixture                    | n/a                                          | `fixture`                   | No vault mutation                         |
| Smoke slice        | `/tmp/ppa-test-slice-smoke`       | `archive_test_slice_smoke`                   | `slice`                     | From `make test-slice-smoke`              |
| Larger slice       | `/tmp/ppa-test-slice`             | `archive_test_slice`                         | `slice`                     | From `make test-slice`                    |
| Local seed dry-run | `$(PPA_SEED_VAULT)` from Makefile | `archive_seed` / this machine uses `ppa`     | `seed`                      | **This machine already applied** on the canonical seed. Do not copy-and-reapply. |
| Local seed (living corpus) | `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` | `ppa` (not a staging copy) | `seed`                      | This **is** the corpus. Written “copy first” / Arnold tail was skipped. |
| Arnold             | Historical only                   | n/a                                          | n/a                         | **Not a v2.5 closer.** Arnold is down. Do not deploy. |

Always also set:

```bash
export PPA_ENGINE=rust
# Local DSN example (from make / docker):
# export PPA_INDEX_DSN='postgresql://...'
```

## Standard Exit Codes

| Code | Meaning                     |
| ---- | --------------------------- |
| `0`  | Success                     |
| `1`  | Runtime failure             |
| `2`  | Validation failed           |
| `3`  | Refused unsafe operation    |
| `4`  | Blocked external dependency |

Do not proceed on `1`/`2`/`3`/`4` unless the gate explicitly allows a documented waiver.

## Gate Recording Pattern

After each successful gate, record evidence (adjust paths to the run directory):

```bash
GATE_DIR="logs/validation-gates/gate-${GATE_NAME}/${RUN_ID}"
mkdir -p "$GATE_DIR"
# retain command JSON/stdout as report.json / summary.md

.venv/bin/python -m archive_cli gates record \
  --gate "$GATE_NAME" \
  --status passed \
  --engine-mode rust \
  --report-path "$GATE_DIR/report.json" \
  --summary-path "$GATE_DIR/summary.md"
```

### Decision run extraction (corpus hygiene)

```bash
REPORT="<path-to-census-report.json>"
DECISION_RUN_ID="$(jq -r '.decision_run_id // .decision_run // empty' "$REPORT")"
test -n "$DECISION_RUN_ID"
```

### Do Not Proceed Unless (every gate)

At end of each gate: tests/commands exited `0` (or documented `3` for intentional refusal tests), artifacts written, `gates record` done when applicable, and the next gate's prerequisites are met.

---

## Gate 0: Branch and Focused Test Baseline

**Purpose:** Confirm the branch is coherent before touching slices or this seed.

```bash
cd /path/to/ppa
git status --short --branch
git log --oneline --decorate ppa/main..HEAD | head -40

.venv/bin/python -m pytest \
  archive_tests/test_validation_gates.py \
  archive_tests/archive_sync/llm_enrichment/test_email_promotion_policy.py \
  archive_tests/corpus_hygiene/ \
  archive_tests/archive_sync/test_gmail_sync_promotion.py \
  archive_tests/source_updaters/ \
  archive_tests/processors/ \
  archive_tests/status/test_section_f_status.py \
  -q
```

**Exit criteria:** focused tests pass; tree clean; D/E Phase 2 commits present.

**Do not proceed unless:** Gate 0 exit criteria met.

---

## Gate 1: Synthetic Fixture Gate

**Purpose:** Prove policy, gates, declarations, and status without real vault mutation.

Same pytest set as Gate 0 is acceptable. Record:

```bash
GATE_NAME=synthetic_fixtures RUN_ID="synth-$(date -u +%Y%m%dT%H%M%SZ)"
# write focused-test-report.json then gates record as above
```

**Exit criteria:** synthetic tests pass; readiness remains not-ready (later gates missing).

**Do not proceed unless:** Gate 1 recorded or focused-test report retained.

---

## Gate 2: Smoke Slice Corpus Dry-Run / Apply / Rollback

**Purpose:** Small blast-radius proof of corpus hygiene.

```bash
make test-slice-smoke
make test-slice-verify-smoke

export PPA_ENGINE=rust
export PPA_PATH=/tmp/ppa-test-slice-smoke
export PPA_INDEX_SCHEMA=archive_test_slice_smoke
export PPA_ARCHIVE_INSTANCE_ROLE=slice

.venv/bin/python -m archive_cli migrate   # if migrations pending

.venv/bin/python -m archive_cli corpus-hygiene email census \
  --dry-run --instance-role slice --format json \
  | tee /tmp/smoke-census.json

DECISION_RUN_ID="$(jq -r '.decision_run_id // empty' /tmp/smoke-census.json)"
test -n "$DECISION_RUN_ID"

# Review buckets from census report (no separate sample subcommand required):
jq '.review_buckets // .review_bucket_counts // .' /tmp/smoke-census.json | head -200

.venv/bin/python -m archive_cli corpus-hygiene email apply \
  --decision-run-id "$DECISION_RUN_ID" --instance-role slice --format json

.venv/bin/python -m archive_cli corpus-hygiene email rollback \
  --decision-run-id "$DECISION_RUN_ID" --instance-role slice --format json
```

**Exit criteria:** dry-run deterministic; apply+rollback succeed; suppressed absent from default retrieval during apply validation.

**Do not proceed unless:** Gate 2 exit criteria met.

---

## Gate 3: Larger Slice Validation

Same pattern as Gate 2 with:

```bash
make test-slice
make test-slice-verify
export PPA_PATH=/tmp/ppa-test-slice
export PPA_INDEX_SCHEMA=archive_test_slice
```

**Exit criteria:** no broad LLM reclassification; no full embed/all-linker; runtime recorded.

**Do not proceed unless:** Gate 3 exit criteria met.

---

## Gate 4: Local Seed Corpus Dry-Run

**Purpose:** Full-seed census without mutation.

```bash
export PPA_ENGINE=rust
export PPA_PATH="${PPA_SEED_VAULT:-/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127}"
export PPA_INDEX_SCHEMA=ppa   # this machine after the campaign; historical write-up used archive_seed
export PPA_ARCHIVE_INSTANCE_ROLE=seed

.venv/bin/python -m archive_cli corpus-hygiene email census \
  --dry-run --seed-scale --instance-role seed --format json \
  | tee /tmp/seed-census.json
```

Review: reuse rate, new LLM count, suppression/quarantine, high-risk buckets.

**Exit criteria:** report reviewed. On this machine the canonical seed **already received** apply after this gate’s historical dry-run; do not treat “no mutation” as a current-state description.

**Do not proceed unless:** report reviewed. Do not copy the seed first. This gate already ran; do not redo it to “unskip” the written sequence.

---

## Gate 5: Local Seed Staging Corpus Apply / Rollback

**Purpose (historical write-up):** seed-scale apply on a **copy** / staging schema. **This machine skipped that.** Hygiene apply already ran on the canonical seed (`PPA_PATH` = seed vault, schema `ppa`). Do not copy the seed and re-apply.

```bash
# This machine (already done — do not redo):
export PPA_PATH=/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127
export PPA_INDEX_SCHEMA=ppa
export PPA_ARCHIVE_INSTANCE_ROLE=seed

# Do not invent a staging copy of this seed to “pass” a written gate.
# export PPA_PATH=<seed_staging_vault_copy>
# export PPA_INDEX_SCHEMA=<seed_staging_schema>
# export PPA_ARCHIVE_INSTANCE_ROLE=seed-staging
```

**Disallowed now:** re-running vault-remove on this seed; copying the seed; Arnold deploy/mutation; deleting quarantine cards. This apply wrote no `rollback.json`.

**Exit criteria:** apply+rollback+rebuild safety; status shows corpus state; readiness still not-ready.

**Do not proceed unless:** Gate 5 exit criteria met.

---

**Note:** `ppa source-updaters run` and `ppa processors run` are **D/E Phase 2 deliverables**. They do not exist on Phase-1-only code. Gate 5b/6b cannot run until those commits land. Until then, status/snapshot CLIs must not be substituted as proof.

### Environment

```bash
export PPA_ENGINE=rust
export PPA_PATH=/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127
export PPA_INDEX_SCHEMA=ppa
export PPA_ARCHIVE_INSTANCE_ROLE=seed
# This machine already --apply'd live updaters on the canonical seed. Do not copy first.
# Soak already ran. Do not re-run SUCCESS streams to manufacture gate rows.
```

### Per-source updater proof

For each **live** source (not a manual export). **This seed SUCCESS:** `calendar`, `contacts`, `otter-transcripts`, `file-libraries`, `beeper`, `imessage`, `gmail-messages`, `gmail-correspondents` (vault-first + `after:last_sync` + HTTP/batch). GitHub campaign fixes landed (`5980464`). **Not run / parked:** `photos`. Do not re-run SUCCESS streams. Do not full-mailbox-walk correspondents. Do not run Finance/LinkedIn/Notion CSV/Health XML/medical dumps/Apple VCF as updaters.

```bash
# Capture cursor before
.venv/bin/python -m archive_cli source-updaters status --format json | tee /tmp/su-before.json

.venv/bin/python -m archive_cli source-updaters run \
  --source <source_key> \
  --dry-run \
  --format json | tee /tmp/su-<source>-dry.json

# Apply only when dry-run looks sane (staging preferred)
.venv/bin/python -m archive_cli source-updaters run \
  --source <source_key> \
  --apply \
  --format json | tee /tmp/su-<source>-apply.json

# Verify report fields
jq '{status, cursor_before, cursor_after, observed, promoted, suppressed, dirty_card_uids_count, errors}' \
  /tmp/su-<source>-apply.json
```

**Pass criteria per source:**

- Exit `0` or documented `4` only if source is intentionally unavailable (then mark blocked, do not fake success).
- Report has `cursor_before` / `cursor_after` (or explicit unchanged with `observed`/`unchanged` counts).
- `dirty_card_uids` present (may be empty if nothing changed — then prove with a controlled fixture or known delta).
- Status after run is not solely from `--record-source-status`.

### Processor proof on dirty set

```bash
DIRTY="$(jq -r '.dirty_uids_path // .artifact_paths.dirty_uids // empty' /tmp/su-gmail-apply.json)"
# or extract dirty_card_uids array to a file

.venv/bin/python -m archive_cli processors plan --dirty-uids "$DIRTY" --format json \
  | tee /tmp/proc-plan.json

.venv/bin/python -m archive_cli processors run \
  --dirty-uids "$DIRTY" \
  --apply \
  --run-id "seed-proc-$(date -u +%Y%m%dT%H%M%SZ)" \
  --format json | tee /tmp/proc-run.json
```

### Maintain cycle

```bash
.venv/bin/python -m archive_cli maintain \
  --run-source-updaters \
  --run-processors \
  --format json | tee /tmp/seed-maintain.json
```

**Exit criteria (this machine, Aug 2026):**

- SUCCESS updater proof already recorded for the streams listed above. Photos parked. Export streams are out of this gate.
- Soak ran. Otter MCP auth persists. F freshness uses live keys.
- Do not require full rebuild / IVFFlat / `--catch-up` for v2.5-done.
- Formal `ready: false` leftover is an accepted local exception (`local_seed_living_corpus`).

**Do not proceed to Arnold.** v2.5-local is complete. Gates 6–9 below are historical.

---

## Gate 6: Arnold Code Deploy (v2.5 branch) — HISTORICAL, NOT REQUIRED

**Status:** not a v2.5 closer. Arnold is down. Do not run this gate to finish v2.5.

**Purpose (historical write-up):** Put D/E Phase 2 code on Arnold **without** corpus apply and without default full rebuild.

### From laptop (hey-arnold repo)

```bash
cd /path/to/hey-arnold
git status --short --branch

# Ensure local ppa checkout is v2.5 and clean, then:
make ppa-sync      # rsync PPA_LOCAL_ROOT → Arnold PPA_WORKSPACE
make ppa-install   # pip install -e . + maturin develop --release for archive_crate
```

### On Arnold (or via SSH)

```bash
ssh arnold@192.168.50.27
# Paths follow hey-arnold defaults; confirm PPA_WORKSPACE / PPA_PATH from env
cd "${PPA_WORKSPACE:-/home/arnold/openclaw/ppa}"

# Migrations only (preferred for code+schema bumps without rebuild)
sudo -u arnold \
  PPA_PATH="$PPA_PATH" PPA_INDEX_DSN="$PPA_INDEX_DSN" PPA_INDEX_SCHEMA="$PPA_INDEX_SCHEMA" \
  /home/arnold/openclaw/venv/bin/python -m archive_cli migrate

sudo systemctl restart ppa-mcp.service
sudo systemctl status ppa-mcp.service --no-pager -l
```

### When **not** to use full Phase 9 deploy

Do **not** run `make ppa-deploy-v2` (or `archive_cli deploy` with rebuild + embedding restore) unless:

- a migration requires full rebuild, or
- index is known corrupt and operator explicitly chooses rebuild.

Vault rsync from seed is **out of scope** for routine v2.5 code promotion.

### Verify deploy

```bash
ssh arnold@192.168.50.27 'cd "$PPA_WORKSPACE" && git rev-parse HEAD 2>/dev/null || true'
# Confirm installed code exposes:
#   ppa source-updaters run --help
#   ppa processors run --help
make -C /path/to/hey-arnold ppa-mcp-status
```

**Exit criteria:** sync+install succeeded; migrate succeeded if needed; MCP restarted; new CLIs available.

**Do not proceed unless:** Gate 6 exit criteria met.

---

## Gate 6b: Arnold Source Updater + Processor Proof — HISTORICAL, NOT REQUIRED

**Status:** not a v2.5 closer. Do not run this gate to finish v2.5.

**Purpose (historical write-up):** Same as Gate 5b on production, with production role. Prefer dry-run first; apply with care.

```bash
export PPA_ENGINE=rust
export PPA_ARCHIVE_INSTANCE_ROLE=production

# status before
.venv/bin/python -m archive_cli source-updaters status --format json | tee /tmp/arnold-su-before.json

# Per source: dry-run then apply (Gmail + Calendar required)
.venv/bin/python -m archive_cli source-updaters run --source <source_key> --dry-run --format json
.venv/bin/python -m archive_cli source-updaters run --source <source_key> --apply --format json \
  | tee /tmp/arnold-su-<source>.json

# Processors on dirty UIDs
.venv/bin/python -m archive_cli processors plan --dirty-uids "$DIRTY" --format json
.venv/bin/python -m archive_cli processors run --dirty-uids "$DIRTY" --apply \
  --run-id "arnold-proc-$(date -u +%Y%m%dT%H%M%SZ)" --format json

.venv/bin/python -m archive_cli status --format json
.venv/bin/python -m archive_cli readiness --format json   # still may be not-ready until corpus apply/soak
```

**Exit criteria:** real updater reports with cursors; processor run evidence; failures isolated; no corpus hygiene apply yet.

**Do not proceed unless:** Gate 6b exit criteria met.

---

## Gate 7: Arnold Corpus Dry-Run — HISTORICAL, NOT REQUIRED

**Status:** not a v2.5 closer. This seed already received hygiene apply.

**Purpose (historical write-up):** Evaluate production corpus cleanup without mutation.

```bash
export PPA_ENGINE=rust
export PPA_ARCHIVE_INSTANCE_ROLE=production

.venv/bin/python -m archive_cli corpus-hygiene email census \
  --dry-run --instance-role production --format json \
  | tee /tmp/arnold-census.json

DECISION_RUN_ID="$(jq -r '.decision_run_id // empty' /tmp/arnold-census.json)"
test -n "$DECISION_RUN_ID"

# Review samples/buckets from census JSON before any apply
jq '.review_buckets // .review_bucket_counts // .' /tmp/arnold-census.json | head -200
```

Record gate `production_dry_run` with review notes.

**Exit criteria:** report reviewed; decision run approved for apply; rollback point identified; **no production mutation**.

**Do not proceed unless:** human review recorded / approved.

---

## Gate 8: Arnold Reviewed Corpus Apply — HISTORICAL, NOT REQUIRED

**Status:** not a v2.5 closer. Do not apply to Arnold.

**Purpose (historical write-up):** Apply reviewed corpus-state changes.

```bash
export PPA_ENGINE=rust
export PPA_ARCHIVE_INSTANCE_ROLE=production

.venv/bin/python -m archive_cli corpus-hygiene email apply \
  --decision-run-id "$DECISION_RUN_ID" \
  --instance-role production \
  --confirm-production \
  --format json | tee /tmp/arnold-apply.json
```

Expected: exit `0`. Missing `--confirm-production` or wrong role → exit `3`.

### Post-apply validation

```bash
.venv/bin/python -m archive_cli status --format json
# Confirm suppressed absent from default search / link candidates (use existing query/smoke tools)
# Confirm derived cards remain active
# Confirm rollback artifact path present in apply report
```

**Exit criteria:** apply succeeded; rollback artifact exists; suppression visible on default surfaces.

**Do not proceed unless:** Gate 8 exit criteria met.

---

## Gate 9: Arnold Soak and Readiness — HISTORICAL, NOT REQUIRED

**Status:** not a v2.5 closer. Local soak already ran on this seed. Formal `ready: false` leftover (`validation_gates` / `corpus_cleanup` review rows) is an accepted local exception (`local_seed_living_corpus`). Do not restage a fake ladder.

**Purpose (historical write-up):** Prove Arnold stays healthy through **real** maintain (updaters + processors), not snapshots alone.

```bash
export PPA_ENGINE=rust
export PPA_ARCHIVE_INSTANCE_ROLE=production

.venv/bin/python -m archive_cli maintain \
  --run-source-updaters \
  --run-processors \
  --record-source-status \
  --record-processor-status

.venv/bin/python -m archive_cli status --format json --write-maintenance-report
.venv/bin/python -m archive_cli readiness --format json | tee /tmp/arnold-readiness.json

jq '.ready, .failed // .checks' /tmp/arnold-readiness.json
```

**Exit criteria:**

- Source freshness from real runs within policy.
- Processor health from real runs.
- Maintenance report written.
- Suppressed email remains excluded.
- `ready: true` only when F required checks (including real-run evidence) pass.
- Gate `production_soak` recorded.

**Do not proceed to v3 unless:** `ppa readiness` reports ready.

---

## Stop Conditions

Stop and ask for review if:

- any required test fails.
- dry-run wants broad new LLM classification unexpectedly.
- report counts nondeterministic across identical dry-runs.
- updater apply would advance cursor past unpersisted work.
- a command would “unskip” Gate 5b by copying this seed and re-applying hygiene or SUCCESS updaters.
- any command would re-run vault-remove or full-mailbox-walk correspondents on this already-applied seed.
- any command would deploy Arnold or mutate an Arnold corpus.
- a next agent restages a fake ladder to flip `ready: true`.
- Rust/Python divergence appears on active/suppressed materialization.

## Completion Artifacts

Leave (v2.5-local — do not invent missing Arnold / enum rows):

- Existing Gate 0–5 / 5b and scale reports under `logs/validation-gates/`.
- This-seed updater run evidence (Gmail + Calendar minimum already ran).
- Soak evidence on this seed. Otter MCP auth persists. F freshness uses live keys.
- Completion note: `local_seed_living_corpus`. Formal `ready: false` leftover from missing `validation_gates` / `corpus_cleanup` review rows is accepted.
- Do **not** require Arnold updater JSONs, Arnold census/apply paths, or `ready: true`.

## Commit Instructions

Section H is operational. Optional docs-only commit if runbook/docs change:

- Subject: `v2.5 section H: validation promotion runbook`
- Do not mix H docs with D/E Phase 2 code commits.
- Evidence artifacts under `logs/` are typically gitignored; do not force-add secrets or production dumps.

## Definition of Done

Section H is complete for **v2.5-local** when this seed is the living high-signal archive: hygiene applied in place, live updaters that this Mac can run have succeeded, soak has run, Otter MCP auth persists, and F freshness uses live keys. Formal `ready: false` leftover is an accepted local exception (`local_seed_living_corpus`). Gates 6–9 (Arnold deploy/apply/soak) are **not** required. Do not copy the seed. Do not deploy Arnold. Do not invent fake gate artifacts.
