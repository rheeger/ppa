# Section H Execution Plan - v2.5 Validation and Promotion Runbook

**Status (Aug 2026, HEAD `3a90bc0`):** Local gates **0–5b are done** (hygiene staging apply+rollback Jul 12; Gmail 5b capped Jul; calendar capped apply on a **minimal** vault via Track C; fixture join proof). D/E Phase 2 are landed. **Scale hot paths landed** on 1pct + 10pct (`3a90bc0`; 10pct apply 8.47s / 243,598 cards). **Remaining local seed work:** 5b re-proof on a **full seed staging copy**, catch-up + processors maintain, local soak. Arnold (6 / 6b / 7) is deferred / out of current scope.

## Objective

Prove that v2.5 makes Arnold a **living, high-signal archive** — not only that Phase-1 contracts and corpus tooling exist.

Prerequisite: **D Phase 2** and **E Phase 2** commits are on `v2.5` (`source updater execution`, `processor dag execution`). Do not run this runbook against Phase-1-only code and call freshness proven.

```text
focused tests
  → synthetic gate
  → smoke / larger slice (corpus + updater fixtures)
  → local seed: corpus dry-run + staging apply (optional if already proven)
  → local seed: SOURCE UPDATER + PROCESSOR proof   ← required for live update
  → Arnold code deploy (sync + install + migrate; no full Phase 9 rebuild by default)
  → Arnold: SOURCE UPDATER + PROCESSOR proof
  → Arnold corpus dry-run → reviewed apply
  → Arnold soak (maintain with real updaters/processors)
  → ppa readiness READY
```

Section H is not a new feature section. It is the runbook that turns implemented v2.5 into production confidence and unblocks v3.

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
- Do not apply corpus hygiene to Arnold before seed **and** Arnold updater proof.
- Do not run full Phase 9 `ppa-deploy-v2` (rebuild + embedding restore) for routine code promotion unless schema/index requires it.
- Do not physically prune markdown.
- Do not start v3 packaging from this runbook.

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
- Confirm operator can reach Arnold (`ssh arnold@192.168.50.27` or `ARNOLD_HOST` from hey-arnold).

Stop immediately if:

- D/E Phase 2 commits are missing.
- the branch is dirty.
- focused tests fail.
- any command would apply to Arnold before Arnold dry-run review.
- updater “proof” is only a status snapshot with no cursor before/after from a real run.

## Standard Environment Matrix

Export these explicitly for every gate. Do not rely on ambient shell state.

| Gate class         | `PPA_PATH`                        | `PPA_INDEX_SCHEMA`                           | `PPA_ARCHIVE_INSTANCE_ROLE` | Notes                                     |
| ------------------ | --------------------------------- | -------------------------------------------- | --------------------------- | ----------------------------------------- |
| Synthetic / unit   | n/a or fixture                    | n/a                                          | `fixture`                   | No vault mutation                         |
| Smoke slice        | `/tmp/ppa-test-slice-smoke`       | `archive_test_slice_smoke`                   | `slice`                     | From `make test-slice-smoke`              |
| Larger slice       | `/tmp/ppa-test-slice`             | `archive_test_slice`                         | `slice`                     | From `make test-slice`                    |
| Local seed dry-run | `$(PPA_SEED_VAULT)` from Makefile | `archive_seed`                               | `seed`                      | **No apply on canonical seed**            |
| Local seed staging | **copy** of seed vault            | staging schema (e.g. `archive_seed_staging`) | `seed-staging`              | Apply/rollback only here                  |
| Arnold             | Arnold vault (`PPA_PATH` on host) | production schema (usually `ppa`)            | `production`                | Requires `--confirm-production` for apply |

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

**Purpose:** Confirm the branch is coherent before touching slices, seed, or Arnold.

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
export PPA_INDEX_SCHEMA=archive_seed
export PPA_ARCHIVE_INSTANCE_ROLE=seed

.venv/bin/python -m archive_cli corpus-hygiene email census \
  --dry-run --seed-scale --instance-role seed --format json \
  | tee /tmp/seed-census.json
```

Review: reuse rate, new LLM count, suppression/quarantine, high-risk buckets.

**Exit criteria:** report reviewed; no mutation of canonical seed.

**Do not proceed unless:** report reviewed.

---

## Gate 5: Local Seed Staging Corpus Apply / Rollback

**Purpose:** Seed-scale apply only on a **copy** / staging schema.

```bash
export PPA_PATH=<seed_staging_vault_copy>
export PPA_INDEX_SCHEMA=<seed_staging_schema>
export PPA_ARCHIVE_INSTANCE_ROLE=seed-staging

# census → DECISION_RUN_ID → apply → rollback (same pattern as Gate 2)
```

**Disallowed:** canonical seed mutation; Arnold mutation; markdown pruning.

**Exit criteria:** apply+rollback+rebuild safety; status shows corpus state; readiness still not-ready.

**Do not proceed unless:** Gate 5 exit criteria met.

---

**Note:** `ppa source-updaters run` and `ppa processors run` are **D/E Phase 2 deliverables**. They do not exist on Phase-1-only code. Gate 5b/6b cannot run until those commits land. Until then, status/snapshot CLIs must not be substituted as proof.

### Environment

```bash
export PPA_ENGINE=rust
export PPA_PATH="${PPA_SEED_VAULT}"
export PPA_INDEX_SCHEMA=archive_seed
export PPA_ARCHIVE_INSTANCE_ROLE=seed
# Prefer staging vault/schema if updater apply would mutate seed cursors you must keep frozen.
# If using canonical seed, prefer --dry-run first; only --apply when intentional.
```

### Per-source updater proof

For each required source (`gmail`, `calendar`, then `imessage`, `photos` as available):

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

**Exit criteria:**

- Gmail + Calendar updater proof recorded (iMessage/Photos if configured).
- At least one processor apply on a non-empty dirty set **or** documented empty-dirty with fixture proof elsewhere.
- Maintain cycle completes without requiring full rebuild/embed/all-linkers.
- Gate recorded as `local_seed_source_updater_proof` (or equivalent).

**Do not proceed unless:** Gate 5b exit criteria met. **Do not go to Arnold without 5b.**

---

## Gate 6: Arnold Code Deploy (v2.5 branch)

**Purpose:** Put D/E Phase 2 code on Arnold **without** corpus apply and without default full rebuild.

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

## Gate 6b: Arnold Source Updater + Processor Proof (REQUIRED)

**Purpose:** Same as Gate 5b on production, with production role. Prefer dry-run first; apply with care.

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

## Gate 7: Arnold Corpus Dry-Run

**Purpose:** Evaluate production corpus cleanup without mutation.

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

## Gate 8: Arnold Reviewed Corpus Apply

**Purpose:** Apply reviewed corpus-state changes.

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

## Gate 9: Arnold Soak and Readiness

**Purpose:** Prove Arnold stays healthy through **real** maintain (updaters + processors), not snapshots alone.

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
- Gate 5b or 6b would be skipped.
- any command would mutate canonical seed before staging gate.
- any command would mutate Arnold corpus before Gate 7 review.
- readiness flips ready on snapshot-only evidence.
- Rust/Python divergence appears on active/suppressed materialization.

## Completion Artifacts

Leave:

- Gate 0–9 reports under `logs/validation-gates/`.
- Seed and Arnold updater run JSONs (Gmail + Calendar minimum).
- Processor plan/run JSONs tied to dirty UIDs.
- Arnold census + apply + rollback paths.
- Final `ppa readiness` JSON with `ready: true`.
- Short operator note: deploy method used (`ppa-sync`/`ppa-install` vs full `ppa-deploy-v2`).

## Commit Instructions

Section H is operational. Optional docs-only commit if runbook/docs change:

- Subject: `v2.5 section H: validation promotion runbook`
- Do not mix H docs with D/E Phase 2 code commits.
- Evidence artifacts under `logs/` are typically gitignored; do not force-add secrets or production dumps.

## Definition of Done

Section H is complete when Gates 0–9 pass, updater/processor proof exists on seed and Arnold, corpus apply is reviewed and applied, soak passes, and `ppa readiness --format json` reports ready with real-run evidence — unblocking v3 packaging work.
