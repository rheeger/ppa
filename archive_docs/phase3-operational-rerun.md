# Phase 3 operational re-run (Steps 8–16)

After extractor rewrites and unit tests pass, re-run the pipeline on your machine with real vault paths.

**Prerequisites**

- `PPA_PATH` (or Makefile defaults) points at the target vault / slice.
- Staging directory empty or versioned for this run.
- `.venv` active: `python -m pip install -e ".[dev]"` (includes `html2text`).

**8 — Graduated extraction**

- Use existing Makefile / CLI targets (e.g. `make extract-emails-staging` or `ppa extract-emails`) per your graduated plan.
- Expect **lower** match→card yield: marketing emails are rejected; cards should have populated critical fields.
- Inspect `staging/_metrics.json`: `rejected_emails`, `field_population`, `yield_by_extractor`.

**9 — Staging report + spot-check**

- `ppa staging-report --staging-dir …` (or Makefile `staging-report`).
- Confirm critical field population table and manually spot-check high-volume types (no generic stubs).

**10 — Promote staging**

- `promote-staging` (dry-run first if available).

**11 — Entity resolution**

- Run `resolve-entities` per your runbook; ensure no stray processes: `pkill -f 'resolve-entities' 2>/dev/null || true` before long runs if needed.

**12 — Vault validation**

- Use existing validate / doctor commands per project docs.

**13 — Idempotency**

- Re-run extract to staging or verify idempotency tests + duplicate detection.

**13b — Arnold production run**

- Per internal Arnold procedure.

**14 — Slice re-fork**

- Regenerate benchmark / slice vaults if part of your release process.

**15 — Tests**

- `make test-unit` (and integration if applicable).

**16 — Definition of Done**

- Field population thresholds met for critical fields per type.
- No stub values (`DoorDash order`, `Airbnb stay`, `Instacart` as store, `NUMBER` PNR, etc.).
- `rejected_emails` non-zero where marketing volume exists.
- Fixture-based extractor tests green.
