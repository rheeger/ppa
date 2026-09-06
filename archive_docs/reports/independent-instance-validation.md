# Independent instance validation (P09-D)

**Slice:** P09-D. **Date:** 2026-09-06. **Base:** P09-C `1cd7a3d`.  
**Production proven:** false. A fixture smoke is not a long production soak.

## What was proven

Person and organization fixture instances can `setup` → import `sample.fixture` → bootstrap → rebuild → maintain/publish → query → unbind/restart → query again without hardcoded owner or source assumptions.

Two runtimes stay isolated through restart:

- same external person UID (`hfa-person-p09c-ada`) is allowed in both
- distinct `archive_id`, vault roots, warehouse schemas, serving paths, and journal checkpoints
- distinct entity types (`person` vs `organization`) and saved scopes (`personal-fixture` vs `org-ops`)

No-op maintain after a published generation is cheap (`nothing_to_do` or skipped processor execution). Stale or down sources stay visible. Missing warehouse / auth / provider capability is `pending` or `unavailable`. Status cannot claim `fresh` because a `ppa.json` manifest exists.

A new instance does **not** inherit `local_seed_living_corpus`. That leftover is recorded and bound to the original local seed path only (`archive_cli/status/instance_policy.py`).

## Docs rewritten

- `README.md` — 36 card types; native crate required; compose-from-cards until P10; graph trust honesty
- `archive_docs/ARCHITECTURE.md` — current packages, Rust serving, instance policy
- `archive_docs/PPA_RUNTIME_CONTRACT.md` — serving-era status; CLI > env > file; Arnold labeled historical
- `archive_docs/MCP_SETUP.md` — local stdio is current; Arnold/Ginger topology is historical
- `archive_docs/INDEXING.md` — warehouse vs serving
- `archive_docs/vision/README.md`, `v3vision.md`, `v4vision.md` — 36 types; empty knowledge; Rust already shipped
- Historical banners on `PPA_BACKUP_AND_RESTORE.md` and `SECURITY_MODEL.md`

## Analytics

CLI/MCP narrative workflows are **pending** (P10). They were not present on P09-C. P04-D closes capability status on the integrated SHA. Do not treat `ppa status` analytics cells as shipped.

## Platform leftovers (from P09-C, still unresolved)

- Proven install path is **macOS arm64 + CPython 3.12** only
- Host default rustc 1.83 cannot compile the current crates.io graph; build used rustup 1.85
- `maturin` needs `--interpreter` when system Python exceeds PyO3 max
- Do not `pip install` into Homebrew Python 3.12 (PEP 668)
- Linux / other Python wheels are unproven
- Live Google auth onboarding is pending
- Source schedules and backup destinations remain owner decisions

## Knowledge

46-facet / living-profile / populated knowledge-cache claims stay **deferred**. `archive_knowledge` is search fallback.

## How to re-run

```bash
unset PPA_TEST_PG_DSN
.venv/bin/python -m pytest -p no:cacheprovider \
  archive_tests/test_independent_instances.py \
  archive_tests/test_instance_health.py \
  archive_tests/status/ --require-integration
.venv/bin/python -m archive_tests.acceptance.run \
  --suite p09 --output logs/plans/p09/P09-D --require-integration
```
