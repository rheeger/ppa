# Product verification

A contributor should be able to test a complete import-and-query path without the maintainer's accounts. The acceptance runner creates isolated archives, publishes their records, and verifies the same CLI and MCP paths a user would query.

Suites add named scenarios to the shared runner. Tests use synthetic records, so their results establish behavior for the tested setup rather than a separate live deployment.

## Command

```bash
python -m archive_tests.acceptance.run --suite baseline --output logs/plans/p04/P04-A --require-integration
```

Also accepted: `--suite p01` … `--suite p10`, `--suite release`, `--suite p31`, `--scale-profile NAME`, `--seed INTEGER`.

A suite with zero registered scenarios is an error. `--require-integration` makes missing Docker or a missing Rust/`archive_crate` engine a failure instead of a skipped check.

Pytest shares the same flag:

```bash
python -m pytest -p no:cacheprovider archive_tests/test_acceptance_harness.py --require-integration
```

Without the flag, ordinary local skips (no Docker) stay skips.

## Isolation

Every runner invocation owns:

- a unique vault under the output `runtime/` tree, marked with `.ppa-acceptance-owned`
- a unique Docker `pgvector` container (never `docker-compose.test.yml`, never `PPA_TEST_PG_DSN`)
- a unique Postgres schema `plan_p04_<id>`
- hash embeddings (`PPA_EMBEDDING_PROVIDER=hash`) and `PPA_ENGINE=rust`

Inherited seed/production `PPA_PATH`, non-loopback `PPA_INDEX_DSN`, and leftover provider API keys are refused. Cleanup removes only the owned container.

## Baseline import and query check

One synthetic person card is written with `write_card`, materialized into the owned warehouse, published into the Rust serving index, then read through MCP `archive_search` / `archive_read` and CLI `search` / `read`.

The exact-cosine oracle (`archive_tests.acceptance.oracle`) is a Python brute-force reference. It does not call native ANN. The serving-retrieval suites compare native approximate nearest-neighbor results against that reference.

## Proof tiers

| Tier | What it proves |
| --- | --- |
| Implementation | Code and unit harness tests exist |
| Isolated integration | Real Docker + native engine + public MCP/CLI |
| Scale | Results for the recorded dataset size, vector dimensions, and test configuration |
| Production | Requires separate evidence from a live deployment; fixture results retain `production_proven=false` |

## Failure and recovery checks

```bash
python -m pytest -p no:cacheprovider archive_tests/test_release_failure_matrix.py --require-integration
python -m archive_tests.acceptance.run --suite p04 --output logs/plans/p04/P04-C --require-integration
```

Scenarios `p04.crash_matrix` and `p04.privacy_restore` kill real child processes at publication/journal boundaries, deny leaked sources/egress, and restore an encrypted vault into a new root. `production_proven` stays false.

## Integrated release gate

```bash
unset PPA_TEST_PG_DSN
python -m archive_tests.acceptance.run --suite release --output logs/plans/program --require-integration
python -m pytest -p no:cacheprovider archive_tests/test_release_manifest.py --require-integration
```

`--suite release` executes `release.integrated_gate` on **this** checkout. It fails if any of p01–p10 has zero scenarios, if a destination proof fails, or if held-out relation/quality evidence is missing. Earlier commit hashes are retained as historical evidence; the current run establishes behavior for the tested checkout.

Report scale, platform, and model-quality results with the configuration and revision tested. A passing fixture run does not establish larger-scale behavior, another platform's support, or live readiness. Dated results belong in [validation reports](reports/README.md).

## Adding a suite

Register a `Scenario` from `archive_tests/acceptance/scenarios/pNN_*.py`. Use the shared runner and report missing capabilities as failures or skips according to the selected mode.
