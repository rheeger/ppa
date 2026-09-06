# Product verification

P04 owns the isolated acceptance runner. Other plans add namespaced scenarios; they do not reimplement the harness.

## Command

```bash
python -m archive_tests.acceptance.run --suite baseline --output logs/plans/p04/P04-A --require-integration
```

Also accepted: `--suite p01` … `--suite p10`, `--suite release`, `--scale-profile NAME`, `--seed INTEGER`.

A suite with zero registered scenarios is an error. `--require-integration` makes missing Docker or a missing Rust/`archive_crate` engine a **failure**, not a skip-green.

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

## Baseline tracer (P04-A)

One synthetic person card is written with `write_card`, materialized into the owned warehouse, published into the Rust serving index, then read through MCP `archive_search` / `archive_read` and CLI `search` / `read`.

The exact-cosine oracle (`archive_tests.acceptance.oracle`) is a Python brute-force reference. It does not call native ANN. IVF recall gates belong to P01 / P04-B.

## Proof tiers

| Tier | What it proves |
| --- | --- |
| Implementation | Code and unit harness tests exist |
| Isolated integration | Real Docker + native engine + public MCP/CLI |
| Scale | Later (P04-B/C profiles) |
| Production | Never implied here. `production_proven=false` until R0/R1 |

## Composed faults (P04-C)

```bash
python -m pytest -p no:cacheprovider archive_tests/test_release_failure_matrix.py --require-integration
python -m archive_tests.acceptance.run --suite p04 --output logs/plans/p04/P04-C --require-integration
```

Scenarios `p04.crash_matrix` and `p04.privacy_restore` kill real child processes at publication/journal boundaries, deny leaked sources/egress, and restore an encrypted vault into a new root. `production_proven` stays false.

## Integrated release gate (P04-D)

```bash
unset PPA_TEST_PG_DSN
python -m archive_tests.acceptance.run --suite release --output logs/plans/program --require-integration
python -m pytest -p no:cacheprovider archive_tests/test_release_manifest.py --require-integration
```

`--suite release` executes `release.integrated_gate` on **this** checkout. It fails if any of p01–p10 has zero scenarios, if a destination proof fails, or if held-out relation/quality evidence is missing. Child-final SHAs are recorded as inherited artifacts; only the current run is current-run proof.

`production_proven` stays false. Million-vector @1536 is **blocked**, not waived. Linux x86_64 remains unproven. Model rerank stays disabled without model-quality proof. R0/R1 are not implied.

## Adding a child suite

Register a `Scenario` from `archive_tests/acceptance/scenarios/pNN_*.py`. P04 does not implement sibling product features to force a green suite.
