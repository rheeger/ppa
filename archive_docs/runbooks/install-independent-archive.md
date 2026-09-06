# Install an independent PPA archive

This runbook is the P09-C clean-install path. Python **3.12** is the first explicitly verified packaging profile. The library floor remains `requires-python = ">=3.10"` — do not silently lower it.

`production_proven` stays **false** until a later approved production install.

## Lock resolver procedure

Locks live in `requirements/` and pin transitive versions **and hashes**.

1. Use CPython 3.12 (`/opt/homebrew/bin/python3.12` on this host).
2. Create a throwaway venv: `python3.12 -m venv /tmp/ppa-lock && /tmp/ppa-lock/bin/pip install -U pip pip-tools`.
3. Compile runtime:  
   `/tmp/ppa-lock/bin/pip-compile --generate-hashes --resolver=backtracking --output-file requirements/runtime-py312.lock requirements/runtime-py312.in`
4. Compile dev:  
   `/tmp/ppa-lock/bin/pip-compile --generate-hashes --resolver=backtracking --output-file requirements/dev-py312.lock requirements/dev-py312.in`
5. Record the generating Python/OS/arch in the release manifest. Platform-specific wheels (macOS arm64 vs Linux x86_64) must be rebuilt on that platform — do not infer a second platform from one host.

## Build wheels from one SHA

```bash
python archive_scripts/build-release.py --output logs/plans/p09/release
```

This writes the root `ppa` wheel, the `archive_crate` native wheel, and `release-manifest.json` (compiler, Python, OS, arch, Cargo.lock hash, lock hashes, wheel hashes).

## Fresh venv outside the checkout

```bash
python3.12 -m venv /tmp/ppa-independent/venv
/tmp/ppa-independent/venv/bin/pip install --require-hashes -r requirements/runtime-py312.lock
/tmp/ppa-independent/venv/bin/pip install --no-deps logs/plans/p09/release/ppa-*.whl logs/plans/p09/release/archive_crate-*.whl
```

Do not use `pip install -e .` as the runtime proof. Confirm:

```bash
/tmp/ppa-independent/venv/bin/python -c "import archive_crate, pathlib; print(pathlib.Path(archive_crate.__file__).resolve())"
```

The printed path must not sit inside the source checkout.

## Setup, maintain, first query

Write a fixture-only SPEC (no seed path, no live Google):

```json
{
  "root": "/tmp/ppa-independent/vault",
  "entity_name": "Ada Example",
  "entity_type": "person",
  "index_schema": "ppa_fixture",
  "fixture": "sample.fixture",
  "embedding_provider": "hash"
}
```

```bash
export PPA_PATH=/tmp/ppa-independent/vault
export PPA_INDEX_SCHEMA=ppa_fixture
export PPA_EMBEDDING_PROVIDER=hash
unset PPA_TEST_PG_DSN
# PPA_INDEX_DSN must be a loopback warehouse you own, or remain unset (status stays pending)

/tmp/ppa-independent/venv/bin/ppa setup --non-interactive --from spec.json          # review only
/tmp/ppa-independent/venv/bin/ppa setup --non-interactive --from spec.json --apply  # writes
/tmp/ppa-independent/venv/bin/ppa instance-status --instance-dir "$PPA_PATH"
/tmp/ppa-independent/venv/bin/ppa bootstrap-postgres
/tmp/ppa-independent/venv/bin/ppa maintain
/tmp/ppa-independent/venv/bin/ppa read hfa-person-p09c-ada
```

Missing warehouse, native extension, live auth, or backup tools are **pending/unavailable**. Status must not claim fresh because `ppa.json` exists.

## Command risk labels

`ppa setup --help`, `ppa config --help`, `ppa connect --help`, `ppa connector --help`, `ppa backup --help`, and `ppa restore --help` state whether the action writes, calls providers, or needs source credentials. Setup never treats a blank interactive answer as apply. An existing root is not overwritten by default.

## Platform gaps

This host proves **macOS arm64 + CPython 3.12 lock generation**. Linux x86_64 wheels and a 3.12 installed-runtime matrix are recorded as unverified until built on that platform.
