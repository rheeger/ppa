# Phase 1+ extensibility contract

When adding a new card type to PPA:

1. Add a synthetic fixture `tests/fixtures/cards/<type>.md` (YAML frontmatter + body) matching vault format.
2. Schema round-trip is covered by `tests/test_fixtures.py::test_fixture_pydantic_roundtrip` once the file exists.
3. Add a small graph under `tests/fixtures/graphs/` proving `DeclEdgeRule` edges for the type.
4. Extend `tests/slice_manifest.json` with FTS/graph/temporal queries that exercise the type.
5. Re-fork `tests/slice_config.json` after extractors (Phase 3) so the slice includes derived types.

Rollback: vault remains source of truth; Postgres can be rebuilt with `ppa rebuild-indexes --force-full` with matching code.
