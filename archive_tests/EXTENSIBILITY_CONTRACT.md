# Test a new card type

A new card type should be useful to someone importing a different account or provider. Synthetic fixtures show how it is stored, queried, and connected without requiring private records.

When adding a type:

1. Add a fixture at `archive_tests/fixtures/cards/<type>.md` with valid YAML frontmatter and a body.
2. Verify the schema round-trip with `archive_tests/test_fixtures.py::test_fixture_pydantic_roundtrip`.
3. Add a small graph under `archive_tests/fixtures/graphs/` that checks the type's declared relationships, including a record that should not connect.
4. Extend `archive_tests/slice_manifest.json` with relevant search, graph, or temporal questions when the type is part of a slice workload.
5. Update the card and projection registrations together, then verify retrieval through the shared runtime.

The vault remains authoritative during recovery; indexes can be rebuilt with compatible code. See [card type contracts](../archive_docs/CARD_TYPE_CONTRACTS.md), [slice testing](../archive_docs/SLICE_TESTING.md), and the [specification](../archive_docs/SPECIFICATION.md).
