from archive_vault.provenance import (
    ProvenanceEntry,
    compute_input_hash,
    read_provenance,
    validate_provenance,
    write_provenance,
)


def test_read_and_write_provenance_roundtrip():
    body = write_provenance(
        "hello",
        {
            "summary": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
            "description": ProvenanceEntry("description_gen", "2026-03-06", "llm", model="mock-v1"),
        },
    )
    parsed = read_provenance(body)
    assert parsed["summary"].method == "deterministic"
    assert parsed["description"].model == "mock-v1"


def test_validate_provenance_rejects_llm_on_deterministic():
    errors = validate_provenance(
        {"summary": "Jane", "emails": ["jane@example.com"]},
        {
            "summary": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
            "emails": ProvenanceEntry("description_gen", "2026-03-06", "llm"),
        },
    )
    assert "deterministic-only" in errors[0]


def test_compute_input_hash_is_deterministic():
    left = compute_input_hash({"a": 1, "b": ["x", "y"]})
    right = compute_input_hash({"b": ["x", "y"], "a": 1})
    assert left == right
