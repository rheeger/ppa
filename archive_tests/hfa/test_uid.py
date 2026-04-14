from archive_vault.uid import generate_uid


def test_generate_uid_is_deterministic():
    assert generate_uid("person", "linkedin", "jane@example.com") == generate_uid(
        "person", "linkedin", "jane@example.com"
    )


def test_generate_uid_has_hfa_prefix():
    assert generate_uid("person", "linkedin", "jane@example.com").startswith("hfa-person-")
