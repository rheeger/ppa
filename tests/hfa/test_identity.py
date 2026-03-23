import json

from hfa.identity import IdentityCache, resolve_any, upsert_identity_map


def test_upsert_identity_map_indexes_arrays(tmp_vault):
    upsert_identity_map(
        tmp_vault,
        "[[jane-smith]]",
        {"name": "Jane Smith", "emails": ["jane@example.com", "j.smith@corp.com"], "phones": ["123", "456"]},
    )
    payload = json.loads((tmp_vault / "_meta" / "identity-map.json").read_text(encoding="utf-8"))
    assert payload["name:jane smith"] == "[[jane-smith]]"
    assert payload["email:jane@example.com"] == "[[jane-smith]]"
    assert payload["phone:456"] == "[[jane-smith]]"


def test_identity_cache_flushes_once(tmp_vault):
    cache = IdentityCache(tmp_vault)
    cache.upsert("[[jane-smith]]", {"name": "Jane Smith", "emails": ["jane@example.com"]})
    assert resolve_any(tmp_vault, "email", "jane@example.com") is None
    cache.flush()
    assert resolve_any(tmp_vault, "email", "jane@example.com") == "[[jane-smith]]"
