"""Step 8a: JSON parity matrix — `serde_yaml` → Rust canonical JSON vs `ruamel.yaml` → `json.dumps` (vault_cache pipeline)."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from ruamel.yaml import YAML


def _yaml() -> YAML:
    y = YAML()
    y.preserve_quotes = True
    y.default_flow_style = False
    y.allow_unicode = True
    y.width = 10_000
    return y


def python_stable_json_from_yaml_frontmatter(fm_yaml: str) -> str:
    """Matches `archive_cli.vault_cache._frontmatter_hash_stable` inner JSON (double dumps + NUL escape strip)."""
    fm = fm_yaml.strip()
    if not fm:
        data: dict | list | str | None = {}
    else:
        data = _yaml().load(fm)
        if data is None:
            data = {}

    def dumps(x: object) -> str:
        return json.dumps(x, sort_keys=True, default=str)

    s1 = dumps(data)
    cleaned = s1.replace("\\u0000", "")
    sanitized = json.loads(cleaned)
    return dumps(sanitized)


# Representative matrix: minimal, nested, lists, unicode, multiline strings, ints
PARITY_CASES: list[tuple[str, str]] = [
    ("empty", ""),
    ("simple_mapping", "type: email_message\nuid: hfa-1\nsummary: Hello"),
    ("nested", "a:\n  b: 1\n  c:\n    d: two"),
    ("list_scalars", "tags:\n  - one\n  - two"),
    ("unicode_key_and_value", '"café": éclair\nplain: ok'),
    ("int_and_float", "n: 42\nf: 1.5"),
    ("bool_yaml", "active: true\ninactive: false"),
    ("null_yaml", "maybe: null\n"),
]


@pytest.mark.parametrize("name,fm_yaml", PARITY_CASES, ids=[c[0] for c in PARITY_CASES])
def test_rust_python_stable_json_parity(name: str, fm_yaml: str) -> None:
    import archive_crate

    py = python_stable_json_from_yaml_frontmatter(fm_yaml)
    ru = archive_crate.stable_json_from_yaml_frontmatter(fm_yaml)
    assert ru == py, f"{name}: rust={ru!r}\npython={py!r}"


def test_stable_json_matches_frontmatter_hash_payload() -> None:
    """Rust stable JSON string is the prefix hashed in `_frontmatter_hash_stable`."""
    import hashlib

    import archive_crate
    from archive_cli.vault_cache import _frontmatter_hash_stable

    fm_yaml = "type: card\nuid: x-1\nn: 3\n"
    py = python_stable_json_from_yaml_frontmatter(fm_yaml)
    ru = archive_crate.stable_json_from_yaml_frontmatter(fm_yaml)
    assert ru == py


    y = _yaml()
    fm_dict = y.load(fm_yaml.strip())
    expected_hash = _frontmatter_hash_stable(fm_dict)
    payload = hashlib.sha256(ru.encode("utf-8")).hexdigest()
    assert payload == expected_hash
