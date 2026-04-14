"""Step 9b: Rust chunk_hash matches Python _chunk_hash (json.dumps byte-for-byte)."""

from __future__ import annotations

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate
from archive_cli.chunk_builders import _chunk_hash


@pytest.mark.parametrize(
    "chunk_type,content,source_fields",
    [
        ("summary", "plain ascii", ["summary"]),
        ("body", "Hello\nWorld", ["body"]),
        ("x", 'quotes " and \\ backslash', ["a"]),
        ("t", "tab\there", ["t"]),
        ("u", "café CJK 中文", ["u"]),
        ("e", "emoji 😀 and \U0001F600", ["e"]),
        ("s", "\U00010000 astral", ["s"]),
        ("m", "multi\nline\nfield", ["a", "b"]),
        ("empty", "", []),
        ("empty_sf", "x", []),
    ],
)
def test_chunk_hash_matches_python(chunk_type, content, source_fields):
    py_h = _chunk_hash(chunk_type, content, source_fields)
    rust_h = archive_crate.chunk_hash(chunk_type, content, source_fields)
    assert rust_h == py_h, (chunk_type, repr(content), source_fields)


def test_chunk_hash_many_strings():
    """~50 strings of varied shapes (ASCII, accents, CJK, escapes)."""
    samples = [
        "",
        "a",
        "a" * 200,
        "line1\nline2",
        "\r\n",
        "\t",
        "\x00",
        "\x1f",
        "\u007f",
        "café",
        "naïve",
        "中文",
        "日本語",
        "한글",
        "🎉",
        "😀😁😂",
        "a😀b",
        "\U0001F600",
        "\U00010000",
        "\U00010FFFF",
        '\\"quoted\\"',
        '""',
        "\\\\",
        "a/b",
        "a\\b",
        '{"a": 1}',
        "[1, 2, 3]",
    ]
    for s in samples:
        py_h = _chunk_hash("t", s, ["f"])
        rust_h = archive_crate.chunk_hash("t", s, ["f"])
        assert rust_h == py_h, repr(s)

    for s in samples:
        py_h = _chunk_hash("type", "content", [s])
        rust_h = archive_crate.chunk_hash("type", "content", [s])
        assert rust_h == py_h, repr(s)
