"""Parity tests for archive_crate.parse_frontmatter vs hfa.yaml_parser.parse_frontmatter."""

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from archive_vault.yaml_parser import parse_frontmatter as parse_py


def _assert_parity(content: str) -> None:
    import archive_crate

    r_fm, r_body = archive_crate.parse_frontmatter(content)
    p_fm, p_body = parse_py(content)
    assert dict(r_fm) == p_fm
    assert r_body == p_body


@pytest.mark.parametrize(
    "content",
    [
        "---\ntype: email_message\nuid: hfa-x\n---\n\nBody here.\n",
        "Just markdown\n",
        "---\n---\n\nok\n",
        '---\nsummary: "Title: Subtitle"\ntags: [a, b, c]\n---\nbody text\n',
        "---\n# just a comment\n---\nbody\n",
        "---\nnull\n---\nbody\n",
        "---\n   \n---\nbody\n",
        "---\n\n---\nbody\n",
        "---\nname: 日本語\n---\n本文\n",
        "---\ndesc: |\n  line1\n  line2\n---\nok\n",
        "---\na: &x 1\nb: *x\n---\n",
        "---\r\ntype: x\r\n---\r\nbody\r\n",
        "---\nnested:\n  k: v\n  n: 1\n---\nout\n",
        "---\nflag: true\nn: 0\n---\n",
    ],
)
def test_parse_frontmatter_parity_parametrized(content: str) -> None:
    _assert_parity(content)


def test_parse_frontmatter_rejects_non_mapping_root() -> None:
    import archive_crate

    content = "---\n- a\n- b\n---\nbody\n"
    with pytest.raises(ValueError, match="Frontmatter must parse to a mapping"):
        parse_py(content)
    with pytest.raises(ValueError, match="Frontmatter must parse to a mapping"):
        archive_crate.parse_frontmatter(content)


def test_parse_frontmatter_scalar_root() -> None:
    import archive_crate

    content = "---\n42\n---\nbody\n"
    with pytest.raises(ValueError, match="Frontmatter must parse to a mapping"):
        parse_py(content)
    with pytest.raises(ValueError, match="Frontmatter must parse to a mapping"):
        archive_crate.parse_frontmatter(content)
