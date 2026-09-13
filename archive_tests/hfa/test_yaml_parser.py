from archive_vault.yaml_parser import parse_frontmatter, render_card, render_frontmatter, split_frontmatter_text


def test_parse_frontmatter_handles_arrays_and_colons():
    frontmatter, body = parse_frontmatter('---\nsummary: "Title: Subtitle"\ntags: [a, b, c]\n---\nbody text\n')
    assert frontmatter["summary"] == "Title: Subtitle"
    assert frontmatter["tags"] == ["a", "b", "c"]
    assert body == "body text\n"


def test_parse_frontmatter_without_block_returns_body_only():
    frontmatter, body = parse_frontmatter("hello")
    assert frontmatter == {}
    assert body == "hello"


def test_render_frontmatter_omits_empty_strings_but_keeps_arrays():
    rendered = render_frontmatter({"summary": "Jane", "description": "", "tags": ["a", "b"], "phones": []})
    assert "summary: Jane" in rendered
    assert "description" not in rendered
    assert "tags: [a, b]" in rendered
    assert "phones: []" in rendered


def test_split_frontmatter_text_ignores_dashes_inside_quoted_snippet():
    content = (
        "---\n"
        "uid: hfa-email-message-reply\n"
        "snippet: 'Robbie ----- Original Message ----- From: Jim'\n"
        "---\n"
        "\n"
        "body\n"
    )
    split = split_frontmatter_text(content)
    assert split is not None
    frontmatter_text, body = split
    assert "----- Original Message -----" in frontmatter_text
    assert body == "\n\nbody\n"


def test_render_card_roundtrips():
    original = {"summary": "Jane Smith", "tags": ["friend"]}
    content = render_card(original, "body")
    frontmatter, body = parse_frontmatter(content)
    assert frontmatter == original
    assert body == "body\n"
