"""YAML frontmatter parsing and rendering."""

from __future__ import annotations

import re
from io import StringIO
from threading import local
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", re.DOTALL)
# Line-only fences. Do not use \\s after the closing ---; that swallows blank
# lines that update_frontmatter_fields must put back unchanged.
_FRONTMATTER_RAW_RE = re.compile(r"^---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(\r?\n.*)?$", re.DOTALL)
_yaml_state = local()


def _get_yaml() -> YAML:
    yaml = getattr(_yaml_state, "yaml", None)
    if yaml is None:
        yaml = YAML()
        yaml.preserve_quotes = True
        yaml.default_flow_style = False
        yaml.allow_unicode = True
        yaml.width = 10_000
        _yaml_state.yaml = yaml
    return yaml


def _flowify(value: Any) -> Any:
    if isinstance(value, dict):
        mapped = CommentedMap()
        for key, inner in value.items():
            mapped[key] = _flowify(inner)
        return mapped
    if isinstance(value, list):
        seq = CommentedSeq(_flowify(item) for item in value)
        seq.fa.set_flow_style()
        return seq
    return value


def split_frontmatter_text(content: str) -> tuple[str, str] | None:
    """Split a card into YAML text and the exact suffix after the closing fence.

    The closing fence must sit on its own line. ``---`` inside a quoted
    frontmatter value (Gmail snippets such as ``----- Original Message -----``)
    does not close the block. The suffix keeps the newline and any blank
    lines after the fence so writers can put the body back unchanged.
    """

    match = _FRONTMATTER_RAW_RE.match(content)
    if not match:
        return None
    return match.group(1), match.group(2) or ""


def parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """Extract YAML frontmatter and markdown body."""

    match = _FRONTMATTER_RE.match(content)
    if not match:
        return {}, content

    frontmatter_text, body = match.groups()
    if not frontmatter_text.strip():
        return {}, body

    parsed = _get_yaml().load(frontmatter_text)
    if parsed is None:
        return {}, body
    if not isinstance(parsed, dict):
        raise ValueError("Frontmatter must parse to a mapping")
    return dict(parsed), body


def render_frontmatter(data: dict[str, Any]) -> str:
    """Render a dict into a YAML frontmatter block."""

    cleaned = {key: value for key, value in data.items() if value is not None and value != ""}
    stream = StringIO()
    _get_yaml().dump(_flowify(cleaned), stream)
    rendered = stream.getvalue().strip()
    return f"---\n{rendered}\n---"


def render_card(frontmatter: dict[str, Any], body: str) -> str:
    """Render a complete markdown card."""

    return f"{render_frontmatter(frontmatter)}\n\n{body.strip()}\n"
