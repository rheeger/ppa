"""The MCP toolset is the live agent contract."""

from __future__ import annotations

from archive_cli.mcp_instructions import (
    FILTER_HINT,
    TOOL_DESCRIPTIONS,
    build_server_instructions,
)
from archive_cli.server import _server_instructions


REQUIRED_INSTRUCTION_NEEDLES = (
    "retrieval engine",
    "canonical",
    "email_message",
    "archive_hybrid_search",
    "archive_query",
    "archive_read",
    "archive_temporal_neighbors",
    "archive_person",
    "people_filter",
    "confidence",
    "fail closed",
    "Don't treat snippets",
    "Don't use hyphenated types",
    "Don't put an email address in people_filter",
)

REQUIRED_RETRIEVAL_TOOLS = (
    "archive_search",
    "archive_search_json",
    "archive_query",
    "archive_hybrid_search",
    "archive_hybrid_search_json",
    "archive_vector_search",
    "archive_read",
    "archive_read_many",
    "archive_person",
    "archive_graph",
    "archive_timeline",
    "archive_temporal_neighbors",
    "archive_knowledge",
    "archive_stats",
    "archive_status_json",
)


def test_server_instructions_cover_contract() -> None:
    text = build_server_instructions("Test Archives")
    assert text.startswith("Test Archives")
    for needle in REQUIRED_INSTRUCTION_NEEDLES:
        assert needle in text, f"missing {needle!r}"


def test_server_module_exports_live_instructions() -> None:
    for needle in ("DO", "DON'T", "ROUTING", "archive_hybrid_search"):
        assert needle in _server_instructions


def test_tool_descriptions_cover_retrieval_surface() -> None:
    assert set(REQUIRED_RETRIEVAL_TOOLS) == set(TOOL_DESCRIPTIONS)
    for name, desc in TOOL_DESCRIPTIONS.items():
        assert len(desc) > 80, name
        assert "email-message" not in desc or "Never hyphens" in desc


def test_filter_hint_forbids_hyphen_types() -> None:
    assert "underscores" in FILTER_HINT
    assert "email_message" in FILTER_HINT
    assert "Never hyphens" in FILTER_HINT


def test_query_and_hybrid_descriptions_teach_filters() -> None:
    query = TOOL_DESCRIPTIONS["archive_query"]
    hybrid = TOOL_DESCRIPTIONS["archive_hybrid_search"]
    assert "people_filter=Sarah" in query
    assert "query=<email>" in hybrid
    assert "trust boundary" in TOOL_DESCRIPTIONS["archive_read"]
