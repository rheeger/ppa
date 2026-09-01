"""The MCP toolset is the live agent contract."""

from __future__ import annotations

from archive_cli.mcp_instructions import (
    CARD_STACK_PLAYBOOK,
    CARD_STACK_PLAYBOOK_HELP,
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
    "archive_evidence",
    "HOW TO COMPOSE QUERIES",
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
    "archive_evidence",
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
    assert "trust boundary" in TOOL_DESCRIPTIONS["archive_read"].lower()


def test_playbook_teaches_composition() -> None:
    text = CARD_STACK_PLAYBOOK.lower()
    for name in (
        "archive_search",
        "archive_hybrid_search",
        "archive_query",
        "archive_evidence",
        "archive_read",
        "archive_person",
    ):
        assert name in CARD_STACK_PLAYBOOK
    assert "follow" in text
    assert "parent" in text and "attachment" in text
    assert "raise limit" in text or "starting points" in text
    assert "do not use archive_search" not in text
    assert "never use" not in text
    assert "only use evidence" not in text
    help_text = CARD_STACK_PLAYBOOK_HELP.lower()
    assert "compose" in help_text
    assert "search" in help_text and "hybrid" in help_text and "query" in help_text
    assert "evidence" in help_text
    evidence = TOOL_DESCRIPTIONS["archive_evidence"]
    assert "compact" in evidence.lower()
    assert "narrative" in evidence
    assert "raise" in evidence.lower()
    read = TOOL_DESCRIPTIONS["archive_read"]
    assert "include_attachment_uids" in read
    assert "link-only" in read or "link lists" in read
    search = TOOL_DESCRIPTIONS["archive_search"].lower()
    assert "discovery only" not in search
    hybrid = TOOL_DESCRIPTIONS["archive_hybrid_search"].lower()
    assert "small limit" not in hybrid
