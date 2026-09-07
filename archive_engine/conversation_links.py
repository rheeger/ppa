"""Proposed same-conversation links. Consumed as proposed_link, never identity."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from archive_engine.contracts import UNKNOWN, ServingEdge

PROPOSAL_METHOD = "same_conversation_handle_pair"


def enumerate_pairs(threads: list[dict[str, Any]], *, limit: int = 64) -> dict[str, Any]:
    """Bounded email↔phone thread pairs that share one person. No first-element smash."""

    by_person: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in threads:
        people = [str(p) for p in (row.get("people") or []) if str(p).strip()]
        handles = [str(h) for h in (row.get("participant_handles") or []) if str(h).strip()]
        uid = str(row.get("uid") or "")
        if len(people) != 1 or len(handles) != 1 or not uid:
            continue
        by_person[people[0]].append({"uid": uid, "handle": handles[0]})
    pairs: list[dict[str, Any]] = []
    truncated = False
    for person, items in by_person.items():
        emails = [t for t in items if "@" in t["handle"]]
        phones = [t for t in items if "@" not in t["handle"]]
        for email in emails:
            for phone in phones:
                if len(pairs) >= limit:
                    truncated = True
                    break
                left, right = sorted((email["uid"], phone["uid"]))
                pairs.append(
                    {
                        "person": person,
                        "email_thread": email["uid"],
                        "phone_thread": phone["uid"],
                        "pair_id": f"{left}::{right}",
                        "edge_type": "same_conversation",
                        "evidence_kind": "proposed_link",
                        "method": PROPOSAL_METHOD,
                    }
                )
            if truncated:
                break
    return {"pairs": pairs, "truncated": truncated, "count": len(pairs)}


def edge_for(pair: dict[str, Any]) -> ServingEdge:
    return ServingEdge(
        method=str(pair.get("method") or PROPOSAL_METHOD),
        confidence=None,
        evidence_uids=(str(pair.get("person") or ""),),
    )


def preview_or_apply(vault: Path, threads: list[dict[str, Any]], *, apply: bool) -> dict[str, Any]:
    """Preview is a no-op. Apply journals proposals for the linker/seed_links consumer."""

    from archive_engine.journaled_state import (
        CONVERSATION_PROPOSALS_REL,
        CONVERSATION_PROPOSALS_UID,
        persist_json_state,
    )

    enumerated = enumerate_pairs(threads)
    if not apply:
        return {**enumerated, "applied": False, "path": "", "mutation_id": ""}
    payload = {
        "status": "proposed",
        "evidence_kind": "proposed_link",
        "method": PROPOSAL_METHOD,
        "pairs": enumerated["pairs"],
        "truncated": enumerated["truncated"],
    }
    record = persist_json_state(
        Path(vault),
        uid=CONVERSATION_PROPOSALS_UID,
        rel=CONVERSATION_PROPOSALS_REL,
        payload=payload,
        source="conversation-links",
    )
    return {
        **enumerated,
        "applied": True,
        "path": CONVERSATION_PROPOSALS_REL,
        "mutation_id": record.mutation_id,
        "edge_defaults": edge_for(enumerated["pairs"][0]).to_payload() if enumerated["pairs"] else {"method": UNKNOWN},
    }


def load_proposals(vault: Path) -> list[dict[str, Any]]:
    from archive_engine.journaled_state import CONVERSATION_PROPOSALS_REL, load_json_state

    payload = load_json_state(Path(vault), CONVERSATION_PROPOSALS_REL)
    pairs = payload.get("pairs") or []
    return [dict(item) for item in pairs if isinstance(item, dict)]
