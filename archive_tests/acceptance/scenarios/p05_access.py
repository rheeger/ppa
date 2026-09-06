"""P05-B acceptance: restricted principal cannot launder a denied source."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from archive_cli.serving_index import get_serving_handle
from archive_cli.store import DefaultArchiveStore
from archive_engine.access import access_request_fields, is_unrestricted, policy_identity
from archive_engine.contracts import AccessContext
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_retrieval_privacy import (
    DENIED_TOKEN,
    GMAIL_UID,
    MEDICAL_UID,
    MIXED_UID,
    UNKNOWN_UID,
    _publish_privacy,
)

PRINCIPALS = (
    ("unrestricted", AccessContext(archive_id="p05b", principal="local-operator", profile="trusted-local")),
    ("alice-gmail", AccessContext(archive_id="p05b", principal="alice", profile="read-only", allowed_sources=("gmail",))),
    ("bob-medical", AccessContext(archive_id="p05b", principal="bob", profile="read-only", allowed_sources=("medical",))),
)


def _matrix_row(handle, access: AccessContext) -> dict[str, Any]:
    policy = access_request_fields(access)
    listed = handle.query(limit=20, **policy)
    uids = sorted({str(row.get("card_uid") or "") for row in listed if row.get("card_uid")})
    graph = handle.graph("Email/gmail.md", hops=2, **policy)
    blob = json.dumps({"listed": listed, "graph": graph})
    return {
        "principal": access.principal,
        "profile": access.profile,
        "allowed_sources": list(access.allowed_sources),
        "policy_identity": policy_identity(access),
        "unrestricted": is_unrestricted(access),
        "uids": uids,
        "has_gmail": GMAIL_UID in uids,
        "has_medical": MEDICAL_UID in uids,
        "has_mixed": MIXED_UID in uids,
        "has_unknown": UNKNOWN_UID in uids,
        "denied_token_leaked": DENIED_TOKEN in blob if access.allowed_sources == ("gmail",) else None,
        "graph_keys": sorted(graph.keys()),
    }


def run_p05_access(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    reset_serving_handle()
    gid = _publish_privacy(Path(runtime.serving_index_path))
    reset_serving_handle()
    handle = get_serving_handle(runtime.vault)
    matrix = [_matrix_row(handle, access) for _, access in PRINCIPALS]
    alice = next(row for row in matrix if row["principal"] == "alice")
    local = next(row for row in matrix if row["principal"] == "local-operator")
    if not local["has_gmail"] or not local["has_medical"]:
        raise AssertionError(f"unrestricted lost P01 visibility: {local}")
    if alice["has_medical"] or alice["has_mixed"] or alice["has_unknown"]:
        raise AssertionError(f"gmail principal leaked denied source: {alice}")
    if not alice["has_gmail"]:
        raise AssertionError(f"gmail principal lost allowed source: {alice}")
    if alice["denied_token_leaked"]:
        raise AssertionError("denied sentinel leaked into gmail payload")

    class _ForceServing(DefaultArchiveStore):
        def _is_warehouse_index(self) -> bool:
            return True

    class _Index:
        def search(self, *args, **kwargs):
            return []

        def query_cards(self, **kwargs):
            return []

        def card_stack_pointers(self, uids):
            return {}

    store = _ForceServing(
        vault=runtime.vault,
        index=_Index(),
        access=AccessContext(archive_id="p05b", principal="alice", profile="read-only", allowed_sources=("gmail",)),
    )
    read = store.read(MEDICAL_UID)
    ev = store.evidence(query="note", limit=8)
    if read.get("found"):
        raise AssertionError(f"denied medical read leaked: {read}")
    if MEDICAL_UID in json.dumps(ev) or DENIED_TOKEN in json.dumps(ev):
        raise AssertionError(f"evidence leaked denied source: {ev}")

    return {
        "id": "p05.access.principal_source_matrix",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "generation": gid,
        "matrix": matrix,
        "denied_read": read,
        "evidence_uids": [hit.get("uid") for hit in ev.get("hits") or []],
    }


register(
    Scenario(
        id="p05.access.principal_source_matrix",
        suite="p05",
        product_guarantee="A restricted principal cannot discover another account through search, graph, evidence, or reads",
        proof_tier="isolated_integration",
        fixture_seed=5,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p05-access.json",),
        run=run_p05_access,
    )
)
