from __future__ import annotations

from archive_engine.identity_resolution import resolve_candidates


def test_resolution_is_once_per_request() -> None:
    calls = {"n": 0}

    def resolve(uids):
        calls["n"] += 1
        return resolve_candidates(uids)

    result = resolve(["a", "b", "a"])
    assert calls["n"] == 1
    assert result.status == "ambiguous"
    assert result.candidate_uids == ("a", "b")
