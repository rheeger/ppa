"""Isolated real-product demonstration for PR31. Never touches the living archive."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from archive_cli.commands.identity_repair import emit_same_conversation_edges, merge_people
from archive_engine.conversation_links import load_proposals
from archive_engine.journaled_state import IDENTITY_PROPOSALS_REL, load_json_state
from archive_sync.adapters.base import deterministic_provenance
from archive_vault.schema import IMessageThreadCard, PersonCard
from archive_vault.vault import write_card


def _vault(tmp_path: Path) -> Path:
    vault = tmp_path / "vault"
    (vault / "People").mkdir(parents=True, exist_ok=True)
    (vault / "Messages").mkdir(parents=True, exist_ok=True)
    meta = vault / "_meta"
    meta.mkdir(exist_ok=True)
    for name, payload in (
        ("identity-map.json", "{}"),
        ("sync-state.json", "{}"),
        ("own-emails.json", "[]"),
        ("nicknames.json", "{}"),
        ("ppa-config.json", "{}"),
        ("llm-config.json", '{"primary": {"provider": "gemini", "model": "fixture"}}'),
    ):
        (meta / name).write_text(payload + "\n", encoding="utf-8")
    return vault


def _write_person(vault: Path, rel: str, uid: str, first: str, last: str, **kwargs) -> None:
    card = PersonCard(
        uid=uid,
        type="person",
        source=["contacts.apple"],
        source_id=uid,
        created="2026-01-01",
        updated="2026-01-01",
        summary=f"{first} {last}".strip() or uid,
        first_name=first,
        last_name=last,
        emails=list(kwargs.get("emails") or []),
        phones=list(kwargs.get("phones") or []),
    )
    write_card(vault, rel, card, body=card.summary, provenance=deterministic_provenance(card, "contacts.apple"))


def _write_thread(vault: Path, rel: str, uid: str, handle: str, person: str) -> None:
    card = IMessageThreadCard(
        uid=uid,
        type="imessage_thread",
        source=["imessage"],
        source_id=uid,
        created="2024-01-01",
        updated="2024-01-01",
        summary="thread",
        imessage_chat_id=uid,
        people=[f"[[{person}]]"],
        participant_handles=[handle],
        message_count=0,
    )
    write_card(vault, rel, card, body=card.summary, provenance=deterministic_provenance(card, "imessage"))


def _seed(vault: Path) -> None:
    _write_person(vault, "People/alice.md", "hfa-person-alice000001", "Alice", "Smith", phones=["+15551230000"])
    _write_person(vault, "People/bob.md", "hfa-person-bob00000001", "Bob", "Smith", phones=["+15551230000"])
    _write_person(vault, "People/pat.md", "hfa-person-pat00000001", "Pat", "Lee", emails=["pat@example.com"])
    _write_person(vault, "People/stub.md", "hfa-person-stub0000001", "", "", emails=["pat@example.com"])
    _write_thread(vault, "Messages/phone.md", "hfa-imessage-thread-t1", "+15551239999", "hfa-person-pat00000001")
    _write_thread(vault, "Messages/email.md", "hfa-imessage-thread-e1", "pat@example.com", "hfa-person-pat00000001")


def test_isolated_cli_product_path(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    env = {**os.environ, "PPA_PATH": str(vault)}
    env.pop("PPA_SERVING_INDEX_PATH", None)
    env.pop("PPA_INDEX_DSN", None)
    env.pop("PPA_TEST_PG_DSN", None)
    env.pop("PPA_CONFIG_PATH", None)

    repo = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = str(repo)
    dry = subprocess.run(
        [sys.executable, "-m", "archive_cli", "identity-repair", "merge"],
        cwd=str(repo),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert dry.returncode == 0, dry.stderr
    dry_payload = json.loads(dry.stdout)
    assert dry_payload["applied"] is False
    assert not (vault / IDENTITY_PROPOSALS_REL).exists()
    assert not any(
        item["winner"] in {"hfa-person-alice000001", "hfa-person-bob00000001"}
        or item["loser"] in {"hfa-person-alice000001", "hfa-person-bob00000001"}
        for item in dry_payload["redirected"]
    )

    applied = subprocess.run(
        [sys.executable, "-m", "archive_cli", "identity-repair", "merge", "--apply"],
        cwd=str(repo),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert applied.returncode == 0, applied.stderr
    apply_payload = json.loads(applied.stdout)
    assert apply_payload["applied"] is True
    touched = {item["winner"] for item in apply_payload["redirected"]} | {
        item["loser"] for item in apply_payload["redirected"]
    }
    assert "hfa-person-alice000001" not in touched
    assert "hfa-person-bob00000001" not in touched
    assert {"hfa-person-pat00000001", "hfa-person-stub0000001"} <= touched
    queued = load_json_state(vault, IDENTITY_PROPOSALS_REL).get("proposals") or []
    assert any("hfa-person-alice000001" in item.get("uids", []) for item in queued)

    preview = subprocess.run(
        [sys.executable, "-m", "archive_cli", "identity-repair", "same-conversation"],
        cwd=str(repo),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert preview.returncode == 0, preview.stderr
    assert json.loads(preview.stdout)["applied"] is False
    assert load_proposals(vault) == []

    convo = subprocess.run(
        [sys.executable, "-m", "archive_cli", "identity-repair", "same-conversation", "--apply"],
        cwd=str(repo),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert convo.returncode == 0, convo.stderr
    assert json.loads(convo.stdout)["applied"] is True
    assert load_proposals(vault)

    from archive_engine.identity_resolution import resolve_candidates

    uids = []
    for path in (vault / "People").glob("*.md"):
        text = path.read_text(encoding="utf-8")
        if "+15551230000" in text or "5551230000" in text:
            for line in text.splitlines():
                if line.startswith("uid:"):
                    uids.append(line.split(":", 1)[1].strip())
    resolution = resolve_candidates(uids)
    assert resolution.status == "ambiguous"
    assert len(resolution.candidate_uids) == 2


def test_isolated_engine_services_match_cli_semantics(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _seed(vault)
    dry = merge_people(vault, apply=False)
    assert dry["applied"] is False
    assert not any(
        item["winner"] in {"hfa-person-alice000001", "hfa-person-bob00000001"}
        or item["loser"] in {"hfa-person-alice000001", "hfa-person-bob00000001"}
        for item in dry["redirected"]
    )
    applied = merge_people(vault, apply=True)
    assert applied["applied"] is True
    preview = emit_same_conversation_edges(vault, apply=False)
    assert preview["applied"] is False
    emit_same_conversation_edges(vault, apply=True)
    assert load_proposals(vault)
