"""P04-B frozen corpus: types, closure, holdout, ANN adversary, no private text."""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from archive_tests.acceptance.corpus import (
    DATA_DIR,
    RELATION_OWNERS,
    assert_query_splits,
    assert_relation_closure,
    build_cards,
    build_queries,
    build_relations,
    canonical_dumps,
    cardinalities,
    collect_corpus_text,
    forbidden_text_hits,
    frozen_payloads,
    instantiate_card,
    materialize_corpus,
    write_frozen_data_files,
)
from archive_tests.acceptance.fixtures import CONTRACT_VERSION, CORPUS_SEED
from archive_tests.acceptance.oracle import (
    build_adversarial_modulo_ivf_dataset,
    cosine_similarity,
    exact_nearest_neighbors,
    ivf_nlist,
    modulo_ivf_knn,
    recall_at_k,
)
from archive_tests.acceptance.registry import load_builtin_scenarios, scenarios_for
from archive_tests.conftest import OWNED_ROOT_MARKER
from archive_vault.schema import CARD_TYPES

REPO = Path(__file__).resolve().parents[1]


def _payloads() -> dict:
    return frozen_payloads()


def test_frozen_json_matches_generator() -> None:
    payloads = _payloads()
    for name in ("corpus_manifest", "expected_queries", "expected_relations"):
        path = DATA_DIR / f"{name}.json"
        assert path.is_file(), f"missing frozen file {path}"
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        assert on_disk == payloads[name]
        assert path.read_text(encoding="utf-8") == canonical_dumps(payloads[name])


def test_all_card_types_represented() -> None:
    cards = build_cards()
    types = {card["type"] for card in cards}
    assert types == set(CARD_TYPES)
    assert len(cards) == cardinalities(cards)["cards"]
    for spec in cards:
        instantiate_card(spec)


def test_required_families_are_labeled() -> None:
    cards = build_cards()
    families = {family for card in cards for family in card["families"]}
    required = {
        "shared_external_id",
        "namesake",
        "aliases",
        "manual_correction",
        "inferred_edge",
        "corpus_state",
        "source_duplicate",
        "derived_duplicate",
        "changed_deleted_chunks",
        "timezone_interval",
        "multi_currency_refund",
        "multi_leg",
        "subscription_ambiguity",
        "high_degree",
        "malformed_attachment",
        "same_trip",
        "same_charge",
        "not_the_same_person",
        "reply_authorization",
    }
    missing = required - families
    assert not missing
    states = {card["corpus_state"] for card in cards}
    assert states >= {"active", "quarantine", "suppressed"}
    inferred = [card for card in cards if card["edge_kind"] == "inferred"]
    deterministic = [card for card in cards if card["edge_kind"] == "deterministic"]
    assert inferred and deterministic


def test_relation_closure_and_owners() -> None:
    cards = build_cards()
    relations = build_relations(cards)
    cards_by_uid = {card["uid"]: card for card in cards}
    assert_relation_closure(relations, cards_by_uid)
    assert {item["family"] for item in relations} == set(RELATION_OWNERS)
    same_trip = next(item for item in relations if item["family"] == "same-trip")
    assert "hfa-flight-p04blook001" in same_trip["negative_evidence_ids"]
    same_charge = next(item for item in relations if item["family"] == "same-charge")
    assert "hfa-finance-p04blook001" in same_charge["negative_evidence_ids"]
    auth = next(item for item in relations if item["family"] == "reply-is-authorization")
    assert auth.get("authorized") is not True
    assert "authorized" not in auth["expected_output_fields"]


def test_missing_required_evidence_fails() -> None:
    cards = build_cards()
    relations = [dict(item) for item in build_relations(cards)]
    relations[0]["positive_evidence_ids"] = list(relations[0]["positive_evidence_ids"]) + ["hfa-person-p04bmissing01"]
    with pytest.raises(AssertionError, match="missing card"):
        assert_relation_closure(relations, {card["uid"]: card for card in cards})


def test_false_positive_connection_fails() -> None:
    cards = build_cards()
    relations = [dict(item) for item in build_relations(cards)]
    lookalike = relations[0]["negative_evidence_ids"][0]
    relations[0]["positive_evidence_ids"] = list(relations[0]["positive_evidence_ids"]) + [lookalike]
    with pytest.raises(AssertionError, match="both positive and negative"):
        assert_relation_closure(relations, {card["uid"]: card for card in cards})


def test_calibration_and_held_out_are_disjoint_and_hashed() -> None:
    queries = build_queries()
    assert_query_splits(queries)
    calibration = {item["query_id"] for item in queries if item["split"] == "calibration"}
    held_out = {item["query_id"] for item in queries if item["split"] == "held_out"}
    assert calibration.isdisjoint(held_out)
    assert "q-p04b-ann-modulo-adversary" in calibration
    assert "q-p04b-agg-same-charge" in held_out
    eur = next(item for item in queries if item["query_id"] == "q-p04b-agg-eur-net")
    assert eur["expected_amount"] == 60.0
    charge = next(item for item in queries if item["query_id"] == "q-p04b-agg-same-charge")
    assert charge["expected_amount"] == 482.0


def test_no_private_or_seed_text() -> None:
    hits = forbidden_text_hits(collect_corpus_text())
    assert hits == []
    for path in DATA_DIR.glob("*.json"):
        assert forbidden_text_hits(path.read_text(encoding="utf-8")) == []


def test_ann_adversary_nlist_gt_32_and_old_algorithm_fails() -> None:
    dataset = build_adversarial_modulo_ivf_dataset(seed=CORPUS_SEED)
    assert dataset.nlist == 33
    assert dataset.nprobe == 32
    assert ivf_nlist(len(dataset.items)) == 33
    assert len(dataset.items) == 1089
    exact = exact_nearest_neighbors(dataset.query, dataset.items, k=1)
    assert exact[0].item_id == dataset.true_neighbor_id
    assert exact[0].score == pytest.approx(1.0)
    simulated = modulo_ivf_knn(dataset.query, dataset.items, k=10)
    assert simulated.nlist == 33
    assert dataset.true_neighbor_id not in [row.item_id for row in simulated.neighbors]
    assert dataset.unprobed_list not in simulated.probed_lists
    assert recall_at_k([row.item_id for row in simulated.neighbors], [dataset.true_neighbor_id], k=10) == 0.0
    assert cosine_similarity(dataset.query, dataset.items[dataset.true_neighbor_index][1]) == pytest.approx(1.0)


def test_ann_oracle_does_not_import_native() -> None:
    import archive_tests.acceptance.oracle as module

    assert "archive_crate" not in module.__dict__
    assert "serving_index" not in module.__dict__
    fn_source = (
        inspect.getsource(exact_nearest_neighbors)
        + inspect.getsource(modulo_ivf_knn)
        + inspect.getsource(build_adversarial_modulo_ivf_dataset)
    )
    assert "archive_crate" not in fn_source
    assert "serving_index" not in fn_source


def test_expected_neighbors_are_not_copied_from_native_output() -> None:
    queries = build_queries()
    ann_query = next(item for item in queries if item["kind"] == "ann")
    assert ann_query["expected_uids"] == ["ann-p04b-0065"]
    notes = ann_query["notes"].lower()
    assert "do not copy" in notes
    assert "native" in notes


def test_materialize_writes_owned_vault(tmp_path: Path) -> None:
    (tmp_path / OWNED_ROOT_MARKER).write_text("ppa-acceptance-owned\n", encoding="utf-8")
    vault = tmp_path / "vault"
    result = materialize_corpus(vault, owned_root=tmp_path)
    assert result["card_count"] == len(build_cards())
    sample = vault / "People/p04-alex-rivera-primary.md"
    assert sample.is_file()
    text = sample.read_text(encoding="utf-8")
    assert "hfa-person-p04balex0001" in text
    assert "P04B-TOKEN-ALEX-PRIMARY" in text


def test_p04_scenario_is_registered() -> None:
    load_builtin_scenarios()
    ids = [item.id for item in scenarios_for("p04")]
    assert "p04.frozen_corpus" in ids


def test_write_frozen_data_files_round_trip(tmp_path: Path) -> None:
    written = write_frozen_data_files(tmp_path)
    for name, path in written.items():
        assert json.loads(path.read_text(encoding="utf-8")) == _payloads()[name]


def test_contract_version_and_seed() -> None:
    manifest = _payloads()["corpus_manifest"]
    assert manifest["contract_version"] == CONTRACT_VERSION
    assert manifest["seed"] == CORPUS_SEED
    assert manifest["type_count"] == len(CARD_TYPES)
