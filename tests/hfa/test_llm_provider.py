import json

from hfa.llm_provider import DEFAULT_LLM_CONFIG, PROVIDER_REGISTRY, decide_same_person, load_llm_config


class FakeProvider:
    name = "mock"

    def __init__(self, model: str = "mock-v1"):
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        return "YES"


def test_load_llm_config_returns_defaults(tmp_vault):
    assert load_llm_config(tmp_vault)["primary"] == DEFAULT_LLM_CONFIG["primary"]


def test_decide_same_person_uses_cache(tmp_vault, monkeypatch):
    monkeypatch.setitem(PROVIDER_REGISTRY, "mock", FakeProvider)
    (tmp_vault / "_meta" / "llm-config.json").write_text(
        json.dumps({"primary": {"provider": "mock", "model": "mock-v1"}}),
        encoding="utf-8",
    )

    first = decide_same_person(tmp_vault, {"summary": "Jane"}, {"summary": "Jane"})
    second = decide_same_person(tmp_vault, {"summary": "Jane"}, {"summary": "Jane"})

    assert first == "YES"
    assert second == "YES"
    cache = json.loads((tmp_vault / "_meta" / "llm-cache.json").read_text(encoding="utf-8"))
    assert len(cache) == 1
