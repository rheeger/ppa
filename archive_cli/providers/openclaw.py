"""OpenClaw model provider -- future stub."""

from __future__ import annotations


class OpenClawModelProvider:
    name = "openclaw"

    def __init__(self, model: str = ""):
        self.model = model or "default"

    def generate(self, prompt: str, max_tokens: int = 1024) -> str:
        raise NotImplementedError("OpenClaw provider is a future stub")

    def is_available(self) -> bool:
        return False

    def estimated_cost_per_1k_tokens(self) -> float:
        return 0.0
