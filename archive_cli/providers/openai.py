"""OpenAI model provider for enrichment and maintenance tasks."""

from __future__ import annotations

import json
import os
from typing import Any

from ..embedding_provider import _post_json, _resolve_openai_api_key


class OpenAIModelProvider:
    name = "openai"

    def __init__(self, model: str = "gpt-4o-mini"):
        self.model = model

    def is_available(self) -> bool:
        key = os.environ.get("OPENAI_API_KEY", "").strip()
        return bool(key)

    def estimated_cost_per_1k_tokens(self) -> float:
        m = self.model.lower()
        if "gpt-4o-mini" in m:
            return 0.00015
        if m.startswith("gpt-4o") or "gpt-4o" in m:
            return 0.005
        return 0.001

    def generate(self, prompt: str, max_tokens: int = 1024) -> str:
        api_key = _resolve_openai_api_key()
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        }
        data = _post_json(url, headers, payload)
        if not data:
            raise RuntimeError("OpenAI chat completion request failed")
        choices = data.get("choices")
        if not choices:
            raise RuntimeError(f"OpenAI response missing choices: {json.dumps(data)[:500]}")
        msg = choices[0].get("message") or {}
        content = msg.get("content")
        if content is None:
            raise RuntimeError("OpenAI response missing message content")
        return str(content)
