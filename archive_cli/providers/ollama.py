"""Ollama local model provider -- free, private, requires Ollama daemon."""

from __future__ import annotations

import json
import os
from urllib import error, request


class OllamaModelProvider:
    name = "ollama"

    def __init__(self, model: str = "llama3.2:3b"):
        self.model = model
        self._base_url = os.environ.get("OLLAMA_HOST", "http://localhost:11434").rstrip("/")

    def is_available(self) -> bool:
        url = f"{self._base_url}/api/tags"
        req = request.Request(url, method="GET")
        try:
            with request.urlopen(req, timeout=2) as resp:
                resp.read(64)
        except (error.URLError, TimeoutError, OSError):
            return False
        return True

    def estimated_cost_per_1k_tokens(self) -> float:
        return 0.0

    def generate(self, prompt: str, max_tokens: int = 1024) -> str:
        url = f"{self._base_url}/api/generate"
        body = json.dumps(
            {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens},
            }
        ).encode("utf-8")
        req = request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return str(data.get("response", ""))
