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
        from archive_engine.egress import EgressDeniedError, authorize_request, guarded_urlopen

        url = f"{self._base_url}/api/tags"
        req = request.Request(url, method="GET")
        try:
            authorize_request(destination="ollama", url=url)
            with guarded_urlopen(req, timeout=2, destination="ollama", urlopen=request.urlopen) as resp:
                resp.read(64)
        except EgressDeniedError:
            return False
        except (error.URLError, TimeoutError, OSError):
            return False
        return True

    def estimated_cost_per_1k_tokens(self) -> float:
        return 0.0

    def generate(self, prompt: str, max_tokens: int = 1024) -> str:
        from archive_engine.egress import guarded_urlopen

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
        with guarded_urlopen(req, timeout=120, destination="ollama", urlopen=request.urlopen) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return str(data.get("response", ""))
