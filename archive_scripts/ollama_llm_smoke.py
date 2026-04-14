#!/usr/bin/env python3
"""Phase 2.75 Step 1b — verify Ollama + OllamaProvider.chat_json against a live daemon.

Run from repo root: ``.venv/bin/python scripts/ollama_llm_smoke.py``

Environment:
  OLLAMA_SMOKE_MODEL   default ``gemma4:31b``
  OLLAMA_SMOKE_BASE    default ``http://localhost:11434``
  OLLAMA_SMOKE_STRICT  if ``1``, exit 1 when Ollama is down; otherwise exit 0 with SKIP
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from archive_vault.llm_provider import OllamaProvider  # noqa: E402


def main() -> int:
    model = os.environ.get("OLLAMA_SMOKE_MODEL", "gemma4:31b")
    base = os.environ.get("OLLAMA_SMOKE_BASE", "http://localhost:11434")
    strict = os.environ.get("OLLAMA_SMOKE_STRICT", "").strip() in {"1", "true", "yes"}

    p = OllamaProvider(model=model, base_url=base)
    if not p.health_check():
        msg = f"Ollama health_check failed (is `ollama serve` running? model {model!r} in `ollama list`?)"
        print(msg, file=sys.stderr)
        if not strict:
            print("SKIP (set OLLAMA_SMOKE_STRICT=1 to exit 1 when Ollama is down)")
        return 1 if strict else 0

    r = p.chat_json(
        [{"role": "user", "content": 'Return JSON only: {"status": "ok"}'}],
        model=model,
        temperature=0.0,
        seed=42,
        max_tokens=128,
    )
    if not r.parsed_json or r.parsed_json.get("status") != "ok":
        print(f"chat_json did not return {{\"status\": \"ok\"}}: {r.content!r}", file=sys.stderr)
        return 1

    tok_s = 0.0
    if r.latency_ms > 0 and r.completion_tokens > 0:
        tok_s = r.completion_tokens / (r.latency_ms / 1000.0)
    print(f"ok model={model} latency_ms={r.latency_ms:.1f} tok_s~={tok_s:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
