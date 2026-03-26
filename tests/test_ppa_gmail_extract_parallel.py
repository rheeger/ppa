from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "scripts" / "ppa-gmail-extract-parallel.py"
    spec = importlib.util.spec_from_file_location("ppa_gmail_extract_parallel", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_window_retryable_matches_quota_and_failed_precondition():
    module = _load_module()
    assert (
        module._window_retryable('{"error":{"code":403,"reason":"rateLimitExceeded","message":"Quota exceeded"}}')
        is True
    )
    assert (
        module._window_retryable(
            '{"error":{"code":400,"reason":"failedPrecondition","message":"Precondition check failed."}}'
        )
        is True
    )
    assert module._window_retryable("some unrelated failure") is False
