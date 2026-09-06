#!/usr/bin/env python3
"""CLI wrapper: ``python archive_scripts/build-release.py --output DIR``."""

from __future__ import annotations

import runpy
from pathlib import Path

if __name__ == "__main__":
    raise SystemExit(runpy.run_path(str(Path(__file__).with_name("build_release.py")), run_name="__main__") or 0)
