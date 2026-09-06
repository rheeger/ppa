"""Isolated product acceptance harness (P04-A)."""

from __future__ import annotations

__all__ = ["SUITE_IDS"]

SUITE_IDS = (
    "baseline",
    "release",
    *(f"p{index:02d}" for index in range(1, 11)),
)
