"""v2.5 corpus hygiene — email cleanup dry-run and (future) apply."""

from __future__ import annotations

from .constants import SECTION_B_COMPLETION_STATE, SECTION_B_DRY_RUN_GATE
from .decisions import EmailCorpusDecisionRecord

__all__ = [
    "EmailCorpusDecisionRecord",
    "SECTION_B_COMPLETION_STATE",
    "SECTION_B_DRY_RUN_GATE",
]
