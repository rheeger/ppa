"""Batch metrics for Gmail promotion runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class GmailPromotionBatchMetrics:
    observed: int = 0
    promoted: int = 0
    suppressed: int = 0
    quarantined: int = 0
    re_promoted: int = 0
    demotion_recommended: int = 0
    classification_failures: int = 0
    dirty_card_uids: list[str] = field(default_factory=list)

    def to_skip_details(self) -> dict[str, int]:
        return {
            "promotion_observed": self.observed,
            "promotion_promoted": self.promoted,
            "promotion_suppressed": self.suppressed,
            "promotion_quarantined": self.quarantined,
            "promotion_re_promoted": self.re_promoted,
            "promotion_demotion_recommended": self.demotion_recommended,
            "promotion_classification_failures": self.classification_failures,
        }

    def merge_skip_details(self, target: dict[str, int]) -> None:
        for key, value in self.to_skip_details().items():
            target[key] = target.get(key, 0) + int(value)

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.to_skip_details(),
            "dirty_card_uids_count": len(self.dirty_card_uids),
        }
