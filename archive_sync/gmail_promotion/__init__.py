"""v2.5 Section C — classify-before-promotion for future Gmail sync."""

from .gate import GmailPromotionGate, PromotionOutcome, promotion_gate_enabled
from .ledger import FilePromotionLedger, PromotionLedger
from .metrics import GmailPromotionBatchMetrics

__all__ = [
    "FilePromotionLedger",
    "GmailPromotionBatchMetrics",
    "GmailPromotionGate",
    "PromotionLedger",
    "PromotionOutcome",
    "promotion_gate_enabled",
]
