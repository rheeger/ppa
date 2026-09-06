"""Exact cosine nearest-neighbor oracle, independent of native ANN.

The serving IVF path is still a known defect (modulo-IVF). This module is the
independent ranking reference: brute-force cosine over the supplied vectors,
implemented in Python with no ``archive_crate`` / serving-index imports.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

Vector = Sequence[float]


@dataclass(frozen=True)
class Neighbor:
    """One exact cosine neighbor."""

    item_id: str
    score: float
    rank: int


def _as_floats(values: Vector) -> list[float]:
    return [float(value) for value in values]


def cosine_similarity(left: Vector, right: Vector) -> float:
    """Exact cosine similarity in ``[-1, 1]``. Empty or zero vectors score 0.0."""

    a = _as_floats(left)
    b = _as_floats(right)
    if not a or not b:
        return 0.0
    if len(a) != len(b):
        raise ValueError(f"vector dimension mismatch: {len(a)} != {len(b)}")
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def exact_nearest_neighbors(
    query: Vector,
    corpus: Sequence[tuple[str, Vector]],
    *,
    k: int = 10,
) -> list[Neighbor]:
    """Return the top-``k`` corpus items by exact cosine similarity.

    Ties break by ``item_id`` ascending so results are deterministic. This
    scans every supplied vector on purpose — it is the oracle, not ANN.
    """

    if k < 1:
        raise ValueError("k must be >= 1")
    scored: list[tuple[float, str]] = []
    for item_id, vector in corpus:
        scored.append((cosine_similarity(query, vector), str(item_id)))
    scored.sort(key=lambda row: (-row[0], row[1]))
    return [Neighbor(item_id=item_id, score=score, rank=index + 1) for index, (score, item_id) in enumerate(scored[:k])]
