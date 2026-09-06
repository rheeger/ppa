"""Exact cosine nearest-neighbor oracle, independent of native ANN.

The serving IVF path is still a known defect (modulo-IVF). This module is the
independent ranking reference: brute-force cosine over the supplied vectors,
implemented in Python with no ``archive_crate`` / serving-index imports.
"""

from __future__ import annotations

import hashlib
import math
import struct
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


def ivf_nlist(n: int) -> int:
    """Match the native list count: ``sqrt(n).clamp(1, 4096)`` as integer truncation."""

    if n < 1:
        return 1
    return max(1, min(int(math.sqrt(n)), 4096))


def ivf_nprobe(nlist: int) -> int:
    """Selective probe count: ``nlist.min(32).max(1)``."""

    return max(1, min(int(nlist), 32))


@dataclass(frozen=True)
class ModuloIvfResult:
    """Independent simulation of the broken modulo-IVF serving path."""

    neighbors: tuple[Neighbor, ...]
    nlist: int
    nprobe: int
    probed_lists: tuple[int, ...]
    missed_lists: tuple[int, ...]


def modulo_ivf_knn(
    query: Vector,
    corpus: Sequence[tuple[str, Vector]],
    *,
    k: int = 10,
    nprobe: int | None = None,
) -> ModuloIvfResult:
    """Simulate the broken modulo-IVF assignment and first-member probe.

    Assignment is ``i % nlist``. Lists are probed by cosine of their *first*
    member only, then at most 32 lists are scanned. This is an independent
    reconstruction of the old algorithm, not a wrapper around native ANN.
    """

    if k < 1:
        raise ValueError("k must be >= 1")
    n = len(corpus)
    nlist = ivf_nlist(n)
    probe_count = ivf_nprobe(nlist) if nprobe is None else max(1, int(nprobe))
    lists: list[list[int]] = [[] for _ in range(nlist)]
    for index in range(n):
        lists[index % nlist].append(index)
    list_scores: list[tuple[float, int]] = []
    for list_id, members in enumerate(lists):
        if not members:
            list_scores.append((0.0, list_id))
            continue
        list_scores.append((cosine_similarity(query, corpus[members[0]][1]), list_id))
    list_scores.sort(key=lambda row: (-row[0], row[1]))
    probed = tuple(list_id for _, list_id in list_scores[:probe_count])
    missed = tuple(list_id for _, list_id in list_scores[probe_count:])
    scored: list[tuple[float, str]] = []
    for list_id in probed:
        for index in lists[list_id]:
            item_id, vector = corpus[index]
            scored.append((cosine_similarity(query, vector), str(item_id)))
    scored.sort(key=lambda row: (-row[0], row[1]))
    neighbors = tuple(
        Neighbor(item_id=item_id, score=score, rank=rank + 1) for rank, (score, item_id) in enumerate(scored[:k])
    )
    return ModuloIvfResult(
        neighbors=neighbors,
        nlist=nlist,
        nprobe=probe_count,
        probed_lists=probed,
        missed_lists=missed,
    )


def recall_at_k(predicted: Sequence[str], relevant: Sequence[str], *, k: int) -> float:
    """Set recall of ``relevant`` IDs inside the first ``k`` predicted IDs."""

    if k < 1:
        raise ValueError("k must be >= 1")
    wanted = {str(item) for item in relevant}
    if not wanted:
        return 1.0
    hit = {str(item) for item in list(predicted)[:k]} & wanted
    return len(hit) / len(wanted)


@dataclass(frozen=True)
class AdversarialIvfDataset:
    """Selective-probe cohort where modulo-IVF misses the exact neighbor."""

    seed: int
    dimension: int
    nlist: int
    nprobe: int
    query: tuple[float, ...]
    items: tuple[tuple[str, tuple[float, ...]], ...]
    true_neighbor_id: str
    true_neighbor_index: int
    unprobed_list: int
    vector_sha256: str


def _ann_item_id(index: int) -> str:
    return f"ann-p04b-{index:04d}"


def build_adversarial_modulo_ivf_dataset(
    *,
    n: int = 1089,
    dimension: int = 8,
    seed: int = 20260906,
) -> AdversarialIvfDataset:
    """Build nlist>32 vectors so the old algorithm skips the true neighbor.

    Construction is closed-form (not copied from native ANN output):

    - ``n = 33**2 = 1089`` ⇒ ``nlist = 33``, ``nprobe = 32``.
    - List ``L`` is ``L, L+33, L+66, …``; first member of list ``L`` is index ``L``.
    - Query ≈ the second member of list 32 (index 65).
    - First member of list 32 is anti-aligned, so that list is the one not probed.
    """

    if n != 1089:
        raise ValueError("adversarial dataset is frozen at n=1089 so nlist is 33")
    if dimension < 2:
        raise ValueError("dimension must be >= 2")
    nlist = ivf_nlist(n)
    if nlist <= 32:
        raise ValueError(f"nlist must be > 32 for a selective probe; got {nlist}")
    nprobe = ivf_nprobe(nlist)
    unprobed_list = nlist - 1
    true_index = unprobed_list + nlist
    query = tuple([1.0] + [0.0] * (dimension - 1))
    items: list[tuple[str, tuple[float, ...]]] = []
    for index in range(n):
        vector = [0.0] * dimension
        if index == true_index:
            vector[0] = 1.0
        elif index == unprobed_list:
            vector[0] = -1.0
        elif index < unprobed_list:
            vector[0] = 0.5
            vector[1] = 0.5
        else:
            vector[2] = 1.0
            vector[3] = ((index * 17 + seed) % 11) * 0.01
        items.append((_ann_item_id(index), tuple(vector)))
    digest = hashlib.sha256()
    for item_id, vector in items:
        digest.update(item_id.encode("utf-8"))
        for value in vector:
            digest.update(struct.pack("<d", float(value)))
    return AdversarialIvfDataset(
        seed=int(seed),
        dimension=int(dimension),
        nlist=nlist,
        nprobe=nprobe,
        query=query,
        items=tuple(items),
        true_neighbor_id=_ann_item_id(true_index),
        true_neighbor_index=true_index,
        unprobed_list=unprobed_list,
        vector_sha256=digest.hexdigest(),
    )
