"""
Reciprocal Rank Fusion (RRF) — combines multiple ranked result lists into
one final ranking without requiring score normalisation.

Background
----------
RRF (Cormack, Clarke & Buettcher, SIGIR 2009) works purely on rank positions,
not raw scores, so it can safely merge BM25 tsvector scores, fuzzy similarity
scores (0-1), cosine similarity scores (0-1), and graph confidence scores (0-1)
— all of which have incompatible scales.

Formula
-------
    RRF(d) = Σ_r   1 / (k + rank_r(d))

where:
  - k = 60  (standard constant — dampens the effect of top-ranked documents
              so middle-ranked docs still contribute meaningfully)
  - rank_r(d) = 1-based rank of document d in list r
  - sum is over every list r in which d appears

A document that appears in ALL four lists (BM25 + fuzzy + vector + graph)
at position 1 each time scores 4 × 1/61 ≈ 0.066.
A document appearing in only one list at rank 50 scores 1/110 ≈ 0.009.

The fusion naturally promotes documents that multiple retrieval methods
agree on, without requiring any score normalisation.

Public API
----------
    def rrf_fuse(
        rank_lists: list[list[tuple[FileMetadata, float]]],
        k: int = 60,
        top_k: int = 20,
    ) -> list[tuple[FileMetadata, float]]
        Returns fused list of (FileMetadata, rrf_score) sorted descending.
        Length ≤ top_k.
"""
from __future__ import annotations

from app.models.file_metadata import FileMetadata

_DEFAULT_K = 60


def rrf_fuse(
    rank_lists: list[list[tuple[FileMetadata, float]]],
    k: int = _DEFAULT_K,
    top_k: int = 20,
) -> list[tuple[FileMetadata, float]]:
    """
    Merge ranked result lists using Reciprocal Rank Fusion.

    Parameters
    ----------
    rank_lists : list of ranked lists, each list is [(FileMetadata, score)]
                 sorted descending by score. Score values are ignored —
                 only rank positions are used.
    k          : RRF constant (default 60). Higher k = less difference between
                 top and bottom ranks in each list.
    top_k      : maximum number of results to return.

    Returns
    -------
    list of (FileMetadata, rrf_score) sorted descending by rrf_score.
    """
    # Accumulate RRF scores keyed by file_id
    rrf_scores: dict[str, float] = {}
    # Keep the FileMetadata object for each file_id (any copy is fine — ORM rows
    # for the same file_id are identical across lists)
    docs: dict[str, FileMetadata] = {}

    for rank_list in rank_lists:
        for rank, (meta, _score) in enumerate(rank_list, start=1):
            fid = meta.file_id
            rrf_scores[fid] = rrf_scores.get(fid, 0.0) + 1.0 / (k + rank)
            docs[fid] = meta

    # Sort file_ids by accumulated RRF score descending
    sorted_ids = sorted(rrf_scores, key=lambda fid: rrf_scores[fid], reverse=True)

    return [(docs[fid], rrf_scores[fid]) for fid in sorted_ids[:top_k]]
