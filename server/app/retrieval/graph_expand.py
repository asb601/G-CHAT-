"""
Retrieval stage 7 — Graph expansion.

Takes a seed set of FileMetadata rows (from BM25 + fuzzy + vector stages)
and expands it by one hop through the file_relationships graph, returning
neighbouring files that share a column with any seed file.

Why graph expansion?
---------------------
The three retrieval stages (BM25, fuzzy, vector) score files by content
similarity to the query. But a question like "show me total payroll by
department" might hit `payroll.csv` by semantic search, while the answer
actually requires joining `payroll.csv` with `departments.csv`. Without
graph expansion, `departments.csv` never makes it into the prompt context
even though it is structurally necessary to answer the question.

Strategy
---------
1. Collect the file IDs of all seed rows.
2. Query file_relationships for any edge where file_a_id OR file_b_id is
   in the seed set, filtering by permission and a minimum confidence threshold.
3. For each edge, the "neighbour" is whichever side is NOT already in the seed.
4. Load FileMetadata for each new neighbour (one query with IN clause).
5. Score neighbours by confidence_score from the relationship edge.
6. Deduplicate: if a neighbour was already in the seed set, skip it.
7. Return neighbours sorted by confidence descending, capped at `limit`.

Permission enforcement
-----------------------
Graph expansion must not leak files the user cannot see. Before loading
neighbour metadata, apply the same permission_clause used in all other stages.
Specifically, the neighbour's file must also pass the permission check.

Public API
----------
    async def graph_expand(
        seed_file_ids: list[str],
        user_id: str,
        is_admin: bool,
        db: AsyncSession,
        min_confidence: float = 0.5,
        limit: int = 20,
    ) -> list[tuple[FileMetadata, float]]
        Returns list of (FileMetadata, confidence_score) for NEIGHBOURS ONLY.
        Does not re-return seed files. Sorted descending by confidence.
        Empty list if no qualifying neighbours found.
"""
from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship
from app.retrieval.filters import build_base_query, permission_clause

_DEFAULT_MIN_CONFIDENCE = 0.85


async def graph_expand(
    seed_file_ids: list[str],
    user_id: str,
    is_admin: bool,
    db: AsyncSession,
    min_confidence: float = _DEFAULT_MIN_CONFIDENCE,
    limit: int = 20,
    allowed_domains: list[str] | None = None,
) -> list[tuple[FileMetadata, float]]:
    """
    Expand a seed set of file IDs by one hop through the relationship graph.

    Returns
    -------
    list of (FileMetadata, confidence_score) for neighbours only — files that
    share a column with any seed file and pass permission checks.
    Empty list if seed is empty or no qualifying neighbours found.
    """
    if not seed_file_ids:
        return []

    seed_set = set(seed_file_ids)

    # ── Step 1: find all edges touching any seed node ─────────────────────────
    edge_q = (
        select(
            FileRelationship.file_a_id,
            FileRelationship.file_b_id,
            FileRelationship.confidence_score,
            FileRelationship.shared_column,
        )
        .where(
            or_(
                FileRelationship.file_a_id.in_(seed_file_ids),
                FileRelationship.file_b_id.in_(seed_file_ids),
            )
        )
        .where(FileRelationship.confidence_score >= min_confidence)
        .order_by(FileRelationship.confidence_score.desc())
    )

    edge_rows = (await db.execute(edge_q)).all()

    if not edge_rows:
        return []

    # ── Step 2: collect neighbour IDs (whichever side is not in seed) ─────────
    # Track best confidence score per neighbour (multiple edges can reach the same file)
    neighbour_score: dict[str, float] = {}

    for a_id, b_id, conf, _ in edge_rows:
        neighbour_id = b_id if a_id in seed_set else a_id
        if neighbour_id not in seed_set:
            if neighbour_id not in neighbour_score or conf > neighbour_score[neighbour_id]:
                neighbour_score[neighbour_id] = conf

    if not neighbour_score:
        return []

    neighbour_ids = list(neighbour_score.keys())

    # ── Step 3: load FileMetadata for neighbours, enforcing permissions + domain ──
    meta_q = (
        build_base_query(user_id=user_id, is_admin=is_admin, allowed_domains=allowed_domains)
        .where(FileMetadata.file_id.in_(neighbour_ids))
    )

    meta_rows = (await db.execute(meta_q)).scalars().all()

    # ── Step 4: pair with scores and sort ─────────────────────────────────────
    results = [
        (meta, neighbour_score[meta.file_id])
        for meta in meta_rows
    ]
    results.sort(key=lambda x: x[1], reverse=True)

    return results[:limit]
