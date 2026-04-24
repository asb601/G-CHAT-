"""
Retrieval stage 6 — Vector / semantic search.

Uses pgvector's HNSW index on `description_embedding` (1536-dim,
text-embedding-3-small) with cosine distance.

Why semantic search on top of BM25 + fuzzy?
- BM25 requires lexical overlap — "show me headcount" misses files
  described as "employee roster by department."
- Fuzzy requires trigram overlap — still needs some shared characters.
- Semantic search finds conceptual matches regardless of exact wording.
  "headcount" and "employee roster" are close in embedding space.

Strategy
---------
1. Embed the user query via embed_text() (same model used at ingestion).
2. Find the closest description_embedding vectors by cosine distance.
3. Apply permission + date filters from build_base_query().
4. Filter out rows with NULL embeddings (not yet backfilled).
5. Convert distance to similarity score: similarity = 1 - cosine_distance.
   Score of 1.0 = identical vectors, 0.0 = orthogonal.
6. Apply minimum similarity threshold (default 0.3) to drop noise.

Graceful degradation
---------------------
- If embed_text() returns a zero vector (Azure endpoint down / missing
  deployment), the function returns [] immediately — vector search is
  silently skipped. BM25 + fuzzy results still flow through to RRF.

Public API
----------
    async def vector_search(
        query: str,
        user_id: str,
        is_admin: bool,
        db: AsyncSession,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 50,
        threshold: float = 0.3,
    ) -> list[tuple[FileMetadata, float]]
        Returns list of (FileMetadata, similarity_score) sorted descending.
        similarity_score in [0.0, 1.0].
"""
from __future__ import annotations

from datetime import date

from pgvector.sqlalchemy import Vector
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.file_metadata import FileMetadata
from app.retrieval.embeddings import embed_text
from app.retrieval.filters import build_base_query

_DEFAULT_THRESHOLD = 0.3
_DIMS = 1536


def _is_zero_vector(vec: list[float]) -> bool:
    """Return True if embed_text returned a zero vector (degraded / offline)."""
    return all(v == 0.0 for v in vec)


async def vector_search(
    query: str,
    user_id: str,
    is_admin: bool,
    db: AsyncSession,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = 50,
    threshold: float = _DEFAULT_THRESHOLD,
    allowed_domains: list[str] | None = None,
) -> list[tuple[FileMetadata, float]]:
    """
    Run HNSW cosine-similarity search against file_metadata.description_embedding.

    Returns
    -------
    list of (FileMetadata row, similarity_score float) sorted descending.
    Empty list if query is blank, embedding fails, or no results pass threshold.
    """
    if not query or not query.strip():
        return []

    # Embed the query using the same model used at ingestion time
    query_vec = await embed_text(query.strip())

    # Graceful degradation — Azure endpoint down or deployment missing
    if _is_zero_vector(query_vec):
        return []

    # Use pgvector's cosine_distance() — handles Python list → vector binding correctly.
    # Returns distance in [0.0, 2.0] for cosine; similarity = 1 - distance.
    distance_expr = FileMetadata.description_embedding.cosine_distance(query_vec)
    similarity_expr = (1.0 - distance_expr).label("vector_score")

    base_q = build_base_query(
        user_id=user_id,
        is_admin=is_admin,
        date_from=date_from,
        date_to=date_to,
        allowed_domains=allowed_domains,
    )

    q = (
        base_q
        .add_columns(similarity_expr)
        # Skip rows not yet embedded
        .where(FileMetadata.description_embedding.is_not(None))
        # Threshold filter
        .where((1.0 - distance_expr) >= threshold)
        .order_by(distance_expr.asc())   # HNSW scans in ascending distance order
        .limit(limit)
    )

    rows = (await db.execute(q)).all()

    return [(row[0], float(row[1])) for row in rows]
