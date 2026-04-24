"""
Retrieval orchestrator — the single entry point that wires together all 9
stages of the retrieval pipeline and returns a ranked top-K list of files.

Pipeline stages
---------------
  Stage 1  parse_temporal  — extract date bounds from query text (pure regex, <1ms)
  Stage 2  permission_clause — applied inside every DB-hitting stage via
                               build_base_query(); not an explicit function call here
  Stage 3  date_overlap     — same as above, baked into build_base_query()
  Stage 4  bm25_search      — tsvector keyword search (GIN index)
  Stage 5  fuzzy_search     — pg_trgm trigram similarity (GIN index)
  Stage 6  vector_search    — HNSW cosine similarity (pgvector)
           (stages 4-6 run sequentially on the shared AsyncSession — SQLAlchemy
            async sessions do not support concurrent operations on one connection)
  Stage 7  rrf_fuse         — Reciprocal Rank Fusion across all three rank lists
  Stage 8  top-K            — return top_k FileMetadata rows (default 20)

Public API
----------
    async def retrieve(
        query: str,
        user_id: str,
        is_admin: bool,
        db: AsyncSession,
        top_k: int = 20,
    ) -> list[FileMetadata]
        Returns up to top_k FileMetadata rows, ranked by RRF score.
        Never raises — returns [] on any error.

    async def retrieve_with_scores(
        query: str,
        user_id: str,
        is_admin: bool,
        db: AsyncSession,
        top_k: int = 20,
    ) -> list[tuple[FileMetadata, float]]
        Same as retrieve() but also returns the RRF scores for debugging /
        AI Pipeline tab rendering.
"""
from __future__ import annotations

from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import chat_logger
from app.models.file_metadata import FileMetadata
from app.retrieval.bm25 import bm25_search
from app.retrieval.embeddings_search import vector_search
from app.retrieval.fuzzy import fuzzy_search
from app.retrieval.rrf import rrf_fuse
from app.retrieval.temporal import parse_temporal

# How many results to pull from each individual stage before fusion.
# More results per stage → better RRF coverage, but more DB work.
_STAGE_LIMIT = 50


async def retrieve_with_scores(
    query: str,
    user_id: str,
    is_admin: bool,
    db: AsyncSession,
    top_k: int = 20,
) -> list[tuple[FileMetadata, float]]:
    """
    Full 9-stage retrieval pipeline.

    Returns list of (FileMetadata, rrf_score) sorted descending.
    Returns [] if no results or on error.
    """
    if not query or not query.strip():
        return []

    # ── Load user's domain restrictions (PHASE 15) ───────────────────────────
    # Admin users are unrestricted. Regular users may have allowed_domains set.
    allowed_domains: list[str] | None = None
    if not is_admin and user_id:
        from app.models.user import User as _User
        user_row = await db.get(_User, user_id)
        if user_row and user_row.allowed_domains:
            allowed_domains = list(user_row.allowed_domains)

    # ── Stage 1: temporal parsing ─────────────────────────────────────────────
    date_from: date | None
    date_to: date | None
    date_from, date_to = parse_temporal(query)

    # ── Stages 4-6: sequential retrieval ─────────────────────────────────────
    # SQLAlchemy async sessions share one connection and do not support
    # concurrent operations. Run BM25, fuzzy, vector sequentially.
    try:
        bm25_results = await bm25_search(
            query, user_id, is_admin, db,
            date_from=date_from, date_to=date_to,
            limit=_STAGE_LIMIT,
            allowed_domains=allowed_domains,
        )
        fuzzy_results = await fuzzy_search(
            query, user_id, is_admin, db,
            date_from=date_from, date_to=date_to,
            limit=_STAGE_LIMIT,
            allowed_domains=allowed_domains,
        )
        vector_results = await vector_search(
            query, user_id, is_admin, db,
            date_from=date_from, date_to=date_to,
            limit=_STAGE_LIMIT,
            allowed_domains=allowed_domains,
        )
    except Exception as exc:
        chat_logger.warning("retrieval_parallel_error", error=str(exc)[:200])
        return []

    # ── Stage 7: RRF fusion ───────────────────────────────────────────────────
    fused = rrf_fuse(
        [bm25_results, fuzzy_results, vector_results],
        top_k=top_k,
    )

    chat_logger.info(
        "retrieval_complete",
        query_preview=query[:80],
        bm25=len(bm25_results),
        fuzzy=len(fuzzy_results),
        vector=len(vector_results),
        fused=len(fused),
        date_from=str(date_from) if date_from else None,
        date_to=str(date_to) if date_to else None,
    )

    return fused


async def retrieve(
    query: str,
    user_id: str,
    is_admin: bool,
    db: AsyncSession,
    top_k: int = 20,
) -> list[FileMetadata]:
    """
    Full 9-stage retrieval pipeline — returns FileMetadata rows only.

    Convenience wrapper over retrieve_with_scores() that strips the scores.
    Use retrieve_with_scores() when you need scores for the AI Pipeline tab.
    """
    results = await retrieve_with_scores(query, user_id, is_admin, db, top_k=top_k)
    return [meta for meta, _ in results]
