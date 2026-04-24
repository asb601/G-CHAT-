"""
Retrieval stage 5 — Fuzzy / trigram similarity search.

Uses PostgreSQL's pg_trgm extension against the `search_text` column
(backed by a GIN gin_trgm_ops index — created in PHASE 1).

Why trigram fuzzy on top of BM25?
- BM25 catches exact and stemmed keyword matches.
- Trigram fuzzy catches typos ("salaery" → "salary"), abbreviations
  ("inv" matches "invoice"), partial names ("Q1 rev" matches
  "Q1 revenue forecast"), and camelCase / concatenated words that
  BM25 tokenises differently.
- They produce different result sets — RRF fusion (PHASE 11) combines both.

Strategy
---------
Two complementary searches are run and merged:

  1. word_similarity — query phrase vs each word in search_text.
     Best for short queries against long descriptions.
     PostgreSQL function: word_similarity(query, search_text)
     Index op: gin_trgm_ops supports this.

  2. strict_word_similarity — tighter; query must match a whole word
     boundary. Less noise, higher precision.
     PostgreSQL function: strict_word_similarity(query, search_text)

We use word_similarity as the primary score because it has better recall
for partial abbreviations and typos. A minimum threshold of 0.2 is applied
to drop unrelated rows before they reach RRF fusion.

Public API
----------
    async def fuzzy_search(
        query: str,
        user_id: str,
        is_admin: bool,
        db: AsyncSession,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 50,
        threshold: float = 0.2,
    ) -> list[tuple[FileMetadata, float]]
        Returns list of (FileMetadata, similarity_score) sorted descending.
        similarity_score is in [0.0, 1.0] — higher = more similar.
"""
from __future__ import annotations

from datetime import date

from sqlalchemy import func, Float
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import literal_column

from app.models.file_metadata import FileMetadata
from app.retrieval.filters import build_base_query

# Minimum similarity score to include a row in results.
# 0.2 keeps abbreviations and 1-char typos while dropping noise.
_DEFAULT_THRESHOLD = 0.2


async def fuzzy_search(
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
    Run trigram (pg_trgm) fuzzy similarity search against file_metadata.search_text.

    Returns
    -------
    list of (FileMetadata row, similarity_score float) sorted descending.
    Empty list if query is blank or no results pass the threshold.
    """
    if not query or not query.strip():
        return []

    query = query.strip()

    # search_text is a plain TEXT column — reference it as a literal column.
    search_text_col = literal_column("file_metadata.search_text")

    # word_similarity(needle, haystack) — pg_trgm function.
    # Returns similarity of the query phrase against the best matching
    # substring (word boundary aware) in search_text.
    similarity_expr = func.word_similarity(query, search_text_col).label("fuzzy_score")

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
        # pg_trgm's <word_similarity threshold operator — uses the GIN index
        .where(literal_column("file_metadata.search_text").op("%>>")(query))
        # Also apply our explicit threshold to be sure
        .where(similarity_expr >= threshold)
        .order_by(similarity_expr.desc())
        .limit(limit)
    )

    rows = (await db.execute(q)).all()

    return [(row[0], float(row[1])) for row in rows]
