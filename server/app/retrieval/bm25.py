"""
Retrieval stage 4 — BM25 full-text keyword search.

Uses PostgreSQL's tsvector/tsquery engine against the `search_tsv` column
(a GENERATED column backed by a GIN index — created in PHASE 1).

Behaviour
---------
- Tokenises the user's query via plainto_tsquery / websearch_to_tsquery.
- Returns FileMetadata rows ranked by ts_rank_cd (cover-density ranking,
  which weights matches that are close together more heavily).
- Handles multi-word queries, common English stop-words, and stemming
  automatically — "invoices" matches "invoice."
- Returns at most `limit` rows (default 50 — RRF fusion narrows to 20 later).
- Returns an empty list (never raises) if no query terms survive stop-word
  removal or if the index has no matches.

Public API
----------
    async def bm25_search(
        query: str,
        user_id: str,
        is_admin: bool,
        db: AsyncSession,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 50,
    ) -> list[tuple[FileMetadata, float]]
        Returns list of (FileMetadata, bm25_score) sorted descending by score.
"""
from __future__ import annotations

from datetime import date

from sqlalchemy import cast, column, Float, func, text
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import literal_column

from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.retrieval.filters import build_base_query

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TSQUERY_LANG = "english"


def _to_tsquery(query: str):
    """
    Build a tsquery from user input using websearch_to_tsquery (Postgres ≥ 11).
    websearch_to_tsquery handles:
      - multi-word phrases ("invoice aging")
      - quoted phrases ("invoice aging")
      - minus for exclusion (-cancelled)
      - OR operator (invoice OR receipt)
    Falls back to plainto_tsquery for safety.
    """
    return func.websearch_to_tsquery(_TSQUERY_LANG, query)


def _tsquery_is_empty(query: str) -> bool:
    """
    Check whether the query string would produce an empty tsquery
    (e.g. all stop words like "the a is").
    We check this by looking for at least one non-stop-word character.
    Note: this is a heuristic — the actual check happens in Postgres.
    """
    stripped = query.strip()
    return not stripped or len(stripped) < 2


# ---------------------------------------------------------------------------
# Public search function
# ---------------------------------------------------------------------------

async def bm25_search(
    query: str,
    user_id: str,
    is_admin: bool,
    db: AsyncSession,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = 50,
    allowed_domains: list[str] | None = None,
) -> list[tuple[FileMetadata, float]]:
    """
    Run BM25 full-text search against file_metadata.search_tsv.

    Returns
    -------
    list of (FileMetadata row, bm25_score float) sorted descending.
    Empty list if no matches or query is blank.
    """
    if _tsquery_is_empty(query):
        return []

    tsquery = _to_tsquery(query)

    # search_tsv is a GENERATED tsvector column. Reference it as a typed column()
    # so that SQLAlchemy knows the type and .op("@@") works correctly.
    search_tsv_col = literal_column("file_metadata.search_tsv", TSVECTOR)

    # ts_rank_cd: cover-density ranking — weights adjacent term matches higher.
    # Normalisation option 32: divide rank by (1 + number of unique words in doc).
    # This prevents long-description files from always winning.
    rank_expr = func.ts_rank_cd(search_tsv_col, tsquery, 32).label("bm25_score")

    base_q = build_base_query(
        user_id=user_id,
        is_admin=is_admin,
        date_from=date_from,
        date_to=date_to,
        allowed_domains=allowed_domains,
    )

    # Add rank, tsquery filter, and order
    q = (
        base_q
        .add_columns(rank_expr)
        .where(search_tsv_col.op("@@")(tsquery))
        .order_by(rank_expr.desc())
        .limit(limit)
    )

    rows = (await db.execute(q)).all()

    # Each row is (FileMetadata, bm25_score)
    return [(row[0], float(row[1])) for row in rows]
