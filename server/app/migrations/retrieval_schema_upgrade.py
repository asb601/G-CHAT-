"""
Retrieval-engine schema upgrade.

Adds pgvector + pg_trgm extensions and the columns/indexes required by the
multi-strategy retrieval engine (PHASE 1 of the retrieval rewrite).

Idempotent — safe to run multiple times. Invoked automatically from
`app.main:lifespan` on every startup.

Run standalone:
    python -m app.migrations.retrieval_schema_upgrade
"""
from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.database import engine


_STATEMENTS: list[str] = [
    # Extensions
    "CREATE EXTENSION IF NOT EXISTS vector",
    "CREATE EXTENSION IF NOT EXISTS pg_trgm",

    # Columns
    "ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS description_embedding vector(1536)",
    "ALTER TABLE file_metadata ADD COLUMN IF NOT EXISTS search_text TEXT",

    # Generated tsvector column. Postgres rejects duplicate ADD COLUMN with
    # GENERATED clauses, so guard with information_schema lookup via DO block.
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'file_metadata' AND column_name = 'search_tsv'
        ) THEN
            ALTER TABLE file_metadata
            ADD COLUMN search_tsv tsvector
            GENERATED ALWAYS AS (to_tsvector('english', coalesce(search_text, ''))) STORED;
        END IF;
    END$$
    """,

    # Indexes
    "CREATE INDEX IF NOT EXISTS idx_file_metadata_tsv "
    "ON file_metadata USING GIN (search_tsv)",

    "CREATE INDEX IF NOT EXISTS idx_file_metadata_search_text_trgm "
    "ON file_metadata USING GIN (search_text gin_trgm_ops)",

    "CREATE INDEX IF NOT EXISTS idx_file_metadata_embedding_hnsw "
    "ON file_metadata USING hnsw (description_embedding vector_cosine_ops)",
]


async def _run(conn: AsyncConnection) -> None:
    for stmt in _STATEMENTS:
        await conn.execute(text(stmt))


async def migrate() -> None:
    async with engine.begin() as conn:
        await _run(conn)
    print("✓ Retrieval schema migration complete.")


if __name__ == "__main__":
    asyncio.run(migrate())
