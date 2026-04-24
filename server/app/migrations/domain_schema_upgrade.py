"""
Domain access-control schema upgrade (PHASE 15).

Adds two columns:
  - folders.domain_tag    TEXT          — single domain label for the folder
                                          (e.g. 'finance', 'hr', 'supply_chain')
  - users.allowed_domains TEXT[]        — domains a regular user may access;
                                          NULL / empty → unrestricted (same as admin)

Idempotent — safe to run multiple times. Invoked from `app.main:lifespan`.

Run standalone:
    python -m app.migrations.domain_schema_upgrade
"""
from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.database import engine

_STATEMENTS: list[str] = [
    # folders.domain_tag — one tag per folder, NULL means no domain restriction
    "ALTER TABLE folders ADD COLUMN IF NOT EXISTS domain_tag TEXT",

    # users.allowed_domains — PostgreSQL native text array; NULL = unrestricted
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS allowed_domains TEXT[]",

    # Index for quickly finding all folders tagged with a given domain
    "CREATE INDEX IF NOT EXISTS idx_folders_domain_tag ON folders (domain_tag) WHERE domain_tag IS NOT NULL",

    # GIN index for array containment queries on users.allowed_domains
    "CREATE INDEX IF NOT EXISTS idx_users_allowed_domains ON users USING GIN (allowed_domains) WHERE allowed_domains IS NOT NULL",
]


async def _run(conn: AsyncConnection) -> None:
    for stmt in _STATEMENTS:
        await conn.execute(text(stmt))


async def migrate() -> None:
    async with engine.begin() as conn:
        await _run(conn)


if __name__ == "__main__":
    asyncio.run(migrate())
    print("domain_schema_upgrade: done")
