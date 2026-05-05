"""
Lazy hydration of full FileMetadata for a small set of file IDs.

The catalog cache (catalog_cache.py) deliberately stores only LEAN per-file
records — enough to rank and discover files but not the heavy fields
(columns_info samples, sample_rows, column_stats). Those heavy fields are
only needed for the small shortlist of files that actually go into the
system prompt or tool responses.

This module fetches the heavy fields for an explicit list of file_ids
(typically 5–30 IDs per query). At ~10 KB per record this is ~100–300 KB
of extra data per query — bounded regardless of whether the workspace has
1 000 or 1 000 000 files.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata


async def hydrate_files(
    db: AsyncSession,
    file_ids: list[str],
) -> dict[str, dict]:
    """Return {file_id -> full_record} for the given IDs.

    Each value carries the heavy fields that the lean cache omits:
      - columns_info:  list of column dicts (name, type, sample_values, uniques)
      - sample_rows:   list of example rows (capped at ingest time)
      - column_stats:  per-column min/max/dtype from FileAnalytics

    Files not found in the database are silently absent from the result.
    """
    if not file_ids:
        return {}

    meta_rows = list(
        (
            await db.execute(
                select(FileMetadata).where(FileMetadata.file_id.in_(file_ids))
            )
        )
        .scalars()
        .all()
    )
    analytics_rows = list(
        (
            await db.execute(
                select(FileAnalytics).where(FileAnalytics.file_id.in_(file_ids))
            )
        )
        .scalars()
        .all()
    )
    stats_by_file = {row.file_id: (row.column_stats or {}) for row in analytics_rows}

    return {
        m.file_id: {
            "columns_info": m.columns_info or [],
            "sample_rows": m.sample_rows or [],
            "column_stats": stats_by_file.get(m.file_id, {}),
        }
        for m in meta_rows
    }


def merge_hydrated(lean_entry: dict, heavy: dict | None) -> dict:
    """Return a new dict combining a lean catalog entry with its heavy fields.

    If ``heavy`` is None, the lean entry is returned unchanged (callers can
    still operate on the lean fields — only schema-level tools need heavy).
    """
    if not heavy:
        return dict(lean_entry)
    out = dict(lean_entry)
    out["columns_info"] = heavy.get("columns_info", [])
    out["sample_rows"] = heavy.get("sample_rows", [])
    out["column_stats"] = heavy.get("column_stats", {})
    return out
