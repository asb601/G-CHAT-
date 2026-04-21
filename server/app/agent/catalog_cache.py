"""
Catalog cache — loads file metadata from Postgres with 5-minute in-memory TTL.
"""
from __future__ import annotations

import threading
import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import chat_logger
from app.models.container import ContainerConfig
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship

_CATALOG_TTL = 300  # seconds

_catalog_cache: dict | None = None
_catalog_cache_time: float = 0.0
_catalog_lock = threading.Lock()


def invalidate_catalog_cache() -> None:
    """Clear the in-memory catalog cache. Call after file ingestion completes."""
    global _catalog_cache, _catalog_cache_time
    with _catalog_lock:
        _catalog_cache = None
        _catalog_cache_time = 0.0
    chat_logger.info("catalog_cache_invalidated")


async def load_catalog(db: AsyncSession) -> dict | None:
    """
    Load catalog data from Postgres, with 5-minute in-memory caching.

    Returns dict with keys: catalog, relationships, connection_string,
    container_name, parquet_blob_path, parquet_paths_all, sample_rows.
    Returns None if no files exist.
    """
    global _catalog_cache, _catalog_cache_time

    with _catalog_lock:
        if _catalog_cache is not None and (time.time() - _catalog_cache_time) < _CATALOG_TTL:
            return _catalog_cache

    # Cache miss — load from DB
    all_meta = list((await db.execute(select(FileMetadata))).scalars().all())
    if not all_meta:
        return None

    catalog = [
        {
            "file_id": m.file_id,
            "blob_path": m.blob_path,
            "container_id": m.container_id,
            "ai_description": m.ai_description or "",
            "good_for": m.good_for or [],
            "key_metrics": m.key_metrics or [],
            "key_dimensions": m.key_dimensions or [],
            "columns_info": m.columns_info or [],
            "date_range_start": str(m.date_range_start) if m.date_range_start else None,
            "date_range_end": str(m.date_range_end) if m.date_range_end else None,
        }
        for m in all_meta
    ]

    all_rels = list((await db.execute(select(FileRelationship))).scalars().all())
    relationships = [
        {
            "file_a_path": r.file_a_path,
            "file_b_path": r.file_b_path,
            "shared_column": r.shared_column,
            "confidence_score": r.confidence_score,
            "join_type": r.join_type,
        }
        for r in all_rels
    ]

    first_meta = all_meta[0]
    container = await db.get(ContainerConfig, first_meta.container_id)
    if not container:
        return None

    all_analytics_rows = list((await db.execute(select(FileAnalytics))).scalars().all())
    analytics_by_file = {row.file_id: row for row in all_analytics_rows}

    parquet_blob_path = None
    parquet_paths_all: dict[str, str] = {}
    for meta in all_meta:
        ar = analytics_by_file.get(meta.file_id)
        if not ar:
            continue
        if parquet_blob_path is None:
            parquet_blob_path = ar.parquet_blob_path
        if ar.parquet_blob_path and meta.blob_path:
            parquet_paths_all[meta.blob_path] = ar.parquet_blob_path

    sample_rows = first_meta.sample_rows or []

    result = {
        "catalog": catalog,
        "relationships": relationships,
        "connection_string": container.connection_string,
        "container_name": container.container_name,
        "parquet_blob_path": parquet_blob_path,
        "parquet_paths_all": parquet_paths_all,
        "sample_rows": sample_rows,
    }

    with _catalog_lock:
        _catalog_cache = result
        _catalog_cache_time = time.time()

    chat_logger.info("catalog_cache_loaded", file_count=len(catalog))
    return result
