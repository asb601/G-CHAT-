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
from app.models.file import File
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.folder import Folder

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


async def load_catalog(
    db: AsyncSession,
    allowed_domains: list[str] | None = None,
) -> dict | None:
    """
    Load catalog data from Postgres, with 5-minute in-memory caching.

    allowed_domains: if set, catalog entries whose folder has a domain_tag NOT
    in the list are excluded from the returned catalog (and from relationships
    and parquet_paths_all). None / empty list = no filtering (admin or unset).

    Returns dict with keys: catalog, connection_string,
    container_name, parquet_blob_path, parquet_paths_all, sample_rows_by_blob.
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

    # Build file_id → domain_tag map via File → Folder join
    file_rows = list((await db.execute(select(File))).scalars().all())
    folder_ids = {f.folder_id for f in file_rows if f.folder_id}
    folder_rows = list(
        (await db.execute(select(Folder).where(Folder.id.in_(folder_ids)))).scalars().all()
        if folder_ids else []
    )
    folder_domain: dict[str, str | None] = {fo.id: fo.domain_tag for fo in folder_rows}
    file_folder: dict[str, str | None] = {f.id: f.folder_id for f in file_rows}

    def _domain_tag(file_id: str) -> str | None:
        fid = file_folder.get(file_id)
        if not fid:
            return None
        return folder_domain.get(fid)

    catalog = [
        {
            "file_id": m.file_id,
            "blob_path": m.blob_path,
            "container_id": m.container_id,
            "domain_tag": _domain_tag(m.file_id),
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

    first_meta = all_meta[0]
    container = await db.get(ContainerConfig, first_meta.container_id)
    if not container:
        return None

    all_analytics_rows = list((await db.execute(select(FileAnalytics))).scalars().all())
    analytics_by_file = {row.file_id: row for row in all_analytics_rows}

    # Augment catalog entries with column_stats (min/max per numeric column) from analytics.
    # These are computed at ingest time and are needed so the LLM knows column value ranges
    # (e.g. PERIOD_YEAR min=2020.0 max=2023.0) without firing an extra probe query.
    for entry in catalog:
        ar = analytics_by_file.get(entry["file_id"])
        entry["column_stats"] = (ar.column_stats or {}) if ar else {}

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

    sample_rows_by_blob = {
        meta.blob_path: (meta.sample_rows or [])
        for meta in all_meta
        if meta.blob_path
    }

    result = {
        "catalog": catalog,
        "connection_string": container.connection_string,
        "container_name": container.container_name,
        "parquet_blob_path": parquet_blob_path,
        "parquet_paths_all": parquet_paths_all,
        "sample_rows_by_blob": sample_rows_by_blob,
    }

    with _catalog_lock:
        _catalog_cache = result
        _catalog_cache_time = time.time()

    chat_logger.info("catalog_cache_loaded", file_count=len(catalog))

    # Apply domain filter AFTER caching — cache always holds the full catalog.
    # Filtering is per-request so different users get different views from the same cache.
    if allowed_domains:
        visible_blobs = {
            e["blob_path"]
            for e in catalog
            if e["domain_tag"] is None or e["domain_tag"] in allowed_domains
        }
        filtered_catalog = [e for e in catalog if e["blob_path"] in visible_blobs]
        filtered_parquets = {k: v for k, v in parquet_paths_all.items() if k in visible_blobs}
        filtered_samples = {k: v for k, v in sample_rows_by_blob.items() if k in visible_blobs}
        return {
            **result,
            "catalog": filtered_catalog,
            "parquet_paths_all": filtered_parquets,
            "sample_rows_by_blob": filtered_samples,
        }

    return result
