"""
Catalog cache — loads LEAN file metadata from Postgres with 5-minute TTL.

Design notes
------------
The cache stores ONE small record per file (~1 KB). Heavy fields
(columns_info samples, sample_rows, column_stats) are NOT cached —
they are lazy-loaded per request for the small shortlist of files
that actually go into the prompt or tool responses (see
catalog_hydration.hydrate_files).

Memory footprint:
    100 K files  ->  ~100 MB
    1   M files  ->  ~1 GB     (still tolerable; for true >1 M scale
                                replace this in-memory index with a
                                Postgres FTS query — the load_catalog
                                / hydrate_files split keeps that swap
                                local to one file)

Every per-request lookup ALWAYS hydrates only the K shortlisted files,
so the request-time memory cost is bounded regardless of catalog size.
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

# Caps applied at cache-build time so a single oversized record can't
# inflate the lean footprint (e.g. an ai_description that is 50 KB).
_MAX_DESCRIPTION_CHARS = 600
_MAX_LIST_ITEMS = 12
_MAX_COLUMN_NAMES = 80

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


def _truncate(text: str | None, n: int) -> str:
    if not text:
        return ""
    text = str(text)
    return text[:n] if len(text) > n else text


def _cap_list(items: list | None, n: int) -> list:
    if not items:
        return []
    return list(items)[:n] if len(items) > n else list(items)


def _extract_column_names(columns_info: list | None) -> list[str]:
    """Return column names only (drops types / samples / uniques).

    The lean cache stores names so search_catalog scoring can match
    column tokens without holding the full per-column payload.
    """
    if not columns_info:
        return []
    names: list[str] = []
    for c in columns_info:
        if isinstance(c, dict) and c.get("name"):
            names.append(c["name"])
        elif isinstance(c, str):
            names.append(c)
        if len(names) >= _MAX_COLUMN_NAMES:
            break
    return names


async def load_catalog(
    db: AsyncSession,
    allowed_domains: list[str] | None = None,
) -> dict | None:
    """
    Load the lean catalog from Postgres, with 5-minute in-memory caching.

    allowed_domains: if set, catalog entries whose folder has a domain_tag NOT
    in the list are excluded from the returned catalog (and from
    parquet_paths_all). None / empty list = no filtering (admin or unset).

    Returns dict with keys:
      catalog              - lean per-file records (no heavy fields)
      connection_string    - Azure connection
      container_name       - Azure container
      parquet_blob_path    - first available parquet (legacy field)
      parquet_paths_all    - {blob_path -> parquet_path}
    Returns None if no files exist.

    Heavy fields (columns_info samples, sample_rows, column_stats) must be
    fetched separately via catalog_hydration.hydrate_files for the small
    set of file_ids that are actually relevant to the current query.
    """
    global _catalog_cache, _catalog_cache_time

    with _catalog_lock:
        if _catalog_cache is not None and (time.time() - _catalog_cache_time) < _CATALOG_TTL:
            cached = _catalog_cache
        else:
            cached = None

    if cached is None:
        cached = await _build_lean_cache(db)
        if cached is None:
            return None
        with _catalog_lock:
            _catalog_cache = cached
            _catalog_cache_time = time.time()
        chat_logger.info("catalog_cache_loaded", file_count=len(cached["catalog"]))

    # Apply per-request domain filter on top of the shared cache.
    if allowed_domains:
        visible_blobs = {
            e["blob_path"]
            for e in cached["catalog"]
            if e["domain_tag"] is None or e["domain_tag"] in allowed_domains
        }
        return {
            **cached,
            "catalog": [
                e for e in cached["catalog"] if e["blob_path"] in visible_blobs
            ],
            "parquet_paths_all": {
                k: v for k, v in cached["parquet_paths_all"].items() if k in visible_blobs
            },
        }

    return cached


async def _build_lean_cache(db: AsyncSession) -> dict | None:
    """One-shot DB read that populates the shared lean cache."""
    all_meta = list((await db.execute(select(FileMetadata))).scalars().all())
    if not all_meta:
        return None

    file_rows = list((await db.execute(select(File))).scalars().all())
    folder_ids = {f.folder_id for f in file_rows if f.folder_id}
    folder_rows = (
        list(
            (await db.execute(select(Folder).where(Folder.id.in_(folder_ids))))
            .scalars()
            .all()
        )
        if folder_ids
        else []
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
            "ai_description": _truncate(m.ai_description, _MAX_DESCRIPTION_CHARS),
            "good_for": _cap_list(m.good_for, _MAX_LIST_ITEMS),
            "key_metrics": _cap_list(m.key_metrics, _MAX_LIST_ITEMS),
            "key_dimensions": _cap_list(m.key_dimensions, _MAX_LIST_ITEMS),
            "column_names": _extract_column_names(m.columns_info),
            "date_range_start": str(m.date_range_start) if m.date_range_start else None,
            "date_range_end": str(m.date_range_end) if m.date_range_end else None,
        }
        for m in all_meta
    ]

    first_meta = all_meta[0]
    container = await db.get(ContainerConfig, first_meta.container_id)
    if not container:
        return None

    # parquet_paths is small ({blob -> parquet_path}); keep it cached.
    parquet_blob_path: str | None = None
    parquet_paths_all: dict[str, str] = {}
    analytics_rows = list((await db.execute(select(FileAnalytics))).scalars().all())
    parquet_by_file = {row.file_id: row.parquet_blob_path for row in analytics_rows}
    for meta in all_meta:
        pq = parquet_by_file.get(meta.file_id)
        if not pq:
            continue
        if parquet_blob_path is None:
            parquet_blob_path = pq
        if meta.blob_path:
            parquet_paths_all[meta.blob_path] = pq

    return {
        "catalog": catalog,
        "connection_string": container.connection_string,
        "container_name": container.container_name,
        "parquet_blob_path": parquet_blob_path,
        "parquet_paths_all": parquet_paths_all,
    }
