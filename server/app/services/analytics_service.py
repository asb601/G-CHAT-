"""
Analytics computation service — runs once at ingest time.

Strategy:
  - All stats (column_stats, value_counts, cross_tabs) are computed from the
    500-row sample already captured in Step 1. Zero DuckDB, zero timeouts.
  - Row count is estimated from file_metadata (sample gives a lower bound;
    we set a flag so the UI can show it as approximate).
  - Parquet conversion is triggered as a separate fire-and-forget background
    task and updates parquet_blob_path when done.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import ingest_logger
from app.models.background_job import BackgroundJob
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.services.parquet_service import convert_csv_to_parquet


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _json_safe_value(v: Any) -> Any:
    """Convert non-JSON-serializable types to strings."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _json_safe_rows(rows: list[dict]) -> list[dict]:
    return [{k: _json_safe_value(v) for k, v in row.items()} for row in rows]


# ── Core analytics (from 500-row sample — no DuckDB, instant) ────────────────

async def compute_and_store_analytics(
    file_id: str,
    blob_path: str,
    connection_string: str,
    container_name: str,
    columns_info: list[dict],
    db: AsyncSession,
) -> FileAnalytics:
    """
    Compute and persist analytics from the 500-row sample already captured in
    file_metadata.sample_rows. No DuckDB calls — completes in <1 second.

    Parquet conversion is NOT done here — call trigger_parquet_conversion()
    separately as a background task.
    """
    pipeline_start = time.perf_counter()
    ingest_logger.info("analytics_compute", status="started", blob_path=blob_path)

    # ── Load sample rows from file_metadata ──
    meta_result = await db.execute(
        select(FileMetadata).where(FileMetadata.file_id == file_id)
    )
    meta = meta_result.scalar_one_or_none()
    sample_rows = (meta.sample_rows or []) if meta else []

    # Use columns_info for row_count (from DuckDB 500-row sample)
    row_count = meta.row_count if meta else 0
    # row_count from sample_file is 500 (sample limit); we store it as-is
    # and flag it as approximate. DuckDB has the real count in metadata.

    df = pd.DataFrame(sample_rows) if sample_rows else pd.DataFrame()

    # ── Column classification ──
    numeric_cols = [c["name"] for c in columns_info if _is_numeric(c)]
    categorical_cols = [c["name"] for c in columns_info if _is_categorical(c)]

    # ── 1. Per-column stats (from sample) ──
    column_stats: dict[str, Any] = {}

    for col in numeric_cols:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        column_stats[col] = {
            "dtype": "numeric",
            "min": _round(series.min()),
            "max": _round(series.max()),
            "mean": _round(series.mean()),
            "sum": _round(series.sum()),
            "std": _round(series.std()),
            "nulls": int(df[col].isna().sum()),
            "note": "estimated from 500-row sample",
        }

    for col in categorical_cols:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        column_stats[col] = {
            "dtype": "categorical",
            "unique": int(series.nunique()),
            "nulls": int(df[col].isna().sum()),
            "note": "from 500-row sample",
        }

    # ── 2. Value counts (from sample) ──
    value_counts: dict[str, Any] = {}
    for col in categorical_cols[:10]:
        if col not in df.columns:
            continue
        vc = df[col].dropna().value_counts().head(20)
        if not vc.empty:
            value_counts[col] = {str(k): int(v) for k, v in vc.items()}
            value_counts[f"{col}__note"] = "from 500-row sample"

    # ── 3. Cross-tab summaries (from sample) ──
    cross_tabs: list[dict] = []
    for dim in categorical_cols[:3]:
        if dim not in df.columns:
            continue
        for metric in numeric_cols[:3]:
            if metric not in df.columns:
                continue
            try:
                num_series = pd.to_numeric(df[metric], errors="coerce")
                tmp = df[[dim]].copy()
                tmp[metric] = num_series
                tmp = tmp.dropna()
                if tmp.empty:
                    continue
                grouped = tmp.groupby(dim)[metric].agg(
                    total="sum", avg="mean", count="count"
                ).reset_index().sort_values("total", ascending=False).head(15)
                cross_tabs.append({
                    "group_by": dim,
                    "metric": metric,
                    "agg": "sum",
                    "data": _json_safe_rows(grouped.rename(columns={dim: "dimension"}).to_dict("records")),
                    "note": "from 500-row sample",
                })
            except Exception:
                pass

    # ── 4. Persist ──
    result = await db.execute(
        select(FileAnalytics).where(FileAnalytics.file_id == file_id)
    )
    analytics = result.scalar_one_or_none()
    if not analytics:
        analytics = FileAnalytics(id=str(uuid.uuid4()), file_id=file_id)
        db.add(analytics)

    analytics.blob_path = blob_path
    analytics.row_count = row_count
    analytics.column_count = len(columns_info)
    analytics.column_stats = column_stats
    analytics.value_counts = value_counts
    analytics.cross_tabs = cross_tabs
    # parquet_blob_path stays None until trigger_parquet_conversion() completes

    await db.commit()

    ingest_logger.info("analytics_compute", status="done",
                       blob_path=blob_path,
                       row_count=row_count,
                       numeric_cols=len(numeric_cols),
                       categorical_cols=len(categorical_cols),
                       cross_tabs=len(cross_tabs),
                       duration_ms=_ms(pipeline_start))

    return analytics


async def trigger_parquet_conversion(
    file_id: str,
    blob_path: str,
    connection_string: str,
    container_name: str,
) -> None:
    """
    Fire-and-forget Parquet conversion using PyArrow + Azure SDK.
    Creates a BackgroundJob record for status tracking.
    Updates file_analytics.parquet_blob_path when done.
    Runs in its own DB session — can take several minutes without blocking anything.
    """
    from datetime import datetime, timezone
    from app.core.database import async_session as _async_session

    job_id = str(uuid.uuid4())

    # ── Create job record: status = "running" ──
    async with _async_session() as db:
        job = BackgroundJob(
            id=job_id,
            file_id=file_id,
            job_type="parquet_conversion",
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        db.add(job)
        await db.commit()

    ingest_logger.info("parquet_conversion", status="started",
                       blob_path=blob_path, job_id=job_id)

    try:
        result = await convert_csv_to_parquet(blob_path, connection_string, container_name, job_id=job_id)
        parquet_path = result["parquet_blob_path"]
        parquet_size = result["size_bytes"]
        total_rows = result.get("total_rows")

        # ── Update analytics + mark job done ──
        async with _async_session() as db:
            analytics_row = (await db.execute(
                select(FileAnalytics).where(FileAnalytics.file_id == file_id)
            )).scalar_one_or_none()
            if analytics_row:
                analytics_row.parquet_blob_path = parquet_path
                analytics_row.parquet_size_bytes = parquet_size
                if total_rows:
                    analytics_row.row_count = total_rows

            # Also update FileMetadata.row_count so the agent sees the real count
            if total_rows:
                meta_row = (await db.execute(
                    select(FileMetadata).where(FileMetadata.file_id == file_id)
                )).scalar_one_or_none()
                if meta_row:
                    meta_row.row_count = total_rows

            job_row = await db.get(BackgroundJob, job_id)
            if job_row:
                job_row.status = "done"
                job_row.completed_at = datetime.now(timezone.utc)

            await db.commit()

        # Invalidate the agent's cached catalog so new files are visible immediately
        from app.agent.graph import invalidate_catalog_cache
        invalidate_catalog_cache()

        ingest_logger.info("parquet_conversion", status="done",
                           blob_path=blob_path, parquet_path=parquet_path,
                           size_bytes=parquet_size, total_rows=total_rows, job_id=job_id)

    except Exception as exc:
        error_msg = str(exc)[:1000]

        # ── Mark job failed with error message ──
        try:
            async with _async_session() as db:
                job_row = await db.get(BackgroundJob, job_id)
                if job_row:
                    job_row.status = "failed"
                    job_row.error_message = error_msg
                    job_row.completed_at = datetime.now(timezone.utc)
                await db.commit()
        except Exception as inner:
            ingest_logger.error("parquet_conversion_job_update_failed",
                                error=str(inner)[:200])

        ingest_logger.warning("parquet_conversion", status="failed",
                              blob_path=blob_path, error=error_msg[:300],
                              job_id=job_id)


# ── Column classification ─────────────────────────────────────────────────────

_NUMERIC_TYPES = {"int64", "float64", "int32", "float32", "double", "bigint",
                  "integer", "decimal", "numeric", "real"}

_SKIP_COLS = {"id", "uuid", "session_id", "ip_address", "email", "phone",
              "description", "name", "created_at", "updated_at"}


def _is_numeric(col: dict) -> bool:
    dtype = (col.get("type") or "").lower()
    name = col.get("name", "").lower()
    if name in _SKIP_COLS:
        return False
    return any(t in dtype for t in _NUMERIC_TYPES)


def _is_categorical(col: dict) -> bool:
    dtype = (col.get("type") or "").lower()
    name = col.get("name", "").lower()
    if name in _SKIP_COLS:
        return False
    return dtype in ("str", "object", "category") or any(
        t in dtype for t in ("varchar", "string", "char", "text")
    )


def _round(val: Any) -> Any:
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (ValueError, TypeError):
        return val

