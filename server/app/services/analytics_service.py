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

import time
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import ingest_logger
from app.models.background_job import BackgroundJob
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.services.analytics_computer import compute_sample_analytics
from app.services.parquet_service import convert_csv_to_parquet


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


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

    meta_result = await db.execute(select(FileMetadata).where(FileMetadata.file_id == file_id))
    meta = meta_result.scalar_one_or_none()
    sample_rows = (meta.sample_rows or []) if meta else []
    row_count = meta.row_count if meta else 0

    computed = compute_sample_analytics(columns_info, sample_rows)

    result = await db.execute(select(FileAnalytics).where(FileAnalytics.file_id == file_id))
    analytics = result.scalar_one_or_none()
    if not analytics:
        analytics = FileAnalytics(id=str(uuid.uuid4()), file_id=file_id)
        db.add(analytics)

    analytics.blob_path = blob_path
    analytics.row_count = row_count
    analytics.column_count = len(columns_info)
    analytics.column_stats = computed["column_stats"]
    analytics.value_counts = computed["value_counts"]
    analytics.cross_tabs = computed["cross_tabs"]

    await db.commit()

    ingest_logger.info(
        "analytics_compute",
        status="done",
        blob_path=blob_path,
        row_count=row_count,
        numeric_cols=len(computed["numeric_cols"]),
        categorical_cols=len(computed["categorical_cols"]),
        cross_tabs=len(computed["cross_tabs"]),
        duration_ms=_ms(pipeline_start),
    )

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

    ingest_logger.info("parquet_conversion", status="started", blob_path=blob_path, job_id=job_id)

    try:
        result = await convert_csv_to_parquet(blob_path, connection_string, container_name, job_id=job_id)
        parquet_path = result["parquet_blob_path"]
        parquet_size = result["size_bytes"]
        total_rows = result.get("total_rows")

        async with _async_session() as db:
            analytics_row = (
                await db.execute(select(FileAnalytics).where(FileAnalytics.file_id == file_id))
            ).scalar_one_or_none()
            if analytics_row:
                analytics_row.parquet_blob_path = parquet_path
                analytics_row.parquet_size_bytes = parquet_size
                if total_rows:
                    analytics_row.row_count = total_rows
            else:
                # No FileAnalytics row exists — create a minimal one so the
                # parquet_blob_path is persisted and the file no longer shows
                # as "missing parquet" on the next check.
                new_analytics = FileAnalytics(
                    id=str(uuid.uuid4()),
                    file_id=file_id,
                    parquet_blob_path=parquet_path,
                    parquet_size_bytes=parquet_size,
                    row_count=total_rows or 0,
                )
                db.add(new_analytics)

            if total_rows:
                meta_row = (
                    await db.execute(select(FileMetadata).where(FileMetadata.file_id == file_id))
                ).scalar_one_or_none()
                if meta_row:
                    meta_row.row_count = total_rows

            job_row = await db.get(BackgroundJob, job_id)
            if job_row:
                job_row.status = "done"
                job_row.completed_at = datetime.now(timezone.utc)

            await db.commit()

        from app.agent.graph.graph import invalidate_catalog_cache

        invalidate_catalog_cache()

        ingest_logger.info(
            "parquet_conversion",
            status="done",
            blob_path=blob_path,
            parquet_path=parquet_path,
            size_bytes=parquet_size,
            total_rows=total_rows,
            job_id=job_id,
        )

    except Exception as exc:
        error_msg = str(exc)[:1000]

        try:
            async with _async_session() as db:
                job_row = await db.get(BackgroundJob, job_id)
                if job_row:
                    job_row.status = "failed"
                    job_row.error_message = error_msg
                    job_row.completed_at = datetime.now(timezone.utc)
                await db.commit()
        except Exception as inner:
            ingest_logger.error("parquet_conversion_job_update_failed", error=str(inner)[:200])

        ingest_logger.warning(
            "parquet_conversion",
            status="failed",
            blob_path=blob_path,
            error=error_msg[:300],
            job_id=job_id,
        )
