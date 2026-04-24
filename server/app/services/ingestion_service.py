import asyncio
import time
import uuid
from datetime import date

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ai_client import generate_file_description
from app.core.database import async_session as _async_session
from app.core.duckdb_client import sample_file
from app.core.logger import ingest_logger
from app.retrieval.embeddings import build_search_text, embed_text
from app.models.container import ContainerConfig
from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.services.analytics_service import compute_and_store_analytics, trigger_parquet_conversion


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


def _ensure_trace(file_id: str) -> None:
    """Bind a trace_id if one isn't already set (background tasks from sync/upload)."""
    ctx = structlog.contextvars.get_contextvars()
    if "trace_id" not in ctx:
        structlog.contextvars.bind_contextvars(
            trace_id=f"ingest-{uuid.uuid4().hex[:12]}",
            pipeline="ingest",
            file_id=file_id,
        )


async def ingest_file(file_id: str, db: AsyncSession) -> None:
    """
    Sample a file with DuckDB, generate AI description, detect relationships.
    Updates file.ingest_status throughout: pending → ingested | failed.
    """
    _ensure_trace(file_id)
    pipeline_start = time.perf_counter()

    try:
        file = await db.get(File, file_id)
        if not file or not file.blob_path:
            ingest_logger.warning("chain_skip", reason="file or blob_path missing")
            return

        container = await db.get(ContainerConfig, file.container_id)
        if not container:
            ingest_logger.warning("chain_skip", reason="container not found")
            return

        ingest_logger.info("chain_start", filename=file.name, blob_path=file.blob_path,
                           container=container.container_name)

        file.ingest_status = "pending"
        await db.commit()

        # ── Step 1/6 · Sample with DuckDB ──
        step_start = time.perf_counter()
        ingest_logger.info("step", step="1/6", name="duckdb_sample", status="started",
                           blob_path=file.blob_path)

        sample = await sample_file(
            blob_path=file.blob_path,
            connection_string=container.connection_string,
            container_name=container.container_name,
        )

        ingest_logger.info("step", step="1/6", name="duckdb_sample", status="done",
                           columns=len(sample["columns_info"]),
                           column_names=sample["column_names"],
                           row_count=sample["row_count"],
                           sample_row_count=len(sample["sample_rows"]),
                           duration_ms=_ms(step_start))

        # ── Step 2/6 · AI description ──
        step_start = time.perf_counter()
        ingest_logger.info("step", step="2/6", name="ai_description", status="started",
                           filename=file.name)

        description = await generate_file_description(
            columns_info=sample["columns_info"],
            sample_rows=sample["sample_rows"],
            filename=file.name,
        )

        ingest_logger.info("step", step="2/6", name="ai_description", status="done",
                           summary=description.get("summary", "")[:200],
                           good_for=description.get("good_for", []),
                           metrics=description.get("key_metrics", []),
                           dimensions=description.get("key_dimensions", []),
                           date_range=f"{description.get('date_range_start')} → {description.get('date_range_end')}",
                           duration_ms=_ms(step_start))

        # ── Step 3/6 · Save metadata ──
        step_start = time.perf_counter()
        ingest_logger.info("step", step="3/6", name="save_metadata", status="started")

        result = await db.execute(
            select(FileMetadata).where(FileMetadata.file_id == file_id)
        )
        metadata = result.scalar_one_or_none()
        is_new = metadata is None
        if not metadata:
            metadata = FileMetadata(id=str(uuid.uuid4()), file_id=file_id)
            db.add(metadata)

        metadata.blob_path = file.blob_path
        metadata.container_id = file.container_id
        metadata.columns_info = sample["columns_info"]
        metadata.row_count = sample["row_count"]
        metadata.ai_description = description.get("summary", "")
        metadata.good_for = description.get("good_for", [])
        metadata.key_metrics = description.get("key_metrics", [])
        metadata.key_dimensions = description.get("key_dimensions", [])
        metadata.sample_rows = sample["sample_rows"]
        metadata.ingest_error = None

        if description.get("date_range_start"):
            try:
                metadata.date_range_start = date.fromisoformat(description["date_range_start"])
            except (ValueError, TypeError):
                pass
        if description.get("date_range_end"):
            try:
                metadata.date_range_end = date.fromisoformat(description["date_range_end"])
            except (ValueError, TypeError):
                pass

        await db.commit()
        ingest_logger.info("step", step="3/6", name="save_metadata", status="done",
                           action="created" if is_new else "updated",
                           duration_ms=_ms(step_start))

        # ── Step 4/6 · Build search text + embed ──
        step_start = time.perf_counter()
        ingest_logger.info("step", step="4/6", name="embed_metadata", status="started")

        try:
            search_text = build_search_text(metadata)
            metadata.search_text = search_text
            metadata.description_embedding = await embed_text(search_text)
            await db.commit()
            ingest_logger.info("step", step="4/6", name="embed_metadata", status="done",
                               search_text_len=len(search_text),
                               has_embedding=metadata.description_embedding is not None
                                             and any(x != 0.0 for x in (metadata.description_embedding or [])),
                               duration_ms=_ms(step_start))
        except Exception as embed_exc:
            # Embedding failure is non-fatal — file is already searchable via BM25/trgm
            ingest_logger.warning("step", step="4/6", name="embed_metadata", status="failed",
                                  error=str(embed_exc)[:200],
                                  duration_ms=_ms(step_start))

        # ── Step 5/5 · Pre-compute analytics + Parquet conversion ──
        # Uses a FRESH DB session — analytics takes 3-10 min of DuckDB work during which
        # the Postgres connection would sit idle and be closed by Neon's idle timeout.
        step_start = time.perf_counter()
        ingest_logger.info("step", step="5/5", name="compute_analytics", status="started")

        # Mark ingested first so the UI shows progress, then run the slow analytics
        file.ingest_status = "ingested"
        await db.commit()

        try:
            async with _async_session() as analytics_db:
                analytics = await compute_and_store_analytics(
                    file_id=file_id,
                    blob_path=file.blob_path,
                    connection_string=container.connection_string,
                    container_name=container.container_name,
                    columns_info=sample["columns_info"],
                    db=analytics_db,
                )
            ingest_logger.info("step", step="5/5", name="compute_analytics", status="done",
                               row_count=analytics.row_count,
                               duration_ms=_ms(step_start))
            # Parquet conversion runs as a background task — can take several minutes
            asyncio.ensure_future(trigger_parquet_conversion(
                file_id=file_id,
                blob_path=file.blob_path,
                connection_string=container.connection_string,
                container_name=container.container_name,
            ))
        except Exception as analytics_exc:
            ingest_logger.warning("step", step="5/5", name="compute_analytics", status="failed",
                                  error=str(analytics_exc)[:300],
                                  duration_ms=_ms(step_start))
            # Analytics failure is non-fatal — file already marked ingested

        ingest_logger.info("chain_end", outcome="success",
                           filename=file.name,
                           total_duration_ms=_ms(pipeline_start))

    except Exception as exc:
        ingest_logger.exception("chain_end", outcome="error",
                                error=str(exc)[:500],
                                total_duration_ms=_ms(pipeline_start))
        try:
            await db.rollback()
            file = await db.get(File, file_id)
            if file:
                file.ingest_status = "failed"
                # Store error in metadata too so the UI can show it
                result = await db.execute(
                    select(FileMetadata).where(FileMetadata.file_id == file_id)
                )
                meta = result.scalar_one_or_none()
                if meta:
                    meta.ingest_error = str(exc)[:1000]
                await db.commit()
        except Exception as inner:
            ingest_logger.error("status_update_failed", error=str(inner)[:300])
