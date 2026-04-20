"""
Admin API — internal endpoints for monitoring and cost tracking.

GET  /api/admin/cost-summary
POST /api/admin/reingest-all
"""
import asyncio
import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.cost_tracker import get_session_summary
from app.core.database import async_session, get_db
from app.core.logger import ingest_logger
from app.core.security import get_current_user, require_admin
from app.agent.graph import invalidate_catalog_cache
from app.models.file import File
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship
from app.models.user import User
from app.services.ingestion_service import ingest_file

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/cost-summary")
async def cost_summary(current_user: User = Depends(get_current_user)) -> dict:
    return get_session_summary()


# ── Re-ingest all files ──────────────────────────────────────────────────────

_REINGEST_SEMAPHORE = asyncio.Semaphore(3)


async def _batch_reingest(file_ids: list[str]) -> None:
    """Re-ingest a list of files with concurrency capped at 3."""
    done = 0
    failed = 0

    async def _one(file_id: str) -> None:
        nonlocal done, failed
        async with _REINGEST_SEMAPHORE:
            trace_id = f"reingest-{uuid.uuid4().hex[:12]}"
            structlog.contextvars.clear_contextvars()
            structlog.contextvars.bind_contextvars(
                trace_id=trace_id, pipeline="reingest", file_id=file_id
            )
            try:
                async with async_session() as db:
                    await ingest_file(file_id, db)
                done += 1
                ingest_logger.info("reingest_progress", done=done, failed=failed,
                                   remaining=len(file_ids) - done - failed)
            except Exception as exc:
                failed += 1
                ingest_logger.exception("reingest_crashed", error=str(exc)[:500])
            finally:
                structlog.contextvars.clear_contextvars()

    await asyncio.gather(*[_one(fid) for fid in file_ids])
    invalidate_catalog_cache()
    ingest_logger.info("reingest_complete", done=done, failed=failed)


@router.post("/reingest-all")
async def reingest_all(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Wipe all metadata / analytics / relationships, reset every
    CSV/TXT/TSV file to not_ingested, and re-run the full pipeline.
    """
    # Find all ingestable files
    result = await db.execute(select(File))
    all_files = list(result.scalars().all())
    ingestable = [
        f for f in all_files
        if (f.name or "").rsplit(".", 1)[-1].lower() in ("csv", "txt", "tsv")
    ]
    if not ingestable:
        raise HTTPException(status_code=400, detail="No CSV/TXT/TSV files found.")

    file_ids = [f.id for f in ingestable]

    # Delete old metadata, analytics, relationships
    await db.execute(delete(FileRelationship))
    await db.execute(delete(FileAnalytics))
    await db.execute(delete(FileMetadata))

    # Reset ingest status
    await db.execute(
        update(File)
        .where(File.id.in_(file_ids))
        .values(ingest_status="not_ingested")
    )
    await db.commit()
    invalidate_catalog_cache()

    ingest_logger.info("reingest_all_started", admin_id=admin.id, file_count=len(file_ids))

    # Fire background task
    asyncio.create_task(_batch_reingest(file_ids))

    return {"message": "Re-ingestion started", "file_count": len(file_ids)}
