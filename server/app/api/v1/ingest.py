from __future__ import annotations

import uuid

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.chat_common import IngestRequest
from app.core.database import async_session, get_db
from app.core.logger import ingest_logger
from app.dependencies import get_current_user, require_admin
from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.models.user import User
from app.services.ingestion_service import ingest_file

router = APIRouter()


async def _run_ingest(file_id: str) -> None:
    """Run ingestion in a background task with its own DB session."""
    trace_id = f"ingest-{uuid.uuid4().hex[:12]}"
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(trace_id=trace_id, pipeline="ingest", file_id=file_id)
    try:
        async with async_session() as db:
            await ingest_file(file_id, db)
    finally:
        structlog.contextvars.clear_contextvars()


@router.post("/ingest")
async def ingest_files(
    body: IngestRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    if not body.file_ids:
        raise HTTPException(status_code=400, detail="No file IDs provided.")

    valid_ids: list[str] = []
    for fid in body.file_ids:
        file = await db.get(File, fid)
        if not file:
            continue
        ext = (file.name or "").rsplit(".", 1)[-1].lower()
        if ext not in ("csv", "txt", "tsv"):
            continue
        valid_ids.append(fid)

    if not valid_ids:
        raise HTTPException(status_code=400, detail="No valid CSV/TXT files found.")

    for fid in valid_ids:
        background_tasks.add_task(_run_ingest, fid)

    ingest_logger.info("ingest_queued", admin_id=admin.id, file_count=len(valid_ids), file_ids=valid_ids)
    return {"queued": len(valid_ids), "file_ids": valid_ids}


@router.get("/ingest-status/{file_id}")
async def ingest_status(
    file_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    file = await db.get(File, file_id)
    if not file:
        raise HTTPException(status_code=404, detail="File not found.")

    result = await db.execute(select(FileMetadata).where(FileMetadata.file_id == file_id))
    metadata = result.scalar_one_or_none()

    return {
        "file_id": file_id,
        "ingest_status": file.ingest_status,
        "ai_description": metadata.ai_description if metadata else None,
        "columns": [c["name"] for c in metadata.columns_info] if metadata and metadata.columns_info else [],
        "row_count": metadata.row_count if metadata else None,
        "error": metadata.ingest_error if metadata else None,
    }
