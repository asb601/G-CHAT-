import asyncio
import uuid

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import async_session, get_db
from app.core.logger import chat_logger, ingest_logger
from app.core.security import get_current_user, require_admin
from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.models.user import User
from app.agent import run_agent_query
from app.services.query_router import classify_intent, answer_from_metadata, answer_from_precomputed
from app.services.ingestion_service import ingest_file

router = APIRouter(prefix="/chat", tags=["chat"])


# ── Schemas ──


class ChatMessageRequest(BaseModel):
    query: str


class IngestRequest(BaseModel):
    file_ids: list[str]


# ── POST /api/chat/message ──


@router.post("/message")
async def chat_message(
    body: ChatMessageRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    query = body.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")
    if len(query) > 2000:
        raise HTTPException(status_code=400, detail="Query too long (max 2000 chars).")

    trace_id = f"chat-{uuid.uuid4().hex[:12]}"
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(trace_id=trace_id, pipeline="chat")

    chat_logger.info("chain_start", user_id=user.id, query=query[:200])

    try:
        intent = classify_intent(query)
        chat_logger.info("query_routed", intent=intent, query=query[:200])

        if intent == "metadata":
            result = await answer_from_metadata(query, db)
        elif intent == "precomputed":
            result = await answer_from_precomputed(query, db)
        else:
            result = await run_agent_query(query, db)

        chat_logger.info("chain_end", outcome="success",
                         route=result.get("route", intent),
                         rows=result.get("row_count", 0))
        return result
    except Exception as exc:
        chat_logger.exception("chain_end", outcome="error", error=str(exc)[:500])
        raise HTTPException(status_code=500, detail="Failed to process query. Please try again.")
    finally:
        structlog.contextvars.clear_contextvars()


# ── POST /api/chat/ingest ──


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

    # Validate files exist and are CSV/TXT
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


# ── GET /api/chat/ingest-status/{file_id} ──


@router.get("/ingest-status/{file_id}")
async def ingest_status(
    file_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    file = await db.get(File, file_id)
    if not file:
        raise HTTPException(status_code=404, detail="File not found.")

    result = await db.execute(
        select(FileMetadata).where(FileMetadata.file_id == file_id)
    )
    metadata = result.scalar_one_or_none()

    return {
        "file_id": file_id,
        "ingest_status": file.ingest_status,
        "ai_description": metadata.ai_description if metadata else None,
        "columns": [c["name"] for c in metadata.columns_info] if metadata and metadata.columns_info else [],
        "row_count": metadata.row_count if metadata else None,
        "error": metadata.ingest_error if metadata else None,
    }
