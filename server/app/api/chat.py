import asyncio
import uuid

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from datetime import datetime, timezone

from app.core.database import async_session, get_db
from app.core.logger import chat_logger, ingest_logger
from app.core.security import get_current_user, require_admin
from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.models.user import User
from app.models.conversation import Conversation, Message
from app.agent import run_agent_query
from app.services.ingestion_service import ingest_file

router = APIRouter(prefix="/chat", tags=["chat"])

# ── Constants ──

MAX_MESSAGES_PER_CONVERSATION = 200
MAX_STORED_DATA_ROWS = 50  # Cap SQL result rows persisted in JSONB to prevent DB bloat


# ── Schemas ──


class ChatMessageRequest(BaseModel):
    query: str
    conversation_id: str | None = None  # omit to start a new conversation


class IngestRequest(BaseModel):
    file_ids: list[str]


class ConversationRenameRequest(BaseModel):
    title: str


# ── POST /api/chat/message ──
# Creates or continues a conversation. Persists user & assistant messages.


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

    # ── Resolve or create conversation ──
    if body.conversation_id:
        conv = await db.get(Conversation, body.conversation_id)
        if not conv or conv.user_id != user.id:
            raise HTTPException(status_code=404, detail="Conversation not found.")
        if conv.archived_at is not None:
            raise HTTPException(status_code=410, detail="Conversation has been deleted.")
    else:
        conv = Conversation(
            user_id=user.id,
            title=query[:100].strip(),
        )
        db.add(conv)
        await db.flush()  # get conv.id

    # ── Determine next message position + enforce cap ──
    pos_result = await db.execute(
        select(func.coalesce(func.max(Message.position), -1))
        .where(Message.conversation_id == conv.id)
    )
    next_pos = pos_result.scalar() + 1

    if next_pos >= MAX_MESSAGES_PER_CONVERSATION:
        raise HTTPException(
            status_code=422,
            detail="Conversation has reached the message limit. Please start a new one.",
        )

    # ── Save user message immediately (survives agent errors) ──
    user_msg = Message(
        conversation_id=conv.id,
        role="user",
        content=query,
        position=next_pos,
    )
    db.add(user_msg)
    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()

    chat_logger.info("chain_start", user_id=user.id, conversation_id=conv.id, query=query[:200])

    try:
        result = await run_agent_query(query, db)

        # ── Truncate data rows to prevent JSONB bloat ──
        # Full data is returned to the client, but we only persist a sample.
        full_data = result.get("data", [])
        stored_data = full_data[:MAX_STORED_DATA_ROWS]

        # ── Save assistant message ──
        assistant_msg = Message(
            conversation_id=conv.id,
            role="assistant",
            content=result.get("answer", ""),
            position=next_pos + 1,
            payload={
                "data": stored_data,
                "data_truncated": len(full_data) > MAX_STORED_DATA_ROWS,
                "chart": result.get("chart"),
                "row_count": result.get("row_count", 0),
                "files_used": result.get("files_used", []),
                "tool_calls": result.get("tool_calls", 0),
            },
        )
        db.add(assistant_msg)
        conv.updated_at = datetime.now(timezone.utc)
        await db.commit()

        chat_logger.info("chain_end", outcome="success",
                         conversation_id=conv.id,
                         route=result.get("route", "agent"),
                         rows=result.get("row_count", 0))

        result["conversation_id"] = conv.id
        return result

    except Exception as exc:
        # User message already committed — save error as assistant reply
        try:
            error_msg = Message(
                conversation_id=conv.id,
                role="assistant",
                content="Failed to process query. Please try again.",
                position=next_pos + 1,
                payload={"error": True},
            )
            db.add(error_msg)
            await db.commit()
        except Exception:
            await db.rollback()

        chat_logger.exception("chain_end", outcome="error", error=str(exc)[:500])
        raise HTTPException(status_code=500, detail="Failed to process query. Please try again.")
    finally:
        structlog.contextvars.clear_contextvars()


# ── GET /api/chat/conversations ──
# List user's conversations, newest first, paginated.


@router.get("/conversations")
async def list_conversations(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    search: str = Query(default="", max_length=200),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    filters = [Conversation.user_id == user.id, Conversation.archived_at.is_(None)]
    if search.strip():
        filters.append(Conversation.title.ilike(f"%{search.strip()}%"))

    q = (
        select(
            Conversation,
            func.count(Message.id).label("message_count"),
        )
        .outerjoin(Message, Message.conversation_id == Conversation.id)
        .where(*filters)
        .group_by(Conversation.id)
        .order_by(Conversation.updated_at.desc())
        .offset(offset)
        .limit(limit)
    )
    rows = list((await db.execute(q)).all())

    count_q = select(func.count(Conversation.id)).where(*filters)
    total = (await db.execute(count_q)).scalar()

    return {
        "conversations": [
            {
                "id": c.id,
                "title": c.title,
                "created_at": c.created_at.isoformat(),
                "updated_at": c.updated_at.isoformat(),
                "message_count": msg_count,
            }
            for c, msg_count in rows
        ],
        "total": total,
    }


# ── GET /api/chat/conversations/{conversation_id} ──
# Full conversation with all messages (for loading a past chat).


@router.get("/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    q = (
        select(Conversation)
        .where(Conversation.id == conversation_id)
        .options(selectinload(Conversation.messages))
    )
    conv = (await db.execute(q)).scalar_one_or_none()
    if not conv or conv.user_id != user.id:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    return {
        "id": conv.id,
        "title": conv.title,
        "created_at": conv.created_at.isoformat(),
        "updated_at": conv.updated_at.isoformat(),
        "messages": [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "payload": m.payload,
                "created_at": m.created_at.isoformat(),
            }
            for m in sorted(conv.messages, key=lambda m: m.position)
        ],
    }


# ── PATCH /api/chat/conversations/{conversation_id} ──
# Rename a conversation.


@router.patch("/conversations/{conversation_id}")
async def rename_conversation(
    conversation_id: str,
    body: ConversationRenameRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = await db.get(Conversation, conversation_id)
    if not conv or conv.user_id != user.id:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    title = body.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title cannot be empty.")
    conv.title = title[:200]
    await db.commit()
    return {"id": conv.id, "title": conv.title}


# ── DELETE /api/chat/conversations/{conversation_id} ──
# Soft-delete (archive) a conversation.


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = await db.get(Conversation, conversation_id)
    if not conv or conv.user_id != user.id:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    conv.archived_at = datetime.now(timezone.utc)
    await db.commit()
    return {"deleted": True}


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
