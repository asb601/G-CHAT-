import asyncio
import uuid

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
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
from app.agent import run_agent_query, run_agent_query_stream
from app.services.ingestion_service import ingest_file
from app.services.context_service import (
    build_conversation_context,
    count_tokens,
    maybe_generate_title,
    maybe_regenerate_summary,
)

router = APIRouter(prefix="/chat", tags=["chat"])

# ── Constants ──

MAX_MESSAGES_PER_CONVERSATION = 200
WARN_MESSAGES_THRESHOLD = 180     # frontend shows "nearing limit" warning
MAX_STORED_DATA_ROWS = 50         # cap SQL result rows persisted in JSONB


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
# Injects conversation context into the agent for multi-turn awareness.


@router.post("/message")
async def chat_message(
    body: ChatMessageRequest,
    background_tasks: BackgroundTasks,
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

    # ── Enforce message cap with graceful handling ──
    msg_count_q = select(func.count(Message.id)).where(Message.conversation_id == conv.id)
    msg_count = (await db.execute(msg_count_q)).scalar() or 0

    if msg_count >= MAX_MESSAGES_PER_CONVERSATION:
        # Auto-archive and create continuation conversation
        conv.archived_at = datetime.now(timezone.utc)
        new_conv = Conversation(
            user_id=user.id,
            title=f"{conv.title} (continued)",
            summary=conv.summary,  # carry forward context
        )
        db.add(new_conv)
        await db.flush()

        # Carry forward the summary as a system message
        if conv.summary:
            system_msg = Message(
                conversation_id=new_conv.id,
                role="system",
                content=f"Previous conversation summary: {conv.summary}",
                token_count=count_tokens(conv.summary),
            )
            db.add(system_msg)

        conv = new_conv

    # ── Build conversation context for multi-turn awareness ──
    conversation_context = await build_conversation_context(conv, db)

    # ── Count and save user message immediately (survives agent errors) ──
    user_token_count = count_tokens(query)
    user_msg = Message(
        conversation_id=conv.id,
        role="user",
        content=query,
        token_count=user_token_count,
    )
    db.add(user_msg)
    conv.updated_at = datetime.now(timezone.utc)
    conv.token_count = (conv.token_count or 0) + user_token_count
    await db.commit()

    chat_logger.info("chain_start", user_id=user.id, conversation_id=conv.id,
                     query=query[:200], has_context=bool(conversation_context))

    try:
        result = await run_agent_query(query, db, conversation_context=conversation_context)

        # ── Truncate data rows to prevent JSONB bloat ──
        full_data = result.get("data", [])
        stored_data = full_data[:MAX_STORED_DATA_ROWS]

        # ── Count and save assistant message ──
        answer_text = result.get("answer", "")
        assistant_token_count = count_tokens(answer_text)

        assistant_msg = Message(
            conversation_id=conv.id,
            role="assistant",
            content=answer_text,
            token_count=assistant_token_count,
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
        conv.token_count = (conv.token_count or 0) + assistant_token_count
        await db.commit()

        chat_logger.info("chain_end", outcome="success",
                         conversation_id=conv.id,
                         route=result.get("route", "agent"),
                         rows=result.get("row_count", 0))

        # ── Background tasks: title gen + summary regen ──
        background_tasks.add_task(_bg_title_and_summary, conv.id)

        # ── Build response ──
        response = {**result, "conversation_id": conv.id}

        # Add nearing-limit warning
        new_count = msg_count + 2  # user + assistant
        if new_count >= WARN_MESSAGES_THRESHOLD:
            response["warning"] = (
                f"This conversation has {new_count}/{MAX_MESSAGES_PER_CONVERSATION} messages. "
                "It will auto-continue in a new thread when full."
            )

        return response

    except Exception as exc:
        # User message already committed — save error as assistant reply
        try:
            error_msg = Message(
                conversation_id=conv.id,
                role="assistant",
                content="Failed to process query. Please try again.",
                token_count=count_tokens("Failed to process query. Please try again."),
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


async def _bg_title_and_summary(conv_id: str) -> None:
    """Background task: generate title + regenerate summary if needed."""
    try:
        async with async_session() as db:
            await maybe_generate_title(conv_id, db)
            await maybe_regenerate_summary(conv_id, db)
    except Exception as exc:
        chat_logger.warning("bg_task_failed", conversation_id=conv_id, error=str(exc)[:200])


# ── POST /api/chat/message/stream ──
# SSE streaming variant — sends events as they happen so the frontend
# can show progress ("thinking...", partial answer, final result).


@router.post("/message/stream")
async def chat_message_stream(
    body: ChatMessageRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """True SSE streaming — tokens arrive as the LLM generates them."""
    import json as _json

    query = body.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")
    if len(query) > 2000:
        raise HTTPException(status_code=400, detail="Query too long (max 2000 chars).")

    trace_id = f"chat-{uuid.uuid4().hex[:12]}"

    # ── Resolve or create conversation ──
    if body.conversation_id:
        conv = await db.get(Conversation, body.conversation_id)
        if not conv or conv.user_id != user.id:
            raise HTTPException(status_code=404, detail="Conversation not found.")
        if conv.archived_at is not None:
            raise HTTPException(status_code=410, detail="Conversation has been deleted.")
    else:
        conv = Conversation(user_id=user.id, title=query[:100].strip())
        db.add(conv)
        await db.flush()

    # Message cap with graceful continuation
    msg_count_q = select(func.count(Message.id)).where(Message.conversation_id == conv.id)
    msg_count = (await db.execute(msg_count_q)).scalar() or 0

    if msg_count >= MAX_MESSAGES_PER_CONVERSATION:
        conv.archived_at = datetime.now(timezone.utc)
        new_conv = Conversation(
            user_id=user.id, title=f"{conv.title} (continued)", summary=conv.summary,
        )
        db.add(new_conv)
        await db.flush()
        if conv.summary:
            db.add(Message(
                conversation_id=new_conv.id, role="system",
                content=f"Previous conversation summary: {conv.summary}",
                token_count=count_tokens(conv.summary),
            ))
        conv = new_conv

    conversation_context = await build_conversation_context(conv, db)

    user_token_count = count_tokens(query)
    db.add(Message(
        conversation_id=conv.id, role="user", content=query, token_count=user_token_count,
    ))
    conv.updated_at = datetime.now(timezone.utc)
    conv.token_count = (conv.token_count or 0) + user_token_count
    await db.commit()

    conv_id = conv.id

    async def event_stream():
        yield f"data: {_json.dumps({'event': 'started', 'conversation_id': conv_id})}\n\n"

        try:
            final_payload = None

            async for evt in run_agent_query_stream(query, db, conversation_context=conversation_context):
                evt_type = evt["type"]

                if evt_type == "token":
                    yield f"data: {_json.dumps({'event': 'token', 'content': evt['content']})}\n\n"

                elif evt_type == "thinking":
                    yield f"data: {_json.dumps({'event': 'thinking', 'tool': evt.get('tool', '')})}\n\n"

                elif evt_type == "tool_result":
                    yield f"data: {_json.dumps({'event': 'tool_result', 'tool': evt.get('tool', '')})}\n\n"

                elif evt_type == "done":
                    final_payload = evt["payload"]

            # ── Persist assistant message ──
            if final_payload:
                answer_text = final_payload.get("answer", "")
                full_data = final_payload.get("data", [])
                stored_data = full_data[:MAX_STORED_DATA_ROWS]
                assistant_token_count = count_tokens(answer_text)

                db.add(Message(
                    conversation_id=conv_id, role="assistant", content=answer_text,
                    token_count=assistant_token_count,
                    payload={
                        "data": stored_data,
                        "data_truncated": len(full_data) > MAX_STORED_DATA_ROWS,
                        "chart": final_payload.get("chart"),
                        "row_count": final_payload.get("row_count", 0),
                        "files_used": final_payload.get("files_used", []),
                        "tool_calls": final_payload.get("tool_calls", 0),
                    },
                ))
                upd_conv = await db.get(Conversation, conv_id)
                if upd_conv:
                    upd_conv.updated_at = datetime.now(timezone.utc)
                    upd_conv.token_count = (upd_conv.token_count or 0) + assistant_token_count
                await db.commit()

                final_payload["conversation_id"] = conv_id

                # Add nearing-limit warning
                new_count = msg_count + 2
                if new_count >= WARN_MESSAGES_THRESHOLD:
                    final_payload["warning"] = (
                        f"This conversation has {new_count}/{MAX_MESSAGES_PER_CONVERSATION} messages. "
                        "It will auto-continue in a new thread when full."
                    )

                yield f"data: {_json.dumps({'event': 'done', 'result': final_payload})}\n\n"

                background_tasks.add_task(_bg_title_and_summary, conv_id)

        except Exception as exc:
            try:
                db.add(Message(
                    conversation_id=conv_id, role="assistant",
                    content="Failed to process query. Please try again.",
                    token_count=count_tokens("Failed to process query. Please try again."),
                    payload={"error": True},
                ))
                await db.commit()
            except Exception:
                await db.rollback()

            yield f"data: {_json.dumps({'event': 'error', 'detail': 'Failed to process query. Please try again.'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


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
    search_term = search.strip()
    base_filters = [Conversation.user_id == user.id, Conversation.archived_at.is_(None)]

    if search_term:
        # Search both title AND message content
        pattern = f"%{search_term}%"
        matching_conv_ids = (
            select(Message.conversation_id)
            .where(Message.content.ilike(pattern))
            .distinct()
            .scalar_subquery()
        )
        search_filter = (
            Conversation.title.ilike(pattern)
            | Conversation.id.in_(matching_conv_ids)
        )
        filters = [*base_filters, search_filter]
    else:
        filters = base_filters

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
                "token_count": c.token_count or 0,
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
        "token_count": conv.token_count or 0,
        "messages": [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "payload": m.payload,
                "created_at": m.created_at.isoformat(),
                "token_count": m.token_count or 0,
            }
            for m in sorted(conv.messages, key=lambda m: m.created_at)
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
