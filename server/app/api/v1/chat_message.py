from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent import run_agent_query
from app.api.v1.chat_common import (
    ChatMessageRequest,
    MAX_MESSAGES_PER_CONVERSATION,
    MAX_STORED_DATA_ROWS,
    WARN_MESSAGES_THRESHOLD,
    bg_title_and_summary,
)
from app.core.logger import chat_logger
from app.dependencies import get_db, get_current_user
from app.models.conversation import Conversation, Message
from app.models.user import User
from app.services.context_service import build_conversation_context, count_tokens

router = APIRouter()


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

    msg_count_q = select(func.count(Message.id)).where(Message.conversation_id == conv.id)
    msg_count = (await db.execute(msg_count_q)).scalar() or 0

    if msg_count >= MAX_MESSAGES_PER_CONVERSATION:
        conv.archived_at = datetime.now(timezone.utc)
        new_conv = Conversation(
            user_id=user.id,
            title=f"{conv.title} (continued)",
            summary=conv.summary,
        )
        db.add(new_conv)
        await db.flush()

        if conv.summary:
            system_msg = Message(
                conversation_id=new_conv.id,
                role="system",
                content=f"Previous conversation summary: {conv.summary}",
                token_count=count_tokens(conv.summary),
            )
            db.add(system_msg)

        conv = new_conv

    conversation_context = await build_conversation_context(conv, db)

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
        result = await run_agent_query(
            query, db,
            conversation_context=conversation_context,
            user_id=user.id,
            is_admin=getattr(user, "is_admin", False),
        )

        full_data = result.get("data", [])
        stored_data = full_data[:MAX_STORED_DATA_ROWS]

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

        background_tasks.add_task(bg_title_and_summary, conv.id)

        response = {**result, "conversation_id": conv.id}

        new_count = msg_count + 2
        if new_count >= WARN_MESSAGES_THRESHOLD:
            response["warning"] = (
                f"This conversation has {new_count}/{MAX_MESSAGES_PER_CONVERSATION} messages. "
                "It will auto-continue in a new thread when full."
            )

        return response

    except Exception as exc:
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
