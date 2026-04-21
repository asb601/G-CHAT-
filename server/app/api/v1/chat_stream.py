from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent import run_agent_query_stream
from app.api.v1.chat_common import (
    ChatMessageRequest,
    MAX_MESSAGES_PER_CONVERSATION,
    MAX_STORED_DATA_ROWS,
    WARN_MESSAGES_THRESHOLD,
    bg_title_and_summary,
)
from app.dependencies import get_db, get_current_user
from app.models.conversation import Conversation, Message
from app.models.user import User
from app.services.context_service import build_conversation_context, count_tokens

router = APIRouter()


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

    _ = f"chat-{uuid.uuid4().hex[:12]}"

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
            db.add(Message(
                conversation_id=new_conv.id,
                role="system",
                content=f"Previous conversation summary: {conv.summary}",
                token_count=count_tokens(conv.summary),
            ))
        conv = new_conv

    conversation_context = await build_conversation_context(conv, db)

    user_token_count = count_tokens(query)
    db.add(Message(
        conversation_id=conv.id,
        role="user",
        content=query,
        token_count=user_token_count,
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

            if final_payload:
                answer_text = final_payload.get("answer", "")
                full_data = final_payload.get("data", [])
                stored_data = full_data[:MAX_STORED_DATA_ROWS]
                assistant_token_count = count_tokens(answer_text)

                db.add(Message(
                    conversation_id=conv_id,
                    role="assistant",
                    content=answer_text,
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

                new_count = msg_count + 2
                if new_count >= WARN_MESSAGES_THRESHOLD:
                    final_payload["warning"] = (
                        f"This conversation has {new_count}/{MAX_MESSAGES_PER_CONVERSATION} messages. "
                        "It will auto-continue in a new thread when full."
                    )

                yield f"data: {_json.dumps({'event': 'done', 'result': final_payload})}\n\n"

                background_tasks.add_task(bg_title_and_summary, conv_id)

        except Exception:
            try:
                db.add(Message(
                    conversation_id=conv_id,
                    role="assistant",
                    content="Failed to process query. Please try again.",
                    token_count=count_tokens("Failed to process query. Please try again."),
                    payload={"error": True},
                ))
                await db.commit()
            except Exception:
                await db.rollback()

            yield (
                f"data: {_json.dumps({'event': 'error', 'detail': 'Failed to process query. Please try again.'})}"
                "\n\n"
            )

    return StreamingResponse(event_stream(), media_type="text/event-stream")
