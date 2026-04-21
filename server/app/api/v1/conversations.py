from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.v1.chat_common import ConversationRenameRequest
from app.dependencies import get_db, get_current_user
from app.models.conversation import Conversation, Message
from app.models.user import User

router = APIRouter()


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
        pattern = f"%{search_term}%"
        matching_conv_ids = (
            select(Message.conversation_id)
            .where(Message.content.ilike(pattern))
            .distinct()
            .scalar_subquery()
        )
        search_filter = (Conversation.title.ilike(pattern) | Conversation.id.in_(matching_conv_ids))
        filters = [*base_filters, search_filter]
    else:
        filters = base_filters

    q = (
        select(Conversation, func.count(Message.id).label("message_count"))
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
