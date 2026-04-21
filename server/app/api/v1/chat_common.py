"""Shared constants, schemas, and helpers for chat API modules."""
from __future__ import annotations

from pydantic import BaseModel

from app.core.database import async_session
from app.core.logger import chat_logger
from app.services.context_service import maybe_generate_title, maybe_regenerate_summary

MAX_MESSAGES_PER_CONVERSATION = 200
WARN_MESSAGES_THRESHOLD = 180     # frontend shows "nearing limit" warning
MAX_STORED_DATA_ROWS = 50         # cap SQL result rows persisted in JSONB


class ChatMessageRequest(BaseModel):
    query: str
    conversation_id: str | None = None  # omit to start a new conversation


class IngestRequest(BaseModel):
    file_ids: list[str]


class ConversationRenameRequest(BaseModel):
    title: str


async def bg_title_and_summary(conv_id: str) -> None:
    """Background task: generate title + regenerate summary if needed."""
    try:
        async with async_session() as db:
            await maybe_generate_title(conv_id, db)
            await maybe_regenerate_summary(conv_id, db)
    except Exception as exc:
        chat_logger.warning("bg_task_failed", conversation_id=conv_id, error=str(exc)[:200])
