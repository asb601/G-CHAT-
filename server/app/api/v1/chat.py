"""Chat API router aggregator.

Keeps the same top-level `/chat` router while delegating endpoints to
focused modules for maintainability.
"""
from fastapi import APIRouter

from app.api.v1.chat_message import router as message_router
from app.api.v1.chat_stream import router as stream_router
from app.api.v1.conversations import router as conversations_router
from app.api.v1.ingest import router as ingest_router

router = APIRouter(prefix="/chat", tags=["chat"])
router.include_router(message_router)
router.include_router(stream_router)
router.include_router(conversations_router)
router.include_router(ingest_router)
