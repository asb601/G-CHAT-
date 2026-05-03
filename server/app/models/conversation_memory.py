"""
ConversationMemory — one row per completed conversation turn.

Stores the user question, a short answer summary, and a 1536-dim embedding
of the question so the get_memory tool can do hybrid recall (semantic +
BM25 + fuzzy) across past turns without replaying the full chat history.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ConversationMemory(Base):
    __tablename__ = "conversation_memory"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    conversation_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("conversations.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[str] = mapped_column(String(36), nullable=False)

    # The exact user question text
    question: Mapped[str] = mapped_column(Text, nullable=False)

    # First 600 chars of the assistant's answer — enough for recall, not huge
    answer_summary: Mapped[str] = mapped_column(Text, nullable=False)

    # Blob paths the agent actually queried — helps the agent know which files
    # were relevant for a given topic in the past
    files_used: Mapped[list] = mapped_column(JSONB, default=list)

    # Embedding of the question for semantic (cosine) search
    question_embedding: Mapped[list[float] | None] = mapped_column(
        Vector(1536), nullable=True
    )

    # Concatenated question + answer_summary for BM25 and trigram fuzzy search
    search_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("ix_conv_memory_user_id", "user_id"),
        Index("ix_conv_memory_conv_id", "conversation_id"),
    )
