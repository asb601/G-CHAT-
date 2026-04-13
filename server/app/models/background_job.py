"""
Background job tracking — records status of long-running tasks (e.g. Parquet conversion).

Why this exists:
  trigger_parquet_conversion() runs fire-and-forget. Without this table:
  - Nobody knows if conversion is running, done, or failed
  - The agent can't tell users "still processing" vs "conversion failed"
  - There is no retry history

This table stores one row per (file_id, job_type) — upserted each run.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class BackgroundJob(Base):
    __tablename__ = "background_jobs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    file_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    )
    job_type: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # "parquet_conversion"

    # "running" | "done" | "failed"
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running"
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
