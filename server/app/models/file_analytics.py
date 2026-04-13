"""
Pre-computed analytics for each ingested file.
Populated once at ingest time, queried at chat time for instant answers.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, BigInteger, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class FileAnalytics(Base):
    __tablename__ = "file_analytics"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    file_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    blob_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    # ── Core counts ──
    row_count: Mapped[int] = mapped_column(BigInteger, default=0)
    column_count: Mapped[int] = mapped_column(BigInteger, default=0)

    # ── Per-column stats (JSONB) ──
    # { "col_name": { "dtype", "nulls", "unique", "min", "max", "mean", "sum", "std", "top_values": [...] } }
    column_stats: Mapped[dict | None] = mapped_column(JSONB, default=dict)

    # ── Categorical breakdowns (JSONB) ──
    # { "status": {"active": 1200, "failed": 340, ...}, "category": {...}, ... }
    value_counts: Mapped[dict | None] = mapped_column(JSONB, default=dict)

    # ── Cross-tab summaries (JSONB) ──
    # [ { "group_by": ["status","category"], "metric": "amount", "agg": "sum", "data": [...] }, ... ]
    cross_tabs: Mapped[list | None] = mapped_column(JSONB, default=list)

    # ── Parquet conversion ──
    parquet_blob_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    parquet_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # ── Timestamps ──
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
