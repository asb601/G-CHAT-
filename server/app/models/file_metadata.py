import uuid
from datetime import datetime, timezone, date

from sqlalchemy import String, BigInteger, Text, Date, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base


class FileMetadata(Base):
    __tablename__ = "file_metadata"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    file_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    blob_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    container_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("container_configs.id", ondelete="CASCADE"), nullable=True
    )
    columns_info: Mapped[list | None] = mapped_column(JSONB, default=list)
    row_count: Mapped[int] = mapped_column(BigInteger, default=0)
    ai_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    good_for: Mapped[list | None] = mapped_column(JSONB, default=list)
    key_metrics: Mapped[list | None] = mapped_column(JSONB, default=list)
    key_dimensions: Mapped[list | None] = mapped_column(JSONB, default=list)
    date_range_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_range_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    delimiter: Mapped[str] = mapped_column(String(10), default=",")
    sample_rows: Mapped[list | None] = mapped_column(JSONB, default=list)
    ingest_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    file: Mapped["File"] = relationship("File")


from app.models.file import File  # noqa: E402, F401
