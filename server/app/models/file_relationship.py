import uuid

from sqlalchemy import String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class FileRelationship(Base):
    __tablename__ = "file_relationships"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    file_a_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    )
    file_b_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    )
    file_a_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    file_b_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    shared_column: Mapped[str] = mapped_column(String(255), nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    join_type: Mapped[str] = mapped_column(String(20), default="LEFT JOIN")

    __table_args__ = (
        UniqueConstraint("file_a_id", "file_b_id", "shared_column"),
    )
