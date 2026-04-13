import uuid
from datetime import datetime, timezone
from sqlalchemy import String, DateTime, ForeignKey, BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base


class File(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    content_type: Mapped[str] = mapped_column(String(255), nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    folder_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("folders.id", ondelete="CASCADE"), nullable=True
    )
    owner_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    uploaded_by_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    container_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("container_configs.id", ondelete="CASCADE"), nullable=True
    )
    blob_path: Mapped[str | None] = mapped_column(String(1000), nullable=True, unique=True)
    ingest_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="not_ingested"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    owner: Mapped["User"] = relationship("User", foreign_keys=[owner_id])
    uploaded_by: Mapped["User | None"] = relationship("User", foreign_keys=[uploaded_by_id])
    folder: Mapped["Folder | None"] = relationship("Folder")
    container: Mapped["ContainerConfig | None"] = relationship("ContainerConfig")


from app.models.folder import Folder  # noqa: E402, F401
from app.models.user import User  # noqa: E402, F401
from app.models.container import ContainerConfig  # noqa: E402, F401
