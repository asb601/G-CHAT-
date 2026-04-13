from datetime import datetime
from pydantic import BaseModel


class FileOut(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    folder_id: str | None
    owner_id: str
    container_id: str | None = None
    blob_path: str | None = None
    ingest_status: str = "not_ingested"
    uploaded_by_id: str | None = None
    uploaded_by_name: str | None = None
    uploaded_by_email: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class FileMoveRequest(BaseModel):
    folder_id: str | None = None


class FileRenameRequest(BaseModel):
    name: str
