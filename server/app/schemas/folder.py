from datetime import datetime
from pydantic import BaseModel
from app.schemas.file import FileOut


class FolderCreate(BaseModel):
    name: str
    parent_id: str | None = None


class FolderUpdate(BaseModel):
    name: str | None = None
    parent_id: str | None = None


class FolderOut(BaseModel):
    id: str
    name: str
    parent_id: str | None
    owner_id: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class FolderContents(BaseModel):
    folders: list[FolderOut]
    files: list[FileOut]
