from datetime import datetime
from pydantic import BaseModel, field_validator


class ContainerCreate(BaseModel):
    name: str
    container_name: str
    connection_string: str

    @field_validator("name", "container_name", "connection_string", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v


class ContainerOut(BaseModel):
    id: str
    name: str
    container_name: str
    last_synced_at: datetime | None
    file_count: int = 0
    created_at: datetime

    model_config = {"from_attributes": True}


class ContainerSyncResponse(BaseModel):
    message: str
    container_id: str
