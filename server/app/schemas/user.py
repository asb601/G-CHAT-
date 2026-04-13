from datetime import datetime
from pydantic import BaseModel


class UserOut(BaseModel):
    id: str
    email: str
    name: str | None
    picture: str | None
    is_admin: bool
    created_at: datetime
    file_count: int = 0

    model_config = {"from_attributes": True}
