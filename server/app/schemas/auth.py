from datetime import datetime
from pydantic import BaseModel


class UserOut(BaseModel):
    id: str
    email: str
    name: str | None
    picture: str | None
    is_admin: bool
    allowed_domains: list[str] | None = None

    model_config = {"from_attributes": True}


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut
