import time

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logger import auth_logger
from app.dependencies import require_admin
from app.models.file import File
from app.models.user import User
from app.schemas.user import UserOut

router = APIRouter(prefix="/users", tags=["users"])


@router.get("", response_model=list[UserOut])
async def list_users(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    auth_logger.info("users_list_requested")

    result = await db.execute(
        select(
            User,
            func.count(File.id).label("file_count"),
        )
        .outerjoin(File, File.uploaded_by_id == User.id)
        .group_by(User.id)
        .order_by(User.created_at)
    )
    rows = result.all()
    users = [
        UserOut(
            id=u.id,
            email=u.email,
            name=u.name,
            picture=u.picture,
            is_admin=u.is_admin,
            created_at=u.created_at,
            file_count=file_count,
        )
        for u, file_count in rows
    ]
    auth_logger.info("users_list_complete", count=len(users), duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return users


@router.patch("/{user_id}/toggle-admin")
async def toggle_admin(
    user_id: str,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot change your own admin status")

    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_admin = not user.is_admin
    await db.commit()

    auth_logger.info("admin_toggled", user_id=user_id, is_admin=user.is_admin)
    return {"id": user.id, "is_admin": user.is_admin}
