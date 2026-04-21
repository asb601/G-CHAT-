import time

from authlib.integrations.starlette_client import OAuth
from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.config import Config

from app.core.config import get_settings
from app.core.database import get_db
from app.core.logger import auth_logger, db_logger
from app.core.security import create_access_token
from app.dependencies import get_current_user
from app.models.user import User
from app.schemas.auth import TokenOut, UserOut

router = APIRouter(prefix="/auth", tags=["auth"])

# ── OAuth setup ──
settings = get_settings()
oauth = OAuth()
oauth.register(
    name="google",
    client_id=settings.GOOGLE_CLIENT_ID,
    client_secret=settings.GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


@router.get("/google/login")
async def google_login(request: Request):
    """Redirect user to Google's consent screen."""
    auth_logger.info("google_login_initiated")
    # Google must redirect back to the SERVER callback, not the frontend
    redirect_uri = request.url_for("google_callback")
    return await oauth.google.authorize_redirect(request, str(redirect_uri))


@router.get("/google/callback")
async def google_callback(request: Request, db: AsyncSession = Depends(get_db)):
    start = time.perf_counter()
    auth_logger.info("google_callback_started")

    token = await oauth.google.authorize_access_token(request)
    user_info = token.get("userinfo")
    if not user_info or not user_info.get("email"):
        auth_logger.warning("google_callback_failed", reason="no_email")
        return RedirectResponse(f"{settings.FRONTEND_URL}/login?error=no_email")

    email = user_info["email"]
    name = user_info.get("name")
    picture = user_info.get("picture")

    # Upsert user
    db_start = time.perf_counter()
    db_logger.info("query_started", query="upsert_user", email=email)
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    if user:
        user.name = name
        user.picture = picture
    else:
        # First ever user becomes admin automatically
        any_user = await db.execute(select(User.id).limit(1))
        is_first_user = any_user.scalar_one_or_none() is None
        user = User(email=email, name=name, picture=picture, is_admin=is_first_user)
        db.add(user)

    await db.commit()
    await db.refresh(user)
    db_logger.info("query_complete", query="upsert_user", duration_ms=round((time.perf_counter() - db_start) * 1000, 2))

    # Create JWT
    access_token = create_access_token({"sub": user.id, "email": user.email})

    auth_logger.info("google_callback_complete", email=email, is_admin=user.is_admin, duration_ms=round((time.perf_counter() - start) * 1000, 2))

    # Redirect to frontend with token in URL fragment (not query param for security)
    return RedirectResponse(f"{settings.FRONTEND_URL}/auth/callback?token={access_token}")


@router.get("/me", response_model=UserOut)
async def get_me(user: User = Depends(get_current_user)):
    """Return the currently authenticated user."""
    auth_logger.info("me_requested", user_id=user.id)
    return user
