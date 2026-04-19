import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import get_settings
from app.core.database import engine, Base
from app.core.logger import upload_logger, folder_logger, container_logger, auth_logger, chat_logger
from app.api.auth import router as auth_router
from app.api.folders import router as folders_router
from app.api.files import router as files_router
from app.api.containers import router as containers_router
from app.api.users import router as users_router
from app.api.chat import router as chat_router
from app.api.admin import router as admin_router
from app.api.logs import router as logs_router
import app.models.file  # ensure File table is created
import app.models.container  # ensure ContainerConfig table is created
import app.models.file_metadata  # ensure FileMetadata table is created
import app.models.file_relationship  # ensure FileRelationship table is created
import app.models.file_analytics  # ensure FileAnalytics table is created
import app.models.background_job  # ensure BackgroundJob table is created
import app.models.conversation  # ensure Conversation + Message tables are created


async def _add_column_if_missing(conn, table: str, column: str, col_type: str) -> None:
    """Safely add a column to an existing table (no-op if it already exists)."""
    result = await conn.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :table AND column_name = :column"
        ),
        {"table": table, "column": column},
    )
    if not result.scalar():
        await conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {col_type}'))


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        # Create any brand-new tables
        await conn.run_sync(Base.metadata.create_all)

        # Migrate existing tables — add columns introduced after initial schema
        await _add_column_if_missing(conn, "files", "container_id", "VARCHAR(36) REFERENCES container_configs(id) ON DELETE CASCADE")
        await _add_column_if_missing(conn, "files", "blob_path", "VARCHAR(1000) UNIQUE")
        await _add_column_if_missing(conn, "files", "ingest_status", "VARCHAR(20) NOT NULL DEFAULT 'not_ingested'")
        await _add_column_if_missing(conn, "files", "uploaded_by_id", "VARCHAR(36) REFERENCES users(id) ON DELETE SET NULL")
        await _add_column_if_missing(conn, "files", "upload_duration_secs", "DOUBLE PRECISION")
        await _add_column_if_missing(conn, "folders", "container_id", "VARCHAR(36) REFERENCES container_configs(id) ON DELETE CASCADE")
        await _add_column_if_missing(conn, "users", "is_admin", "BOOLEAN NOT NULL DEFAULT FALSE")
    yield
    await engine.dispose()


settings = get_settings()

app = FastAPI(title="Gen-Chatbot API", lifespan=lifespan)

# Session middleware required by authlib OAuth
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    path = request.url.path
    method = request.method
    status_code = response.status_code

    if "/files" in path:
        upload_logger.info("request", method=method, path=path, status=status_code, duration_ms=duration_ms)
    elif "/folders" in path:
        folder_logger.info("request", method=method, path=path, status=status_code, duration_ms=duration_ms)
    elif "/containers" in path:
        container_logger.info("request", method=method, path=path, status=status_code, duration_ms=duration_ms)
    elif "/auth" in path:
        auth_logger.info("request", method=method, path=path, status=status_code, duration_ms=duration_ms)
    elif "/chat" in path:
        chat_logger.info("request", method=method, path=path, status=status_code, duration_ms=duration_ms)

    return response


app.include_router(auth_router, prefix="/api")
app.include_router(folders_router, prefix="/api")
app.include_router(files_router, prefix="/api")
app.include_router(containers_router, prefix="/api")
app.include_router(users_router, prefix="/api")
app.include_router(chat_router, prefix="/api")
app.include_router(admin_router, prefix="/api")
app.include_router(logs_router, prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok"}
