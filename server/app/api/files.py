import asyncio
import mimetypes
import time
import uuid
from datetime import datetime, timezone, timedelta

import structlog
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import async_session, get_db
from app.core.logger import upload_logger, blob_logger, db_logger, ingest_logger
from app.core.security import get_current_user, require_admin
from app.services.ingestion_service import ingest_file
from app.models.background_job import BackgroundJob
from app.models.background_job import BackgroundJob
from app.models.background_job import BackgroundJob
from app.models.container import ContainerConfig
from app.models.file import File
from app.models.folder import Folder
from app.models.user import User
from app.schemas.file import FileMoveRequest, FileOut, FileRenameRequest

router = APIRouter(prefix="/files", tags=["files"])


def _parse_connection_string(conn_str: str) -> tuple[str, str]:
    """Extract AccountName and AccountKey from an Azure connection string."""
    parts = dict(part.split("=", 1) for part in conn_str.split(";") if "=" in part)
    account_name = parts.get("AccountName", "")
    account_key = parts.get("AccountKey", "")
    return account_name, account_key


async def _get_container_config(db: AsyncSession, container_id: str) -> ContainerConfig:
    config = await db.get(ContainerConfig, container_id)
    if not config:
        raise HTTPException(status_code=404, detail="Container config not found")
    return config


def _file_to_out(file: File) -> FileOut:
    """Convert a File ORM object to FileOut, populating uploaded_by fields."""
    return FileOut(
        id=file.id,
        name=file.name,
        content_type=file.content_type,
        size=file.size,
        folder_id=file.folder_id,
        owner_id=file.owner_id,
        container_id=file.container_id,
        blob_path=file.blob_path,
        ingest_status=file.ingest_status,
        uploaded_by_id=file.uploaded_by_id,
        uploaded_by_name=file.uploaded_by.name if file.uploaded_by else None,
        uploaded_by_email=file.uploaded_by.email if file.uploaded_by else None,
        created_at=file.created_at,
    )


# ── SAS-based direct upload (supports 10 GB+) ───────────────────────────────

class UploadUrlRequest(BaseModel):
    filename: str
    content_type: str | None = None
    folder_id: str | None = None
    container_id: str


class UploadUrlResponse(BaseModel):
    file_id: str
    sas_url: str
    blob_name: str


class ConfirmUploadRequest(BaseModel):
    file_id: str
    blob_name: str
    filename: str
    content_type: str | None = None
    size: int
    folder_id: str | None = None
    container_id: str


@router.post("/upload-url", response_model=UploadUrlResponse)
async def get_upload_url(
    body: UploadUrlRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Generate a SAS URL for direct browser-to-Azure upload. Supports files of any size."""
    start = time.perf_counter()
    upload_logger.info("sas_token_requested", filename=body.filename, container_id=body.container_id)

    config = await _get_container_config(db, body.container_id)
    account_name, account_key = _parse_connection_string(config.connection_string)
    if not account_name or not account_key:
        upload_logger.error("sas_token_failed", reason="Invalid container connection string")
        raise HTTPException(status_code=500, detail="Container connection string is invalid")

    file_id = str(uuid.uuid4())
    safe_filename = body.filename.replace(" ", "_")
    blob_name = f"{file_id}_{safe_filename}"

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=config.container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(write=True, create=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=2),
    )

    sas_url = f"https://{account_name}.blob.core.windows.net/{config.container_name}/{blob_name}?{sas_token}"

    upload_logger.info("sas_token_generated", blob_name=blob_name, expires_in="2h", duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return UploadUrlResponse(file_id=file_id, sas_url=sas_url, blob_name=blob_name)


@router.post("/confirm-upload", response_model=FileOut)
async def confirm_upload(
    body: ConfirmUploadRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Called after browser finishes direct Azure upload. Saves file metadata to DB."""
    start = time.perf_counter()
    upload_logger.info("confirm_started", file_id=body.file_id, filename=body.filename, size_bytes=body.size, blob_name=body.blob_name)

    if body.folder_id:
        db_start = time.perf_counter()
        db_logger.info("query_started", query="check_folder_exists", folder_id=body.folder_id)
        result = await db.execute(select(Folder).where(Folder.id == body.folder_id))
        db_logger.info("query_complete", query="check_folder_exists", duration_ms=round((time.perf_counter() - db_start) * 1000, 2))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Folder not found")

    mime = body.content_type or mimetypes.guess_type(body.filename)[0] or "application/octet-stream"

    db_file = File(
        id=body.file_id,
        name=body.filename,
        content_type=mime,
        size=body.size,
        folder_id=body.folder_id,
        container_id=body.container_id,
        owner_id=admin.id,
        uploaded_by_id=admin.id,
        blob_path=body.blob_name,
        ingest_status="not_ingested",
    )
    db.add(db_file)

    db_start = time.perf_counter()
    db_logger.info("query_started", query="insert_file", file_id=body.file_id)
    await db.commit()
    await db.refresh(db_file)
    db_logger.info("query_complete", query="insert_file", duration_ms=round((time.perf_counter() - db_start) * 1000, 2))

    upload_logger.info("confirm_complete", file_id=body.file_id, filename=body.filename, duration_ms=round((time.perf_counter() - start) * 1000, 2))

    # Auto-ingest CSV/TXT files in the background
    ext = (body.filename or "").rsplit(".", 1)[-1].lower()
    if ext in ("csv", "txt", "tsv"):
        asyncio.create_task(_background_ingest(body.file_id))

    return _file_to_out(db_file)


async def _background_ingest(file_id: str) -> None:
    """Run file ingestion in a background task with its own DB session."""
    trace_id = f"ingest-{uuid.uuid4().hex[:12]}"
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(trace_id=trace_id, pipeline="ingest", file_id=file_id)
    try:
        async with async_session() as db:
            await ingest_file(file_id, db)
    except Exception as exc:
        ingest_logger.exception("background_ingest_crashed", error=str(exc)[:500])
    finally:
        structlog.contextvars.clear_contextvars()


@router.get("/{file_id}/signed-url")
async def get_signed_url(
    file_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    blob_logger.info("signed_url_requested", file_id=file_id)

    file = await db.get(File, file_id)
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    if not file.blob_path:
        raise HTTPException(status_code=400, detail="File has no blob path")
    if not file.container_id:
        raise HTTPException(status_code=400, detail="File has no container config")

    config = await _get_container_config(db, file.container_id)
    account_name, account_key = _parse_connection_string(config.connection_string)

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=config.container_name,
        blob_name=file.blob_path,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=15),
        content_disposition=f'inline; filename="{file.name}"',
        content_type=file.content_type or "application/octet-stream",
    )

    signed_url = (
        f"https://{account_name}.blob.core.windows.net/"
        f"{config.container_name}/{file.blob_path}?{sas_token}"
    )
    blob_logger.info("signed_url_generated", file_id=file_id, blob_path=file.blob_path, container_id=file.container_id)
    return {"signed_url": signed_url, "expires_in": 900}


@router.patch("/{file_id}", response_model=FileOut)
async def rename_file(
    file_id: str,
    body: FileRenameRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    upload_logger.info("rename_started", file_id=file_id, new_name=body.name)

    result = await db.execute(select(File).where(File.id == file_id))
    file = result.scalar_one_or_none()
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    file.name = body.name
    await db.commit()
    await db.refresh(file)

    upload_logger.info("rename_complete", file_id=file_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return _file_to_out(file)


@router.patch("/{file_id}/move", response_model=FileOut)
async def move_file(
    file_id: str,
    body: FileMoveRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Move a file to a different folder (or root if folder_id is null)."""
    start = time.perf_counter()
    upload_logger.info("move_started", file_id=file_id, target_folder=body.folder_id)

    result = await db.execute(select(File).where(File.id == file_id))
    file = result.scalar_one_or_none()
    if not file:
        raise HTTPException(status_code=404, detail="File not found")

    if body.folder_id:
        result = await db.execute(select(Folder).where(Folder.id == body.folder_id))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Target folder not found")

    file.folder_id = body.folder_id
    await db.commit()
    await db.refresh(file)

    upload_logger.info("move_complete", file_id=file_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return _file_to_out(file)


@router.delete("/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file(
    file_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    upload_logger.info("delete_started", file_id=file_id)

    result = await db.execute(select(File).where(File.id == file_id))
    file = result.scalar_one_or_none()
    if not file:
        raise HTTPException(status_code=404, detail="File not found")

    # Remove from Azure Blob Storage using container config
    blob_name = file.blob_path or file.id
    if file.container_id:
        try:
            config = await db.get(ContainerConfig, file.container_id)
            if config:
                blob_start = time.perf_counter()
                blob_logger.info("blob_delete_started", blob_name=blob_name)
                blob_service = await asyncio.to_thread(
                    BlobServiceClient.from_connection_string, config.connection_string
                )
                container_client = await asyncio.to_thread(
                    blob_service.get_container_client, config.container_name
                )
                await asyncio.to_thread(container_client.delete_blob, blob_name)
                blob_logger.info("blob_delete_complete", blob_name=blob_name, duration_ms=round((time.perf_counter() - blob_start) * 1000, 2))
        except Exception as exc:
            blob_logger.warning("blob_delete_failed", blob_name=blob_name, error=str(exc))

    db_start = time.perf_counter()
    db_logger.info("query_started", query="delete_file", file_id=file_id)
    await db.delete(file)
    await db.commit()
    db_logger.info("query_complete", query="delete_file", duration_ms=round((time.perf_counter() - db_start) * 1000, 2))

    upload_logger.info("delete_complete", file_id=file_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
    if not file:
        raise HTTPException(status_code=404, detail="File not found")

    # Remove from Azure Blob Storage
    blob_name = file.blob_path or file.id
    try:
        blob_start = time.perf_counter()
        blob_logger.info("blob_delete_started", blob_name=blob_name)
        container = _blob_container()
        await asyncio.to_thread(container.delete_blob, blob_name)
        blob_logger.info("blob_delete_complete", blob_name=blob_name, duration_ms=round((time.perf_counter() - blob_start) * 1000, 2))
    except Exception as exc:
        blob_logger.warning("blob_delete_failed", blob_name=blob_name, error=str(exc))

    db_start = time.perf_counter()
    db_logger.info("query_started", query="delete_file", file_id=file_id)
    await db.delete(file)
    await db.commit()
    db_logger.info("query_complete", query="delete_file", duration_ms=round((time.perf_counter() - db_start) * 1000, 2))

    upload_logger.info("delete_complete", file_id=file_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))


# ── GET /api/files/{file_id}/job-status ──────────────────────────────────────

@router.get("/{file_id}/job-status")
async def get_job_status(
    file_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Return the latest background job status for a file.
    Useful for polling Parquet conversion progress from the frontend.
    """
    file = await db.get(File, file_id)
    if not file:
        raise HTTPException(status_code=404, detail="File not found")

    result = await db.execute(
        select(BackgroundJob)
        .where(BackgroundJob.file_id == file_id)
        .order_by(BackgroundJob.started_at.desc())
    )
    job = result.scalars().first()

    if not job:
        return {"status": "not_started", "job_type": None,
                "error_message": None, "started_at": None, "completed_at": None,
                "progress_pct": None, "progress_phase": None}

    from app.services.parquet_service import get_progress
    live = get_progress(job.id) if job.status == "running" else None

    return {
        "job_type": job.job_type,
        "status": job.status,
        "error_message": job.error_message,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "progress_pct": live["pct"] if live else None,
        "progress_phase": live["phase"] if live else None,
    }
