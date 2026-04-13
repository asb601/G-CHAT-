import asyncio
import mimetypes
import time
import uuid
from datetime import datetime, timezone

import structlog
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import async_session, get_db
from app.core.logger import container_logger, blob_logger, db_logger, ingest_logger
from app.core.security import require_admin, get_current_user
from app.services.ingestion_service import ingest_file
from app.models.background_job import BackgroundJob
from app.models.container import ContainerConfig
from app.models.file import File
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.folder import Folder
from app.models.user import User
from app.schemas.container import ContainerCreate, ContainerOut, ContainerSyncResponse

router = APIRouter(prefix="/containers", tags=["containers"])


# ── helpers ──────────────────────────────────────────────────────────────────


async def _get_or_create_folder_path(
    path_parts: list[str],
    container_id: str,
    owner_id: str,
    db: AsyncSession,
) -> str | None:
    """Recursively create folder hierarchy from blob path segments. Return deepest folder id."""
    if not path_parts:
        return None
    parent_id: str | None = None
    for part in path_parts:
        result = await db.execute(
            select(Folder).where(
                Folder.name == part,
                Folder.parent_id == parent_id if parent_id else Folder.parent_id.is_(None),
                Folder.container_id == container_id,
            )
        )
        folder = result.scalar_one_or_none()
        if not folder:
            folder = Folder(
                id=str(uuid.uuid4()),
                name=part,
                parent_id=parent_id,
                owner_id=owner_id,
                container_id=container_id,
            )
            db.add(folder)
            await db.flush()
        parent_id = folder.id
    return parent_id


def _guess_mime(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"


async def sync_container(container_id: str, user_id: str) -> None:
    """Background task: list all blobs in an Azure container and upsert file records."""
    async with async_session() as db:
        try:
            start = time.perf_counter()
            container_logger.info("sync_started", container_id=container_id)

            result = await db.execute(
                select(ContainerConfig).where(ContainerConfig.id == container_id)
            )
            config = result.scalar_one_or_none()
            if not config:
                container_logger.error("sync_failed", container_id=container_id, reason="not_found")
                return

            # All Azure SDK calls are synchronous — run in thread
            blob_start = time.perf_counter()
            blob_logger.info("blob_list_started", container_name=config.container_name)
            blob_service = await asyncio.to_thread(
                BlobServiceClient.from_connection_string, config.connection_string
            )
            container_client = await asyncio.to_thread(
                blob_service.get_container_client, config.container_name
            )
            blobs = await asyncio.to_thread(lambda: list(container_client.list_blobs()))
            blob_logger.info("blob_list_complete", blob_count=len(blobs), duration_ms=round((time.perf_counter() - blob_start) * 1000, 2))

            # Build set of blob paths currently in Azure
            remote_paths = {blob.name for blob in blobs if not blob.name.endswith("/")}

            # ── Remove files deleted from Azure ──
            db_files_result = await db.execute(
                select(File).where(File.container_id == container_id)
            )
            db_files = list(db_files_result.scalars().all())
            removed = 0
            for db_file in db_files:
                if db_file.blob_path not in remote_paths:
                    await db.delete(db_file)
                    removed += 1
            if removed:
                container_logger.info("sync_removed_stale", container_id=container_id, removed=removed)

            # ── Add new files from Azure ──
            synced = 0
            for blob in blobs:
                if blob.name.endswith("/"):
                    continue

                # Check if already synced
                existing = await db.execute(
                    select(File.id).where(File.blob_path == blob.name, File.container_id == container_id)
                )
                if existing.scalar_one_or_none():
                    continue

                parts = blob.name.split("/")
                filename = parts[-1]
                folder_parts = parts[:-1]

                parent_folder_id = await _get_or_create_folder_path(
                    folder_parts, container_id, user_id, db
                )

                file = File(
                    id=str(uuid.uuid4()),
                    name=filename,
                    content_type=_guess_mime(filename),
                    size=blob.size or 0,
                    folder_id=parent_folder_id,
                    owner_id=user_id,
                    container_id=container_id,
                    blob_path=blob.name,
                    ingest_status="not_ingested",
                )
                db.add(file)
                synced += 1

            # ── Clean up empty folders for this container ──
            orphan_result = await db.execute(
                select(Folder).where(Folder.container_id == container_id)
            )
            orphan_folders = list(orphan_result.scalars().all())
            cleaned_folders = 0
            for folder in orphan_folders:
                child_files = await db.execute(
                    select(File.id).where(File.folder_id == folder.id).limit(1)
                )
                child_folders = await db.execute(
                    select(Folder.id).where(Folder.parent_id == folder.id).limit(1)
                )
                if not child_files.scalar_one_or_none() and not child_folders.scalar_one_or_none():
                    await db.delete(folder)
                    cleaned_folders += 1
            if cleaned_folders:
                container_logger.info("sync_cleaned_folders", container_id=container_id, removed=cleaned_folders)

            config.last_synced_at = datetime.now(timezone.utc)
            await db.commit()
            container_logger.info("sync_complete", container_id=container_id, added=synced, removed=removed, duration_ms=round((time.perf_counter() - start) * 1000, 2))

            # ── Auto-ingest un-ingested CSV/TXT files ──
            ingestable_result = await db.execute(
                select(File).where(
                    File.container_id == container_id,
                    File.ingest_status == "not_ingested",
                )
            )
            ingestable = [
                f for f in ingestable_result.scalars().all()
                if (f.name or "").rsplit(".", 1)[-1].lower() in ("csv", "txt", "tsv")
            ]
            if ingestable:
                ingest_logger.info("sync_auto_ingest", container_id=container_id, file_count=len(ingestable))
                asyncio.create_task(_batch_ingest_from_sync(
                    [f.id for f in ingestable], container_id
                ))

        except Exception as exc:
            container_logger.exception("sync_failed", container_id=container_id, error=str(exc))
            await db.rollback()


async def _background_ingest_from_sync(file_id: str) -> None:
    """Run ingestion for a single file in its own DB session after sync."""
    trace_id = f"ingest-{uuid.uuid4().hex[:12]}"
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(trace_id=trace_id, pipeline="ingest", file_id=file_id)
    try:
        async with async_session() as db:
            await ingest_file(file_id, db)
    except Exception as exc:
        ingest_logger.exception("sync_ingest_crashed", error=str(exc)[:500])
    finally:
        structlog.contextvars.clear_contextvars()


# Max concurrent ingests during a sync — prevents RAM saturation and Azure OpenAI rate limits.
# 3 = one file reading from Azure, one running AI description, one doing Parquet conversion.
_INGEST_SEMAPHORE = asyncio.Semaphore(3)


async def _batch_ingest_from_sync(file_ids: list[str], container_id: str) -> None:
    """Process file IDs with concurrency capped at 3. Replaces fire-all-at-once."""
    ingest_logger.info("batch_ingest_started", container_id=container_id, total=len(file_ids))
    done = 0
    failed = 0

    async def _one(file_id: str) -> None:
        nonlocal done, failed
        async with _INGEST_SEMAPHORE:
            trace_id = f"ingest-{uuid.uuid4().hex[:12]}"
            structlog.contextvars.clear_contextvars()
            structlog.contextvars.bind_contextvars(
                trace_id=trace_id, pipeline="ingest", file_id=file_id
            )
            try:
                async with async_session() as db:
                    await ingest_file(file_id, db)
                done += 1
                ingest_logger.info("batch_ingest_progress", container_id=container_id,
                                   done=done, failed=failed,
                                   remaining=len(file_ids) - done - failed)
            except Exception as exc:
                failed += 1
                ingest_logger.exception("sync_ingest_crashed", error=str(exc)[:500])
            finally:
                structlog.contextvars.clear_contextvars()

    await asyncio.gather(*[_one(fid) for fid in file_ids])
    ingest_logger.info("batch_ingest_complete", container_id=container_id,
                       done=done, failed=failed)


# ── endpoints ────────────────────────────────────────────────────────────────


@router.get("", response_model=list[ContainerOut])
async def list_containers(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    container_logger.info("list_requested")

    result = await db.execute(
        select(
            ContainerConfig,
            func.count(File.id).label("file_count"),
        )
        .outerjoin(File, File.container_id == ContainerConfig.id)
        .group_by(ContainerConfig.id)
        .order_by(ContainerConfig.created_at.desc())
    )
    rows = result.all()
    containers = [
        ContainerOut(
            id=config.id,
            name=config.name,
            container_name=config.container_name,
            last_synced_at=config.last_synced_at,
            file_count=file_count,
            created_at=config.created_at,
        )
        for config, file_count in rows
    ]
    container_logger.info("list_complete", count=len(containers), duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return containers


@router.get("/{container_id}", response_model=ContainerOut)
async def get_container(
    container_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(
            ContainerConfig,
            func.count(File.id).label("file_count"),
        )
        .outerjoin(File, File.container_id == ContainerConfig.id)
        .where(ContainerConfig.id == container_id)
        .group_by(ContainerConfig.id)
    )
    row = result.one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Container not found")
    config, file_count = row
    return ContainerOut(
        id=config.id,
        name=config.name,
        container_name=config.container_name,
        last_synced_at=config.last_synced_at,
        file_count=file_count,
        created_at=config.created_at,
    )


@router.post("", response_model=ContainerOut, status_code=status.HTTP_201_CREATED)
async def create_container(
    body: ContainerCreate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    container_logger.info("create_started", name=body.name, container_name=body.container_name)

    # Validate connection by trying to access the container
    try:
        blob_start = time.perf_counter()
        blob_logger.info("container_validate_started", container_name=body.container_name)
        blob_service = await asyncio.to_thread(
            BlobServiceClient.from_connection_string, body.connection_string
        )
        container_client = await asyncio.to_thread(
            blob_service.get_container_client, body.container_name
        )
        await asyncio.to_thread(container_client.get_container_properties)
        blob_logger.info("container_validate_complete", duration_ms=round((time.perf_counter() - blob_start) * 1000, 2))
    except Exception as e:
        container_logger.error("create_failed", reason="connection_error", error=str(e))
        raise HTTPException(
            status_code=400,
            detail=f"Cannot connect to Azure container '{body.container_name}': {e}",
        )

    config = ContainerConfig(
        name=body.name,
        container_name=body.container_name,
        connection_string=body.connection_string,
        created_by=admin.id,
    )
    db.add(config)
    await db.commit()
    await db.refresh(config)

    # Kick off background sync
    asyncio.create_task(sync_container(config.id, admin.id))

    container_logger.info("create_complete", container_id=config.id, duration_ms=round((time.perf_counter() - start) * 1000, 2))

    return ContainerOut(
        id=config.id,
        name=config.name,
        container_name=config.container_name,
        last_synced_at=config.last_synced_at,
        file_count=0,
        created_at=config.created_at,
    )


@router.post("/{container_id}/sync", response_model=ContainerSyncResponse)
async def trigger_sync(
    container_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ContainerConfig).where(ContainerConfig.id == container_id)
    )
    config = result.scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail="Container not found")

    asyncio.create_task(sync_container(container_id, admin.id))
    return ContainerSyncResponse(message="Sync started", container_id=container_id)


@router.delete("/{container_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_container(
    container_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    container_logger.info("delete_started", container_id=container_id)

    result = await db.execute(
        select(ContainerConfig).where(ContainerConfig.id == container_id)
    )
    config = result.scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail="Container not found")

    # Delete all files and folders belonging to this container
    # First get all file IDs so we can clean up dependent tables
    file_id_rows = (await db.execute(
        select(File.id).where(File.container_id == container_id)
    )).scalars().all()

    if file_id_rows:
        await db.execute(
            FileMetadata.__table__.delete().where(FileMetadata.file_id.in_(file_id_rows))
        )
        await db.execute(
            FileAnalytics.__table__.delete().where(FileAnalytics.file_id.in_(file_id_rows))
        )
        await db.execute(
            BackgroundJob.__table__.delete().where(BackgroundJob.file_id.in_(file_id_rows))
        )

    await db.execute(
        File.__table__.delete().where(File.container_id == container_id)
    )
    await db.execute(
        Folder.__table__.delete().where(Folder.container_id == container_id)
    )
    await db.delete(config)
    await db.commit()

    # Evict DuckDB thread-local connection so the connection string is no longer held in RAM
    from app.core.duckdb_client import _clear_connection
    _clear_connection(config.connection_string)

    container_logger.info("delete_complete", container_id=container_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
