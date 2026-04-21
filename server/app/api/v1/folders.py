import time

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logger import folder_logger, db_logger
from app.dependencies import get_current_user, require_admin
from app.api.v1.files import _file_to_out
from app.models.file import File
from app.models.folder import Folder
from app.models.user import User
from app.schemas.folder import FolderContents, FolderCreate, FolderOut, FolderUpdate

router = APIRouter(prefix="/folders", tags=["folders"])


@router.get("/{folder_id}/contents")
async def get_folder_contents(
    folder_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get direct children (subfolders + files) of a folder. Use folder_id='root' for root."""
    start = time.perf_counter()
    folder_logger.info("contents_requested", folder_id=folder_id)

    is_root = folder_id == "root"

    # Subfolders — org-wide, no owner filter
    folder_stmt = select(Folder).order_by(Folder.name)
    if is_root:
        folder_stmt = folder_stmt.where(Folder.parent_id.is_(None))
    else:
        folder_stmt = folder_stmt.where(Folder.parent_id == folder_id)

    db_start = time.perf_counter()
    db_logger.info("query_started", query="select_subfolders", folder_id=folder_id)
    folders_result = await db.execute(folder_stmt)
    folders = list(folders_result.scalars().all())
    db_logger.info("query_complete", query="select_subfolders", count=len(folders), duration_ms=round((time.perf_counter() - db_start) * 1000, 2))

    # Files — org-wide, eager load uploaded_by user
    from sqlalchemy.orm import selectinload
    file_stmt = select(File).options(selectinload(File.uploaded_by)).order_by(File.name)
    if is_root:
        file_stmt = file_stmt.where(File.folder_id.is_(None))
    else:
        file_stmt = file_stmt.where(File.folder_id == folder_id)

    db_start = time.perf_counter()
    db_logger.info("query_started", query="select_files", folder_id=folder_id)
    files_result = await db.execute(file_stmt)
    files = list(files_result.scalars().all())
    db_logger.info("query_complete", query="select_files", count=len(files), duration_ms=round((time.perf_counter() - db_start) * 1000, 2))

    folder_logger.info("contents_fetched", folder_id=folder_id, folders_count=len(folders), files_count=len(files), duration_ms=round((time.perf_counter() - start) * 1000, 2))

    from app.schemas.folder import FolderOut
    return {
        "folders": [FolderOut.model_validate(f) for f in folders],
        "files": [_file_to_out(f) for f in files],
    }


@router.post("", response_model=FolderOut, status_code=status.HTTP_201_CREATED)
async def create_folder(
    body: FolderCreate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    folder_logger.info("create_started", name=body.name, parent_id=body.parent_id)

    folder = Folder(name=body.name, parent_id=body.parent_id, owner_id=admin.id)
    db.add(folder)
    await db.commit()
    await db.refresh(folder)

    folder_logger.info("create_complete", folder_id=folder.id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return folder


@router.patch("/{folder_id}", response_model=FolderOut)
async def update_folder(
    folder_id: str,
    body: FolderUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    folder_logger.info("update_started", folder_id=folder_id)

    result = await db.execute(select(Folder).where(Folder.id == folder_id))
    folder = result.scalar_one_or_none()
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")

    if body.name is not None:
        folder.name = body.name
    if body.parent_id is not None:
        folder.parent_id = body.parent_id

    await db.commit()
    await db.refresh(folder)

    folder_logger.info("update_complete", folder_id=folder_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
    return folder


@router.delete("/{folder_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_folder(
    folder_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    start = time.perf_counter()
    folder_logger.info("delete_started", folder_id=folder_id)

    result = await db.execute(select(Folder).where(Folder.id == folder_id))
    folder = result.scalar_one_or_none()
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")

    await db.delete(folder)
    await db.commit()

    folder_logger.info("delete_complete", folder_id=folder_id, duration_ms=round((time.perf_counter() - start) * 1000, 2))
