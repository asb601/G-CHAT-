"""
Admin API — internal endpoints for monitoring and cost tracking.

GET  /api/admin/cost-summary
POST /api/admin/reingest-all
GET  /api/admin/domains
PATCH /api/admin/users/{user_id}/domains
PATCH /api/admin/folders/{folder_id}/domain
GET  /api/admin/files/eligible
GET  /api/admin/departments/{domain_name}/files
POST /api/admin/departments/{domain_name}/ai-assign
POST /api/admin/departments/{domain_name}/assign
DELETE /api/admin/departments/{domain_name}/files/{file_id}
GET  /api/admin/missing-parquet
POST /api/admin/retry-parquet
"""
import asyncio
import re
import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.cost_tracker import get_session_summary
from app.core.database import async_session, get_db
from app.core.logger import ingest_logger
from app.dependencies import get_current_user, require_admin
from app.agent.graph.graph import invalidate_catalog_cache
from app.models.file import File
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship
from app.models.folder import Folder
from app.models.user import User
from app.services.ingestion_service import ingest_file

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/cost-summary")
async def cost_summary(current_user: User = Depends(get_current_user)) -> dict:
    return get_session_summary()


# ── Re-ingest all files ──────────────────────────────────────────────────────

_REINGEST_SEMAPHORE = asyncio.Semaphore(2)  # 2 concurrent ingests — safer for Azure API limits


async def _batch_reingest(file_ids: list[str]) -> None:
    """Re-ingest a list of files with concurrency capped at 2."""
    done = 0
    failed = 0

    async def _one(file_id: str) -> None:
        nonlocal done, failed
        async with _REINGEST_SEMAPHORE:
            trace_id = f"reingest-{uuid.uuid4().hex[:12]}"
            structlog.contextvars.clear_contextvars()
            structlog.contextvars.bind_contextvars(
                trace_id=trace_id, pipeline="reingest", file_id=file_id
            )
            try:
                async with async_session() as db:
                    await ingest_file(file_id, db)
                # ingest_file catches all exceptions internally and sets
                # ingest_status="failed" — check the actual outcome so the
                # failed counter is accurate (not always 0).
                async with async_session() as check_db:
                    f = await check_db.get(File, file_id)
                if f and f.ingest_status == "failed":
                    failed += 1
                else:
                    done += 1
                ingest_logger.info("reingest_progress", done=done, failed=failed,
                                   remaining=len(file_ids) - done - failed)
            except Exception as exc:
                failed += 1
                ingest_logger.exception("reingest_crashed", error=str(exc)[:500])
            finally:
                structlog.contextvars.clear_contextvars()

    await asyncio.gather(*[_one(fid) for fid in file_ids])
    invalidate_catalog_cache()
    ingest_logger.info("reingest_complete", done=done, failed=failed)


@router.post("/reingest-all")
async def reingest_all(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Wipe all metadata / analytics / relationships, reset every
    CSV/TXT/TSV file to not_ingested, and re-run the full pipeline.
    """
    # Find all ingestable files
    result = await db.execute(select(File))
    all_files = list(result.scalars().all())
    ingestable = [
        f for f in all_files
        if (f.name or "").rsplit(".", 1)[-1].lower()
        in ("csv", "txt", "tsv", "tab", "xlsx", "xls", "xlsm")
    ]
    if not ingestable:
        raise HTTPException(status_code=400, detail="No ingestable files found.")

    file_ids = [f.id for f in ingestable]

    # Delete old metadata, analytics, relationships
    await db.execute(delete(FileRelationship))
    await db.execute(delete(FileAnalytics))
    await db.execute(delete(FileMetadata))

    # Reset ingest status
    await db.execute(
        update(File)
        .where(File.id.in_(file_ids))
        .values(ingest_status="not_ingested")
    )
    await db.commit()
    invalidate_catalog_cache()

    ingest_logger.info("reingest_all_started", admin_id=admin.id, file_count=len(file_ids))

    # Fire background task
    asyncio.create_task(_batch_reingest(file_ids))

    return {"message": "Re-ingestion started", "file_count": len(file_ids)}


# ── Domain access control ────────────────────────────────────────────────────

class _UserDomainsBody(BaseModel):
    allowed_domains: list[str] | None  # None = unrestricted


class _FolderDomainBody(BaseModel):
    domain_tag: str | None  # None = untagged (public)


class _CreateDomainBody(BaseModel):
    name: str


@router.get("/domains")
async def list_domains(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return all distinct non-null domain tags currently in use on folders."""
    rows = (await db.execute(
        select(Folder.domain_tag).where(Folder.domain_tag.isnot(None)).distinct()
    )).scalars().all()
    return {"domains": sorted(rows)}


@router.post("/domains")
async def create_domain(
    body: _CreateDomainBody,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Create a new domain by registering a top-level system folder tagged with it.
    The folder becomes the home for files belonging to that domain.
    """
    import uuid as _uuid
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Domain name cannot be empty")

    # Check for existing domain folder with same tag
    existing = (await db.execute(
        select(Folder).where(Folder.domain_tag == name).limit(1)
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail=f"Domain '{name}' already exists")

    folder = Folder(
        id=str(_uuid.uuid4()),
        name=name,
        owner_id=admin.id,
        parent_id=None,
        container_id=None,
        domain_tag=name,
    )
    db.add(folder)
    await db.commit()
    return {"domain": name, "folder_id": folder.id}


@router.patch("/users/{user_id}/domains")
async def set_user_domains(
    user_id: str,
    body: _UserDomainsBody,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Set a user's allowed_domains list.
    Pass null to remove restrictions (unrestricted access).
    """
    user = await db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    # Normalise: empty list → None (unrestricted)
    domains = body.allowed_domains if body.allowed_domains else None
    await db.execute(
        update(User).where(User.id == user_id).values(allowed_domains=domains)
    )
    await db.commit()
    return {"user_id": user_id, "allowed_domains": domains}


@router.patch("/folders/{folder_id}/domain")
async def set_folder_domain(
    folder_id: str,
    body: _FolderDomainBody,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Tag a folder with a domain string (e.g. "finance", "hr").
    Pass null to remove the tag (folder becomes public).
    """
    folder = await db.get(Folder, folder_id)
    if folder is None:
        raise HTTPException(status_code=404, detail="Folder not found")
    await db.execute(
        update(Folder).where(Folder.id == folder_id).values(domain_tag=body.domain_tag or None)
    )
    await db.commit()
    return {"folder_id": folder_id, "domain_tag": body.domain_tag or None}


# ── Department ↔ File assignment ─────────────────────────────────────────────

class _BulkAssignBody(BaseModel):
    file_ids: list[str]


def _score_file_for_domain(good_for: list, ai_description: str | None,
                            key_metrics: list, key_dimensions: list,
                            file_name: str, domain_name: str) -> float:
    """
    Keyword-score a file's metadata against a domain name.
    Returns a float ≥ 0. Files with score ≥ 1.0 are considered a match.
    """
    domain_words = set(re.split(r"[\s\-_/,]+", domain_name.lower())) - {
        "", "and", "the", "of", "for", "a", "an", "in", "to"
    }
    if not domain_words:
        return 0.0

    score = 0.0
    desc_lower = (ai_description or "").lower()
    name_lower = file_name.lower()

    for word in domain_words:
        # good_for describes use cases → highest weight
        for item in (good_for or []):
            if word in str(item).lower():
                score += 2.0

        # ai_description
        if word in desc_lower:
            score += 1.0

        # key_metrics
        for m in (key_metrics or []):
            if word in str(m).lower():
                score += 1.5

        # key_dimensions
        for d in (key_dimensions or []):
            if word in str(d).lower():
                score += 1.0

        # file name itself
        if word in name_lower:
            score += 0.5

    return score


@router.get("/files/eligible")
async def list_eligible_files(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Return all files with their AI metadata — used by the manual file picker
    and the AI-sort preview in the Profile → Domains tab.
    """
    rows = (await db.execute(
        select(File, FileMetadata, Folder)
        .join(FileMetadata, FileMetadata.file_id == File.id, isouter=True)
        .join(Folder, Folder.id == File.folder_id, isouter=True)
        .order_by(File.name)
    )).all()

    files = []
    for file, meta, folder in rows:
        files.append({
            "file_id": file.id,
            "name": file.name,
            "folder_id": file.folder_id,
            "current_domain": folder.domain_tag if folder else None,
            "ai_description": meta.ai_description if meta else None,
            "good_for": meta.good_for if meta else [],
            "key_metrics": meta.key_metrics if meta else [],
            "key_dimensions": meta.key_dimensions if meta else [],
            "ingest_status": file.ingest_status,
        })

    return {"files": files, "count": len(files)}


@router.get("/departments/{domain_name}/files")
async def list_department_files(
    domain_name: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return files currently assigned to a department's folder."""
    domain_folder = (await db.execute(
        select(Folder).where(Folder.domain_tag == domain_name).limit(1)
    )).scalar_one_or_none()
    if not domain_folder:
        raise HTTPException(status_code=404, detail=f"Department '{domain_name}' not found")

    files = (await db.execute(
        select(File).where(File.folder_id == domain_folder.id).order_by(File.name)
    )).scalars().all()

    return {
        "domain": domain_name,
        "folder_id": domain_folder.id,
        "files": [
            {"file_id": f.id, "name": f.name, "ingest_status": f.ingest_status}
            for f in files
        ],
        "count": len(files),
    }


@router.post("/departments/{domain_name}/ai-assign")
async def ai_assign_files_to_department(
    domain_name: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    AI keyword-match: reads each file's good_for, ai_description, key_metrics
    and assigns files where score ≥ 1.0 to this department's folder.
    Only touches files that are not already in a domain-tagged folder.
    """
    domain_folder = (await db.execute(
        select(Folder).where(Folder.domain_tag == domain_name).limit(1)
    )).scalar_one_or_none()
    if not domain_folder:
        raise HTTPException(status_code=404, detail=f"Department '{domain_name}' not found")

    rows = (await db.execute(
        select(File, FileMetadata, Folder)
        .join(FileMetadata, FileMetadata.file_id == File.id, isouter=True)
        .join(Folder, Folder.id == File.folder_id, isouter=True)
    )).all()

    assigned_ids: list[str] = []
    assigned_names: list[str] = []

    for file, meta, existing_folder in rows:
        # Skip files already pinned to a domain (respect existing assignments)
        if existing_folder and existing_folder.domain_tag:
            continue
        # Skip files without metadata (not yet ingested)
        if not meta:
            continue

        score = _score_file_for_domain(
            good_for=meta.good_for or [],
            ai_description=meta.ai_description,
            key_metrics=meta.key_metrics or [],
            key_dimensions=meta.key_dimensions or [],
            file_name=file.name,
            domain_name=domain_name,
        )

        if score >= 1.0:
            assigned_ids.append(file.id)
            assigned_names.append(file.name)

    if assigned_ids:
        await db.execute(
            update(File)
            .where(File.id.in_(assigned_ids))
            .values(folder_id=domain_folder.id)
        )
        await db.commit()
        invalidate_catalog_cache()

    return {
        "domain": domain_name,
        "assigned_count": len(assigned_ids),
        "assigned_files": assigned_names,
    }


@router.post("/departments/{domain_name}/assign")
async def manually_assign_files_to_department(
    domain_name: str,
    body: _BulkAssignBody,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Manually assign a list of file IDs to a department's folder."""
    if not body.file_ids:
        raise HTTPException(status_code=400, detail="No file_ids provided")

    domain_folder = (await db.execute(
        select(Folder).where(Folder.domain_tag == domain_name).limit(1)
    )).scalar_one_or_none()
    if not domain_folder:
        raise HTTPException(status_code=404, detail=f"Department '{domain_name}' not found")

    await db.execute(
        update(File)
        .where(File.id.in_(body.file_ids))
        .values(folder_id=domain_folder.id)
    )
    await db.commit()
    invalidate_catalog_cache()

    return {"domain": domain_name, "assigned_count": len(body.file_ids)}


@router.delete("/departments/{domain_name}/files/{file_id}")
async def remove_file_from_department(
    domain_name: str,
    file_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Remove a file from a department by unsetting its folder assignment."""
    domain_folder = (await db.execute(
        select(Folder).where(Folder.domain_tag == domain_name).limit(1)
    )).scalar_one_or_none()
    if not domain_folder:
        raise HTTPException(status_code=404, detail=f"Department '{domain_name}' not found")

    file = await db.get(File, file_id)
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    if file.folder_id != domain_folder.id:
        raise HTTPException(status_code=400, detail="File is not in this department")

    await db.execute(
        update(File).where(File.id == file_id).values(folder_id=None)
    )
    await db.commit()
    invalidate_catalog_cache()

    return {"file_id": file_id, "domain": domain_name, "unassigned": True}


# ── Parquet status / retry ────────────────────────────────────────────────────

@router.get("/missing-parquet")
async def list_missing_parquet(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Return all CSV/TSV files that have been ingested but whose Parquet
    conversion record is missing or incomplete.
    """
    from app.models.file_analytics import FileAnalytics as _FA

    rows = (await db.execute(
        select(File, _FA)
        .join(_FA, _FA.file_id == File.id, isouter=True)
        .where(File.ingest_status == "ingested")
    )).all()

    missing = []
    for file, analytics in rows:
        ext = (file.name or "").rsplit(".", 1)[-1].lower()
        if ext not in ("csv", "txt", "tsv"):
            continue
        # A file is "missing parquet" if analytics doesn't exist OR parquet path is empty
        if analytics is None or not analytics.parquet_blob_path:
            missing.append({
                "file_id": file.id,
                "name": file.name,
                "blob_path": file.blob_path,
                "has_analytics": analytics is not None,
            })

    return {"files": missing, "count": len(missing)}


@router.post("/retry-parquet")
async def retry_missing_parquet(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Smart retry for files missing their Parquet conversion OR whose preprocessed
    blob was deleted.  Two categories are handled:

    1. Orphaned preprocessed blobs — files with is_preprocessed=True but the
       preprocessed/ CSV no longer exists in Azure.  ingest_file() will auto-
       recover these by finding the original raw blob and re-running the full
       pipeline (preprocessing → AI description → parquet).

    2. Ingested CSV files with no parquet_blob_path — normal parquet-only retry.
    """
    # Category 1: files that look preprocessed but blob_path is missing or
    # points to a preprocessed/ path (likely orphaned after a clean-up)
    orphaned_result = await db.execute(
        select(File).where(
            File.is_preprocessed == True,  # noqa: E712
            File.blob_path.like("preprocessed/%"),
        )
    )
    orphaned = list(orphaned_result.scalars().all())
    orphaned_ids = [
        f.id for f in orphaned
        if (f.name or "").rsplit(".", 1)[-1].lower() in ("csv", "txt", "tsv", "xlsx", "xls")
    ]

    # Category 2: ingested CSVs with no parquet yet
    no_parquet_result = await db.execute(
        select(File, FileAnalytics)
        .join(FileAnalytics, FileAnalytics.file_id == File.id, isouter=True)
        .where(
            File.ingest_status == "ingested",
            File.blob_path.isnot(None),
        )
    )
    no_parquet_ids = [
        f.id for f, fa in no_parquet_result.all()
        if (f.name or "").rsplit(".", 1)[-1].lower() in ("csv", "txt", "tsv")
        and (fa is None or not fa.parquet_blob_path)
        and f.id not in orphaned_ids
    ]

    all_ids = orphaned_ids + no_parquet_ids

    ingest_logger.info(
        "retry_parquet_started",
        admin_id=admin.id,
        orphaned=len(orphaned_ids),
        missing_parquet=len(no_parquet_ids),
        total=len(all_ids),
    )
    if all_ids:
        asyncio.create_task(_batch_reingest(all_ids))

    return {
        "message": "Parquet retry started",
        "orphaned_blobs": len(orphaned_ids),
        "missing_parquet": len(no_parquet_ids),
        "total": len(all_ids),
    }
