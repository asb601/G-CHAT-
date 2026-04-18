"""
Logs API — stream log files from the server for debugging.

GET /api/logs/files           → list available log files
GET /api/logs/{filename}      → tail N lines from a log file
GET /api/logs/{filename}/search?q=...  → search a log file
GET /api/logs/file-timings    → upload + ingestion + parquet timing per file

Auth: admin only (ADMIN_EMAIL from settings).
"""
from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logger import LOG_DIR
from app.core.security import require_admin
from app.models.background_job import BackgroundJob
from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.models.user import User

router = APIRouter(prefix="/logs", tags=["logs"])

# Only allow reading known log files — prevent path traversal
_ALLOWED_FILES = {"system.log", "ai_pipeline.log", "llm_calls.log", "costs.log"}


def _safe_log_path(filename: str) -> Path:
    """Resolve filename and ensure it's within LOG_DIR and in the allowed set."""
    # Strip any path components — only allow bare filenames
    clean = Path(filename).name
    if clean not in _ALLOWED_FILES:
        raise HTTPException(status_code=404, detail=f"Unknown log file: {clean}")
    path = (LOG_DIR / clean).resolve()
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Log file not found: {clean}")
    return path


@router.get("/files")
async def list_log_files(_: User = Depends(require_admin)) -> dict:
    """List available log files with sizes."""
    files = []
    for name in sorted(_ALLOWED_FILES):
        path = LOG_DIR / name
        if path.exists():
            size_kb = round(path.stat().st_size / 1024, 1)
            files.append({"name": name, "size_kb": size_kb})
    return {"log_dir": str(LOG_DIR), "files": files}


@router.get("/file-timings")
async def file_timings(
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    """Return upload, ingestion, and parquet conversion timing per file (most recent first)."""
    files_result = await db.execute(
        select(File).order_by(File.created_at.desc()).limit(limit)
    )
    files = files_result.scalars().all()
    if not files:
        return {"files": []}

    file_ids = [f.id for f in files]

    meta_result = await db.execute(
        select(FileMetadata).where(FileMetadata.file_id.in_(file_ids))
    )
    meta_map = {m.file_id: m for m in meta_result.scalars().all()}

    jobs_result = await db.execute(
        select(BackgroundJob).where(
            BackgroundJob.file_id.in_(file_ids),
            BackgroundJob.job_type == "parquet_conversion",
        )
    )
    jobs_map = {j.file_id: j for j in jobs_result.scalars().all()}

    rows = []
    for f in files:
        meta = meta_map.get(f.id)
        job = jobs_map.get(f.id)

        upload_secs = f.upload_duration_secs

        ingestion_secs = None
        if meta and meta.ingested_at and f.created_at:
            ingestion_secs = round((meta.ingested_at - f.created_at).total_seconds(), 1)

        parquet_secs = None
        if job and job.completed_at and job.started_at:
            parquet_secs = round((job.completed_at - job.started_at).total_seconds(), 1)

        # Processing = ingestion + parquet (complete server-side time)
        processing_secs = None
        if ingestion_secs is not None:
            processing_secs = ingestion_secs
            if parquet_secs is not None:
                processing_secs = round(processing_secs + parquet_secs, 1)

        # Total = upload + processing (end-to-end)
        total_secs = None
        if upload_secs is not None and processing_secs is not None:
            total_secs = round(upload_secs + processing_secs, 1)

        rows.append({
            "file_id": f.id,
            "name": f.name,
            "size": f.size,
            "ingest_status": f.ingest_status,
            "uploaded_at": f.created_at.isoformat() if f.created_at else None,
            "upload_secs": upload_secs,
            "ingested_at": meta.ingested_at.isoformat() if meta and meta.ingested_at else None,
            "ingestion_secs": ingestion_secs,
            "parquet_status": job.status if job else None,
            "parquet_secs": parquet_secs,
            "processing_secs": processing_secs,
            "total_secs": total_secs,
            "parquet_error": job.error_message if job else None,
        })

    return {"files": rows}


@router.get("/{filename}")
async def tail_log(
    filename: str,
    lines: int = Query(default=100, ge=1, le=2000),
    _: User = Depends(require_admin),
) -> dict:
    """Return the last N lines of a log file (default 100, max 2000)."""
    path = _safe_log_path(filename)
    all_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = all_lines[-lines:]

    # Try to parse each line as JSON for structured output
    parsed = []
    for line in tail:
        try:
            parsed.append(json.loads(line))
        except (json.JSONDecodeError, ValueError):
            parsed.append({"raw": line})

    return {"file": filename, "total_lines": len(all_lines), "returned": len(parsed), "lines": parsed}


@router.get("/{filename}/search")
async def search_log(
    filename: str,
    q: str = Query(..., min_length=1, max_length=200),
    lines: int = Query(default=50, ge=1, le=500),
    _: User = Depends(require_admin),
) -> dict:
    """Search a log file for lines containing query string (case-insensitive)."""
    path = _safe_log_path(filename)
    q_lower = q.lower()
    all_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    matches = []
    for i, line in enumerate(all_lines):
        if q_lower in line.lower():
            try:
                matches.append({"line_num": i + 1, "data": json.loads(line)})
            except (json.JSONDecodeError, ValueError):
                matches.append({"line_num": i + 1, "data": {"raw": line}})
            if len(matches) >= lines:
                break

    return {"file": filename, "query": q, "matches": len(matches), "lines": matches}
