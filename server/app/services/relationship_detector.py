"""Relationship detection between ingested files based on columns and value overlap."""
from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import ingest_logger
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship


async def detect_relationships(
    file_id: str,
    blob_path: str,
    columns_info: list[dict],
    db: AsyncSession,
) -> int:
    """
    Compare column names + sample values against every other ingested file.
    Returns count of new relationships created.
    """
    result = await db.execute(select(FileMetadata).where(FileMetadata.file_id != file_id))
    other_files = list(result.scalars().all())

    this_columns = {c["name"].lower(): c for c in columns_info}
    this_values = {
        c["name"].lower(): set(str(v) for v in (c.get("unique_values") or c.get("sample_values") or []))
        for c in columns_info
    }

    created = 0
    for other in other_files:
        if not other.columns_info:
            continue

        other_columns = {c["name"].lower(): c for c in other.columns_info}
        other_values = {
            c["name"].lower(): set(str(v) for v in (c.get("unique_values") or c.get("sample_values") or []))
            for c in other.columns_info
        }

        for col_name in this_columns:
            if col_name not in other_columns:
                continue

            this_vals = this_values.get(col_name, set())
            other_vals = other_values.get(col_name, set())
            if this_vals and other_vals:
                overlap = len(this_vals & other_vals)
                value_score = overlap / max(len(this_vals), len(other_vals))
            else:
                value_score = 0.0

            confidence = 0.5 + (value_score * 0.5)
            if confidence < 0.3:
                continue

            join_type = "INNER JOIN" if confidence > 0.7 else "LEFT JOIN"

            ingest_logger.debug(
                "relationship_candidate",
                column=col_name,
                other_file=other.blob_path,
                confidence=round(confidence, 3),
                value_overlap=round(value_score, 3),
                join_type=join_type,
            )

            for a_id, a_path, b_id, b_path in [
                (file_id, blob_path, other.file_id, other.blob_path),
                (other.file_id, other.blob_path, file_id, blob_path),
            ]:
                existing = await db.execute(
                    select(FileRelationship).where(
                        FileRelationship.file_a_id == a_id,
                        FileRelationship.file_b_id == b_id,
                        FileRelationship.shared_column == col_name,
                    )
                )
                rel = existing.scalar_one_or_none()
                if not rel:
                    rel = FileRelationship(
                        id=str(uuid.uuid4()),
                        file_a_id=a_id,
                        file_b_id=b_id,
                        file_a_path=a_path,
                        file_b_path=b_path,
                        shared_column=col_name,
                        confidence_score=confidence,
                        join_type=join_type,
                    )
                    db.add(rel)
                    created += 1
                else:
                    rel.confidence_score = confidence

    await db.commit()
    return created
