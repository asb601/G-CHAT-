"""
Retrieval stages 2 + 3 + 4 — permission filter, date overlap, and domain filter.

Stage 2 — Permission filter
----------------------------
Returns a SQLAlchemy WHERE clause fragment that restricts FileMetadata rows
to only those the requesting user is allowed to see.

Rules:
  - Admin users (user.is_admin=True) see all files.
  - Regular users see files where:
      file.owner_id = user_id
      OR folder.owner_id = user_id  (file is in a folder they own)

Stage 3 — Date overlap filter
------------------------------
Returns a SQLAlchemy WHERE clause fragment that keeps only files whose
stored date range [date_range_start, date_range_end] overlaps the query
window [date_from, date_to].

Overlap condition (Allen's interval algebra):
  NOT (file_end < date_from  OR  file_start > date_to)
  ≡  file_start <= date_to  AND  file_end >= date_from

Files with NULL date range are included — they were ingested without a
date range (e.g. static lookup tables) and should never be filtered out.

Stage 4 — Domain filter (PHASE 15)
------------------------------------
Each folder can have an optional domain_tag (e.g. 'finance', 'hr').
Each user can have an optional allowed_domains list.

Rules:
  - If allowed_domains is None or empty → no domain restriction (sees all).
  - If allowed_domains is set → user may only see files in folders whose
    domain_tag is in the user's allowed_domains list, OR files in folders
    with no domain_tag (NULL), OR files not in any folder (folder_id IS NULL).

Public API
----------
    permission_clause(user_id, is_admin) -> ColumnElement
    date_overlap_clause(date_from, date_to) -> ColumnElement | None
    domain_clause(allowed_domains) -> ColumnElement | None
    build_base_query(user_id, is_admin, date_from, date_to, allowed_domains) -> Select
"""
from __future__ import annotations

from datetime import date

from sqlalchemy import and_, or_, select
from sqlalchemy.sql.elements import ColumnElement

from app.models.file import File
from app.models.file_metadata import FileMetadata
from app.models.folder import Folder


# ---------------------------------------------------------------------------
# Stage 2 — Permission filter
# ---------------------------------------------------------------------------

def permission_clause(user_id: str, is_admin: bool) -> ColumnElement:
    """
    Return a WHERE fragment for FileMetadata that enforces visibility rules.

    Admins: no restriction — all files visible.
    Regular users: file.owner_id = user_id
                   OR folder the file lives in is owned by user_id.

    The clause JOINs File on FileMetadata.file_id and (optionally) Folder.
    Callers must ensure the query already JOINs to File.
    """
    if is_admin:
        # Literal true — no filtering needed
        from sqlalchemy import true
        return true()

    # File owned directly by this user
    owned_directly = File.owner_id == user_id

    # File lives in a folder that this user owns (sub-select — avoids extra JOIN)
    folder_subq = (
        select(Folder.id)
        .where(Folder.owner_id == user_id)
        .scalar_subquery()
    )
    owned_via_folder = File.folder_id.in_(folder_subq)

    return or_(owned_directly, owned_via_folder)


# ---------------------------------------------------------------------------
# Stage 3 — Date overlap filter
# ---------------------------------------------------------------------------

def date_overlap_clause(
    date_from: date | None,
    date_to: date | None,
) -> ColumnElement | None:
    """
    Return a WHERE fragment that keeps files whose date range overlaps
    [date_from, date_to].

    Returns None if both date_from and date_to are None (no filter).
    Files with NULL date range are always included.
    """
    if date_from is None and date_to is None:
        return None

    conditions = []

    if date_from is not None:
        # File must end on or after query start (or has no end date stored)
        conditions.append(
            or_(
                FileMetadata.date_range_end.is_(None),
                FileMetadata.date_range_end >= date_from,
            )
        )

    if date_to is not None:
        # File must start on or before query end (or has no start date stored)
        conditions.append(
            or_(
                FileMetadata.date_range_start.is_(None),
                FileMetadata.date_range_start <= date_to,
            )
        )

    return and_(*conditions) if len(conditions) > 1 else conditions[0]


# ---------------------------------------------------------------------------
# Stage 4 — Domain filter (PHASE 15)
# ---------------------------------------------------------------------------

def domain_clause(allowed_domains: list[str] | None) -> ColumnElement | None:
    """
    Return a WHERE fragment that restricts files by folder domain tag.

    Returns None when allowed_domains is None or empty (no restriction).

    When set, the user may see:
      - Files whose folder has domain_tag IN allowed_domains
      - Files whose folder has domain_tag IS NULL (untagged folder — always visible)
      - Files with folder_id IS NULL (not in any folder — always visible)

    Implementation: correlated sub-select on the folders table avoids an
    extra JOIN in the calling query (same pattern as permission_clause).
    """
    if not allowed_domains:
        return None

    # Folders the user is allowed to access (tagged with one of their domains)
    allowed_folder_subq = (
        select(Folder.id)
        .where(Folder.domain_tag.in_(allowed_domains))
        .scalar_subquery()
    )

    return or_(
        # File not in any folder
        File.folder_id.is_(None),
        # File in a folder with no domain tag (untagged = public)
        File.folder_id.in_(
            select(Folder.id).where(Folder.domain_tag.is_(None)).scalar_subquery()
        ),
        # File in a domain-tagged folder that the user is allowed to access
        File.folder_id.in_(allowed_folder_subq),
    )


# ---------------------------------------------------------------------------
# Combined helper — builds the base SELECT used by all retrieval stages
# ---------------------------------------------------------------------------

def build_base_query(
    user_id: str,
    is_admin: bool,
    date_from: date | None = None,
    date_to: date | None = None,
    allowed_domains: list[str] | None = None,
) -> "Select":
    """
    Return a SELECT on FileMetadata that:
      1. JOINs to File (needed for permission + domain checks)
      2. Applies permission filter (stage 2)
      3. Applies date overlap filter (stage 3) if date_from/date_to given
      4. Applies domain filter (stage 4) if allowed_domains given

    Callers add their own WHERE / ORDER BY / LIMIT on top.
    """
    q = (
        select(FileMetadata)
        .join(File, File.id == FileMetadata.file_id)
        .where(permission_clause(user_id, is_admin))
    )

    date_filter = date_overlap_clause(date_from, date_to)
    if date_filter is not None:
        q = q.where(date_filter)

    dom_filter = domain_clause(allowed_domains)
    if dom_filter is not None:
        q = q.where(dom_filter)

    return q
