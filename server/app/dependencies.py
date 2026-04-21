"""
dependencies.py — Centralised FastAPI dependency injection exports.

All route files import from here instead of directly from core modules,
keeping a single point of change if the underlying implementations move.

Imports from: core.database, core.security
"""

from app.core.database import get_db  # noqa: F401
from app.core.security import get_current_user, require_admin  # noqa: F401

__all__ = ["get_db", "get_current_user", "require_admin"]
