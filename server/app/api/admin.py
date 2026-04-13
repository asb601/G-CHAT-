"""
Admin API — internal endpoints for monitoring and cost tracking.

GET /api/admin/cost-summary
    Returns the live session cost breakdown: LLM tokens + USD, Azure blob USD, combined total.
    Auth: any logged-in user (not restricted to admins — it's a read-only view).
"""
from fastapi import APIRouter, Depends

from app.core.cost_tracker import get_session_summary
from app.core.security import get_current_user
from app.models.user import User

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/cost-summary")
async def cost_summary(current_user: User = Depends(get_current_user)) -> dict:
    """
    Return the cumulative cost and usage for the current server session.

    Resets to zero when the server restarts. Covers:
      - Every LLM call (generate_sql, select_relevant_files, format_response, …)
      - Every Azure Blob download/upload triggered by a Parquet conversion

    Example response:
    {
      "llm_calls": 14,
      "llm_cost_usd": 0.002134,
      "llm_prompt_tokens": 18400,
      "llm_completion_tokens": 3200,
      "azure_ops": 2,
      "azure_cost_usd": 0.260874,
      "azure_bytes_in_mb": 3072.4,
      "azure_bytes_out_mb": 614.2,
      "total_cost_usd": 0.263008
    }
    """
    return get_session_summary()
