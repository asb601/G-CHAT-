"""Backward-compatible facade for AI client utilities.

Implementation now lives in smaller modules:
- openai_client.py
- token_counter.py
- llm_tasks.py
"""
from app.core.llm_tasks import generate_file_description  # noqa: F401
from app.core.openai_client import get_client as _get_client  # noqa: F401
from app.core.token_counter import (  # noqa: F401
    calc_cost as _calc_cost,
    count_tokens as _count_tokens,
    elapsed_ms as _ms,
    track_and_log as _track_and_log,
)

__all__ = [
    "generate_file_description",
    "_get_client",
    "_count_tokens",
    "_calc_cost",
    "_track_and_log",
    "_ms",
]
