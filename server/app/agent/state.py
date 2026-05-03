"""Agent state type shared across all modules."""
from __future__ import annotations

from typing import Annotated

from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    catalog: list[dict]
    connection_string: str
    container_name: str
    parquet_blob_path: str | None  # prefer Parquet reads when available
    tool_call_count: int
    request_id: str
    broaden_nudges: int  # how many times we've forced a "search wider" retry
    is_first_turn: bool  # True = turn 1 → gpt-4o; False = follow-up → gpt-4o-mini


MAX_TOOL_CALLS = 8
