"""Agent state type shared across all modules."""
from __future__ import annotations

from typing import Annotated

from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    catalog: list[dict]
    relationships: list[dict]
    connection_string: str
    container_name: str
    parquet_blob_path: str | None  # prefer Parquet reads when available
    tool_call_count: int
    request_id: str


MAX_TOOL_CALLS = 6  # reduced from 12 — keeps responses under 3 minutes
