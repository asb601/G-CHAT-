"""
LangGraph agent graph — construction, nodes, and public entry point.
"""
from __future__ import annotations

import asyncio
import json
import time
import threading
import uuid
from typing import Any, Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.llm import get_llm
from app.agent.state import AgentState, MAX_TOOL_CALLS
from app.agent.tools.catalog import build_catalog_tools
from app.agent.tools.sql import build_sql_tools
from app.agent.tools.analytics import build_analytics_tool
from app.agent.tools.stats import build_stats_tool
from app.agent.tools.sample import build_sample_tool
from app.core.logger import chat_logger, llm_logger
from app.models.container import ContainerConfig
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship

# Per-request mutable stores (keyed by request_id)
_request_stores: dict[str, dict] = {}
_stores_lock = threading.Lock()


# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT_TEMPLATE = """You are a sharp, data-driven analyst with direct SQL access to structured data files.

Container: {container_name}
SQL file reference syntax:
  CSV:     read_csv_auto('az://{container_name}/filename.csv')
  Parquet: read_parquet('az://{container_name}/filename.parquet')
  Get the actual filename from search_catalog first — never use 'blob_path' literally.
{parquet_note}
{sample_note}

--- TOOLS AVAILABLE ---
- query_precomputed_analytics: instant pre-computed stats (totals, distributions, cross-tabs). Try this first.
- query_sample_rows: instant access to ~500 ingested sample rows. No SQL needed for quick row browsing.
- search_catalog: find files and their metadata.
- get_file_schema: get exact column names and sample values.
- run_aggregation / run_sql: run real SQL against the full dataset. Use parquet path when available.
- summarise_dataframe: statistical breakdown of query results.

--- HOW TO ANSWER ---
Pick the right tool(s) based on what the question actually needs:
- Pre-computed stats cover most aggregation and distribution questions instantly.
- Sample rows are useful for "show me some data" or browsing the first few rows.
- SQL is the right choice for anything that needs exact numbers, filtering, ordering, specific rows, or calculations on the full dataset — especially when parquet is available.
- You can and should combine tools: e.g. run SQL for exact counts, then use precomputed cross-tabs for the breakdown.

Always give a direct answer with the actual data. Lead with the key finding, then supporting numbers in bold.
End with one notable trend or anomaly if visible in the data.
Max {max_calls} tool calls — be efficient.
"""


# ── Agent node ────────────────────────────────────────────────────────────────

def _build_agent_node(all_tools: list):
    """Create the agent node closure with all tools pre-bound."""

    def agent_node(state: AgentState) -> dict:
        count = state.get("tool_call_count", 0)
        if count >= MAX_TOOL_CALLS:
            return {"messages": [AIMessage(content="I've gathered enough data. Let me summarise.")]}

        llm_with_tools = get_llm().bind_tools(all_tools)

        t = time.perf_counter()
        response = llm_with_tools.invoke(state["messages"])
        duration_ms = round((time.perf_counter() - t) * 1000, 2)

        usage = getattr(response, "usage_metadata", None)
        p_tok = usage.get("input_tokens", 0) if usage else 0
        c_tok = usage.get("output_tokens", 0) if usage else 0
        llm_logger.info("llm_call",
                        function="agent_node",
                        model=get_llm().deployment_name,
                        prompt_tokens=p_tok,
                        completion_tokens=c_tok,
                        total_tokens=p_tok + c_tok,
                        duration_ms=duration_ms,
                        tool_calls=len(getattr(response, "tool_calls", []) or []),
                        iteration=count + 1)

        return {
            "messages": [response],
            "tool_call_count": count + (1 if getattr(response, "tool_calls", None) else 0),
        }

    return agent_node


def _route(state: AgentState) -> Literal["tools", "__end__"]:
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and getattr(last, "tool_calls", None):
        return "tools"
    return END


# ── Graph builder ─────────────────────────────────────────────────────────────

def _build_graph(all_tools: list) -> Any:
    """Build a fresh compiled StateGraph per request."""
    tool_node = ToolNode(all_tools)
    agent_node = _build_agent_node(all_tools)

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tool_node)
    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", _route)
    builder.add_edge("tools", "agent")

    return builder.compile()


# ── Public entry point ────────────────────────────────────────────────────────

async def run_agent_query(query: str, db: AsyncSession) -> dict:
    """
    Main entry point for the agentic query pipeline.
    Returns {answer, data, chart, route, row_count, files_used, tool_calls}.
    """
    pipeline_start = time.perf_counter()

    # ── Load catalog from Postgres ──
    all_meta = list((await db.execute(select(FileMetadata))).scalars().all())
    if not all_meta:
        return {
            "answer": "No files have been ingested yet. Please upload and ingest some files first.",
            "data": [], "chart": None,
        }

    catalog = [
        {
            "file_id": m.file_id,
            "blob_path": m.blob_path,
            "container_id": m.container_id,
            "ai_description": m.ai_description or "",
            "good_for": m.good_for or [],
            "key_metrics": m.key_metrics or [],
            "key_dimensions": m.key_dimensions or [],
            "columns_info": m.columns_info or [],
            "date_range_start": str(m.date_range_start) if m.date_range_start else None,
            "date_range_end": str(m.date_range_end) if m.date_range_end else None,
        }
        for m in all_meta
    ]

    all_rels = list((await db.execute(select(FileRelationship))).scalars().all())
    relationships = [
        {
            "file_a_path": r.file_a_path,
            "file_b_path": r.file_b_path,
            "shared_column": r.shared_column,
            "confidence_score": r.confidence_score,
        }
        for r in all_rels
    ]

    # ── Resolve connection details ──
    # Use the first file's container for the DuckDB connection.
    # All files are assumed to be in the same Azure container (current design constraint).
    first_meta = all_meta[0]
    container = await db.get(ContainerConfig, first_meta.container_id)
    if not container:
        return {"answer": "Container configuration not found.", "data": [], "chart": None}

    connection_string = container.connection_string
    container_name = container.container_name

    # ── Load pre-computed analytics for ALL files ──
    # Not just [0] — each file may have its own analytics and parquet path.
    all_analytics_rows = list((await db.execute(select(FileAnalytics))).scalars().all())

    analytics_by_file: dict[str, FileAnalytics] = {
        row.file_id: row for row in all_analytics_rows
    }

    # Build precomputed dict and collect ALL known parquet paths across every file
    precomputed = None
    parquet_blob_path = None  # used by SQL tools as default hint
    parquet_paths_all: dict[str, str] = {}  # blob_path → parquet_blob_path

    for meta in all_meta:
        analytics_row = analytics_by_file.get(meta.file_id)
        if not analytics_row:
            continue
        if precomputed is None:
            # Use first available analytics for the precomputed route
            precomputed = {
                "row_count": analytics_row.row_count,
                "column_count": analytics_row.column_count,
                "column_stats": analytics_row.column_stats,
                "value_counts": analytics_row.value_counts,
                "cross_tabs": analytics_row.cross_tabs,
            }
            parquet_blob_path = analytics_row.parquet_blob_path
        if analytics_row.parquet_blob_path and meta.blob_path:
            parquet_paths_all[meta.blob_path] = analytics_row.parquet_blob_path

    sample_rows: list[dict] = []
    if first_meta.sample_rows:
        sample_rows = first_meta.sample_rows

    # ── Set up per-request state store ──
    req_id = uuid.uuid4().hex
    store: dict = {}
    with _stores_lock:
        _request_stores[req_id] = store

    # ── Build tools for this request ──
    all_tools = []
    all_tools.extend(build_analytics_tool(precomputed))
    all_tools.extend(build_catalog_tools(catalog, relationships))
    all_tools.extend(build_sql_tools(connection_string, container_name, parquet_blob_path, store))
    all_tools.extend(build_stats_tool(store))
    all_tools.extend(build_sample_tool(sample_rows))

    # ── Build graph ──
    graph = _build_graph(all_tools)

    # ── System prompt ──
    parquet_note = ""
    if parquet_paths_all:
        lines = [f"  {csv} → {pq}" for csv, pq in parquet_paths_all.items()]
        parquet_note = "Parquet files available (prefer read_parquet() for speed):\n" + "\n".join(lines)
    elif parquet_blob_path:
        parquet_note = f"\nParquet file available: {parquet_blob_path} — prefer read_parquet() for faster queries."

    sample_note = ""
    if sample_rows:
        sample_note = f"\nIngest-time sample available: {len(sample_rows)} rows (first rows of the file) — query_sample_rows() gives instant access to these."

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        container_name=container_name,
        max_calls=MAX_TOOL_CALLS,
        parquet_note=parquet_note,
        sample_note=sample_note,
    )

    initial_state: AgentState = {
        "messages": [
            SystemMessage(content=system_prompt),
            HumanMessage(content=query),
        ],
        "catalog": catalog,
        "relationships": relationships,
        "connection_string": connection_string,
        "container_name": container_name,
        "parquet_blob_path": parquet_blob_path,
        "tool_call_count": 0,
        "request_id": req_id,
    }

    chat_logger.info("agent_start",
                     query=query[:200],
                     file_count=len(catalog),
                     container=container_name,
                     has_precomputed=precomputed is not None,
                     has_parquet=parquet_blob_path is not None)

    try:
        final_state = await asyncio.to_thread(graph.invoke, initial_state)
    except Exception as exc:
        chat_logger.exception("agent_error", error=str(exc)[:400])
        return {
            "answer": "An error occurred while processing your query. Please try again.",
            "data": [], "chart": None,
        }
    finally:
        with _stores_lock:
            _request_stores.pop(req_id, None)

    # ── Extract final answer ──
    final_msgs = final_state["messages"]
    answer = ""
    for msg in reversed(final_msgs):
        if isinstance(msg, AIMessage) and not getattr(msg, "tool_calls", None) and msg.content:
            answer = msg.content if isinstance(msg.content, str) else str(msg.content)
            break

    sql_results = store.get("sql_results", [])
    tool_calls_made = final_state.get("tool_call_count", 0)
    total_ms = round((time.perf_counter() - pipeline_start) * 1000, 2)

    chat_logger.info("agent_complete",
                     tool_calls=tool_calls_made,
                     row_count=len(sql_results),
                     total_duration_ms=total_ms,
                     answer_preview=answer[:200])

    chart = _infer_chart(answer, sql_results)

    return {
        "answer": answer,
        "data": sql_results,
        "chart": chart,
        "route": "agent",
        "row_count": len(sql_results),
        "files_used": list({
            blob
            for msg in final_msgs
            if isinstance(msg, ToolMessage)
            for blob in _extract_blob_paths(msg.content)
        }),
        "tool_calls": tool_calls_made,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _infer_chart(answer: str, rows: list[dict]) -> dict | None:
    if not rows:
        return None
    cols = list(rows[0].keys())
    numeric_cols = [c for c in cols if isinstance(rows[0].get(c), (int, float))]
    if not numeric_cols:
        return None

    low = answer.lower()
    chart_type = "bar"
    if any(w in low for w in ("over time", "trend", "monthly", "daily", "weekly", "yearly")):
        chart_type = "line"
    elif any(w in low for w in ("distribution", "proportion", "share", "percent")):
        chart_type = "pie"
    elif len(rows) > 50:
        chart_type = "table"

    return {"type": chart_type, "x_column": cols[0], "y_column": numeric_cols[0], "title": None}


def _extract_blob_paths(content: str | Any) -> list[str]:
    if not isinstance(content, str):
        return []
    try:
        data = json.loads(content)
        files = data.get("files", [])
        if isinstance(files, list):
            return [f.get("blob_path", "") for f in files if isinstance(f, dict)]
    except Exception:
        pass
    return []
