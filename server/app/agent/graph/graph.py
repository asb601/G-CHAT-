"""
LangGraph agent — public entry points (sync + streaming).

This module orchestrates the pipeline:
  1. Load catalog (catalog_cache)
  2. Build tools
  3. Build system prompt (prompt_builder)
  4. Construct LangGraph (graph_builder)
  5. Run and extract results (response_helpers)
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import AsyncIterator

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.catalog_cache import invalidate_catalog_cache, load_catalog  # re-export
from app.agent.graph.graph_builder import build_graph
from app.agent.prompts.prompt_builder import build_system_prompt
from app.agent.response_helpers import (
    extract_answer,
    extract_blob_paths,
    fallback_answer,
    fallback_answer_from_outputs,
    infer_chart,
)
from app.agent.state import AgentState
from app.agent.tools.catalog import build_catalog_tools
from app.agent.tools.sample import build_sample_tool
from app.agent.tools.sql import build_sql_tools
from app.agent.tools.stats import build_stats_tool
from app.core.logger import chat_logger

# Per-request mutable stores (keyed by request_id)
_request_stores: dict[str, dict] = {}
_stores_lock = threading.Lock()

_NO_FILES_MSG = "No files have been ingested yet. Please upload and ingest some files first."


# ── Shared context builder ────────────────────────────────────────────────────

async def _build_agent_context(
    query: str,
    db: AsyncSession,
    conversation_context: str = "",
) -> dict | None:
    """
    Shared setup for both streaming and non-streaming entry points.
    Returns None if no catalog data exists.
    """
    cached = await load_catalog(db)
    if not cached:
        return None

    catalog = cached["catalog"]
    relationships = cached["relationships"]
    connection_string = cached["connection_string"]
    container_name = cached["container_name"]
    parquet_blob_path = cached["parquet_blob_path"]
    parquet_paths_all = cached["parquet_paths_all"]
    sample_rows = cached["sample_rows"]

    # Per-request state store
    req_id = uuid.uuid4().hex
    store: dict = {}
    with _stores_lock:
        _request_stores[req_id] = store

    # Build tools
    all_tools = []
    all_tools.extend(build_sql_tools(connection_string, container_name, parquet_blob_path, store))
    all_tools.extend(build_catalog_tools(catalog, relationships, parquet_paths_all, container_name))
    all_tools.extend(build_stats_tool(store))
    all_tools.extend(build_sample_tool(sample_rows))

    # Build graph
    graph = build_graph(all_tools)

    # Build system prompt
    system_prompt = build_system_prompt(
        catalog=catalog,
        relationships=relationships,
        parquet_paths_all=parquet_paths_all,
        parquet_blob_path=parquet_blob_path,
        container_name=container_name,
        sample_rows=sample_rows,
        conversation_context=conversation_context,
    )

    initial_state: AgentState = {
        "messages": [SystemMessage(content=system_prompt), HumanMessage(content=query)],
        "catalog": catalog,
        "relationships": relationships,
        "connection_string": connection_string,
        "container_name": container_name,
        "parquet_blob_path": parquet_blob_path,
        "tool_call_count": 0,
        "request_id": req_id,
    }

    return {
        "graph": graph,
        "initial_state": initial_state,
        "store": store,
        "req_id": req_id,
        "catalog_len": len(catalog),
        "container_name": container_name,
        "parquet_blob_path": parquet_blob_path,
    }


# ── Public entry point ────────────────────────────────────────────────────────

async def run_agent_query(query: str, db: AsyncSession, *, conversation_context: str = "") -> dict:
    """
    Main entry point for the agentic query pipeline.
    Returns {answer, data, chart, route, row_count, files_used, tool_calls}.
    """
    pipeline_start = time.perf_counter()

    ctx = await _build_agent_context(query, db, conversation_context)
    if not ctx:
        return {"answer": _NO_FILES_MSG, "data": [], "chart": None}

    graph = ctx["graph"]
    initial_state = ctx["initial_state"]
    store = ctx["store"]
    req_id = ctx["req_id"]

    chat_logger.info("agent_start",
                     query=query[:200],
                     file_count=ctx["catalog_len"],
                     container=ctx["container_name"],
                     has_parquet=ctx["parquet_blob_path"] is not None)

    try:
        final_state = await graph.ainvoke(initial_state)
    except Exception as exc:
        chat_logger.exception("agent_error", error=str(exc)[:400])
        return {
            "answer": "An error occurred while processing your query. Please try again.",
            "data": [], "chart": None,
        }
    finally:
        with _stores_lock:
            _request_stores.pop(req_id, None)

    # Extract results
    final_msgs = final_state["messages"]
    answer = extract_answer(final_msgs)
    sql_results = store.get("sql_results", [])
    tool_calls_made = final_state.get("tool_call_count", 0)
    total_ms = round((time.perf_counter() - pipeline_start) * 1000, 2)

    chat_logger.info("agent_complete",
                     tool_calls=tool_calls_made,
                     row_count=len(sql_results),
                     total_duration_ms=total_ms,
                     answer_preview=answer[:200])

    if not answer and sql_results:
        answer = "Here are the results:"
    elif not answer and not sql_results:
        answer = fallback_answer(final_msgs)

    chart = infer_chart(answer, sql_results)

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
            for blob in extract_blob_paths(msg.content)
        }),
        "tool_calls": tool_calls_made,
    }


# ── Streaming entry point ────────────────────────────────────────────────────

async def run_agent_query_stream(
    query: str,
    db: AsyncSession,
    *,
    conversation_context: str = "",
) -> AsyncIterator[dict]:
    """
    Streaming variant of run_agent_query.

    Yields dicts:
      {"type": "thinking", "tool": tool_name}
      {"type": "token", "content": str}
      {"type": "tool_result", "tool": name, "preview": str}
      {"type": "done", "payload": {answer, data, chart, ...}}
    """
    pipeline_start = time.perf_counter()

    ctx = await _build_agent_context(query, db, conversation_context)
    if not ctx:
        yield {
            "type": "done",
            "payload": {
                "answer": _NO_FILES_MSG,
                "data": [], "chart": None, "route": "agent", "row_count": 0,
                "files_used": [], "tool_calls": 0,
            },
        }
        return

    graph = ctx["graph"]
    initial_state = ctx["initial_state"]
    store = ctx["store"]
    req_id = ctx["req_id"]

    chat_logger.info("agent_stream_start", query=query[:200], file_count=ctx["catalog_len"])

    answer_tokens: list[str] = []
    tool_calls_made = 0
    files_used: set[str] = set()
    tool_outputs: list[str] = []

    try:
        async for event in graph.astream_events(initial_state, version="v2"):
            kind = event["event"]

            if kind == "on_chat_model_stream":
                chunk = event["data"].get("chunk")
                if chunk:
                    tool_calls = getattr(chunk, "tool_calls", None) or getattr(chunk, "tool_call_chunks", None)
                    if tool_calls:
                        continue
                    content = chunk.content if hasattr(chunk, "content") else ""
                    if content and isinstance(content, str):
                        answer_tokens.append(content)
                        yield {"type": "token", "content": content}

            elif kind == "on_tool_start":
                tool_name = event.get("name", "")
                tool_calls_made += 1
                yield {"type": "thinking", "tool": tool_name}

            elif kind == "on_tool_end":
                tool_output = event["data"].get("output", "")
                if isinstance(tool_output, str):
                    files_used.update(extract_blob_paths(tool_output))
                    tool_outputs.append(tool_output)

    except Exception as exc:
        chat_logger.exception("agent_stream_error", error=str(exc)[:400])
        yield {
            "type": "done",
            "payload": {
                "answer": "An error occurred while processing your query. Please try again.",
                "data": [], "chart": None, "route": "agent",
                "row_count": 0, "files_used": [], "tool_calls": 0,
            },
        }
        return
    finally:
        with _stores_lock:
            _request_stores.pop(req_id, None)

    final_answer = "".join(answer_tokens) if answer_tokens else ""
    sql_results = store.get("sql_results", [])
    total_ms = round((time.perf_counter() - pipeline_start) * 1000, 2)

    chat_logger.info("agent_stream_complete",
                     tool_calls=tool_calls_made,
                     row_count=len(sql_results),
                     total_duration_ms=total_ms,
                     answer_len=len(final_answer))

    if not final_answer and sql_results:
        final_answer = "Here are the results:"
    elif not final_answer and not sql_results:
        final_answer = fallback_answer_from_outputs(tool_outputs)

    chart = infer_chart(final_answer, sql_results)

    yield {
        "type": "done",
        "payload": {
            "answer": final_answer,
            "data": sql_results,
            "chart": chart,
            "route": "agent",
            "row_count": len(sql_results),
            "files_used": list(files_used),
            "tool_calls": tool_calls_made,
        },
    }
