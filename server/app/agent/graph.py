"""
LangGraph agent graph — construction, nodes, and public entry point.
"""
from __future__ import annotations

import asyncio
import json
import time
import threading
import uuid
from typing import Any, AsyncIterator, Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode
from openai import RateLimitError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.llm import get_llm
from app.agent.state import AgentState, MAX_TOOL_CALLS
from app.agent.tools.catalog import build_catalog_tools
from app.agent.tools.sql import build_sql_tools
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

# ── Catalog cache (5-minute TTL) ─────────────────────────────────────────────

_CATALOG_TTL = 300  # seconds

_catalog_cache: dict | None = None
_catalog_cache_time: float = 0.0
_catalog_lock = threading.Lock()


def invalidate_catalog_cache() -> None:
    """Clear the in-memory catalog cache. Call after file ingestion completes."""
    global _catalog_cache, _catalog_cache_time
    with _catalog_lock:
        _catalog_cache = None
        _catalog_cache_time = 0.0
    chat_logger.info("catalog_cache_invalidated")


async def _load_catalog(db: AsyncSession) -> dict | None:
    """
    Load catalog data from Postgres, with 5-minute in-memory caching.
    Returns dict with keys: catalog, relationships, connection_string,
    container_name, parquet_blob_path, parquet_paths_all, sample_rows.
    Returns None if no files exist.
    """
    global _catalog_cache, _catalog_cache_time

    with _catalog_lock:
        if _catalog_cache is not None and (time.time() - _catalog_cache_time) < _CATALOG_TTL:
            return _catalog_cache

    # Cache miss — load from DB
    all_meta = list((await db.execute(select(FileMetadata))).scalars().all())
    if not all_meta:
        return None

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
            "join_type": r.join_type,
        }
        for r in all_rels
    ]

    first_meta = all_meta[0]
    container = await db.get(ContainerConfig, first_meta.container_id)
    if not container:
        return None

    all_analytics_rows = list((await db.execute(select(FileAnalytics))).scalars().all())
    analytics_by_file = {row.file_id: row for row in all_analytics_rows}

    parquet_blob_path = None
    parquet_paths_all: dict[str, str] = {}
    for meta in all_meta:
        ar = analytics_by_file.get(meta.file_id)
        if not ar:
            continue
        if parquet_blob_path is None:
            parquet_blob_path = ar.parquet_blob_path
        if ar.parquet_blob_path and meta.blob_path:
            parquet_paths_all[meta.blob_path] = ar.parquet_blob_path

    sample_rows = first_meta.sample_rows or []

    result = {
        "catalog": catalog,
        "relationships": relationships,
        "connection_string": container.connection_string,
        "container_name": container.container_name,
        "parquet_blob_path": parquet_blob_path,
        "parquet_paths_all": parquet_paths_all,
        "sample_rows": sample_rows,
    }

    with _catalog_lock:
        _catalog_cache = result
        _catalog_cache_time = time.time()

    chat_logger.info("catalog_cache_loaded", file_count=len(catalog))
    return result


# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT_TEMPLATE = """You are a sharp, data-driven analyst with direct SQL access to structured data files stored in Azure Blob Storage.

Container: {container_name}
{parquet_note}
{sample_note}

--- TOOLS ---
1. run_sql: Execute any DuckDB SQL. File paths and column names are listed above.
2. search_catalog: Find which file(s) to query when the paths above don't cover the question.
3. get_file_schema: Get full column names, types, and sample values for a specific file.
4. inspect_data_format: Preview a few rows to check value formats (e.g. date format, casing) before writing SQL. Not for answering — use run_sql.
5. summarise_dataframe: Compute stats on the last run_sql result in memory.

--- RULES ---
- If file paths and columns are listed above, use them directly in run_sql. No need for search_catalog or get_file_schema.
- Write complete SQL with proper column names from above. Do not guess column names.
- For multi-file questions, prefer JOINs. Always use LIMIT (default 100, or N if user asks for "top N").
- Always check the JOIN RELATIONSHIPS section before writing any JOIN. Use the exact column name listed. Never guess JOIN columns.
- If two files need to be JOINed but NO relationship is listed for them, call get_file_schema on BOTH files first.
- If a JOIN returns 0 rows — stop immediately. Call get_file_schema on both files to verify the exact column names and types, then rewrite the JOIN once with the correct columns.
- Give a direct answer with actual data. Bold the key numbers.
- Max {max_calls} tool calls.
"""


# ── Agent node ────────────────────────────────────────────────────────────────

_MAX_LLM_RETRIES = 3
_RETRY_BASE_DELAY = 5  # seconds, doubles each retry


def _build_agent_node(all_tools: list):
    """Create the async agent node closure with all tools pre-bound."""

    async def agent_node(state: AgentState) -> dict:
        count = state.get("tool_call_count", 0)
        if count >= MAX_TOOL_CALLS:
            return {"messages": [AIMessage(content="I've gathered enough data. Let me summarise.")]}

        llm_with_tools = get_llm().bind_tools(all_tools)

        last_exc: Exception | None = None
        for attempt in range(_MAX_LLM_RETRIES + 1):
            try:
                t = time.perf_counter()
                response = await llm_with_tools.ainvoke(state["messages"])
                duration_ms = round((time.perf_counter() - t) * 1000, 2)
                break
            except RateLimitError as exc:
                last_exc = exc
                if attempt < _MAX_LLM_RETRIES:
                    delay = _RETRY_BASE_DELAY * (2 ** attempt)
                    llm_logger.warning("llm_rate_limited",
                                       attempt=attempt + 1,
                                       retry_after_s=delay,
                                       error=str(exc)[:200])
                    await asyncio.sleep(delay)
                else:
                    llm_logger.error("llm_rate_limited_exhausted",
                                     attempts=_MAX_LLM_RETRIES + 1,
                                     error=str(exc)[:200])
                    return {
                        "messages": [AIMessage(
                            content="I'm currently experiencing high demand. Please try again in a minute."
                        )],
                    }
        else:
            raise last_exc  # type: ignore[misc]

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
                        iteration=count + 1,
                        retries=attempt)

        n_calls = len(getattr(response, "tool_calls", None) or [])
        return {
            "messages": [response],
            "tool_call_count": count + (1 if n_calls else 0),
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


# ── Shared setup ──────────────────────────────────────────────────────────────

_NO_FILES_MSG = "No files have been ingested yet. Please upload and ingest some files first."


async def _build_agent_context(
    query: str,
    db: AsyncSession,
    conversation_context: str = "",
) -> dict | None:
    """
    Shared setup for both streaming and non-streaming entry points.

    Returns dict with keys: graph, initial_state, store, req_id, catalog_len,
    container_name, parquet_blob_path.
    Returns None if no catalog data exists.
    """
    cached = await _load_catalog(db)
    if not cached:
        return None

    catalog = cached["catalog"]
    relationships = cached["relationships"]
    connection_string = cached["connection_string"]
    container_name = cached["container_name"]
    parquet_blob_path = cached["parquet_blob_path"]
    parquet_paths_all = cached["parquet_paths_all"]
    sample_rows = cached["sample_rows"]

    # ── Per-request state store ──
    req_id = uuid.uuid4().hex
    store: dict = {}
    with _stores_lock:
        _request_stores[req_id] = store

    # ── Build tools ──
    all_tools = []
    all_tools.extend(build_sql_tools(connection_string, container_name, parquet_blob_path, store))
    all_tools.extend(build_catalog_tools(catalog, relationships))
    all_tools.extend(build_stats_tool(store))
    all_tools.extend(build_sample_tool(sample_rows))

    # ── Build graph ──
    graph = _build_graph(all_tools)

    # ── System prompt ──
    # Build a lookup from blob_path → full columns_info for enrichment
    catalog_by_blob: dict[str, dict] = {}
    for entry in catalog:
        bp = entry.get("blob_path")
        if bp:
            catalog_by_blob[bp] = entry

    parquet_note = ""
    if parquet_paths_all:
        lines = []
        for blob, pq in parquet_paths_all.items():
            line = f"  read_parquet('az://{container_name}/{pq}')"
            entry = catalog_by_blob.get(blob)
            cols_info = (entry.get("columns_info") or []) if entry else []

            if cols_info:
                col_names = [c["name"] for c in cols_info]
                line += f"\n    Columns: {', '.join(col_names)}"

                identifiers = []
                enums = []
                for c in cols_info:
                    uv = c.get("unique_values") or c.get("sample_values") or []
                    name_lower = c["name"].lower()
                    col_type = c.get("type", "")
                    n_unique = len(uv)

                    is_id_like = any(
                        name_lower.endswith(s)
                        for s in ("_id", "_key", "_number", "_code")
                    )
                    if is_id_like and n_unique > 5:
                        sample_str = ", ".join(str(v) for v in uv[:5])
                        identifiers.append(f"{c['name']} ({col_type}, e.g. {sample_str})")
                    elif 1 <= n_unique <= 10 and "datetime" not in col_type.lower():
                        enums.append(f"{c['name']} [{', '.join(str(v) for v in uv)}]")

                if identifiers:
                    line += f"\n    Identifiers: {'; '.join(identifiers)}"
                if enums:
                    line += f"\n    Enums: {'; '.join(enums[:8])}"

            desc = entry.get("ai_description") if entry else None
            if desc:
                line += f"\n    Description: {desc}"
            lines.append(line)
        parquet_note = (
            "Available parquet files (use directly in run_sql — no search_catalog needed):\n"
            + "\n".join(lines)
            + "\nParquet covers the FULL dataset. Use it for any ordering, filtering, counting, or row retrieval."
        )
    elif parquet_blob_path:
        parquet_note = (
            f"Parquet path (use directly in run_sql — no search_catalog needed):\n"
            f"  read_parquet('az://{container_name}/{parquet_blob_path}')"
            "\nParquet covers the FULL dataset. Use it for any ordering, filtering, counting, or row retrieval."
        )

    sample_note = ""
    if sample_rows:
        sample_note = (
            f"\nData format preview: {len(sample_rows)} example rows from ingest available via"
            " inspect_data_format() — use to understand column formats before writing SQL."
        )

    # ── JOIN relationships section (deduplicated, filtered, capped) ──
    join_note = ""
    usable_rels = [r for r in relationships if r.get("confidence_score", 0) >= 0.7]
    if usable_rels and parquet_paths_all:
        seen_pairs: set[tuple[str, str, str]] = set()
        # Track per file-pair count to cap noise
        pair_counts: dict[tuple[str, str], int] = {}
        join_lines = []
        for rel in sorted(usable_rels, key=lambda r: r["confidence_score"], reverse=True):
            a_path, b_path = rel["file_a_path"], rel["file_b_path"]
            # Deduplicate bidirectional pairs
            pair_key = (min(a_path, b_path), max(a_path, b_path))
            col_key = (*pair_key, rel["shared_column"])
            if col_key in seen_pairs:
                continue
            seen_pairs.add(col_key)

            # Max 3 relationships per file pair — keeps the best, drops noise
            if pair_counts.get(pair_key, 0) >= 3:
                continue
            pair_counts[pair_key] = pair_counts.get(pair_key, 0) + 1

            pq_a = parquet_paths_all.get(a_path)
            pq_b = parquet_paths_all.get(b_path)
            if pq_a and pq_b:
                shared = rel["shared_column"]
                # Cross-column relationships show both column names
                if "=" in shared:
                    col_a, col_b = shared.split("=", 1)
                    join_on = f"a.{col_a} = b.{col_b}"
                else:
                    join_on = shared
                join_lines.append(
                    f"  az://{container_name}/{pq_a}  ←→  az://{container_name}/{pq_b}\n"
                    f"  JOIN ON: {join_on}\n"
                    f"  Type: {rel.get('join_type', 'LEFT JOIN')}  "
                    f"Confidence: {rel['confidence_score']}"
                )
            if len(join_lines) >= 30:  # global cap
                break
        if join_lines:
            join_note = (
                "\n--- JOIN RELATIONSHIPS (use these exact column names) ---\n"
                "These files share columns and can be JOINed directly:\n"
                + "\n".join(join_lines)
                + "\nAlways use these exact column names when writing JOIN conditions.\n"
            )

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        container_name=container_name,
        max_calls=MAX_TOOL_CALLS,
        parquet_note=parquet_note,
        sample_note=sample_note,
    )
    if join_note:
        system_prompt += join_note
    if conversation_context:
        system_prompt += (
            "\n\n--- CONVERSATION HISTORY ---\n"
            "The user is continuing a conversation. Use this context to understand "
            "follow-up questions, pronouns ('it', 'that', 'those'), and references "
            "to previous queries or results.\n\n"
            f"{conversation_context}\n"
            "---\n"
        )

    chat_logger.info("system_prompt_size",
                     chars=len(system_prompt),
                     words=len(system_prompt.split()),
                     parquet_file_count=len(parquet_paths_all),
                     has_conversation_context=bool(conversation_context))

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

    # ── Validate & enrich response ──
    if not answer and sql_results:
        answer = "Here are the results:"

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


# ── Streaming entry point (astream_events) ───────────────────────────────────

async def run_agent_query_stream(
    query: str,
    db: AsyncSession,
    *,
    conversation_context: str = "",
) -> AsyncIterator[dict]:
    """
    Streaming variant of run_agent_query.

    Yields dicts:
      {"type": "thinking", "tool": tool_name}          — tool call started
      {"type": "token", "content": str}                 — LLM token chunk
      {"type": "tool_result", "tool": name, "preview": str}  — tool finished
      {"type": "done", "payload": {answer, data, chart, ...}} — final result
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

    # ── Stream events from LangGraph ──
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
                    files_used.update(_extract_blob_paths(tool_output))
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

    # ── Validate & enrich response ──
    if not final_answer and sql_results:
        final_answer = "Here are the results:"

    chart = _infer_chart(final_answer, sql_results)

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
