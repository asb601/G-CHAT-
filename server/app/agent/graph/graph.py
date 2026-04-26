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

import re
import threading
import time
import uuid
from typing import AsyncIterator

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.catalog_cache import invalidate_catalog_cache, load_catalog  # re-export
from app.agent.graph.graph_builder import build_graph
from app.agent.prompts.prompt_builder import build_system_prompt
from app.retrieval.orchestrator import retrieve_with_scores
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
from app.core.logger import chat_logger, pipeline_logger
from app.retrieval.embeddings import build_search_text

# Per-request mutable stores (keyed by request_id)
_request_stores: dict[str, dict] = {}
_stores_lock = threading.Lock()

_NO_FILES_MSG = "No files have been ingested yet. Please upload and ingest some files first."
_RETRIEVAL_STOPWORDS = {
    "a", "an", "and", "are", "bucket", "by", "for", "from", "given", "how", "in",
    "invoice", "is", "it", "of", "on", "or", "show", "the", "to", "what", "with",
}


def _tokenize_query(text: str) -> list[str]:
    return [
        token for token in re.split(r"[^a-z0-9_]+", text.lower())
        if len(token) >= 2 and token not in _RETRIEVAL_STOPWORDS
    ]


# ── Shared context builder ────────────────────────────────────────────────────

async def _build_agent_context(
    query: str,
    db: AsyncSession,
    conversation_context: str = "",
    user_id: str = "",
    is_admin: bool = True,
    allowed_domains: list[str] | None = None,
) -> dict | None:
    """
    Shared setup for both streaming and non-streaming entry points.
    Returns None if no catalog data exists.
    """
    # ── STEP 1: USER QUERY RECEIVED ──────────────────────────────────────────
    pipeline_logger.info(
        "query_received",
        query=query,
        has_conversation_context=bool(conversation_context),
        conversation_context_preview=(conversation_context[:300] if conversation_context else ""),
    )

    cached = await load_catalog(db, allowed_domains=None if is_admin else allowed_domains)
    if not cached:
        pipeline_logger.warning("catalog_empty", query=query, reason="no files ingested yet")
        return None

    # ── STEP 2: CATALOG LOADED ───────────────────────────────────────────────
    pipeline_logger.info(
        "catalog_loaded",
        query=query,
        container=cached["container_name"],
        file_count=len(cached["catalog"]),
        parquet_count=len(cached["parquet_paths_all"]),
        files=[f.get("blob_path", "") for f in cached["catalog"]],
    )

    full_catalog = cached["catalog"]
    connection_string = cached["connection_string"]
    container_name = cached["container_name"]
    parquet_blob_path = cached["parquet_blob_path"]
    all_parquet_paths = cached["parquet_paths_all"]
    sample_rows_by_blob = cached["sample_rows_by_blob"]

    # ── STEP 2.5: RETRIEVAL — filter catalog to top-K relevant files ─────────
    # Run the 9-stage retrieval pipeline (temporal → BM25 → fuzzy → vector →
    # graph_expand → RRF). Only the relevant files go into the system prompt.
    # The full catalog is still passed to build_catalog_tools so search_catalog
    # can still scan all files if needed.
    retrieved_with_scores = []
    if user_id:
        try:
            retrieved_with_scores = await retrieve_with_scores(
                query, user_id, is_admin, db, top_k=8
            )
        except Exception as exc:
            chat_logger.warning("retrieval_error_fallback", error=str(exc)[:200])

    if retrieved_with_scores:
        retrieved_ids = {meta.file_id for meta, _ in retrieved_with_scores}
        catalog = [e for e in full_catalog if e.get("file_id") in retrieved_ids]
        parquet_paths_all = {
            k: v for k, v in all_parquet_paths.items()
            if k in {e.get("blob_path") for e in catalog}
        }
        pipeline_logger.info(
            "retrieval_filtered",
            query=query,
            total_files=len(full_catalog),
            retrieved_files=len(catalog),
            top_scores=[(meta.file_id, round(s, 4)) for meta, s in retrieved_with_scores[:5]],
        )
    else:
        # Fallback: retrieval returned 0 — do in-memory keyword match on catalog
        # so we still show at most top_k=8 files, not all 27.
        q_words = _tokenize_query(query)

        def _score(e: dict) -> int:
            search_text = build_search_text(e).lower()
            score = sum(1 for w in q_words if w in search_text)

            column_text = " ".join(
                c.get("name", "")
                for c in (e.get("columns_info") or [])
                if isinstance(c, dict)
            ).lower()
            score += sum(2 for w in q_words if w in column_text)

            blob_path = (e.get("blob_path") or "").lower()
            score += sum(1 for w in q_words if w in blob_path)
            return score

        scored = sorted(full_catalog, key=_score, reverse=True)
        catalog = scored[:8]
        parquet_paths_all = {
            k: v for k, v in all_parquet_paths.items()
            if k in {e.get("blob_path") for e in catalog}
        }
        pipeline_logger.info(
            "retrieval_fallback",
            query=query,
            reason="no retrieval results" if user_id else "no user_id",
            total_files=len(full_catalog),
            fallback_files=[e.get("blob_path") for e in catalog],
        )

    # Per-request state store
    req_id = uuid.uuid4().hex
    store: dict = {}
    with _stores_lock:
        _request_stores[req_id] = store

    # Build tools
    all_tools = []
    all_tools.extend(build_sql_tools(connection_string, container_name, parquet_blob_path, store))
    # search_catalog tool uses the full catalog so it can find any file
    all_tools.extend(build_catalog_tools(full_catalog, all_parquet_paths, container_name))
    all_tools.extend(build_stats_tool(store))
    all_tools.extend(build_sample_tool(sample_rows_by_blob))

    # Build graph
    graph = build_graph(all_tools)

    # Build system prompt
    system_prompt = build_system_prompt(
        catalog=catalog,
        parquet_paths_all=parquet_paths_all,
        parquet_blob_path=parquet_blob_path,
        container_name=container_name,
        sample_rows_by_blob=sample_rows_by_blob,
        conversation_context=conversation_context,
    )

    # ── Log the complete system prompt so we can audit exactly what the LLM sees ──
    pipeline_logger.info(
        "system_prompt_built",
        query=query,
        container=container_name,
        catalog_file_count=len(catalog),
        parquet_file_count=len(parquet_paths_all),
        has_conversation_context=bool(conversation_context),
        system_prompt=system_prompt,  # full prompt, no truncation
    )

    initial_state: AgentState = {
        "messages": [SystemMessage(content=system_prompt), HumanMessage(content=query)],
        "catalog": catalog,
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
        "total_files": len(full_catalog),
        "container_name": container_name,
        "parquet_blob_path": parquet_blob_path,
    }


# ── Public entry point ────────────────────────────────────────────────────────

async def run_agent_query(
    query: str,
    db: AsyncSession,
    *,
    conversation_context: str = "",
    user_id: str = "",
    is_admin: bool = True,
    allowed_domains: list[str] | None = None,
) -> dict:
    """
    Main entry point for the agentic query pipeline.
    Returns {answer, data, chart, route, row_count, files_used, tool_calls}.
    """
    pipeline_start = time.perf_counter()

    ctx = await _build_agent_context(query, db, conversation_context, user_id, is_admin, allowed_domains)
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

    # ── FINAL STEP: ANSWER READY ─────────────────────────────────────────────
    pipeline_logger.info(
        "final_answer",
        query=query,
        answer=answer,
        row_count=len(sql_results),
        tool_calls=tool_calls_made,
        total_duration_ms=total_ms,
    )

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
    user_id: str = "",
    is_admin: bool = True,
    allowed_domains: list[str] | None = None,
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

    ctx = await _build_agent_context(query, db, conversation_context, user_id, is_admin, allowed_domains)
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

    # Emit retrieval summary so the frontend can show "Searching N files…"
    yield {
        "type": "pipeline_step",
        "step": "retrieval",
        "retrieved_files": ctx["catalog_len"],
        "total_files": ctx["total_files"],
    }

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

            elif kind == "on_chat_model_start":
                # Log the full message list going into the LLM for this iteration
                raw_msgs = event["data"].get("input", {}).get("messages", [])
                # langgraph batches messages as [[msg1, msg2, ...]]
                flat = raw_msgs[0] if raw_msgs and isinstance(raw_msgs[0], list) else raw_msgs
                pipeline_logger.debug(
                    "llm_stream_input",
                    iteration=tool_calls_made,
                    message_count=len(flat),
                    messages=[
                        {
                            "type": type(m).__name__,
                            "content": str(m.content) if hasattr(m, "content") else "",
                            "tool_calls": [
                                {"name": tc.get("name"), "args": tc.get("args")}
                                for tc in (getattr(m, "tool_calls", None) or [])
                            ],
                        }
                        for m in flat
                    ],
                )

            elif kind == "on_chat_model_end":
                # Log the complete LLM response for this iteration
                resp = event["data"].get("output")
                if resp:
                    usage = getattr(resp, "usage_metadata", None)
                    pipeline_logger.debug(
                        "llm_stream_output",
                        iteration=tool_calls_made,
                        content=str(resp.content) if resp.content else "",
                        tool_calls=[
                            {"name": tc.get("name"), "args": tc.get("args")}
                            for tc in (getattr(resp, "tool_calls", None) or [])
                        ],
                        prompt_tokens=usage.get("input_tokens", 0) if usage else 0,
                        completion_tokens=usage.get("output_tokens", 0) if usage else 0,
                    )

            elif kind == "on_tool_start":
                tool_name = event.get("name", "")
                tool_input = event["data"].get("input", {})
                tool_calls_made += 1
                pipeline_logger.info(
                    "tool_call_start",
                    tool=tool_name,
                    iteration=tool_calls_made,
                    input=tool_input,  # full args, no truncation
                )
                yield {"type": "thinking", "tool": tool_name}

            elif kind == "on_tool_end":
                tool_name = event.get("name", "")
                tool_output = event["data"].get("output", "")
                pipeline_logger.info(
                    "tool_call_end",
                    tool=tool_name,
                    iteration=tool_calls_made,
                    output=str(tool_output),  # full output, no truncation
                )
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

    # ── FINAL STEP: ANSWER READY ─────────────────────────────────────────────
    pipeline_logger.info(
        "final_answer",
        query=query,
        answer=final_answer,
        row_count=len(sql_results),
        tool_calls=tool_calls_made,
        total_duration_ms=total_ms,
    )

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
            "retrieved_files": ctx["catalog_len"],
            "total_files": ctx["total_files"],
        },
    }
