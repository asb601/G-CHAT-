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
from app.agent.search_normalization import tokenize_search_query
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

# How many files to surface in the prompt shortlist.
# 8 was too tight for queries that need both a metric file and a lookup file.
# 12 still fits comfortably in the prompt window for very large catalogs.
_SHORTLIST_TOP_K = 12

# How many slots in the shortlist to reserve for "lookup / master" files —
# generic dimension tables (parties, accounts, masters, dim_*) that almost
# every entity-lookup query needs but which never rank well on metric tokens
# like "invoice" / "amount" / "ageing".
_LOOKUP_RESERVED_SLOTS = 3

# Filename / description signals that mark a file as a master / lookup table.
# Pure structural heuristic — applies to any catalog, any query.
_LOOKUP_KEYWORDS = (
    "master", "masters", "parties", "party", "accounts", "account",
    "lookup", "directory", "reference", "dimension",
)
# Column-name suffixes that indicate the column holds entity names / labels —
# the kind of column you would resolve a literal user-supplied value against.
_NAME_COLUMN_SUFFIXES = ("_name", "name", "_desc", "_description", "_label", "_title")


def _is_lookup_file(entry: dict) -> bool:
    """Heuristic: does this file look like a master / lookup / dimension table?

    A file qualifies if ANY of:
      - blob_path contains a lookup keyword (master, parties, lookup, dim_, ...)
      - ai_description contains a lookup keyword
      - it has at least one column whose name ends in _NAME / _DESC / _LABEL
        (these are the universal markers of an entity-name column)
    """
    blob = (entry.get("blob_path") or "").lower()
    if any(kw in blob for kw in _LOOKUP_KEYWORDS):
        return True
    desc = (entry.get("ai_description") or "").lower()
    if any(kw in desc for kw in _LOOKUP_KEYWORDS):
        return True
    for col in (entry.get("columns_info") or []):
        if not isinstance(col, dict):
            continue
        name = (col.get("name") or "").lower()
        if any(name.endswith(sfx) for sfx in _NAME_COLUMN_SUFFIXES):
            return True
    return False


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
    retrieval_error: str | None = None
    if user_id:
        try:
            retrieved_with_scores = await retrieve_with_scores(
                query, user_id, is_admin, db, top_k=_SHORTLIST_TOP_K
            )
        except Exception as exc:
            retrieval_error = str(exc)[:200]
            chat_logger.warning("retrieval_error_fallback", error=retrieval_error)

    # ── In-memory keyword scorer (used for fallback AND for lookup-slot fill) ─
    q_words = tokenize_search_query(query)

    def _kw_score(e: dict) -> int:
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

    if retrieved_with_scores:
        retrieved_ids = {meta.file_id for meta, _ in retrieved_with_scores}
        catalog = [e for e in full_catalog if e.get("file_id") in retrieved_ids]
        # ── Reserve slots for master / lookup files ───────────────────────────
        # Retrieval ranks by token relevance, which under-weights name-lookup
        # tables for queries about metrics ("show X for entity Y"). Make sure
        # at least a few generic master/lookup tables make it into the prompt.
        already_in = {e.get("blob_path") for e in catalog}
        lookup_pool = [
            e for e in full_catalog
            if _is_lookup_file(e) and e.get("blob_path") not in already_in
        ]
        # Rank lookup pool by keyword score (still query-aware: a "supplier
        # master" outranks "calendar lookup" when the query is about suppliers).
        lookup_pool.sort(key=_kw_score, reverse=True)
        injected_lookups = lookup_pool[:_LOOKUP_RESERVED_SLOTS]
        catalog = catalog + injected_lookups

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
            lookup_slots_added=[e.get("blob_path") for e in injected_lookups],
        )
    else:
        # Fallback: retrieval returned 0 (or errored) — do in-memory keyword
        # match on the catalog so we still show a reasonable shortlist.
        scored = sorted(full_catalog, key=_kw_score, reverse=True)
        # Take the top metric/transactional matches by keyword, then enrich
        # with the highest-scoring lookup files so name-resolution queries
        # always have a master table to consult.
        primary = scored[: _SHORTLIST_TOP_K - _LOOKUP_RESERVED_SLOTS]
        primary_blobs = {e.get("blob_path") for e in primary}
        lookup_pool = [
            e for e in scored
            if _is_lookup_file(e) and e.get("blob_path") not in primary_blobs
        ]
        catalog = primary + lookup_pool[:_LOOKUP_RESERVED_SLOTS]
        parquet_paths_all = {
            k: v for k, v in all_parquet_paths.items()
            if k in {e.get("blob_path") for e in catalog}
        }
        if retrieval_error:
            reason = f"retrieval_error: {retrieval_error}"
        elif user_id:
            reason = "no retrieval results"
        else:
            reason = "no user_id"
        pipeline_logger.info(
            "retrieval_fallback",
            query=query,
            reason=reason,
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
        total_file_count=len(full_catalog),
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
        "broaden_nudges": 0,
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
    # Buffer chunks for the CURRENT LLM call. We only flush them to the user
    # at on_chat_model_end *if* the response has no tool_calls (i.e. it is the
    # final user-facing answer). Intermediate planning / "let me check the
    # schema next" narration is discarded so the user never sees it.
    pending_chunks: list[str] = []

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
                        # Buffer only — do NOT yield yet. We don't know whether
                        # this LLM call is the final answer or an intermediate
                        # reasoning turn until on_chat_model_end fires.
                        pending_chunks.append(content)

            elif kind == "on_chat_model_start":
                # New LLM call starting — reset the buffer.
                pending_chunks = []
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
                resp_tool_calls = getattr(resp, "tool_calls", None) if resp else None
                if resp:
                    usage = getattr(resp, "usage_metadata", None)
                    pipeline_logger.debug(
                        "llm_stream_output",
                        iteration=tool_calls_made,
                        content=str(resp.content) if resp.content else "",
                        tool_calls=[
                            {"name": tc.get("name"), "args": tc.get("args")}
                            for tc in (resp_tool_calls or [])
                        ],
                        prompt_tokens=usage.get("input_tokens", 0) if usage else 0,
                        completion_tokens=usage.get("output_tokens", 0) if usage else 0,
                    )

                # Flush buffered chunks ONLY if this LLM turn produced no tool
                # calls — i.e. it is the final answer the user should see.
                # Intermediate planning / "now I'll check the schema" narration
                # is dropped on the floor so the user only sees the result.
                if pending_chunks and not resp_tool_calls:
                    for piece in pending_chunks:
                        answer_tokens.append(piece)
                        yield {"type": "token", "content": piece}
                pending_chunks = []

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
