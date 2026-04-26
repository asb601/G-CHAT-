"""
Graph builder — LangGraph StateGraph construction, agent node, and routing.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode
from openai import RateLimitError

from app.agent.llm import get_llm
from app.agent.state import AgentState, MAX_TOOL_CALLS
from app.core.logger import llm_logger, pipeline_logger


def _fmt_message(m) -> dict:
    """Serialize a LangChain message for pipeline logging."""
    content = m.content if hasattr(m, "content") else ""
    tool_calls = [
        {"name": tc.get("name"), "args": tc.get("args")}
        for tc in (getattr(m, "tool_calls", None) or [])
    ]
    # For ToolMessage, also capture the tool output
    tool_call_id = getattr(m, "tool_call_id", None)
    base = {
        "type": type(m).__name__,
        "content": str(content),  # no truncation — full content to pipeline.log
    }
    if tool_calls:
        base["tool_calls"] = tool_calls
    if tool_call_id:
        base["tool_call_id"] = tool_call_id
    return base

_MAX_LLM_RETRIES = 3
_RETRY_BASE_DELAY = 5  # seconds, doubles each retry


def build_agent_node(all_tools: list):
    """Create the async agent node closure with all tools pre-bound."""

    async def agent_node(state: AgentState) -> dict:
        count = state.get("tool_call_count", 0)
        if count >= MAX_TOOL_CALLS:
            return {"messages": [AIMessage(content="I've gathered enough data. Let me summarise.")]}

        llm_with_tools = get_llm().bind_tools(all_tools)

        # ── Log every message going into the LLM this iteration ──────────────
        pipeline_logger.debug(
            "llm_input",
            iteration=count + 1,
            message_count=len(state["messages"]),
            messages=[_fmt_message(m) for m in state["messages"]],
        )

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

        # ── Log the full LLM response: content + every tool call with full args ──
        tool_calls_out = getattr(response, "tool_calls", None) or []
        pipeline_logger.debug(
            "llm_output",
            iteration=count + 1,
            prompt_tokens=p_tok,
            completion_tokens=c_tok,
            duration_ms=duration_ms,
            content=str(response.content) if response.content else "",
            tool_calls=[
                {"name": tc.get("name"), "args": tc.get("args")}
                for tc in tool_calls_out
            ],
        )

        llm_logger.info("llm_call",
                        function="agent_node",
                        model=get_llm().deployment_name,
                        prompt_tokens=p_tok,
                        completion_tokens=c_tok,
                        total_tokens=p_tok + c_tok,
                        duration_ms=duration_ms,
                        tool_calls=len(tool_calls_out),
                        iteration=count + 1,
                        retries=attempt)

        n_calls = len(getattr(response, "tool_calls", None) or [])
        return {
            "messages": [response],
            "tool_call_count": count + (1 if n_calls else 0),
        }

    return agent_node


def _had_zero_row_sql(messages: list) -> bool:
    """True if any prior run_sql ToolMessage returned an empty result set."""
    for m in messages:
        if isinstance(m, ToolMessage) and getattr(m, "name", "") == "run_sql":
            content = m.content if isinstance(m.content, str) else str(m.content)
            if '"row_count": 0' in content or '"total_rows": 0' in content:
                return True
    return False


def _had_sql_error(messages: list) -> bool:
    """True if any prior run_sql ToolMessage returned a SQL error payload.

    Catches type-conversion / cast failures (string vs int join keys),
    syntax errors, missing-column errors, etc. We treat these the same as
    a 0-row result for nudge purposes — the agent should discover an
    alternate file rather than report the raw error and surrender.
    """
    for m in messages:
        if isinstance(m, ToolMessage) and getattr(m, "name", "") == "run_sql":
            content = m.content if isinstance(m.content, str) else str(m.content)
            if '"error"' in content:
                return True
    return False


_RELATIVE_TIME_MARKERS = (
    "CURRENT_DATE", "CURRENT_TIMESTAMP", "NOW(", "GETDATE(",
    "INTERVAL ", "DATE_SUB", "DATE_ADD", "DATEADD(",
)


def _zero_rows_only_from_relative_time(messages: list) -> bool:
    """True if every zero-row run_sql had a relative-time predicate.

    An empty result for a query like 'last 7 days' against a historical
    dataset is a LEGITIMATE empty answer, not a missing-entity bug.
    Forcing search_catalog discovery in that case wastes the rest of the
    tool budget and produces stacked surrender messages — we suppress the
    nudge and let the model report the empty window directly.
    """
    saw_zero = False
    for m in messages:
        if not (isinstance(m, ToolMessage) and getattr(m, "name", "") == "run_sql"):
            continue
        content = m.content if isinstance(m.content, str) else str(m.content)
        is_zero = '"row_count": 0' in content or '"total_rows": 0' in content
        if not is_zero:
            continue
        saw_zero = True
        # Locate the SQL string for this ToolMessage in the preceding AIMessage.
        tcid = getattr(m, "tool_call_id", None)
        sql_text = ""
        if tcid:
            for prev in messages:
                if not isinstance(prev, AIMessage):
                    continue
                for tc in (getattr(prev, "tool_calls", None) or []):
                    if tc.get("id") == tcid:
                        sql_text = (tc.get("args") or {}).get("sql", "") or ""
                        break
                if sql_text:
                    break
        upper = sql_text.upper()
        if not any(marker in upper for marker in _RELATIVE_TIME_MARKERS):
            return False  # at least one zero-row SQL was NOT relative-time
    return saw_zero


def _called_search_catalog(messages: list) -> bool:
    """True if search_catalog has been invoked at any point this session."""
    return any(
        isinstance(m, ToolMessage) and getattr(m, "name", "") == "search_catalog"
        for m in messages
    )


def _had_any_nonzero_sql(messages: list) -> bool:
    """True if at least one run_sql ToolMessage returned > 0 rows."""
    import json as _json
    for m in messages:
        if isinstance(m, ToolMessage) and getattr(m, "name", "") == "run_sql":
            content = m.content if isinstance(m.content, str) else str(m.content)
            try:
                data = _json.loads(content)
                if isinstance(data, dict) and data.get("row_count", 0) > 0:
                    return True
            except Exception:
                # If parsing fails, fall back to a permissive substring check
                if '"row_count": 0' not in content and '"total_rows": 0' not in content:
                    return True
    return False


def _should_force_broaden(state: AgentState) -> bool:
    """Decide whether to force one more iteration to broaden the search.

    Triggers when ALL of these hold:
      * the last message is a final AIMessage with no further tool calls
      * at least one run_sql either returned 0 rows OR errored
        (a JOIN cast error means we never even got to query the data;
        treat it the same as 0 rows so the agent gets nudged to find
        a compatible alternate file rather than surrender)
      * NO run_sql call returned > 0 rows
      * search_catalog has NOT been called yet
      * the failures were not exclusively relative-time-window queries
        (an empty 'last 7 days' result against historical data is a
        legitimate empty answer, not a missing entity)
      * we have not already forced this nudge in this session
      * we still have tool budget remaining
    """
    if state.get("broaden_nudges", 0) >= 1:
        return False
    if state.get("tool_call_count", 0) >= MAX_TOOL_CALLS:
        return False
    msgs = state["messages"]
    if not msgs:
        return False
    last = msgs[-1]
    if not isinstance(last, AIMessage):
        return False
    if getattr(last, "tool_calls", None):
        return False
    had_zero = _had_zero_row_sql(msgs)
    had_err = _had_sql_error(msgs)
    if not (had_zero or had_err):
        return False
    if _had_any_nonzero_sql(msgs):
        # The model already got real rows from somewhere — don't second-guess.
        return False
    if _called_search_catalog(msgs):
        return False
    # Suppress nudge for legitimate empty-time-window results
    if had_zero and not had_err and _zero_rows_only_from_relative_time(msgs):
        return False
    return True


_BROADEN_NUDGE = (
    "Stop. You produced a final answer admitting the requested value could not be "
    "found, but you have not yet broadened your search. The initial file shortlist "
    "is only a starting point — the catalog contains additional files that may hold "
    "this value (alternate name tables, party masters, account masters, alias / "
    "search-term columns, etc.). Before concluding the value is absent you MUST:\n"
    "  1. Call search_catalog with semantic terms describing the type of file that "
    "would naturally store this value (for example: 'party name', 'customer "
    "account', 'account master', 'name lookup', 'parties', 'site name').\n"
    "  2. Inspect the schema of the most promising new candidate with get_file_schema.\n"
    "  3. Run a lookup query (exact, then case-insensitive partial) against that file.\n"
    "Only after those three steps return nothing may you tell the user the value "
    "could not be located."
)


def broaden_nudge_node(state: AgentState) -> dict:
    """Inject a corrective system message and bump the nudge counter."""
    pipeline_logger.info(
        "broaden_nudge_injected",
        reason="agent gave up without calling search_catalog after a 0-row SQL",
        tool_call_count=state.get("tool_call_count", 0),
    )
    return {
        "messages": [SystemMessage(content=_BROADEN_NUDGE)],
        "broaden_nudges": state.get("broaden_nudges", 0) + 1,
    }


def route(state: AgentState) -> Literal["tools", "broaden_nudge", "__end__"]:
    """Route to tools if the LLM wants more tools; else either nudge or end."""
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and getattr(last, "tool_calls", None):
        return "tools"
    if _should_force_broaden(state):
        return "broaden_nudge"
    return END


def build_graph(all_tools: list) -> Any:
    """Build a fresh compiled StateGraph per request."""
    tool_node = ToolNode(all_tools)
    agent_node = build_agent_node(all_tools)

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tool_node)
    builder.add_node("broaden_nudge", broaden_nudge_node)
    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", route)
    builder.add_edge("tools", "agent")
    builder.add_edge("broaden_nudge", "agent")

    return builder.compile()
