"""
Graph builder — LangGraph StateGraph construction, agent node, and routing.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from langchain_core.messages import AIMessage
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


def route(state: AgentState) -> Literal["tools", "__end__"]:
    """Route to tools if the last message has tool calls, else end."""
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and getattr(last, "tool_calls", None):
        return "tools"
    return END


def build_graph(all_tools: list) -> Any:
    """Build a fresh compiled StateGraph per request."""
    tool_node = ToolNode(all_tools)
    agent_node = build_agent_node(all_tools)

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tool_node)
    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", route)
    builder.add_edge("tools", "agent")

    return builder.compile()
