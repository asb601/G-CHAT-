"""
Response helpers — answer extraction, fallback generation, chart inference.
"""
from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage


def extract_answer(messages: list) -> str:
    """Extract the last non-tool-call AI message as the answer."""
    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and not getattr(msg, "tool_calls", None) and msg.content:
            return msg.content if isinstance(msg.content, str) else str(msg.content)
    return ""


def fallback_answer(messages: list) -> str:
    """Build a user-facing message when the LLM produced no text answer."""
    errors = _extract_errors_from_messages(messages)
    return _format_error_answer(errors)


def fallback_answer_from_outputs(tool_outputs: list[str]) -> str:
    """Build a user-facing message from raw tool output strings (streaming path)."""
    errors = _extract_errors_from_strings(tool_outputs)
    return _format_error_answer(errors)


def infer_chart(answer: str, rows: list[dict]) -> dict | None:
    """Suggest a chart type based on the answer text and result shape."""
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


def extract_blob_paths(content: str | Any) -> list[str]:
    """Extract blob_path values from a tool message's JSON content."""
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


# ── Internal helpers ──

def _extract_errors_from_messages(messages: list) -> list[str]:
    errors = []
    for msg in messages:
        if not isinstance(msg, ToolMessage):
            continue
        content = msg.content if isinstance(msg.content, str) else str(msg.content)
        if '"error"' in content:
            try:
                data = json.loads(content)
                if "error" in data:
                    errors.append(data["error"][:200])
            except Exception:
                pass
    return errors


def _extract_errors_from_strings(outputs: list[str]) -> list[str]:
    errors = []
    for output in outputs:
        if '"error"' in output:
            try:
                data = json.loads(output)
                if "error" in data:
                    errors.append(data["error"][:200])
            except Exception:
                pass
    return errors


def _format_error_answer(errors: list[str]) -> str:
    if errors:
        unique = list(dict.fromkeys(errors))[:3]
        detail = "\n".join(f"- {e}" for e in unique)
        return (
            "I tried to query your data but ran into errors:\n\n"
            f"{detail}\n\n"
            "Try rephrasing with specific column names from the file manager, "
            "or check that the relevant files are uploaded."
        )
    return (
        "I wasn't able to find an answer. Try rephrasing your question "
        "with specific file or column names you see in the file manager."
    )
