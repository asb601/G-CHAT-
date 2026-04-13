"""
Query router — classifies user intent and dispatches to the fastest path.

Routes:
  1. metadata   → answer from Postgres (file list, column names, row counts)
  2. precomputed → answer from file_analytics table (stats, distributions, cross-tabs)
  3. agent       → LangGraph agent with DuckDB (complex / ad-hoc queries)
"""
from __future__ import annotations

import json
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import chat_logger
from app.models.file_analytics import FileAnalytics
from app.models.file_metadata import FileMetadata
from app.models.file_relationship import FileRelationship


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


# ── Intent classification (keyword-based, fast, no LLM) ──────────────────────

_METADATA_KEYWORDS = {
    "how many files", "list files", "what files", "show files",
    "what columns", "column names", "schema", "which columns",
    "how many rows", "row count", "how many records",
    "describe the file", "file description",
}

# These patterns force agent routing even when metadata/precomputed keywords also match.
# Use regex-style substrings — if ANY of these appear, skip metadata/precomputed.
_AGENT_OVERRIDE_PATTERNS = [
    "last ", "first ", "top ", "bottom ",   # row/record retrieval
    "show me row", "get me row", "fetch row",
    "10th row", "nth row", "specific row",
    "where ", "filter ", "group by",
    "join ", "between ",
]

_PRECOMPUTED_KEYWORDS = {
    "analytics", "statistics", "summary", "overview",
    "average", "mean", "total", "sum of", "min of", "max of",
    "distribution", "breakdown", "by category", "by status",
    "by region", "by country", "by currency",
    "value counts", "how many unique", "top values",
    "give me entire analytics", "full analytics", "complete analytics",
    "standard deviation", "std dev",
}


def classify_intent(query: str) -> str:
    """
    Returns one of: 'metadata', 'precomputed', 'agent'.
    Agent override patterns take priority — queries that mix data retrieval with
    metadata (e.g. "last 10th row and how many rows?") go to the agent.
    """
    q = query.lower().strip()

    # Agent override: data retrieval patterns always win
    for pattern in _AGENT_OVERRIDE_PATTERNS:
        if pattern in q:
            return "agent"

    for kw in _METADATA_KEYWORDS:
        if kw in q:
            return "metadata"

    for kw in _PRECOMPUTED_KEYWORDS:
        if kw in q:
            return "precomputed"

    return "agent"


# ── Metadata route ────────────────────────────────────────────────────────────

async def answer_from_metadata(
    query: str, db: AsyncSession
) -> dict[str, Any]:
    """Answer file-catalog questions directly from Postgres."""
    start = time.perf_counter()

    result = await db.execute(select(FileMetadata))
    all_meta = list(result.scalars().all())

    q = query.lower()

    if any(kw in q for kw in ("how many files", "list files", "what files", "show files")):
        file_list = [
            {"blob_path": m.blob_path, "rows": m.row_count, "columns": len(m.columns_info or [])}
            for m in all_meta
        ]
        answer = f"There are **{len(all_meta)}** file(s) in the system:\n"
        for f in file_list:
            answer += f"- `{f['blob_path']}` — **{f['rows']:,}** rows, **{f['columns']}** columns\n"
        return _route_result("metadata", answer, file_list, start)

    if any(kw in q for kw in ("what columns", "column names", "schema", "which columns")):
        for m in all_meta:
            if m.columns_info:
                cols = [c["name"] for c in m.columns_info]
                col_list = ", ".join(f"`{c}`" for c in cols)
                answer = f"**{m.blob_path}** has **{len(cols)}** columns: {col_list}"
                return _route_result("metadata", answer, m.columns_info, start)

    if any(kw in q for kw in ("how many rows", "row count", "how many records")):
        for m in all_meta:
            # row_count stores the DuckDB sample size (max 500), not the true total
            row_note = " (sample limit — actual count is much higher)" if m.row_count <= 500 else ""
            answer = f"**{m.blob_path}** has at least **{m.row_count:,}** rows{row_note}. The exact count requires a full scan of the 3GB file."
            return _route_result("metadata", answer, [], start)

    if any(kw in q for kw in ("describe", "description")):
        for m in all_meta:
            answer = m.ai_description or "No description available."
            return _route_result("metadata", answer, [], start)

    return _route_result("metadata", "No matching metadata found.", [], start)


# ── Pre-computed analytics route ──────────────────────────────────────────────

async def answer_from_precomputed(
    query: str, db: AsyncSession
) -> dict[str, Any]:
    """Answer analytics questions from pre-computed stats in file_analytics."""
    start = time.perf_counter()

    result = await db.execute(select(FileAnalytics))
    all_analytics = list(result.scalars().all())

    if not all_analytics:
        return _route_result(
            "precomputed",
            "No pre-computed analytics available. Files may still be processing.",
            [], start,
        )

    q = query.lower()
    # Pick the analytics row most likely to answer this query.
    # Prefer the one with highest row_count (most data), then fall back to first.
    analytics = max(all_analytics, key=lambda a: a.row_count or 0)

    # ── "Give me entire/full/complete analytics" ──
    if any(kw in q for kw in ("entire analytics", "full analytics", "complete analytics",
                               "analytics", "overview", "summary", "statistics")):
        return _format_full_analytics(analytics, start)

    # ── Specific stat questions ──
    if any(kw in q for kw in ("average", "mean")):
        return _format_stat(analytics, "mean", start)
    if any(kw in q for kw in ("total", "sum")):
        return _format_stat(analytics, "sum", start)
    if "min" in q and "of" in q:
        return _format_stat(analytics, "min", start)
    if "max" in q and "of" in q:
        return _format_stat(analytics, "max", start)

    # ── Distribution / breakdown ──
    if any(kw in q for kw in ("distribution", "breakdown", "value counts", "by category",
                               "by status", "by region", "by country", "by currency")):
        return _format_distributions(analytics, q, start)

    # ── Generic fallback with full stats ──
    return _format_full_analytics(analytics, start)


def _format_full_analytics(analytics: FileAnalytics, start: float) -> dict[str, Any]:
    """Format a complete analytics report from pre-computed stats."""
    lines = [
        f"## Analytics for `{analytics.blob_path}`\n",
        f"- **Rows**: {analytics.row_count:,}" + (" *(sample estimate — actual file is much larger)*" if analytics.row_count <= 500 else ""),
        f"- **Columns**: {analytics.column_count}",
        "",
    ]

    # Numeric column stats
    col_stats = analytics.column_stats or {}
    numeric_cols = {k: v for k, v in col_stats.items() if isinstance(v, dict) and v.get("dtype") == "numeric"}
    if numeric_cols:
        lines.append("### Numeric Columns\n")
        for col, stats in numeric_cols.items():
            lines.append(f"**{col}**:")
            lines.append(f"  - Min: **{_fmt(stats.get('min'))}** | Max: **{_fmt(stats.get('max'))}**")
            lines.append(f"  - Mean: **{_fmt(stats.get('mean'))}** | Sum: **{_fmt(stats.get('sum'))}**")
            if stats.get("std"):
                lines.append(f"  - Std Dev: **{_fmt(stats.get('std'))}**")
            lines.append("")

    # Categorical value counts
    value_counts = analytics.value_counts or {}
    cat_cols = {k: v for k, v in value_counts.items()
                if isinstance(v, dict) and not k.endswith("__note")}
    if cat_cols:
        lines.append("### Categorical Distributions\n")
        for col, counts in cat_cols.items():
            lines.append(f"**{col}**:")
            # Sort by count desc
            sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
            for val, cnt in sorted_counts[:10]:
                lines.append(f"  - {val}: **{cnt:,}**")
            lines.append("")

    # Cross-tab highlights
    cross_tabs = analytics.cross_tabs or []
    if cross_tabs:
        lines.append("### Cross-Tab Summaries\n")
        for ct in cross_tabs[:5]:
            lines.append(f"**{ct.get('metric', '?')}** by **{ct.get('group_by', '?')}** (top values):")
            for row in (ct.get("data") or [])[:5]:
                dim = row.get("dimension", "?")
                total = row.get("total", "?")
                avg = row.get("avg", "?")
                lines.append(f"  - {dim}: total **{_fmt(total)}**, avg **{_fmt(avg)}**")
            lines.append("")

    answer = "\n".join(lines)

    # Return tabular data for the frontend grid
    data = []
    for col, stats in numeric_cols.items():
        data.append({"column": col, **{k: v for k, v in stats.items() if k != "dtype"}})

    return _route_result("precomputed", answer, data, start)


def _format_stat(analytics: FileAnalytics, stat_key: str, start: float) -> dict[str, Any]:
    col_stats = analytics.column_stats or {}
    lines = []
    data = []
    for col, stats in col_stats.items():
        if isinstance(stats, dict) and stats.get("dtype") == "numeric":
            val = stats.get(stat_key)
            if val is not None:
                lines.append(f"- **{col}** {stat_key}: **{_fmt(val)}**")
                data.append({"column": col, stat_key: val})

    answer = "\n".join(lines) if lines else f"No {stat_key} data available."
    return _route_result("precomputed", answer, data, start)


def _format_distributions(
    analytics: FileAnalytics, query: str, start: float
) -> dict[str, Any]:
    value_counts = analytics.value_counts or {}
    lines = []
    data = []

    # Try to find the specific column mentioned
    for col, counts in value_counts.items():
        if not isinstance(counts, dict) or col.endswith("__note"):
            continue
        if col.lower() in query or f"by {col.lower()}" in query:
            lines.append(f"**{col}** distribution:")
            sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
            for val, cnt in sorted_counts[:15]:
                lines.append(f"  - {val}: **{cnt:,}**")
                data.append({"value": val, "count": cnt})
            break

    # If no specific column matched, show all
    if not lines:
        for col, counts in value_counts.items():
            if not isinstance(counts, dict) or col.endswith("__note"):
                continue
            lines.append(f"\n**{col}**:")
            sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
            for val, cnt in sorted_counts[:10]:
                lines.append(f"  - {val}: **{cnt:,}**")
                data.append({"column": col, "value": val, "count": cnt})

    answer = "\n".join(lines) if lines else "No distribution data available."
    return _route_result("precomputed", answer, data, start)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _route_result(
    route: str, answer: str, data: list | dict, start: float
) -> dict[str, Any]:
    total_ms = _ms(start)
    chat_logger.info("query_router", route=route, duration_ms=total_ms,
                     answer_preview=answer[:200])
    return {
        "answer": answer,
        "data": data if isinstance(data, list) else [],
        "chart": None,
        "route": route,
        "duration_ms": total_ms,
    }


def _fmt(val: Any) -> str:
    if val is None:
        return "N/A"
    try:
        f = float(val)
        if f == int(f) and abs(f) > 1:
            return f"{int(f):,}"
        return f"{f:,.4f}" if abs(f) < 1 else f"{f:,.2f}"
    except (ValueError, TypeError):
        return str(val)
