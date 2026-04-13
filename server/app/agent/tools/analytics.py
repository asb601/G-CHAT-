"""Query pre-computed analytics tool — answers stat questions instantly from Postgres."""
from __future__ import annotations

import json
from langchain_core.tools import tool


def build_analytics_tool(precomputed: dict | None) -> list:
    """Return a tool that queries the pre-computed analytics stored at ingest time."""

    @tool
    def query_precomputed_analytics(question: str) -> str:
        """Look up pre-computed statistics for the file without running any SQL.
        This is INSTANT and should be tried FIRST for questions about:
        - totals, averages, min, max, standard deviation
        - value distributions (e.g., count by status, by category)
        - cross-tab summaries (e.g., sum of amount by region)
        - row counts, column counts
        Returns the matching pre-computed data."""
        if not precomputed:
            return json.dumps({"error": "No pre-computed analytics available for this file."})

        q = question.lower()
        result: dict = {}

        # Row/column counts
        if any(kw in q for kw in ("row", "record", "count", "size")):
            result["row_count"] = precomputed.get("row_count")
            result["column_count"] = precomputed.get("column_count")

        # Per-column stats
        col_stats = precomputed.get("column_stats") or {}
        if any(kw in q for kw in ("average", "mean", "total", "sum", "min", "max", "std", "stats", "statistic")):
            result["column_stats"] = col_stats

        # Value distributions
        value_counts = precomputed.get("value_counts") or {}
        if any(kw in q for kw in ("distribution", "breakdown", "by ", "value count", "unique", "category", "status", "region", "country", "currency")):
            # Try to find specific column
            for col_name, counts in value_counts.items():
                if isinstance(counts, dict) and not col_name.endswith("__note"):
                    if col_name.lower() in q:
                        result[f"distribution_{col_name}"] = counts
            if not any(k.startswith("distribution_") for k in result):
                result["all_distributions"] = value_counts

        # Cross-tab summaries
        cross_tabs = precomputed.get("cross_tabs") or []
        if any(kw in q for kw in ("cross", "by ", "group", "pivot", "breakdown")):
            result["cross_tabs"] = cross_tabs

        # If nothing matched, return everything
        if not result:
            result = {
                "row_count": precomputed.get("row_count"),
                "column_count": precomputed.get("column_count"),
                "column_stats": col_stats,
                "value_counts": value_counts,
                "cross_tabs": cross_tabs,
            }

        return json.dumps(result, default=str)

    return [query_precomputed_analytics]
