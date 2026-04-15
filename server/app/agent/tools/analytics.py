"""Query pre-computed analytics tool — answers stat questions instantly from Postgres."""
from __future__ import annotations

import json
from langchain_core.tools import tool


def build_analytics_tool(precomputed: dict | None) -> list:
    """Return a tool that queries the pre-computed analytics stored at ingest time."""

    @tool
    def query_precomputed_analytics(question: str) -> str:
        """Return pre-computed schema stats captured from a 500-row sample at ingest time.
        Useful for understanding column types, value ranges, and what categories/values exist.
        WARNING: All counts and totals in this data are from a 500-row sample only —
        they do NOT reflect the full dataset. For accurate counts, totals, or any
        aggregation on the full data, use run_sql with the parquet path instead."""
        if not precomputed:
            return json.dumps({"error": "No pre-computed analytics available for this file."})

        col_stats = precomputed.get("column_stats") or {}
        value_counts = precomputed.get("value_counts") or {}
        cross_tabs = precomputed.get("cross_tabs") or []

        result = {
            "WARNING": "All numbers below are from a 500-row ingest sample. Use run_sql on the parquet path for accurate full-dataset results.",
            "column_stats": col_stats,
            "value_counts": value_counts,
            "cross_tabs": cross_tabs,
        }

        return json.dumps(result, default=str)

    return [query_precomputed_analytics]
