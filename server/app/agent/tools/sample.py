"""inspect_data_format tool — preview a few rows to understand structure before writing SQL."""
from __future__ import annotations

import json
from langchain_core.tools import tool


def build_sample_tool(sample_rows: list[dict]) -> list:
    """Return a tool that previews ingest-time sample rows for schema understanding."""

    @tool
    def inspect_data_format(n: int = 5) -> str:
        """Preview a few example rows from the file to understand data format, column names,
        value patterns, and date formats before writing SQL.
        Use this when you need to know what the data looks like — e.g. whether a region is
        stored as 'us-east' or 'US East', or what date format is used.
        These rows are from the beginning of the file only — do NOT use them as the answer
        to the user's question. Always run SQL on the parquet for actual results."""
        if not sample_rows:
            return json.dumps({"error": "No sample rows available."})

        n = max(1, min(n, 20))
        return json.dumps({
            "format_preview": sample_rows[:n],
            "note": "These are example rows for understanding data format only. Use run_sql on the parquet path for real answers.",
        }, default=str)

    return [inspect_data_format]
