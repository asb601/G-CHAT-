"""query_sample_rows tool — instant row access from Postgres-stored sample."""
from __future__ import annotations

import json
from langchain_core.tools import tool


def build_sample_tool(sample_rows: list[dict]) -> list:
    """Return a tool that queries the in-memory sample rows without any DuckDB call."""

    @tool
    def query_sample_rows(n: int = 10, offset: int = 0) -> str:
        """Return rows from the file's ingest-time sample (first ~500 rows of the file).
        Instant — no SQL, no network call.
        Parameters:
          n:      number of rows to return (default 10, max 100)
          offset: starting row index within the sample (0 = first row)"""
        if not sample_rows:
            return json.dumps({"error": "No sample rows available for this file."})

        n = max(1, min(n, 100))
        offset = max(0, offset)
        window = sample_rows[offset: offset + n]

        if not window:
            return json.dumps({
                "error": f"Offset {offset} is beyond the available sample ({len(sample_rows)} rows)."
            })

        return json.dumps({
            "rows": window,
            "returned": len(window),
            "sample_total": len(sample_rows),
        }, default=str)

    return [query_sample_rows]
