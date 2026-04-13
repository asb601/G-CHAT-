"""query_sample_rows tool — instant row access from Postgres-stored sample."""
from __future__ import annotations

import json
from langchain_core.tools import tool


def build_sample_tool(sample_rows: list[dict]) -> list:
    """Return a tool that queries the in-memory sample rows without any DuckDB call."""

    @tool
    def query_sample_rows(n: int = 10, offset: int = 0) -> str:
        """Return rows from the file's stored sample (captured at ingest time).
        IMPORTANT LIMITATIONS — be upfront with the user about these:
        - This sample contains only the FIRST rows of the file (rows 1 to ~500)
        - It CANNOT answer: last row, bottom N rows, rows by specific ID/filter beyond the sample
        - For those, the user needs to wait for Parquet conversion to complete
        Use for: 'show me some rows', 'what does the data look like', 'give me the first N rows'
        Do NOT use for: 'last row', 'bottom 10', 'row number 5000', specific-position queries
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
                "error": f"Offset {offset} is beyond the sample size ({len(sample_rows)} rows). "
                         "This sample only covers the beginning of the file. "
                         "Queries for specific positions beyond the sample require Parquet conversion."
            })

        return json.dumps({
            "rows": window,
            "returned": len(window),
            "sample_total": len(sample_rows),
            "limitation": (
                f"SAMPLE ONLY: rows {offset + 1}–{offset + len(window)} from the beginning of the file. "
                f"Cannot access rows beyond position {len(sample_rows)}. "
                "Full file queries require Parquet conversion (runs in background after ingest)."
            ),
        }, default=str)

    return [query_sample_rows]
