"""SQL execution tool — run_sql.

Key properties:
  - Uses Parquet path when available (10-50x faster than CSV)
  - Proper async timeout (120s)
  - Results capped at 1000 rows server-side with truncation warning
"""
from __future__ import annotations

import asyncio
import json

from langchain_core.tools import tool

from app.core.duckdb_client import execute_query


def build_sql_tools(
    connection_string: str,
    container_name: str,
    parquet_blob_path: str | None,
    state_store: dict,
) -> list:
    """Return SQL tools bound to connection context."""

    @tool
    def run_sql(sql: str) -> str:
        """Execute a DuckDB SQL query against Azure Blob Storage files.
        The file paths and column names are in the system prompt — use them directly.
        Parquet syntax: read_parquet('az://CONTAINER/filename.parquet')
        CSV syntax:     read_csv_auto('az://CONTAINER/filename.csv')
        Use TRY_CAST for date columns. Results are capped at 1000 rows server-side.
        Returns row count, column names, 5-row preview, and stores full results."""
        sql_upper = sql.strip().upper()
        for bad in ("DROP ", "DELETE ", "UPDATE ", "INSERT ", "CREATE ", "ALTER ", "TRUNCATE "):
            if bad in sql_upper:
                return json.dumps({"error": f"DML statement not allowed: {bad.strip()}"})

        try:
            loop = asyncio.new_event_loop()
            try:
                rows, total = loop.run_until_complete(
                    execute_query(sql, connection_string, timeout_seconds=120)
                )
            finally:
                loop.close()

            state_store["sql_results"] = rows
            preview = rows[:5]
            resp: dict = {
                "row_count": len(rows),
                "total_rows": total,
                "columns": list(rows[0].keys()) if rows else [],
                "preview_rows": preview,
            }
            if total > len(rows):
                resp["warning"] = (
                    f"Results truncated: showing {len(rows)} of {total} total rows. "
                    "Add a LIMIT, WHERE, or GROUP BY to get complete results."
                )
            return json.dumps(resp, default=str)
        except asyncio.TimeoutError:
            return json.dumps({"error": "Query timed out after 120 seconds."})
        except Exception as exc:
            return json.dumps({"error": str(exc)[:500]})

    return [run_sql]
