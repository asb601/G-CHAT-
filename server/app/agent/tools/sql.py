"""SQL execution tool — run_sql.

Key properties:
  - Uses Parquet path when available (10-50x faster than CSV)
  - Synchronous — runs inside LangGraph's thread pool, no event loop needed
  - Results capped at 1000 rows server-side with truncation warning
  - 60-second timeout to prevent hanging on huge files
"""
from __future__ import annotations

import json
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

from langchain_core.tools import tool

from app.core.duckdb_client import execute_query_sync
from app.core.logger import chat_logger

_SQL_TIMEOUT = 60  # seconds


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
            # Run with timeout to prevent hanging on huge files
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(execute_query_sync, sql, connection_string)
                try:
                    rows, total = future.result(timeout=_SQL_TIMEOUT)
                except FuturesTimeout:
                    chat_logger.warning("run_sql_timeout", sql_preview=sql[:200],
                                        timeout_s=_SQL_TIMEOUT)
                    return json.dumps({"error": f"Query timed out after {_SQL_TIMEOUT}s. "
                                       "Try adding a LIMIT or more specific WHERE clause."})

            chat_logger.info("run_sql_result",
                             sql_preview=sql[:200],
                             rows_returned=len(rows),
                             total_rows=total)

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
        except FuturesTimeout:
            return json.dumps({"error": f"Query timed out after {_SQL_TIMEOUT}s."})
        except Exception as exc:
            return json.dumps({"error": str(exc)[:500]})

    return [run_sql]
