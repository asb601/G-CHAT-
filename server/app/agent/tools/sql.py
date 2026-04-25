"""SQL execution tool — run_sql.

Key properties:
  - Uses Parquet path when available (10-50x faster than CSV)
  - Synchronous — runs inside LangGraph's thread pool, no event loop needed
  - Results capped at 1000 rows server-side with truncation warning
"""
from __future__ import annotations

import json
import time

from langchain_core.tools import tool

from app.core.duckdb_client import execute_query_sync
from app.core.logger import chat_logger, pipeline_logger


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

        # ── Log the complete SQL before execution ──────────────────────────────
        pipeline_logger.info("sql_execute_start", sql=sql)

        t_exec = time.perf_counter()
        try:
            rows, total = execute_query_sync(sql, connection_string)
            duration_ms = round((time.perf_counter() - t_exec) * 1000, 2)

            # ── Log full result: columns + first 20 rows + timing ──────────────
            pipeline_logger.info(
                "sql_execute_done",
                sql=sql,
                duration_ms=duration_ms,
                rows_returned=len(rows),
                total_rows=total,
                columns=list(rows[0].keys()) if rows else [],
                preview_rows=rows[:20],  # first 20 rows in the log
            )

            chat_logger.info("run_sql_result",
                             sql_preview=sql[:300],
                             rows_returned=len(rows),
                             total_rows=total,
                             duration_ms=duration_ms)

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
            # Detect failed join: SQL has JOIN but joined columns came back entirely null
            if rows and "JOIN" in sql_upper:
                all_null_cols = [
                    col for col in rows[0].keys()
                    if all(
                        row.get(col) is None or row.get(col) == ""
                        for row in rows
                    )
                ]
                if all_null_cols:
                    resp["join_warning"] = (
                        f"JOIN produced 0 matches: columns {all_null_cols} are entirely null. "
                        "The two files use incompatible ID systems — do NOT retry or recast the join. "
                        "STOP. Query the primary file alone using its own IDs, return that data, "
                        "and tell the user which columns could not be enriched and why."
                    )
            return json.dumps(resp, default=str)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - t_exec) * 1000, 2)
            pipeline_logger.error(
                "sql_execute_error",
                sql=sql,
                duration_ms=duration_ms,
                error=str(exc),  # full error, no truncation
            )
            return json.dumps({"error": str(exc)[:500]})

    return [run_sql]
