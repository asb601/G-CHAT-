"""SQL execution tools — run_sql and run_aggregation.

Key improvements over the old monolith:
  - Uses Parquet path when available (10-50x faster)
  - Proper async timeout (30s default) instead of asyncio.run() bypass
  - Results capped at 1000 rows
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
    """Return run_sql and run_aggregation tools bound to connection context."""

    @tool
    def run_sql(sql: str) -> str:
        """Execute a DuckDB SQL query against Azure Blob Storage files.
        The ready-to-use parquet path is in the system prompt — use it directly.
        Parquet syntax: read_parquet('az://CONTAINER/filename.parquet')
        CSV syntax:     read_csv_auto('az://CONTAINER/filename.csv')
        Always LIMIT results (max 1000 rows). Use TRY_CAST for date columns.
        Returns row count, column names, 5-row preview, and stores full results."""
        sql_upper = sql.strip().upper()
        for bad in ("DROP ", "DELETE ", "UPDATE ", "INSERT ", "CREATE ", "ALTER ", "TRUNCATE "):
            if bad in sql_upper:
                return json.dumps({"error": f"DML statement not allowed: {bad.strip()}"})

        try:
            loop = asyncio.new_event_loop()
            try:
                rows = loop.run_until_complete(
                    execute_query(sql, connection_string, timeout_seconds=120)
                )
            finally:
                loop.close()

            state_store["sql_results"] = rows
            preview = rows[:5]
            return json.dumps({
                "row_count": len(rows),
                "columns": list(rows[0].keys()) if rows else [],
                "preview_rows": preview,
                "note": f"Full {len(rows)} rows in memory for the final answer.",
            }, default=str)
        except asyncio.TimeoutError:
            return json.dumps({"error": "Query timed out after 120 seconds."})
        except Exception as exc:
            return json.dumps({"error": str(exc)[:500]})

    @tool
    def run_aggregation(blob_path: str, group_by: str, metric_col: str, agg_func: str = "sum") -> str:
        """Run a fast GROUP BY aggregation on a file without writing full SQL.
        Parameters:
          blob_path: the file path
          group_by:  comma-separated column names to group by
          metric_col: the numeric column to aggregate
          agg_func:  one of sum, mean, count, max, min
        Returns top 20 groups by descending metric."""
        valid_aggs = {"sum", "mean", "count", "max", "min"}
        if agg_func not in valid_aggs:
            return json.dumps({"error": f"agg_func must be one of {valid_aggs}"})

        groups = [c.strip() for c in group_by.split(",") if c.strip()]
        if not groups:
            return json.dumps({"error": "group_by must not be empty"})

        # Prefer Parquet when available
        if parquet_blob_path:
            source = f"read_parquet('az://{container_name}/{parquet_blob_path}')"
        else:
            source = f"read_csv_auto('az://{container_name}/{blob_path}')"

        group_sql = ", ".join(f'"{g}"' for g in groups)
        metric_expr = (
            f"COUNT(*)" if agg_func == "count"
            else f"{agg_func.upper()}(TRY_CAST(\"{metric_col}\" AS DOUBLE))"
        )
        sql = (
            f"SELECT {group_sql}, {metric_expr} AS {agg_func}_{metric_col} "
            f"FROM {source} "
            f"GROUP BY {group_sql} "
            f"ORDER BY {agg_func}_{metric_col} DESC NULLS LAST "
            f"LIMIT 20"
        )

        try:
            loop = asyncio.new_event_loop()
            try:
                rows = loop.run_until_complete(
                    execute_query(sql, connection_string, timeout_seconds=120)
                )
            finally:
                loop.close()

            state_store["sql_results"] = rows
            return json.dumps({"rows": rows, "row_count": len(rows), "sql_used": sql}, default=str)
        except asyncio.TimeoutError:
            return json.dumps({"error": "Aggregation timed out after 120 seconds. Try query_precomputed_analytics for instant results instead."})
        except Exception as exc:
            return json.dumps({"error": str(exc)[:400], "sql_attempted": sql})

    return [run_sql, run_aggregation]
