"""Summarise dataframe tool — computes stats from an in-memory result set."""
from __future__ import annotations

import json
from typing import Any

import pandas as pd
from langchain_core.tools import tool

from app.core.logger import pipeline_logger


def build_stats_tool(state_store: dict) -> list:
    """Return summarise_dataframe tool bound to the request's state store."""

    @tool
    def summarise_dataframe(focus: str = "") -> str:
        """Compute statistical summary of the last run_sql result already in memory.
        focus: optional comma-separated column names to focus on.
        Returns counts, means, top-values, nulls for numeric+categorical columns."""
        rows = state_store.get("sql_results", [])
        pipeline_logger.info(
            "summarise_dataframe_start",
            focus=focus or "(all columns)",
            rows_in_memory=len(rows),
        )
        if not rows:
            return json.dumps({"error": "No SQL result in memory. Call run_sql first."})

        df = pd.DataFrame(rows)
        if focus:
            cols_to_use = [c.strip() for c in focus.split(",") if c.strip() in df.columns]
            if cols_to_use:
                df = df[cols_to_use]

        summary: dict[str, Any] = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
        }

        for col in df.columns:
            col_info: dict[str, Any] = {"dtype": str(df[col].dtype), "nulls": int(df[col].isna().sum())}
            if pd.api.types.is_numeric_dtype(df[col]):
                described = df[col].describe()
                col_info.update({
                    "min": round(float(described["min"]), 4) if not pd.isna(described["min"]) else None,
                    "max": round(float(described["max"]), 4) if not pd.isna(described["max"]) else None,
                    "mean": round(float(described["mean"]), 4) if not pd.isna(described["mean"]) else None,
                    "sum": round(float(df[col].sum()), 4),
                    "std": round(float(described["std"]), 4) if not pd.isna(described["std"]) else None,
                })
            else:
                vc = df[col].value_counts().head(10)
                col_info["top_values"] = {str(k): int(v) for k, v in vc.items()}
                col_info["unique_count"] = int(df[col].nunique())
            summary["columns"][col] = col_info

        pipeline_logger.info(
            "summarise_dataframe_done",
            focus=focus or "(all columns)",
            row_count=summary["row_count"],
            column_count=summary["column_count"],
            columns_summary=summary["columns"],
        )

        return json.dumps(summary, default=str)

    return [summarise_dataframe]
