"""inspect_data_format tool — preview a few rows to understand structure before writing SQL."""
from __future__ import annotations

import json
import os
from langchain_core.tools import tool

from app.core.logger import pipeline_logger


def build_sample_tool(sample_rows_by_blob: dict[str, list[dict]]) -> list:
    """Return a tool that previews ingest-time sample rows for schema understanding."""

    def _resolve_blob_path(blob_path: str) -> str | None:
        if not blob_path:
            return None

        query = blob_path.lower().strip()
        if query.startswith("az://"):
            query = query.split("/", 3)[-1]

        exact_match = next(
            (candidate for candidate in sample_rows_by_blob if candidate.lower() == query),
            None,
        )
        if exact_match:
            return exact_match

        query_stem = os.path.splitext(query)[0]
        stem_match = next(
            (
                candidate
                for candidate in sample_rows_by_blob
                if query_stem
                and query_stem in os.path.splitext(candidate.lower())[0]
            ),
            None,
        )
        if stem_match:
            return stem_match

        return None

    @tool
    def inspect_data_format(blob_path: str, n: int = 5) -> str:
        """Preview a few example rows from a specific file to understand data format,
        column names, value patterns, and date formats before writing SQL.
        Use this when you need to know what the data looks like — e.g. whether a region is
        stored as 'us-east' or 'US East', or what date format is used.
        These rows are from the beginning of the file only — do NOT use them as the answer
        to the user's question. Always run SQL on the parquet for actual results."""
        resolved_blob_path = _resolve_blob_path(blob_path)
        if not resolved_blob_path:
            available_files = list(sample_rows_by_blob.keys())[:15]
            pipeline_logger.info(
                "inspect_data_format",
                blob_path=blob_path,
                resolved_blob_path=None,
                n=n,
                available=False,
            )
            return json.dumps({
                "error": f"File '{blob_path}' not found.",
                "available_files": available_files,
                "hint": "Pass a blob_path from search_catalog/get_file_schema before calling inspect_data_format.",
            })

        sample_rows = sample_rows_by_blob.get(resolved_blob_path) or []
        if not sample_rows:
            pipeline_logger.info(
                "inspect_data_format",
                blob_path=blob_path,
                resolved_blob_path=resolved_blob_path,
                n=n,
                available=False,
            )
            return json.dumps({"error": "No sample rows available."})

        n = max(1, min(n, 20))
        rows = sample_rows[:n]
        pipeline_logger.info(
            "inspect_data_format",
            blob_path=blob_path,
            resolved_blob_path=resolved_blob_path,
            n=n,
            available=True,
            total_sample_rows=len(sample_rows),
            columns=list(rows[0].keys()) if rows else [],
            rows=rows,
        )
        return json.dumps({
            "blob_path": resolved_blob_path,
            "format_preview": rows,
            "note": "These are example rows for understanding data format only. Use run_sql on the parquet path for real answers.",
        }, default=str)

    return [inspect_data_format]
