"""Catalog & schema tools — search files and inspect columns."""
from __future__ import annotations

import json
from langchain_core.tools import tool


def build_catalog_tools(
    catalog: list[dict],
    relationships: list[dict],
    parquet_paths: dict[str, str] | None = None,
    container_name: str = "",
) -> list:
    """Return search_catalog and get_file_schema tools bound to the catalog."""

    def _sql_path(blob_path: str) -> str:
        """Return the SQL-ready expression for a blob_path."""
        if parquet_paths and blob_path in parquet_paths:
            return f"read_parquet('az://{container_name}/{parquet_paths[blob_path]}')"
        return blob_path

    @tool
    def search_catalog(query: str) -> str:
        """Search the ingested file catalog to find files relevant to the user's question.
        Returns file paths, descriptions, columns, and what they are good for.
        Use when you need to discover which file to query or what columns are available."""
        if not catalog:
            return json.dumps({"error": "No files have been ingested yet."})

        results = []
        q_lower = query.lower()
        for f in catalog:
            desc = (f.get("ai_description") or "").lower()
            cols = " ".join(c["name"].lower() for c in (f.get("columns_info") or []))
            good_for = " ".join((f.get("good_for") or [])).lower()
            if any(word in desc + cols + good_for for word in q_lower.split()):
                results.append({
                    "blob_path": f["blob_path"],
                    "sql_path": _sql_path(f["blob_path"]),
                    "description": f.get("ai_description", ""),
                    "columns": [c["name"] for c in (f.get("columns_info") or [])],
                    "key_metrics": f.get("key_metrics") or [],
                    "key_dimensions": f.get("key_dimensions") or [],
                    "good_for": f.get("good_for") or [],
                    "date_range": f"{f.get('date_range_start')} → {f.get('date_range_end')}",
                    "relationships": [
                        f"{r['file_b_path']} via {r['shared_column']}"
                        for r in relationships
                        if r["file_a_path"] == f["blob_path"] and r["confidence_score"] > 0.5
                    ],
                })

        if not results:
            results = [
                {
                    "blob_path": f["blob_path"],
                    "sql_path": _sql_path(f["blob_path"]),
                    "description": f.get("ai_description", ""),
                    "columns": [c["name"] for c in (f.get("columns_info") or [])],
                }
                for f in catalog[:10]
            ]

        return json.dumps({"files": results, "total": len(results)}, default=str)

    @tool
    def get_file_schema(blob_path: str) -> str:
        """Get the full column schema, sample values, and data types for a specific file.
        Use this to understand exact column names and types before writing SQL."""
        # Exact match first
        match = next((f for f in catalog if f["blob_path"] == blob_path), None)

        # Fuzzy fallback: try substring match on blob_path
        if not match:
            q = blob_path.lower()
            match = next(
                (f for f in catalog if q in f["blob_path"].lower() or f["blob_path"].lower() in q),
                None,
            )

        # Fuzzy fallback: try matching against description
        if not match:
            match = next(
                (f for f in catalog if q in (f.get("ai_description") or "").lower()),
                None,
            )

        if not match:
            available = [f["blob_path"] for f in catalog[:15]]
            return json.dumps({
                "error": f"File '{blob_path}' not found.",
                "available_files": available,
                "hint": "Use one of the blob_path values above, or call search_catalog to find the right file.",
            })

        cols = []
        for c in (match.get("columns_info") or []):
            cols.append({
                "name": c["name"],
                "type": c.get("type", "unknown"),
                "sample_values": c.get("sample_values", [])[:5],
                "unique_count": len(c.get("unique_values", [])),
            })

        return json.dumps({
            "blob_path": match["blob_path"],
            "sql_path": _sql_path(match["blob_path"]),
            "sql_hint": "Use the sql_path value directly in your SQL FROM clause.",
            "columns": cols,
            "key_metrics": match.get("key_metrics") or [],
            "key_dimensions": match.get("key_dimensions") or [],
            "date_range": {
                "start": match.get("date_range_start"),
                "end": match.get("date_range_end"),
            },
        }, default=str)

    return [search_catalog, get_file_schema]
