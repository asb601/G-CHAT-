"""
inspect_column tool — return column-level facts (dtype, samples, suggested
predicate) for a specific (blob_path, column_name) pair.

This is the discovery primitive that lets us delete swaths of "rules-style"
prose from the system prompt. Instead of telling the LLM in English how to
filter Oracle DD-MON-YYYY strings or float64 year columns, the LLM calls
inspect_column and gets back a concrete suggested_predicate it can paste
into a WHERE clause.

The tool runs synchronously (LangGraph executes tools in a thread pool)
and reads from the column metadata that the ingestion pipeline already
captured, plus a lazy DuckDB probe for distinct count when needed.
"""
from __future__ import annotations

import json
import re
from typing import Iterable

from langchain_core.tools import tool

from app.core.duckdb_client import execute_query_sync
from app.core.logger import pipeline_logger


# Tokens in column names that strongly imply identifier semantics — used to
# steer the suggested predicate away from numeric / date coercion.
_IDENTIFIER_HINTS = ("_id", "_num", "_no", "_key", "_code", "_ref", "_uuid")


def _is_identifier_name(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(h) for h in _IDENTIFIER_HINTS) or n in {
        "id", "uuid", "guid", "sku", "isin", "cusip", "ein", "ssn",
    }


def _looks_like_oracle_date(samples: Iterable) -> bool:
    """True if samples look like '19-MAR-2018' / '21-AUG-2022'."""
    pattern = re.compile(r"^\d{1,2}-[A-Za-z]{3}-\d{2,4}$")
    matched = 0
    total = 0
    for s in samples:
        if s is None:
            continue
        total += 1
        if pattern.match(str(s).strip()):
            matched += 1
        if total >= 5:
            break
    return total > 0 and matched / total >= 0.6


def _suggest_predicate(col_name: str, dtype: str, samples: list) -> str:
    """Return a one-line suggested WHERE-clause snippet for this column.

    The agent should treat this as a hint, not a contract. It encodes the
    same knowledge that used to live as English rules in the system prompt
    (Oracle date strings, float-typed year columns, identifier preservation).
    """
    name_l = (col_name or "").lower()
    dtype_l = (dtype or "").lower()

    if _is_identifier_name(col_name):
        return (
            f"{col_name} = '<value>'  "
            f"-- identifier column; compare as string, never CAST or EXTRACT"
        )

    if _looks_like_oracle_date(samples):
        return (
            f"strptime({col_name}, '%d-%b-%Y') BETWEEN DATE '<start>' AND DATE '<end>'  "
            f"-- Oracle DD-MON-YYYY string; or use {col_name} LIKE '%-MMM-YYYY' for month filter"
        )

    if "year" in name_l or name_l.endswith("_yr"):
        if "float" in dtype_l:
            return f"{col_name} = 2024.0  -- column is float; match with .0"
        if "int" in dtype_l:
            return f"{col_name} = 2024"
        return f"{col_name} = '2024'  -- column is string; match as string"

    if "date" in dtype_l or "timestamp" in dtype_l:
        return (
            f"{col_name} BETWEEN DATE '<start>' AND DATE '<end>'  "
            f"-- or EXTRACT(YEAR FROM {col_name}) = 2024"
        )

    if "int" in dtype_l or "float" in dtype_l or "decimal" in dtype_l:
        return f"{col_name} > 0  -- numeric column"

    return f"{col_name} = '<value>'  -- string column; use LIKE for partial match"


def _sql_path(blob_path: str, parquet_paths: dict[str, str], container: str) -> str:
    if parquet_paths and blob_path in parquet_paths:
        return f"read_parquet('az://{container}/{parquet_paths[blob_path]}')"
    return (
        f"read_csv_auto('az://{container}/{blob_path}', "
        f"sample_size=500, null_padding=true, ignore_errors=true)"
    )


def build_column_tool(
    catalog: list[dict],
    parquet_paths: dict[str, str],
    container_name: str,
    connection_string: str,
) -> list:
    """Return the inspect_column tool bound to the request's catalog + DuckDB."""

    catalog_by_blob = {e["blob_path"]: e for e in catalog if e.get("blob_path")}

    @tool
    def inspect_column(blob_path: str, column_name: str) -> str:
        """Return facts about a single column: dtype, sample values, an
        optional distinct count, and a one-line suggested WHERE predicate.

        Use this BEFORE writing a filter when you are unsure how the
        column is stored — for example: "is PERIOD_YEAR an int or a float?",
        "does INVOICE_DATE store ISO dates or '19-MAR-2018' strings?", or
        "what does ACCOUNT_TYPE look like as a value?"

        The suggested_predicate is a hint; you may adapt it. Always paste
        the dtype + sample_values into your reasoning before writing SQL.
        """
        entry = catalog_by_blob.get(blob_path)
        if not entry:
            # Allow stem match (same forgiving behaviour as get_file_schema).
            stem = blob_path.lower()
            if stem.startswith("az://"):
                stem = stem.split("/", 3)[-1]
            if "." in stem:
                stem = stem.rsplit(".", 1)[0]
            for path, e in catalog_by_blob.items():
                p = path.lower()
                p_stem = p.rsplit(".", 1)[0] if "." in p else p
                if stem == p_stem or stem in p_stem:
                    entry = e
                    blob_path = path
                    break

        if not entry:
            return json.dumps({
                "error": f"File '{blob_path}' not found in catalog.",
                "hint": "Call search_catalog first to find the correct blob_path.",
            })

        # Try the heavy columns_info first (present when the file is in the
        # request's hydrated shortlist). Fall back to the lean column_names
        # list, which carries names only.
        col_record: dict | None = None
        for c in (entry.get("columns_info") or []):
            if isinstance(c, dict) and c.get("name", "").lower() == column_name.lower():
                col_record = c
                column_name = c["name"]  # canonical case
                break

        dtype = "unknown"
        samples: list = []
        unique_count = 0
        if col_record:
            dtype = col_record.get("type", "unknown")
            samples = list(col_record.get("sample_values") or [])[:5]
            unique_count = len(col_record.get("unique_values") or [])
        else:
            # Column wasn't hydrated; verify it at least exists by name.
            lean_names = entry.get("column_names") or []
            match_name = next(
                (n for n in lean_names if n.lower() == column_name.lower()),
                None,
            )
            if not match_name:
                return json.dumps({
                    "error": f"Column '{column_name}' not found in {blob_path}.",
                    "available_columns": lean_names[:30],
                })
            column_name = match_name

        # If we still don't have samples, do a tiny DuckDB probe for them.
        # Bounded to 5 distinct values and a top-of-file sample.
        if not samples:
            try:
                sql = (
                    f"SELECT DISTINCT \"{column_name}\" AS v "
                    f"FROM {_sql_path(blob_path, parquet_paths, container_name)} "
                    f"WHERE \"{column_name}\" IS NOT NULL "
                    f"LIMIT 5"
                )
                rows, _total = execute_query_sync(sql, connection_string, max_rows=5)
                samples = [r.get("v") for r in rows if r.get("v") is not None]
                # Dtype inference from the first non-null value.
                if dtype == "unknown" and samples:
                    py_type = type(samples[0]).__name__
                    dtype = {
                        "str": "string",
                        "int": "int64",
                        "float": "float64",
                        "bool": "boolean",
                    }.get(py_type, py_type)
            except Exception as exc:
                pipeline_logger.warning(
                    "inspect_column_probe_failed",
                    blob_path=blob_path,
                    column=column_name,
                    error=str(exc)[:200],
                )

        suggested = _suggest_predicate(column_name, dtype, samples)

        result = {
            "blob_path": blob_path,
            "column": column_name,
            "dtype": dtype,
            "sample_values": samples,
            "distinct_count": unique_count or None,
            "suggested_predicate": suggested,
        }
        pipeline_logger.info("inspect_column", **result)
        return json.dumps(result, default=str)

    return [inspect_column]
