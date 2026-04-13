import asyncio
import hashlib
import threading
import time

import duckdb

from app.core.logger import ingest_logger, chat_logger


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


# ── Thread-local connection cache ─────────────────────────────────────────────
# Each worker thread gets its own DuckDB connection, reused across calls.
# INSTALL/LOAD azure runs only once per thread — not once per query.
_thread_local = threading.local()


def _get_connection(connection_string: str) -> duckdb.DuckDBPyConnection:
    """Return a cached DuckDB connection for the current thread."""
    key = hashlib.md5(connection_string.encode()).hexdigest()
    cache: dict = getattr(_thread_local, "connections", None)
    if cache is None:
        _thread_local.connections = {}
        cache = _thread_local.connections
    if key not in cache:
        conn = duckdb.connect()
        conn.execute("INSTALL azure; LOAD azure;")
        safe_conn = connection_string.replace("'", "''")
        conn.execute(f"SET azure_storage_connection_string='{safe_conn}';")
        cache[key] = conn
    return cache[key]


def _clear_connection(connection_string: str) -> None:
    """Evict a potentially corrupted connection so the next call recreates it."""
    key = hashlib.md5(connection_string.encode()).hexdigest()
    cache: dict = getattr(_thread_local, "connections", {})
    cache.pop(key, None)


async def sample_file(
    blob_path: str, connection_string: str, container_name: str
) -> dict:
    """
    Read first 500 rows from a CSV/TXT in Azure Blob via DuckDB.
    COUNT(*) is skipped — uses sample size as row count to avoid full-file scans.
    """

    def _run() -> dict:
        try:
            conn = _get_connection(connection_string)
            azure_path = f"az://{container_name}/{blob_path}"

            t = time.perf_counter()
            df = conn.execute(
                f"""
                SELECT * FROM read_csv_auto(
                    '{azure_path}',
                    sample_size=500,
                    null_padding=true,
                    ignore_errors=true
                ) LIMIT 500
                """
            ).df()
            read_ms = _ms(t)

            columns_info: list[dict] = []
            for col in df.columns:
                unique_vals = df[col].dropna().unique().tolist()[:20]
                sample_vals = df[col].dropna().head(3).tolist()
                columns_info.append(
                    {
                        "name": col,
                        "type": str(df[col].dtype),
                        "sample_values": [str(v) for v in sample_vals],
                        "unique_values": [str(v) for v in unique_vals],
                    }
                )

            def _json_safe(rows: list[dict]) -> list[dict]:
                """Convert any non-JSON-serializable values (Timestamp, etc.) to strings."""
                safe = []
                for row in rows:
                    safe.append({
                        k: v.isoformat() if hasattr(v, "isoformat") else
                           (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
                        for k, v in row.items()
                    })
                return safe

            return {
                "columns_info": columns_info,
                "sample_rows": _json_safe(df.fillna("").to_dict("records")),
                "row_count": len(df),
                "row_count_approx": len(df) == 500,
                "column_names": list(df.columns),
                "_read_ms": read_ms,
            }
        except Exception:
            _clear_connection(connection_string)
            raise

    start = time.perf_counter()
    ingest_logger.info("duckdb", operation="sample_file", status="started",
                       blob_path=blob_path)
    result = await asyncio.to_thread(_run)
    approx = result.pop("row_count_approx")
    read_ms = result.pop("_read_ms")
    ingest_logger.info("duckdb", operation="sample_file", status="done",
                       blob_path=blob_path,
                       columns=len(result["columns_info"]),
                       row_count=result["row_count"],
                       row_count_note="500+ (sample limit)" if approx else "exact",
                       duration_ms=read_ms)
    return result


async def execute_query(
    sql: str, connection_string: str, timeout_seconds: int = 30
) -> list[dict]:
    """
    Execute DuckDB SQL against Azure Blob.
    Raises on failure — caller handles retry.
    Default timeout reduced to 30s to keep chat responsive.
    """

    def _run() -> list[dict]:
        try:
            conn = _get_connection(connection_string)
            result = conn.execute(sql).df()

            def _json_safe(rows: list[dict]) -> list[dict]:
                safe = []
                for row in rows:
                    safe.append({
                        k: v.isoformat() if hasattr(v, "isoformat") else
                           (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
                        for k, v in row.items()
                    })
                return safe

            return _json_safe(result.head(1000).fillna("").to_dict("records"))
        except Exception:
            _clear_connection(connection_string)
            raise

    start = time.perf_counter()
    chat_logger.info("duckdb", operation="execute_query", status="started",
                     sql_preview=sql[:300])
    result = await asyncio.wait_for(
        asyncio.to_thread(_run), timeout=timeout_seconds
    )
    chat_logger.info("duckdb", operation="execute_query", status="done",
                     row_count=len(result), duration_ms=_ms(start))
    return result


def _resolve_data_path(
    blob_path: str, connection_string: str, container_name: str,
    parquet_blob_path: str | None,
) -> str:
    """
    Return the fastest read path for a file.
    Prefer Parquet if available, fall back to CSV.
    """
    if parquet_blob_path:
        return f"az://{container_name}/{parquet_blob_path}"
    return f"az://{container_name}/{blob_path}"
