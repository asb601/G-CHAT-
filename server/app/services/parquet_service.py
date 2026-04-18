"""
Parquet conversion service — converts CSV to Parquet using DuckDB's az:// reader.

Why DuckDB instead of PyArrow:
  PyArrow required downloading the full CSV to disk first (50+ min on slow connections).
  DuckDB reads directly from az://container/blob.csv via HTTP range requests — no local
  CSV temp file. It also uses all CPU cores for CSV parsing (multi-threaded by default).
  The old "DuckDB az:// bug" (1MB buffer / thousands of HTTP calls) was fixed in DuckDB 1.1.0.
  We're on 1.5.1.

Steps:
  1. DuckDB COPY reads az://container/blob.csv → local Parquet temp file
     (streaming HTTP, parallel reads, ZSTD compression, 100k rows per row group)
  2. Azure SDK uploads Parquet back to blob storage (3-5x smaller than CSV)
  3. Temp Parquet file deleted

Peak memory: DuckDB internal buffers (~256–512MB), regardless of file size.
For a 3GB CSV: total ~2-5 minute conversion + ~1 min upload.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import time

import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient

from app.core.cost_tracker import track_azure_blob
from app.core.duckdb_client import _clear_connection, _get_connection
from app.core.logger import ingest_logger

# ── In-memory progress store (keyed by job_id) ────────────────────────────────
# { job_id: {"phase": "converting"|"uploading", "pct": 0-100} }
_PROGRESS: dict[str, dict] = {}


def get_progress(job_id: str) -> dict | None:
    """Called by the job-status API to get live conversion progress."""
    return _PROGRESS.get(job_id)


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


def _run_conversion(
    blob_path: str,
    connection_string: str,
    container_name: str,
    parquet_blob_path: str,
    job_id: str | None = None,
) -> dict:
    """
    Synchronous conversion. Runs inside asyncio.to_thread() — never blocks the event loop.

    Steps:
      1. DuckDB reads CSV from az://container/blob.csv → local Parquet temp file
      2. Azure SDK uploads Parquet back to Azure Blob Storage
      3. Temp Parquet file deleted

    Returns {"parquet_blob_path": str, "size_bytes": int}
    """
    tmp_parquet_path = None

    try:
        azure_csv_path = f"az://{container_name}/{blob_path}"
        client = BlobServiceClient.from_connection_string(connection_string)

        # Get CSV size upfront for cost tracking
        csv_size_bytes: int = (
            client.get_blob_client(container=container_name, blob=blob_path)
            .get_blob_properties()
            .size
        )

        # ── Step 1: DuckDB converts CSV from Azure → local Parquet temp file ──
        t = time.perf_counter()
        ingest_logger.info("parquet_service", step="convert", status="started",
                           blob_path=blob_path, csv_size_mb=round(csv_size_bytes / 1024 / 1024, 1))

        if job_id:
            _PROGRESS[job_id] = {"phase": "converting", "pct": 0}

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_parquet_path = tmp.name

        conn = _get_connection(connection_string)
        safe_csv = azure_csv_path.replace("'", "''")
        safe_parquet = tmp_parquet_path.replace("'", "''")

        try:
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_csv_auto(
                        '{safe_csv}',
                        null_padding=true,
                        ignore_errors=true
                    )
                )
                TO '{safe_parquet}'
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """)
        except Exception:
            _clear_connection(connection_string)
            raise

        parquet_size_bytes = os.path.getsize(tmp_parquet_path)

        # Row count from Parquet metadata — cheap, no full scan needed
        pf = pq.ParquetFile(tmp_parquet_path)
        total_rows = pf.metadata.num_rows
        num_row_groups = pf.metadata.num_row_groups

        convert_ms = _ms(t)
        ingest_logger.info("parquet_service", step="convert", status="done",
                           parquet_size_mb=round(parquet_size_bytes / 1024 / 1024, 1),
                           total_rows=total_rows,
                           row_groups=num_row_groups,
                           compression_ratio=round(csv_size_bytes / parquet_size_bytes, 2),
                           duration_ms=convert_ms)
        track_azure_blob("download", blob_path, csv_size_bytes, convert_ms)

        # ── Step 2: upload Parquet back to Azure ──────────────────────────────
        t = time.perf_counter()
        ingest_logger.info("parquet_service", step="upload", status="started",
                           parquet_blob_path=parquet_blob_path)
        if job_id:
            _PROGRESS[job_id] = {"phase": "uploading", "pct": 0}

        parquet_blob_client = client.get_blob_client(container=container_name, blob=parquet_blob_path)
        with open(tmp_parquet_path, "rb") as f:
            parquet_blob_client.upload_blob(f, overwrite=True)

        ingest_logger.info("parquet_service", step="upload", status="done",
                           size_mb=round(parquet_size_bytes / 1024 / 1024, 1),
                           duration_ms=_ms(t))
        track_azure_blob("upload", parquet_blob_path, parquet_size_bytes, _ms(t))

        if job_id:
            _PROGRESS.pop(job_id, None)  # clean up — job is done

        return {"parquet_blob_path": parquet_blob_path, "size_bytes": parquet_size_bytes}

    finally:
        if tmp_parquet_path and os.path.exists(tmp_parquet_path):
            try:
                os.unlink(tmp_parquet_path)
            except OSError:
                pass


async def convert_csv_to_parquet(
    blob_path: str,
    connection_string: str,
    container_name: str,
    job_id: str | None = None,
) -> dict:
    """
    Convert a CSV blob to Parquet using DuckDB's az:// reader.
    Runs synchronous conversion in a thread — never blocks the FastAPI event loop.

    Returns {"parquet_blob_path": str, "size_bytes": int}
    Raises on any failure — caller is responsible for catching and recording the error.
    """
    # Handle .csv.gz → .parquet (strip .gz then .csv/.tsv/.txt)
    base = blob_path
    if base.endswith(".gz"):
        base = base[:-3]
    parquet_blob_path = base.rsplit(".", 1)[0] + ".parquet"

    start = time.perf_counter()
    ingest_logger.info("parquet_service", operation="convert_csv_to_parquet",
                       status="started", blob_path=blob_path,
                       target_path=parquet_blob_path)

    result = await asyncio.to_thread(
        _run_conversion,
        blob_path,
        connection_string,
        container_name,
        parquet_blob_path,
        job_id,
    )

    ingest_logger.info("parquet_service", operation="convert_csv_to_parquet",
                       status="done", total_duration_ms=_ms(start),
                       size_bytes=result.get("size_bytes"))

    return result


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


def _run_conversion(
    blob_path: str,
    connection_string: str,
    container_name: str,
    parquet_blob_path: str,
    job_id: str | None = None,
) -> dict:
    """
    Synchronous conversion. Runs inside asyncio.to_thread() — never blocks the event loop.

    Steps:
      1. Download CSV stream → temp file on disk (no RAM for the file itself)
      2. PyArrow open_csv() → ParquetWriter block by block (256MB at a time)
      3. Upload Parquet temp file back to Azure
      4. Clean up both temp files

    Returns {"parquet_blob_path": str, "size_bytes": int}
    """
    tmp_csv_path = None
    tmp_parquet_path = None

    try:
        client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = client.get_blob_client(container=container_name, blob=blob_path)

        # ── Step 1: stream CSV from Azure → temp file ─────────────────────────
        # readinto() uses one persistent HTTP connection and writes directly to disk.
        # Azure SDK handles chunking internally without creating per-MB round trips.
        t = time.perf_counter()
        ingest_logger.info("parquet_service", step="download", status="started",
                           blob_path=blob_path)

        downloader = blob_client.download_blob(max_concurrency=4)
        total_bytes = downloader.size  # known from blob properties

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_csv:
            tmp_csv_path = tmp_csv.name
            written = 0
            last_pct = -1
            if job_id:
                _PROGRESS[job_id] = {"phase": "downloading", "pct": 0}

            # chunk loop — logs every 5% and updates progress dict
            for chunk in downloader.chunks():
                tmp_csv.write(chunk)
                written += len(chunk)
                if total_bytes:
                    pct = min(int(written / total_bytes * 100), 99)
                    if job_id:
                        _PROGRESS[job_id]["pct"] = pct
                    if pct // 5 != last_pct // 5:
                        last_pct = pct
                        ingest_logger.info("parquet_service", step="download",
                                           status="progress",
                                           pct=pct,
                                           downloaded_mb=round(written / 1024 / 1024, 1),
                                           total_mb=round(total_bytes / 1024 / 1024, 1))

        csv_size_bytes = os.path.getsize(tmp_csv_path)
        if job_id:
            _PROGRESS[job_id] = {"phase": "converting", "pct": 0}
        ingest_logger.info("parquet_service", step="download", status="done",
                           size_mb=round(csv_size_bytes / 1024 / 1024, 1),
                           duration_ms=_ms(t))
        track_azure_blob("download", blob_path, csv_size_bytes, _ms(t))

        # ── Step 2: PyArrow CSV → Parquet (256MB blocks, ~1GB peak RAM) ───────
        # open_csv() reads block_size bytes at a time.
        # ParquetWriter writes each block as a row group with min/max statistics.
        # Those statistics are what lets DuckDB skip row groups during queries later.
        t = time.perf_counter()
        ingest_logger.info("parquet_service", step="convert", status="started",
                           csv_size_mb=round(csv_size_bytes / 1024 / 1024, 1))

        read_opts = pa_csv.ReadOptions(block_size=256 * 1024 * 1024)  # 256MB per block

        reader = pa_csv.open_csv(tmp_csv_path, read_options=read_opts)

        # .schema works on all PyArrow versions (schema_arrow was removed)
        schema = reader.schema

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet:
            tmp_parquet_path = tmp_parquet.name

        row_groups = 0
        total_rows = 0
        with pq.ParquetWriter(tmp_parquet_path, schema=schema, compression="zstd") as writer:
            for batch in reader:
                writer.write_batch(batch)
                row_groups += 1
                total_rows += len(batch)

        parquet_size_bytes = os.path.getsize(tmp_parquet_path)
        ingest_logger.info("parquet_service", step="convert", status="done",
                           row_groups=row_groups,
                           total_rows=total_rows,
                           parquet_size_mb=round(parquet_size_bytes / 1024 / 1024, 1),
                           compression_ratio=round(csv_size_bytes / parquet_size_bytes, 2),
                           duration_ms=_ms(t))

        # ── Step 3: upload Parquet back to Azure ──────────────────────────────
        t = time.perf_counter()
        ingest_logger.info("parquet_service", step="upload", status="started",
                           parquet_blob_path=parquet_blob_path)
        if job_id:
            _PROGRESS[job_id] = {"phase": "uploading", "pct": 0}

        parquet_blob_client = client.get_blob_client(container=container_name, blob=parquet_blob_path)
        with open(tmp_parquet_path, "rb") as f:
            parquet_blob_client.upload_blob(f, overwrite=True)

        ingest_logger.info("parquet_service", step="upload", status="done",
                           size_mb=round(parquet_size_bytes / 1024 / 1024, 1),
                           duration_ms=_ms(t))
        track_azure_blob("upload", parquet_blob_path, parquet_size_bytes, _ms(t))

        if job_id:
            _PROGRESS.pop(job_id, None)  # clean up — job is done

        return {"parquet_blob_path": parquet_blob_path, "size_bytes": parquet_size_bytes, "total_rows": total_rows}

    finally:
        # Always clean up temp files — even on failure
        if tmp_csv_path and os.path.exists(tmp_csv_path):
            try:
                os.unlink(tmp_csv_path)
            except OSError:
                pass
        if tmp_parquet_path and os.path.exists(tmp_parquet_path):
            try:
                os.unlink(tmp_parquet_path)
            except OSError:
                pass


async def convert_csv_to_parquet(
    blob_path: str,
    connection_string: str,
    container_name: str,
    job_id: str | None = None,
) -> dict:
    """
    Convert a CSV blob to Parquet using Azure SDK + PyArrow streaming.
    Runs the synchronous conversion in a thread so the FastAPI event loop is not blocked.

    Returns {"parquet_blob_path": str, "size_bytes": int}
    Raises on any failure — caller is responsible for catching and recording the error.
    """
    # Handle .csv.gz → .parquet (strip .gz then .csv/.tsv/.txt)
    base = blob_path
    if base.endswith(".gz"):
        base = base[:-3]
    parquet_blob_path = base.rsplit(".", 1)[0] + ".parquet"

    start = time.perf_counter()
    ingest_logger.info("parquet_service", operation="convert_csv_to_parquet",
                       status="started", blob_path=blob_path,
                       target_path=parquet_blob_path)

    result = await asyncio.to_thread(
        _run_conversion,  # type: ignore[arg-type]
        blob_path,
        connection_string,
        container_name,
        parquet_blob_path,
        job_id,
    )

    ingest_logger.info("parquet_service", operation="convert_csv_to_parquet",
                       status="done", total_duration_ms=_ms(start),
                       size_bytes=result.get("size_bytes"))

    return result
