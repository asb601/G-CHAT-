"""
Benchmark: CSV (read_csv_auto) vs Parquet (read_parquet) vs Excel (.xlsx via pandas)

Tests three extraction paths for the SAME data and reports read speed, query speed,
and a plain-English verdict on whether Parquet conversion is worthwhile.

Usage (against a real Azure Blob file):
  python bench_csv_vs_parquet.py \
    --connection-string "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;" \
    --container "my-container" \
    --csv-blob "uploads/data.csv" \
    --parquet-blob "parquet/data.parquet"

  For Excel comparison, also pass:
    --excel-path "/local/path/to/data.xlsx"

Output shows per-operation timings and an overall verdict.
"""
from __future__ import annotations

import argparse
import os
import statistics
import time
from typing import Optional

import duckdb


# ── DuckDB connection ─────────────────────────────────────────────────────────

_CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt"
if os.path.exists(_CA_BUNDLE):
    os.environ["CURL_CA_BUNDLE"] = _CA_BUNDLE
    os.environ["SSL_CERT_FILE"] = _CA_BUNDLE


def _make_conn(connection_string: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("SET azure_transport_option_type = 'curl';")
    safe = connection_string.replace("'", "''")
    conn.execute(f"SET azure_storage_connection_string='{safe}';")
    return conn


# ── Timing helper ─────────────────────────────────────────────────────────────

def _timed(conn: duckdb.DuckDBPyConnection, sql: str, runs: int = 3) -> dict:
    """Run SQL `runs` times. Returns median, min, max in milliseconds."""
    times_ms: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        conn.execute(sql).fetchall()
        times_ms.append((time.perf_counter() - t0) * 1000.0)
    times_ms.sort()
    return {
        "median_ms": round(times_ms[len(times_ms) // 2], 1),
        "min_ms":    round(times_ms[0], 1),
        "max_ms":    round(times_ms[-1], 1),
    }


def _print_row(label: str, result: dict):
    print(
        f"  {label:<44}  "
        f"median={result['median_ms']:8.1f} ms  "
        f"(min={result['min_ms']:.1f}  max={result['max_ms']:.1f})"
    )


# ── Detect first two columns for generic GROUP BY ────────────────────────────

def _first_two_cols(conn: duckdb.DuckDBPyConnection, source_sql: str) -> tuple[str, str]:
    """Return the names of the first and second columns of a query."""
    schema = conn.execute(f"DESCRIBE ({source_sql} LIMIT 0)").fetchall()
    col1 = schema[0][0]
    col2 = schema[1][0] if len(schema) > 1 else schema[0][0]
    return col1, col2


# ── Main benchmark ────────────────────────────────────────────────────────────

def run_benchmark(
    connection_string: str,
    container: str,
    csv_blob: str,
    parquet_blob: str,
    excel_path: Optional[str] = None,
    runs: int = 3,
) -> None:
    print("\n" + "=" * 70)
    print("  G-CHAT  |  CSV vs Parquet (vs Excel) extraction benchmark")
    print("=" * 70)
    print(f"  CSV     : az://{container}/{csv_blob}")
    print(f"  Parquet : az://{container}/{parquet_blob}")
    if excel_path:
        print(f"  Excel   : {excel_path}")
    print(f"  Runs    : {runs} per operation (median reported)")

    conn = _make_conn(connection_string)
    csv_path     = f"az://{container}/{csv_blob}"
    parquet_path = f"az://{container}/{parquet_blob}"
    csv_src    = f"read_csv_auto('{csv_path}', ignore_errors=true, null_padding=true)"
    parquet_src = f"read_parquet('{parquet_path}')"

    # ── discover column names once (avoid repeating DESCRIBE on every run) ───
    col1, col2 = _first_two_cols(conn, f"SELECT * FROM {parquet_src}")
    print(f"  Columns tested: GROUP BY '{col1}', aggregate count('{col2}')\n")

    results: dict[str, dict[str, dict]] = {}

    # ── TEST 1: Full table scan ───────────────────────────────────────────────
    print("── TEST 1  Full table scan (SELECT *) ──────────────────────────")
    r_csv = _timed(conn, f"SELECT * FROM {csv_src}", runs)
    _print_row("CSV   read_csv_auto SELECT *", r_csv)
    r_pq  = _timed(conn, f"SELECT * FROM {parquet_src}", runs)
    _print_row("Parquet read_parquet SELECT *", r_pq)
    results["full_scan"] = {"csv": r_csv, "parquet": r_pq}

    # ── TEST 2: Row count ────────────────────────────────────────────────────
    print("\n── TEST 2  Row count (SELECT count(*)) ─────────────────────────")
    r_csv = _timed(conn, f"SELECT count(*) FROM {csv_src}", runs)
    _print_row("CSV   count(*)", r_csv)
    r_pq  = _timed(conn, f"SELECT count(*) FROM {parquet_src}", runs)
    _print_row("Parquet count(*)", r_pq)
    results["count"] = {"csv": r_csv, "parquet": r_pq}

    # ── TEST 3: GROUP BY + aggregate ─────────────────────────────────────────
    print(f"\n── TEST 3  GROUP BY {col1!r} + count ───────────────────────────")
    agg_sql_csv = f"SELECT {col1}, count(*) FROM {csv_src} GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
    agg_sql_pq  = f"SELECT {col1}, count(*) FROM {parquet_src} GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
    r_csv = _timed(conn, agg_sql_csv, runs)
    _print_row("CSV   GROUP BY + count", r_csv)
    r_pq  = _timed(conn, agg_sql_pq, runs)
    _print_row("Parquet GROUP BY + count", r_pq)
    results["group_by"] = {"csv": r_csv, "parquet": r_pq}

    # ── TEST 4: WHERE filter ─────────────────────────────────────────────────
    print(f"\n── TEST 4  Filtered scan (WHERE {col1} IS NOT NULL) ───────────")
    r_csv = _timed(conn, f"SELECT * FROM {csv_src} WHERE {col1} IS NOT NULL LIMIT 1000", runs)
    _print_row("CSV   WHERE IS NOT NULL", r_csv)
    r_pq  = _timed(conn, f"SELECT * FROM {parquet_src} WHERE {col1} IS NOT NULL LIMIT 1000", runs)
    _print_row("Parquet WHERE IS NOT NULL", r_pq)
    results["filter"] = {"csv": r_csv, "parquet": r_pq}

    # ── TEST 5: Excel (optional) ──────────────────────────────────────────────
    if excel_path:
        print("\n── TEST 5  Excel (.xlsx) via pandas → DuckDB ───────────────────")
        try:
            import pandas as pd
            t0 = time.perf_counter()
            df = pd.read_excel(excel_path)
            conn.register("_excel_df", df)
            load_ms = round((time.perf_counter() - t0) * 1000, 1)
            print(f"  {'Excel  initial load (pandas.read_excel)':<44}  {load_ms:8.1f} ms  (one-time)")
            r_xl = _timed(conn, "SELECT * FROM _excel_df", runs)
            _print_row("Excel  SELECT * (from memory)", r_xl)
            r_xl_cnt = _timed(conn, "SELECT count(*) FROM _excel_df", runs)
            _print_row("Excel  count(*) (from memory)", r_xl_cnt)
            results["excel_scan"]  = {"excel": r_xl}
            results["excel_count"] = {"excel": r_xl_cnt}
            results["excel_load_ms"] = load_ms
        except ImportError:
            print("  [SKIP] pandas not installed. Run: pip install pandas openpyxl")
        except Exception as e:
            print(f"  [ERROR] Excel test failed: {e}")

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)

    speedups: list[float] = []
    test_names = [("full_scan", "Full scan"), ("count", "Count"), ("group_by", "GROUP BY"), ("filter", "WHERE filter")]
    for key, label in test_names:
        if key in results:
            csv_ms = results[key]["csv"]["median_ms"]
            pq_ms  = results[key]["parquet"]["median_ms"]
            sp = csv_ms / pq_ms if pq_ms > 0 else 1.0
            speedups.append(sp)
            faster = "Parquet faster" if pq_ms < csv_ms else "CSV faster"
            print(f"  {label:<20}  CSV={csv_ms:8.1f}ms  Parquet={pq_ms:8.1f}ms  speedup={sp:.2f}x  ({faster})")

    if "excel_scan" in results:
        xl_ms = results["excel_scan"]["excel"]["median_ms"]
        pq_ms = results["full_scan"]["parquet"]["median_ms"]
        sp = xl_ms / pq_ms if pq_ms > 0 else 1.0
        print(f"  {'Excel vs Parquet':<20}  Excel={xl_ms:8.1f}ms  Parquet={pq_ms:8.1f}ms  speedup={sp:.2f}x  (Parquet faster)" if pq_ms < xl_ms else f"  {'Excel vs Parquet':<20}  Excel={xl_ms:8.1f}ms  Parquet={pq_ms:8.1f}ms  speedup={sp:.2f}x  (Excel in-memory faster)")
        if "excel_load_ms" in results:
            print(f"  NOTE: Excel requires {results['excel_load_ms']}ms one-time python load (not shown above)")

    if speedups:
        avg_sp = statistics.mean(speedups)
        print()
        if avg_sp >= 2.0:
            verdict = (
                f"Parquet is ~{avg_sp:.1f}x faster on average.\n"
                "  VERDICT: Parquet conversion is HIGHLY WORTHWHILE. Keep it."
            )
        elif avg_sp >= 1.3:
            verdict = (
                f"Parquet is ~{avg_sp:.1f}x faster on average.\n"
                "  VERDICT: Parquet helps for large files (>100k rows). Marginal for smaller files.\n"
                "  RECOMMENDATION: Keep Parquet for large files; consider skipping for tiny CSVs."
            )
        else:
            verdict = (
                f"Parquet shows only {avg_sp:.1f}x speedup on average.\n"
                "  VERDICT: For this file size, Parquet conversion overhead may not be worth it.\n"
                "  RECOMMENDATION: Consider skipping Parquet conversion for files under ~50k rows."
            )
        for line in verdict.splitlines():
            print(f"  {line}")

    print()


# ── Excel-only note (no Azure credentials needed) ────────────────────────────

def print_excel_note():
    print("""
EXCEL SUPPORT NOTE
──────────────────
DuckDB cannot read .xlsx files natively from Azure Blob.
To support Excel files, the pipeline would need to:
  1. Download the .xlsx blob to a temp file
  2. pandas.read_excel(path)  →  returns a DataFrame
  3. duckdb_conn.register("tbl", df)  →  query via DuckDB in memory

Performance implications:
  - Initial load adds 100–3000 ms depending on file size (pandas decode)
  - After load, in-memory DuckDB queries are FASTER than Azure CSV reads
  - No need to convert Excel to Parquet — just keep in-memory per request
  - For very large Excel files (>500k rows), Parquet is still faster

Supported Excel format: .xlsx  (openpyxl required: pip install openpyxl)
Not supported:          .xls   (legacy format — recommend asking users to re-save as .xlsx)
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark CSV vs Parquet (vs Excel) extraction via DuckDB"
    )
    parser.add_argument("--connection-string", required=True,
                        help="Azure Blob Storage connection string")
    parser.add_argument("--container", required=True,
                        help="Azure container name")
    parser.add_argument("--csv-blob", required=True,
                        help="Blob path to the CSV file (e.g. uploads/mydata.csv)")
    parser.add_argument("--parquet-blob", required=True,
                        help="Blob path to the Parquet file (e.g. parquet/mydata.parquet)")
    parser.add_argument("--excel-path", default=None,
                        help="(Optional) Local path to an .xlsx file with same data")
    parser.add_argument("--runs", type=int, default=3,
                        help="Number of timed runs per test (default: 3)")
    parser.add_argument("--excel-note", action="store_true",
                        help="Print Excel support notes and exit")
    args = parser.parse_args()

    if args.excel_note:
        print_excel_note()
    else:
        run_benchmark(
            connection_string=args.connection_string,
            container=args.container,
            csv_blob=args.csv_blob,
            parquet_blob=args.parquet_blob,
            excel_path=args.excel_path,
            runs=args.runs,
        )
