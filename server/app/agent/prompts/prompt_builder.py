"""
System prompt builder — assembles the prompt from catalog data,
parquet paths, and conversation context.
"""
from __future__ import annotations

from app.agent.state import MAX_TOOL_CALLS
from app.core.logger import chat_logger


SYSTEM_PROMPT_TEMPLATE = """You are a sharp, data-driven analyst with direct SQL access to structured data files stored in Azure Blob Storage.

Container: {container_name}
{parquet_note}
{sample_note}

--- TOOLS ---
1. run_sql            — Execute DuckDB SQL against any parquet file listed above.
2. get_file_schema    — Returns exact column names, types, and sample values for a file. Call this before writing any SQL.
3. search_catalog     — Use when the file list above does not clearly match the question.
4. inspect_data_format — Preview raw rows to verify value formats (dates, status codes, ID formats). Use before writing filter conditions.
5. summarise_dataframe — Compute aggregated stats on the last SQL result.

--- WORKFLOW (execute these steps in order every time) ---

STEP 1 · PLAN
Write 3 lines before touching any tool:
  a) What metric/dimension/filter does the question need?
  b) Which file(s) from the list above best match by name and description?
  c) Will a JOIN be needed, and if so, which columns look like they could be the shared key?

STEP 2 · GET SCHEMAS
Call get_file_schema for every file identified in Step 1. Call them in parallel if more than one.
Read the column names and sample values carefully — every file is different. Use only what the tool returns.

STEP 3 · JOIN DECISION (skip if single-file query)
Place the sample values of the two candidate join columns side by side:
  → Same value space (e.g. both are large integers like 6962036 / 34574131, or both are 'CUST001' / 'CUST002'):
      The key likely aligns. Proceed to Step 4A.
  → Different value space (e.g. one side is 'CUST001' and the other is 6962036, or 'ACCT00001' vs 1/2/3):
      These are different identifier systems. No cast or transform will make them match.
      Skip Step 4A entirely and go straight to Step 4B.

STEP 4A · EXECUTE JOIN QUERY
Write and run the JOIN SQL using the validated columns. Then:
  → Query returns rows: go to Step 5.
  → Query returns a type error or 0 rows: update your plan with one sentence explaining what failed,
    then go to Step 4B. This is the only retry — write genuinely different SQL each time.

STEP 4B · SINGLE-TABLE FALLBACK
Query only the primary file — the one that holds the financial metric 
(e.g. AMOUNT_DUE_REMAINING in AR_PAYMENT_SCHEDULES_ALL, invoice totals in RA_CUSTOMER_TRX_ALL).
Return the results using whatever ID column is in that file (e.g. CUSTOMER_ID, CUSTOMER_TRX_ID).
Append to your answer: "Note: [dimension, e.g. customer name] could not be enriched — identifier formats differ across files."

STEP 5 · DELIVER THE ANSWER
Present results as a formatted table. Bold key numbers.
Honour the exact row count requested — "top 20" means LIMIT 20. Use LIMIT 100 when unspecified.

--- DuckDB SYNTAX ---
- Date diff:  datediff('day', start_col, end_col)   ← always 3-argument form
- Timestamps: datediff('day', ts_col::DATE, current_date)
- AR aging buckets: CASE WHEN datediff('day', DUE_DATE, current_date) BETWEEN 0 AND 30 THEN '0-30' ... END
- String cast: col::VARCHAR

Max {max_calls} tool calls total.
"""


def build_parquet_note(
    catalog: list[dict],
    parquet_paths_all: dict[str, str],
    parquet_blob_path: str | None,
    container_name: str,
) -> str:
    """Build the file-listing section of the system prompt."""
    catalog_by_blob: dict[str, dict] = {}
    for entry in catalog:
        bp = entry.get("blob_path")
        if bp:
            catalog_by_blob[bp] = entry

    if parquet_paths_all:
        lines = []
        for blob, pq in parquet_paths_all.items():
            line = f"  read_parquet('az://{container_name}/{pq}')"
            entry = catalog_by_blob.get(blob)
            cols_info = (entry.get("columns_info") or []) if entry else []

            desc = entry.get("ai_description") if entry else None
            if desc:
                line += f"\n    Description: {desc}"
            good_for = (entry.get("good_for") or []) if entry else []
            if good_for:
                line += f"\n    Good for: {', '.join(good_for[:5])}"
            lines.append(line)

        note = (
            "Available parquet files (use directly in run_sql — no search_catalog needed):\n"
            + "\n".join(lines)
            + "\nParquet covers the FULL dataset. Use it for any ordering, filtering, counting, or row retrieval."
        )

        # Also list CSV-only files (no parquet conversion)
        csv_only = [e for e in catalog if e.get("blob_path") and e["blob_path"] not in parquet_paths_all]
        if csv_only:
            csv_lines = []
            for entry in csv_only:
                bp = entry["blob_path"]
                csv_line = f"  read_csv_auto('az://{container_name}/{bp}', sample_size=500, null_padding=true, ignore_errors=true)"
                desc = entry.get("ai_description")
                if desc:
                    csv_line += f"\n    Description: {desc}"
                good_for = entry.get("good_for") or []
                if good_for:
                    csv_line += f"\n    Good for: {', '.join(good_for[:5])}"
                csv_lines.append(csv_line)
            note += (
                "\n\nCSV-only files (no parquet — may be slower for large files):\n"
                + "\n".join(csv_lines)
            )
        return note

    if parquet_blob_path:
        return (
            f"Parquet path (use directly in run_sql — no search_catalog needed):\n"
            f"  read_parquet('az://{container_name}/{parquet_blob_path}')"
            "\nParquet covers the FULL dataset. Use it for any ordering, filtering, counting, or row retrieval."
        )

    return ""


def build_system_prompt(
    catalog: list[dict],
    parquet_paths_all: dict[str, str],
    parquet_blob_path: str | None,
    container_name: str,
    sample_rows: list,
    conversation_context: str = "",
) -> str:
    """Assemble the full system prompt for the agent."""
    parquet_note = build_parquet_note(
        catalog, parquet_paths_all, parquet_blob_path, container_name,
    )

    sample_note = ""
    if sample_rows:
        sample_note = (
            f"\nData format preview: {len(sample_rows)} example rows from ingest available via"
            " inspect_data_format() — use to understand column formats before writing SQL."
        )

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        container_name=container_name,
        max_calls=MAX_TOOL_CALLS,
        parquet_note=parquet_note,
        sample_note=sample_note,
    )

    if conversation_context:
        system_prompt += (
            "\n\n--- CONVERSATION HISTORY ---\n"
            "The user is continuing a conversation. Use this context to understand "
            "follow-up questions, pronouns ('it', 'that', 'those'), and references "
            "to previous queries or results.\n\n"
            f"{conversation_context}\n"
            "---\n"
        )

    chat_logger.info("system_prompt_size",
                     chars=len(system_prompt),
                     words=len(system_prompt.split()),
                     parquet_file_count=len(parquet_paths_all),
                     has_conversation_context=bool(conversation_context))

    return system_prompt
