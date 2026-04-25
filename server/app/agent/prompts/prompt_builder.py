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
1. run_sql: Execute any DuckDB SQL query.
2. get_file_schema: Get the EXACT column names, data types, and sample values for a file by blob_path. Call this before writing any SQL.
3. search_catalog: Discover which file(s) match the question when the list above is unclear.
4. inspect_data_format: Preview rows to check value formats (date format, STATUS casing, etc). Not for answering.
5. summarise_dataframe: Compute stats on the last run_sql result.

--- RULES ---
- PLAN FIRST: Before calling any tool, write a 2-3 line plan: (1) what the question needs, (2) which file(s) you'll use, (3) any join required. After each tool failure, update the plan with what failed and your new approach.
- STEP 1 — Pick file(s): From the file list above, identify which file(s) match the question by name and description.
- STEP 2 — Get schema: Call get_file_schema(blob_path) for EVERY file you plan to query. Use the EXACT column names it returns. NEVER guess or assume column names — they differ per file.
- STEP 3 — Write SQL: Use only the column names from get_file_schema. You can call get_file_schema for multiple files in parallel.
- JOIN VERIFY: Before writing any JOIN, compare the sample values of both join columns from the schemas you already have. If the values clearly don't match in format or value space (e.g. 'ACCT0000000001' vs 'CUST001', or int 6962036 vs int 1), they are NOT the same key — pick a different column or skip the join. Only join columns whose samples look like they could overlap.
- JOIN FALLBACK: If a JOIN query returns 0 rows or a type error, the foreign keys do not match across these files. Do NOT report "no data". Instead retry with a single-table query on the primary file and show the numeric ID column in place of the name.
- ALWAYS honour the exact count the user asks for. "top 20" means LIMIT 20. Default LIMIT 100 if unspecified.
- DuckDB date arithmetic: always use `datediff('day', start_date, end_date)` — NEVER `datediff(date1, date2)` (2-arg form does not exist). For timestamps: `datediff('day', col::DATE, current_date)`.
- Give a direct answer with actual data. Bold key numbers. Show ALL rows returned.
- Max {max_calls} tool calls total.
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
