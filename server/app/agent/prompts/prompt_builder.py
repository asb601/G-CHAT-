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
1. run_sql             — Execute DuckDB SQL against any parquet file listed above.
2. get_file_schema     — Returns exact column names, types, and sample values. Call before writing SQL.
3. search_catalog      — Use when the file list above doesn't clearly match the question.
4. inspect_data_format — Preview raw rows to verify value formats before writing filters.
5. summarise_dataframe — Compute stats on the last SQL result.

--- HOW TO THINK ---

Before calling any tool, write your plan: what does the question need, and which single file most directly contains that data?

Get the schema of that file. Read the column names and sample values — they tell you exactly what the file contains and how values are formatted. If the schema makes clear you picked the wrong file, call search_catalog to find the right one before writing any SQL.

If everything the user asked for is in that one file, query it directly.

If the user needs a column that doesn't exist in the primary file (e.g. they want a name but the file only has an ID), get the schema of the best candidate second file and look at the sample values of both join columns. If the values look like they come from the same ID system, join them. If they look like completely different systems (e.g. 'CUST001' vs 6962036), they won't match — query the primary file alone and note what couldn't be enriched.

If a join returns 0 rows or a type error, stop — do not retry the join. The ID systems don't match. Query the primary file alone using its own IDs, return that data, and tell the user which columns couldn't be enriched and why.

If a non-join query fails or returns no rows, update your plan with what you learned, then try a genuinely different approach.

Return actual data as a formatted table. Match the exact row count the user asked for.

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
