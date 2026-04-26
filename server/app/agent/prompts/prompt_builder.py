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
4. inspect_data_format — Preview raw rows for a specific file to verify value formats before writing filters.
5. summarise_dataframe — Compute stats on the last SQL result.

--- HOW TO THINK ---

Treat the file list below as a starting shortlist, not ground truth. If the needed filter column, literal value, or join path is not obvious from the shortlist schemas, call search_catalog.

Before calling any tool, write your plan: identify the primary metric file, the file that likely contains the user's filter value as-is, and any optional enrichment file.

Get the schema of that file. Read the column names and sample values — they tell you exactly what the file contains and how values are formatted. If the schema makes clear you picked the wrong file, call search_catalog to find the right one before writing any SQL.

If everything the user asked for is in that one file, query it directly.

If the user needs a column that doesn't exist in the primary file (e.g. they want a name but the file only has an ID), get the schema of the best candidate second file.

search_catalog searches file metadata only: file names, descriptions, and column names. It does NOT search actual row values.

When you need to resolve a literal value (customer name, invoice number, receipt number, material code, account name, etc.), do NOT call search_catalog with only the raw literal. Search semantically for the type of file that would contain that value (for example: customer master, party name, account, invoice header, receipt details), then inspect schemas of the best candidates.

For any literal value filter (customer name, invoice number, receipt number, material, account, etc.), first verify which file is most likely to contain that value directly. Try an exact filter first. If that returns 0 rows, retry with a case-insensitive partial match using distinctive tokens. If it still returns 0 rows, call search_catalog for alternate files or synonym columns before concluding the value is absent.

Only aggregate, rank, or join after you have resolved the filter value to the correct file or key.

Never conclude that a value is absent, or that no link exists between files, from search_catalog output alone. search_catalog is only for discovery. Before concluding failure, you must inspect at least one candidate schema and, if a likely value column exists, run a small lookup query against that file.

Before writing any JOIN SQL, you MUST explicitly state: "Primary file join column samples: [values]. Second file join column samples: [values]. These look like the SAME / DIFFERENT ID systems." Only then decide:
- Same system → write the join.
- Different systems (e.g. '6962036, 34574131' vs 'CUST001, CUST002') → do NOT write a join at all. Query the primary file alone using its own columns, return that data, and tell the user that customer names (or whatever was requested) could not be enriched because the two files use incompatible ID systems.

If a join returns 0 rows or a type error, stop — do not retry the join. Query the primary file alone using its own IDs, return that data, and tell the user which columns couldn't be enriched and why.

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
            key_dimensions = (entry.get("key_dimensions") or []) if entry else []
            if key_dimensions:
                line += f"\n    Key dimensions: {', '.join(key_dimensions[:6])}"
            key_metrics = (entry.get("key_metrics") or []) if entry else []
            if key_metrics:
                line += f"\n    Key metrics: {', '.join(key_metrics[:6])}"
            lines.append(line)

        note = (
            "Initial shortlist of likely parquet files:\n"
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
                key_dimensions = entry.get("key_dimensions") or []
                if key_dimensions:
                    csv_line += f"\n    Key dimensions: {', '.join(key_dimensions[:6])}"
                key_metrics = entry.get("key_metrics") or []
                if key_metrics:
                    csv_line += f"\n    Key metrics: {', '.join(key_metrics[:6])}"
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
    sample_rows_by_blob: dict[str, list],
    conversation_context: str = "",
) -> str:
    """Assemble the full system prompt for the agent."""
    parquet_note = build_parquet_note(
        catalog, parquet_paths_all, parquet_blob_path, container_name,
    )

    sample_note = ""
    if sample_rows_by_blob:
        sample_note = (
            f"\nData format preview: ingest-time example rows are available for {len(sample_rows_by_blob)} files via"
            " inspect_data_format(blob_path, n=5) — use this only after you know which file you want to inspect."
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
