"""
System prompt builder — assembles the prompt from catalog data,
parquet paths, relationships, and conversation context.
"""
from __future__ import annotations

from app.agent.state import MAX_TOOL_CALLS
from app.core.logger import chat_logger


SYSTEM_PROMPT_TEMPLATE = """You are a sharp, data-driven analyst with direct SQL access to structured data files stored in Azure Blob Storage.

Container: {container_name}
{parquet_note}
{sample_note}

--- TOOLS ---
1. run_sql: Execute any DuckDB SQL. File paths and column names are listed above.
2. search_catalog: Find which file(s) to query when the paths above don't cover the question.
3. get_file_schema: Get full column names, types, and sample values for a specific file.
4. inspect_data_format: Preview a few rows to check value formats (e.g. date format, casing) before writing SQL. Not for answering — use run_sql.
5. summarise_dataframe: Compute stats on the last run_sql result in memory.

--- RULES ---
- If file paths and columns are listed above, use them directly in run_sql. No need for search_catalog or get_file_schema.
- BEFORE writing SQL, identify which file best matches the question. Match on file name AND description — e.g. "receipts" → a file with "RECEIPT" or "RECEIVABLE" in the name, "invoices" → "INVOICE" or "TRX", etc.
- Write complete SQL with proper column names from above. Do not guess column names.
- ALWAYS honour the exact count the user asks for. "top 20" means LIMIT 20, "top 50" means LIMIT 50. Default LIMIT 100 if no count specified. NEVER return fewer rows than requested unless the data genuinely has fewer.
- For multi-file questions, prefer JOINs.
- Always check the JOIN RELATIONSHIPS section before writing any JOIN. Use the exact column name listed. Never guess JOIN columns.
- If two files need to be JOINed but NO relationship is listed for them, call get_file_schema on BOTH files first.
- If a JOIN returns 0 rows — stop immediately. Call get_file_schema on both files to verify the exact column names and types, then rewrite the JOIN once with the correct columns.
- Give a direct answer with actual data. Bold the key numbers. Show ALL rows returned by the query, not a subset.
- Max {max_calls} tool calls.
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

            if cols_info:
                col_names = [c["name"] for c in cols_info]
                line += f"\n    Columns: {', '.join(col_names)}"

                identifiers = []
                enums = []
                for c in cols_info:
                    uv = c.get("unique_values") or c.get("sample_values") or []
                    name_lower = c["name"].lower()
                    col_type = c.get("type", "")
                    n_unique = len(uv)

                    is_id_like = any(
                        name_lower.endswith(s)
                        for s in ("_id", "_key", "_number", "_code")
                    )
                    if is_id_like and n_unique > 5:
                        sample_str = ", ".join(str(v) for v in uv[:5])
                        identifiers.append(f"{c['name']} ({col_type}, e.g. {sample_str})")
                    elif 1 <= n_unique <= 10 and "datetime" not in col_type.lower():
                        enums.append(f"{c['name']} [{', '.join(str(v) for v in uv)}]")

                if identifiers:
                    line += f"\n    Identifiers: {'; '.join(identifiers)}"
                if enums:
                    line += f"\n    Enums: {'; '.join(enums[:8])}"

            desc = entry.get("ai_description") if entry else None
            if desc:
                line += f"\n    Description: {desc}"
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
                cols_info = entry.get("columns_info") or []
                if cols_info:
                    col_names = [c["name"] for c in cols_info]
                    csv_line += f"\n    Columns: {', '.join(col_names)}"
                desc = entry.get("ai_description")
                if desc:
                    csv_line += f"\n    Description: {desc}"
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


def build_join_note(
    relationships: list[dict],
    parquet_paths_all: dict[str, str],
    container_name: str,
) -> str:
    """Build the JOIN relationships section of the system prompt."""
    usable_rels = [r for r in relationships if r.get("confidence_score", 0) >= 0.7]
    if not usable_rels or not parquet_paths_all:
        return ""

    seen_pairs: set[tuple[str, str, str]] = set()
    pair_counts: dict[tuple[str, str], int] = {}
    join_lines = []

    for rel in sorted(usable_rels, key=lambda r: r["confidence_score"], reverse=True):
        a_path, b_path = rel["file_a_path"], rel["file_b_path"]
        pair_key = (min(a_path, b_path), max(a_path, b_path))
        col_key = (*pair_key, rel["shared_column"])
        if col_key in seen_pairs:
            continue
        seen_pairs.add(col_key)

        if pair_counts.get(pair_key, 0) >= 3:
            continue
        pair_counts[pair_key] = pair_counts.get(pair_key, 0) + 1

        pq_a = parquet_paths_all.get(a_path)
        pq_b = parquet_paths_all.get(b_path)
        if pq_a and pq_b:
            shared = rel["shared_column"]
            if "=" in shared:
                col_a, col_b = shared.split("=", 1)
                join_on = f"a.{col_a} = b.{col_b}"
            else:
                join_on = shared
            join_lines.append(
                f"  az://{container_name}/{pq_a}  ←→  az://{container_name}/{pq_b}\n"
                f"  JOIN ON: {join_on}\n"
                f"  Type: {rel.get('join_type', 'LEFT JOIN')}  "
                f"Confidence: {rel['confidence_score']}"
            )
        if len(join_lines) >= 30:
            break

    if not join_lines:
        return ""

    return (
        "\n--- JOIN RELATIONSHIPS (use these exact column names) ---\n"
        "These files share columns and can be JOINed directly:\n"
        + "\n".join(join_lines)
        + "\nAlways use these exact column names when writing JOIN conditions.\n"
    )


def build_system_prompt(
    catalog: list[dict],
    relationships: list[dict],
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

    join_note = build_join_note(relationships, parquet_paths_all, container_name)

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        container_name=container_name,
        max_calls=MAX_TOOL_CALLS,
        parquet_note=parquet_note,
        sample_note=sample_note,
    )

    if join_note:
        system_prompt += join_note

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
