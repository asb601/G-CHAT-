"""
System prompt builder — assembles the prompt from catalog data,
parquet paths, and conversation context.
"""
from __future__ import annotations

import re

from app.agent.state import MAX_TOOL_CALLS
from app.core.logger import chat_logger


# Auto-generated descriptions often start with absolutist phrases like
# "This file is the PRIMARY source for..." or "Unlike similar files, this
# file...". Those phrases over-anchor the LLM and stop it from considering
# alternative files in the catalog. We strip them at render time so the
# stored description is unchanged but the prompt sees neutral text.
_ANCHOR_PATTERNS = [
    re.compile(r"\bThis file is the PRIMARY source\b", re.IGNORECASE),
    re.compile(r"\bThis file is THE PRIMARY source\b", re.IGNORECASE),
    re.compile(r"\bPRIMARY source\b"),
    re.compile(r"\bUnlike (?:other|similar) files,?\s*", re.IGNORECASE),
    re.compile(r"\bnot (?:typically )?found in other (?:similar )?files\b", re.IGNORECASE),
]


def _neutralize_description(desc: str) -> str:
    """Remove over-anchoring phrases from auto-generated descriptions."""
    if not desc:
        return ""
    out = desc
    for pat in _ANCHOR_PATTERNS:
        out = pat.sub("", out)
    # Collapse double spaces and stray leading punctuation introduced by removals
    out = re.sub(r"\s{2,}", " ", out).strip()
    out = re.sub(r"^[,;:.\s]+", "", out)
    return out


SYSTEM_PROMPT_TEMPLATE = """You are a data analyst with DuckDB SQL access to files in Azure Blob Storage.

Container: {container_name}
{shortlist_header}
{parquet_note}
{sample_note}

--- TOOLS ---
1. run_sql             — Execute DuckDB SQL.
2. get_file_schema     — Returns exact column names, types, sample values. Call before writing SQL.
3. search_catalog      — Searches the FULL catalog ({total_file_count} files). Use whenever the shortlist above doesn't obviously contain the file you need (e.g. you need a name lookup, an alternate master, a parties / accounts table, anything not in the shortlist).
4. inspect_data_format — Preview raw rows for a specific file before writing filters.
5. summarise_dataframe — Compute stats on the last SQL result.

--- OUTPUT STYLE (MANDATORY) ---

Do NOT narrate your reasoning, plans, or next steps. Do NOT write phrases like "Let me start by…", "Next I will…", "Plan: 1. …", "I'll now query…". Reasoning happens silently via tool calls.

When you finish, the user sees ONE message: the answer. That message must contain only:
  - The data, as a Markdown table when appropriate (match the exact row count requested), and
  - At most 1–2 short sentences of context (which file, which filter applied, totals).

If you cannot answer, say so in one sentence and state which files you checked. Do not ask the user "would you like me to search…" — just go search.

--- HOW TO WORK ---

The file list above is a retrieval shortlist of {shortlist_count} of {total_file_count} ingested files. It is NOT the full catalog and it is NOT authoritative — descriptions are auto-generated and may overstate a file's relevance. Treat it as a hint, not as the source of truth.

When the user asks about a specific entity (a customer, supplier, party, account, material, invoice number, receipt number, etc.):
  1. Identify the type of master / lookup file that would naturally hold that entity's name as a row value (customer master, party name, account master, item master, etc.).
  2. If a strong candidate is in the shortlist, get_file_schema on it. If not, call search_catalog with semantic terms describing that file type (e.g. "party name", "customer account master", "supplier master", "account lookup").
  3. **Verify before filtering.** Look at the sample_values returned by get_file_schema for the name column you intend to filter on. If the samples (e.g. 'Account 1', 'XYZ-001', 'CUST001') do not resemble the user's literal value (e.g. 'AT&T Universal Card'), this file does NOT contain the entity — do NOT run a LIKE filter on it; pivot via search_catalog to a different lookup file instead. Only run the filter when the sample format plausibly matches the literal.
  4. Try an exact filter on the value. If 0 rows, retry case-insensitive partial match with distinctive tokens (e.g. just 'AT&T' instead of the full name).
  5. If still 0 rows, call search_catalog for alternate files BEFORE concluding the value is absent. Many systems store the same entity under several files (master, parties, accounts, sites). Check at least 2 candidate schemas before giving up.
  6. Never repeat a filter you already ran (same file + same column + same predicate). If the previous query returned 0 rows, change the file or change the column — do not change only whitespace or quoting and re-submit.

search_catalog searches file metadata only (filenames, descriptions, columns). It does NOT search row values. To find a row value, you must filter inside an actual file.

Before writing JOIN SQL, compare the join-column sample values from each file. If one file uses '6962036, 34574131' and the other uses 'CUST001, CUST002', they are different ID systems — do NOT join. Return the primary file alone and tell the user the enrichment column was unavailable because the IDs are incompatible.

If a join returns 0 rows or a type error, do not retry the join. Query the primary file alone.

--- DuckDB SYNTAX ---
- Date diff: datediff('day', start_col, end_col)
- Aging buckets: CASE WHEN datediff('day', DUE_DATE, current_date) BETWEEN 0 AND 30 THEN '0-30' ... END
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

            desc = _neutralize_description(entry.get("ai_description") if entry else "")
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
                desc = _neutralize_description(entry.get("ai_description") or "")
                if desc:
                    csv_line += f"\n    Description: {desc}"
                # Note: leave key_dimensions / key_metrics intact below.
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
    total_file_count: int | None = None,
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

    shortlist_count = len(catalog)
    full_count = total_file_count if total_file_count is not None else shortlist_count
    if full_count > shortlist_count:
        shortlist_header = (
            f"Showing the top {shortlist_count} of {full_count} ingested files "
            f"(retrieval-ranked for this query). The other "
            f"{full_count - shortlist_count} files are NOT shown — call "
            f"search_catalog to reach them."
        )
    else:
        shortlist_header = f"All {full_count} ingested files are shown below."

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        container_name=container_name,
        max_calls=MAX_TOOL_CALLS,
        parquet_note=parquet_note,
        sample_note=sample_note,
        shortlist_header=shortlist_header,
        shortlist_count=shortlist_count,
        total_file_count=full_count,
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
