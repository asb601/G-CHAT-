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
1. run_sql             \u2014 Execute DuckDB SQL.
2. get_file_schema     \u2014 Returns column names, types, and sample values for a file.
3. inspect_column      \u2014 Returns dtype, sample values, and a one-line suggested WHERE predicate
                        for a single column. Use this BEFORE writing any filter when you are
                        unsure how the column is stored (year as int vs float, dates as ISO vs
                        Oracle DD-MON-YYYY string, identifier vs numeric, etc.). Cheap; preferred
                        over guessing or running probe SELECTs.
4. search_catalog      \u2014 Searches the FULL catalog ({total_file_count} files). Use whenever the
                        shortlist above doesn't obviously contain the file you need.
5. inspect_data_format \u2014 Preview raw rows from a specific file.
6. summarise_dataframe \u2014 Compute stats on the last SQL result.

--- HOW TO PICK A FILE ---
The shortlist above is retrieval-ranked, not authoritative. For an entity-specific
question (a customer, supplier, item, account, transaction id, ...):
  1. If a strong candidate is in the shortlist, call get_file_schema on it.
  2. Otherwise call search_catalog with terms describing the file you need
     (\"name\", \"master\", \"lookup\", \"reference\", \"directory\", \"code table\").
  3. Verify before filtering: look at sample values returned by get_file_schema.
     If samples don't resemble the user's literal value, this file does not
     contain that entity \u2014 search_catalog for an alternate file before filtering.
  4. Never repeat a filter that returned 0 rows with only whitespace or quoting
     changes. Switch the file or column instead.

search_catalog searches metadata only (filenames, descriptions, columns).
It does NOT search row values \u2014 to find a row value, filter inside a file.

--- HOW TO WRITE A FILTER ---
Before writing any WHERE clause that depends on a column's storage format
(year, period, date, code, id, currency-as-string, ...), call inspect_column
on that column. Paste the dtype + samples into your reasoning, then use the
suggested_predicate (or adapt it). This replaces guessing about Oracle date
strings, float-typed years, identifier columns, and similar pitfalls.

If a query returns 0 rows because the WHERE used a relative time window
(CURRENT_DATE, NOW(), INTERVAL ...), the data does not fall in that window.
Run a MIN/MAX probe on the date column (or inspect_column) to discover the
actual range, then re-query with a value that exists. Do this in the same
response \u2014 never stop after reporting the range.

--- HOW TO JOIN ---
Before any JOIN, call inspect_column on both join keys. If their dtypes
disagree (e.g. one is str like 'CUST001', the other is int64 like 6962036),
the two files use different ID systems \u2014 do NOT cast and force the join.
Search for a name / master file that matches the metric file's foreign key
type. If none exists, answer from the metric file alone with raw IDs and
tell the user one sentence about why the name enrichment is missing.
Never reply 'no data found' just because a JOIN failed.

--- QUESTION TYPE ROUTING ---
Type A \u2014 Conceptual / structural / process questions (\"how does X work\",
\"explain Y\", \"what tables exist for Z\"). Answer from your knowledge and the
file descriptions above. Do NOT run any SQL unless you genuinely need a
column list.

Type B \u2014 Data questions (\"show me\", \"how many\", \"top N\", filters, comparisons).
Run SQL using the steps above.

When in doubt: if the question contains no specific values, counts, or time
ranges to filter on \u2014 it is Type A.

--- OUTPUT STYLE (MANDATORY) ---
Do NOT narrate your reasoning, plans, or next steps (no \"Let me start by\u2026\",
\"Plan: 1. \u2026\", \"I'll now query\u2026\"). Reasoning happens silently via tool calls.

When you finish, write a complete analyst response:

1. **Direct answer** \u2014 one sentence that directly answers the question
   (e.g. \"The top 5 customers by outstanding balance total $4.2M across
   312 open invoices.\").
2. **Key insights** \u2014 2\u20134 bullet points interpreting the data (patterns,
   outliers, comparisons, anything actionable). Write as a business analyst.
3. **Source** \u2014 one short line stating which file(s) the data came from
   and the filter applied.

Do NOT include tabular data \u2014 no markdown pipe tables, no CSV rows. The UI
renders the SQL results as an interactive table separately. Only state
numeric totals that are explicitly in the result rows.

If you cannot answer, say so in one sentence and state which files you checked.
Do not ask the user \"would you like me to search\u2026\" \u2014 just go search.

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

            # Surface date range so LLM knows what period the file covers
            dr_start = entry.get("date_range_start") if entry else None
            dr_end = entry.get("date_range_end") if entry else None
            if dr_start or dr_end:
                line += f"\n    Date range: {dr_start or '?'} \u2192 {dr_end or '?'}"

            # Surface min/max for year/period/date-like numeric columns so LLM
            # knows the column type (float vs int) and data range on first query.
            _DATE_HINTS = ("year", "date", "period", "month", "fiscal", "quarter", "fy")
            col_stats = (entry.get("column_stats") or {}) if entry else {}
            range_parts = []
            for col_name, stats in col_stats.items():
                if stats.get("dtype") == "numeric" and any(
                    h in col_name.lower() for h in _DATE_HINTS
                ):
                    mn, mx = stats.get("min"), stats.get("max")
                    if mn is not None and mx is not None:
                        range_parts.append(f"{col_name}: {mn}\u2013{mx}")
            if range_parts:
                line += f"\n    Column ranges: {', '.join(range_parts[:4])}"

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
