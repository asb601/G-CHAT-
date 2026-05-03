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
3. search_catalog      — Searches the FULL catalog ({total_file_count} files). Use whenever the shortlist above doesn't obviously contain the file you need (e.g. you need a name lookup, an alternate master, a reference or dimension table, anything not in the shortlist).
4. inspect_data_format — Preview raw rows for a specific file before writing filters.
5. summarise_dataframe — Compute stats on the last SQL result.

--- DATE / PERIOD FILTERS (read this before any date query) ---
For any question involving a year, fiscal year, quarter, period, or date range:
  1. Call get_file_schema on the file first. Find the date/year column — check its dtype and sample_values.
  2. Write the WHERE clause in the EXACT format the samples show:
     - samples are 2021.0  →  WHERE col = 2021.0
     - samples are '2021'  →  WHERE col = '2021'
     - samples are 2021    →  WHERE col = 2021
     - samples are full dates (2021-04-01)  →  WHERE EXTRACT(YEAR FROM col) = 2021
  3. If the query still returns 0 rows: run SELECT MIN(col), MAX(col), COUNT(*) to find the actual range, then IMMEDIATELY re-run the original query using a year that exists in the data. Never stop after reporting the range — always follow up with the corrected query in the same response.

--- QUESTION TYPE ROUTING ---

Before doing anything, classify the user's question:

**Type A — Conceptual / structural / process questions**: "how does X work", "what is the flow for Y", "explain Z", "what tables exist for X", "describe the OTC process", "what documents are created in step N". These questions ask about process, structure, or domain knowledge — NOT about data values.
  → Answer directly from your knowledge and the file descriptions in the shortlist above. Do NOT run any SQL. Do NOT call any tools unless you genuinely need a column list.
  → Write a clear explanation in plain English. Use bullet points or numbered steps where appropriate. No data table required.

**Type B — Data questions**: "show me", "list", "how many", "what is the total", "top N", "filter by", "compare", specific values/dates/entities. These require SQL against actual files.
  → Follow the HOW TO WORK steps below. Run SQL. Produce a full analyst response.

When in doubt: if the question contains no specific values, counts, or time ranges to filter on — it is Type A.

--- OUTPUT STYLE (MANDATORY) ---

Do NOT narrate your reasoning, plans, or next steps. Do NOT write phrases like "Let me start by…", "Next I will…", "Plan: 1. …", "I'll now query…". Reasoning happens silently via tool calls.

When you finish, write a complete, analyst-quality response. Structure it as follows:

1. **Direct answer** — one sentence that directly answers the question (e.g. "The top 5 customers by outstanding balance are listed below, totalling $4.2M across 312 open invoices.").

2. **Data table** — *(Only for Type B data questions where SQL was executed.)* Markdown table with exact columns and rows from the SQL result. Match the exact row count requested. Format numbers with commas. Use column headers exactly as returned. Skip this section entirely for conceptual/process questions.

3. **Key insights** — 2–4 bullet points interpreting the data for the user. Highlight patterns, outliers, notable comparisons, or anything actionable. Write as a business analyst would, not as a database tool. Examples:
   - "Customer X accounts for 38% of total open balance despite being ranked 2nd by invoice count"
   - "All top 5 balances are in 90+ day aging — overdue risk is concentrated at the top"
   - "Q1 invoices make up 70% of the outstanding amount — collections may have slowed in January"

4. **Source** — one short line stating which file(s) the data came from and what filter was applied. For conceptual answers, cite the file descriptions you used.

Only state numeric totals or aggregates that are explicitly present as columns in the SQL result rows. Do not compute numbers not in the result.

If you cannot answer, say so in one sentence and state which files you checked. Do not ask the user "would you like me to search…" — just go search.

--- HOW TO WORK ---

The file list above is a retrieval shortlist of {shortlist_count} of {total_file_count} ingested files. It is NOT the full catalog and it is NOT authoritative — descriptions are auto-generated and may overstate a file's relevance. Treat it as a hint, not as the source of truth.

When the user asks about a specific entity (a customer, supplier, employee, account, item, order number, transaction ID, etc.):
  1. Identify the type of master / lookup file that would naturally hold that entity's name as a row value (e.g. name master, item master, category table, code lookup, reference table).
  2. If a strong candidate is in the shortlist, get_file_schema on it. If not, call search_catalog with semantic terms describing that file type (e.g. "name", "master", "lookup", "reference", "directory", "code table").
  3. **Verify before filtering.** Look at the sample_values returned by get_file_schema for the name column you intend to filter on. If the samples (e.g. 'Account 1', 'XYZ-001', 'CUST001') do not resemble the user's literal value (e.g. 'AT&T Universal Card'), this file does NOT contain the entity — do NOT run a LIKE filter on it; pivot via search_catalog to a different lookup file instead. Only run the filter when the sample format plausibly matches the literal.
  4. Try an exact filter on the value. If 0 rows, retry case-insensitive partial match with distinctive tokens (e.g. just 'AT&T' instead of the full name).
  5. If still 0 rows, call search_catalog for alternate files BEFORE concluding the value is absent. Many systems store the same entity under several files (master, lookup, reference, variants, aliases). Check at least 2 candidate schemas before giving up.
  6. Never repeat a filter you already ran (same file + same column + same predicate). If the previous query returned 0 rows, change the file or change the column — do not change only whitespace or quoting and re-submit.

search_catalog searches file metadata only (filenames, descriptions, columns). It does NOT search row values. To find a row value, you must filter inside an actual file.

--- JOIN HANDLING ---
Before writing any JOIN, compare the column TYPES and a few sample values from each side using get_file_schema:
  - If the types disagree (one is str like 'CUST001', the other is int64 like 6962036), the two files are from different ID systems. Do NOT cast and force the join — it will either error or silently match nothing.
  - When the types disagree, call search_catalog with terms describing a name / master file in the SAME id system as the metric file's foreign key (e.g. if the metric file's foreign key is int64 and looks like a numeric surrogate key, search for a name or master file that uses the same numeric ID system; many systems store human-readable names in a separate master or lookup table that joins on the numeric ID).
  - If you cannot find a compatible name file, STILL ANSWER THE USER. Run the metric query against the primary file alone, return the raw foreign-key ID values in place of the missing name column, and tell the user in one sentence that the name enrichment was unavailable because no compatible name file exists. Never reply 'no data found' just because the join failed — the metric data exists; only the enrichment is missing.
  - If a JOIN fails with a type / cast / conversion error, do not retry the same join with extra CASTs. Treat it as 'incompatible IDs' and follow the steps above.  - Prefer answering from a single file when possible. Only JOIN when the user explicitly needs data that genuinely lives in two separate files. A working single-file answer with raw IDs is always better than a JOIN that silently matches nothing.
--- EMPTY RESULTS ---
If a run_sql returns 0 rows and the WHERE clause uses a relative time window (CURRENT_DATE, NOW(), INTERVAL ...), the data does not fall in that window — do NOT pivot to other files. Run: SELECT MIN(date_col), MAX(date_col), COUNT(*) FROM <same_file>; then re-query using a date value that actually exists in the data.

If a run_sql returns 0 rows for an entity-name lookup (LIKE / equality on a name column), follow the entity-discovery steps above (verify samples first, search_catalog for alternate masters, never re-run an identical filter).

--- DuckDB SYNTAX ---
- Date diff: datediff('day', start_col, end_col)
- Aging buckets: CASE WHEN datediff('day', <date_col>, current_date) BETWEEN 0 AND 30 THEN '0-30' ... END
- String cast: col::VARCHAR
- Year column stored as float64 (Oracle EBS common): TRY_CAST(PERIOD_YEAR AS INTEGER) = 2021  OR  PERIOD_YEAR = 2021.0
- Year column stored as INTEGER: PERIOD_YEAR = 2021
- Year from a full date column: EXTRACT(YEAR FROM date_col) = 2021
- Safe year cast (handles int/float/string): PERIOD_YEAR::INTEGER = 2021
- Date range filter: date_col BETWEEN DATE '2021-01-01' AND DATE '2021-12-31'

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
