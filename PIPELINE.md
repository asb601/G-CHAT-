# Query Pipeline — How It Actually Works

This documents the path a user query takes from the chat input to the final answer. Written against the actual code, not the ideal description.

---

## Overview

```
User query
    │
    ▼
1. Catalog load (DB / in-memory cache)
    │
    ▼
2. Retrieval — 3 DB searches + RRF fusion → shortlist of files
    │
    ▼
3. Shortlist adjustment — inject lookup/master files the retrieval missed
    │
    ▼
4. Build tools + system prompt
    │
    ▼
5. LangGraph agent loop (LLM ↔ tools, max 8 tool calls)
    │
    ├─ broaden_nudge (conditional, fires once only)
    │
    ▼
6. Extract final answer
```

---

## Stage 1 — Catalog Load

`agent/catalog_cache.py`

Loads all ingested file metadata from the database. Includes for each file: blob path, AI-generated description, column names + types + sample values, key metrics/dimensions, date ranges, and pre-signed Parquet paths.

The result is cached in memory per container. Subsequent requests within the same process reuse the cache and skip the DB round-trip. Cache is invalidated when a file is ingested or deleted.

---

## Stage 2 — Retrieval

`retrieval/orchestrator.py`

Runs three separate database searches against the file metadata table, then fuses their rank lists.

**Stage 2a — Temporal parsing** (`retrieval/temporal.py`)  
Pure regex, no DB call. Extracts date bounds from the query text if any are mentioned ("last quarter", "2024", etc.). These bounds are passed to the DB searches as an extra filter — only files whose stored date range overlaps are included. No date bounds in the query → filter skipped.

**Stage 2b — BM25 keyword search** (`retrieval/bm25.py`)  
PostgreSQL `tsvector` / `tsquery` with a GIN index. Tokenises the query and matches against the stored search text for each file (description + column names). Reasonable for exact-word matches. Fails silently when the user's vocabulary differs from how the file was described (e.g. user says "customer master", file metadata says "HZ_PARTIES").

**Stage 2c — Fuzzy search** (`retrieval/fuzzy.py`)  
PostgreSQL `pg_trgm` trigram similarity. Better than BM25 for partial names and typos, worse for entirely different vocabulary.

**Stage 2d — Vector search** (`retrieval/embeddings_search.py`)  
HNSW cosine similarity via pgvector. Each file has a stored embedding. Should handle vocabulary mismatch better than BM25 — in practice it depends on embedding quality and how the file was described at ingest time.

All three searches run sequentially on the same DB session (SQLAlchemy async does not support concurrent operations on one connection). Each returns up to 50 candidates.

**Stage 2e — RRF fusion** (`retrieval/rrf.py`)  
Reciprocal Rank Fusion across the three ranked lists:

$$score(d) = \sum_{r \in \text{rankers}} \frac{1}{k + rank_r(d)}$$

where $k = 60$ (standard). A file that appears in all three lists at middling rank outscores one that appears top in only one. The top `_SHORTLIST_TOP_K = 12` files by RRF score are kept.

If retrieval fails or no `user_id` is provided, the agent falls back to a simple in-memory keyword count against the full catalog.

---

## Stage 3 — Shortlist Adjustment

`agent/graph/graph.py`, around `_LOOKUP_RESERVED_SLOTS`

Retrieval scores files by relevance to the query tokens. A query like "show open invoices grouped by customer name" scores `AR_PAYMENT_SCHEDULES_ALL` highly (it has "invoice", "amount", "due_date") but scores `HZ_PARTIES` poorly (it has "party_name" — a dimension table with no metric tokens). The agent then tries to JOIN the two and discovers they use different ID systems, wastes iterations, and may give up.

The adjustment reserves up to 3 slots in the 12-file shortlist for files that `is_lookup_file()` identifies as master/dimension tables (heuristic: filename or description contains words like "party", "master", "lookup", "dim_", "account", "customer", etc.). Those slots are filled by the highest keyword-scoring lookup files not already in the shortlist.

This is a heuristic. It does not guarantee the right master file is included — it just biases the shortlist toward having at least a few name-lookup tables.

---

## Stage 4 — Tool and Prompt Construction

**Tools built per request:**

| Tool | What it actually does |
|---|---|
| `run_sql` | Runs a DuckDB query against Azure Blob Storage files. Prefers Parquet over CSV. Results capped at 1000 rows. Returns `row_count`, `columns`, first 5 preview rows. On exception, returns `{"error": "..."}` — no rows, just the error string. Detects null-columns after a JOIN and attaches a warning asking the model to stop and query the primary file alone. |
| `get_file_schema` | Returns column names, types, and sample values for a file. Reads from the cached catalog — does not actually query the file. |
| `search_catalog` | Keyword match against the full catalog (not just the shortlist). Pads results with up to 5 lookup files even if they had zero keyword match, to reduce vocabulary-mismatch failures. Does not do semantic search. |
| `inspect_data_format` | Returns raw sample rows from the catalog cache. |
| `summarise_dataframe` | Computes stats on whatever `run_sql` returned last. |

**System prompt construction** (`agent/prompts/prompt_builder.py`):

- Lists all shortlisted files with their Parquet paths, descriptions, key metrics, and key dimensions.
- Auto-generated descriptions are cleaned of "over-anchoring" phrases like "This file is the PRIMARY source" before being sent to the LLM, because those phrases caused the model to stop considering alternatives.
- Includes HOW TO WORK instructions (entity lookup rules, JOIN handling, empty result handling, DuckDB syntax hints).
- Hard-codes `MAX_TOOL_CALLS = 8` as the budget.

---

## Stage 5 — LangGraph Agent Loop

`agent/graph/graph_builder.py`, `agent/graph/graph.py`

The graph has three nodes: `agent_node → tools → broaden_nudge → END`.

```
START
  │
  ▼
agent_node  ──tool calls──▶  tools (ToolNode)
  │  ◀────────── result ──────────│
  │
  │ [no tool calls in response OR budget hit]
  ▼
route()
  ├─ _should_force_broaden? → broaden_nudge → agent_node
  └─ else → END
```

### agent_node

Calls the LLM with the full message history and all tools bound. On `RateLimitError`, retries up to 3 times with exponential backoff (5s → 10s → 20s). On exhaustion, returns a "high demand" message.

Each call to `agent_node` increments `tool_call_count` by 1 if the response included tool calls. At `tool_call_count >= 8` the node injects a fixed message "I've gathered enough data. Let me summarise." and routes to END — the LLM is not called again regardless of what it was doing.

### broaden_nudge

A structural safety valve, not an AI decision. After the LLM emits a final answer (an AIMessage with no tool calls), a Python function checks whether:

1. At least one `run_sql` returned 0 rows **or** returned an error payload
2. No `run_sql` returned > 0 rows
3. `search_catalog` was not called at any point this session
4. The zero-row SQLs were not exclusively relative-time-window queries (see below)
5. `broaden_nudges` counter is still 0
6. Tool budget has not been exhausted

If all hold, a `SystemMessage` is injected into the conversation: "You reported no results, but you have not called search_catalog yet. Call search_catalog now with the entity type you were looking for, then get_file_schema on any promising result, and retry the query before giving a final answer." The agent then gets one more pass.

The nudge fires at most once per request. If the second pass also fails, the agent gives its final answer.

**Relative-time suppression:** If the zero-row SQL contained `CURRENT_DATE`, `INTERVAL`, `NOW(`, etc., the nudge is suppressed. A query returning 0 rows for "last 7 days" against a dataset whose most recent record is from 2025 is a *correct empty result* — calling `search_catalog` there just wastes the remaining tool budget.

---

## Stage 6 — Response Extraction

`agent/response_helpers.py`

Walks the message history backwards to find the last non-empty AIMessage content. Chart type is inferred from the result shape (time series → line, categories → bar, single aggregate → none). Blob paths are extracted from whichever `run_sql` calls succeeded.

---

---

## Ingestion Pipeline — CSV/Excel to Parquet

This is a separate pipeline that runs when a file is uploaded, not at query time. The query pipeline above reads files that have already been through this.

### Entry point: `ingest_file(file_id, db)`

`services/ingestion_service.py`

---

### Step 1/5 — DuckDB sample

`core/duckdb_client.py` → `sample_file()`

There is no `pd.read_csv()` anywhere. The file never touches disk on the server. DuckDB reads the raw CSV directly from Azure Blob Storage via HTTP range requests using the `azure` extension:

```sql
SELECT * FROM read_csv_auto(
    'az://container/blob.csv',
    sample_size=500,
    null_padding=true,
    ignore_errors=true
) LIMIT 500
```

DuckDB returns a pandas DataFrame (`.df()`). From that the code extracts:
- Column names and dtypes
- Up to 3 sample values per column (stored as strings)
- Up to 20 unique values per column

These are stored in memory as `columns_info` (list of `{name, type, sample_values, unique_values}`). This is what `get_file_schema` returns to the LLM at query time — it is from the ingestion sample, not the live file.

**DuckDB connection is thread-local and reused.** `_get_connection()` checks `threading.local()` for an existing connection keyed by an MD5 hash of the connection string. If none exists, it connects, installs the `azure` extension, sets curl as the transport, and stores the session on the thread. Subsequent calls on the same thread reuse it. `asyncio.to_thread()` wraps the blocking call so the FastAPI event loop is not blocked.

---

### Step 2/5 — AI description

`core/ai_client.py` → `generate_file_description()`

Sends `columns_info` + the 500-row sample to the LLM (GPT-4o mini by default). The LLM returns a structured JSON:

```json
{
  "summary": "...",
  "good_for": ["..."],
  "key_metrics": ["AMOUNT_DUE_REMAINING", "..."],
  "key_dimensions": ["CUSTOMER_ID", "..."],
  "date_range_start": "2023-01-01",
  "date_range_end": "2025-04-16"
}
```

`date_range_start` / `date_range_end` are the LLM's best guess from the sample — they may be wrong if the 500 rows are not representative of the full file.

The summary is stored as-is in `FileMetadata.ai_description`. At query time, `_neutralize_description()` strips phrases like "This file is the PRIMARY source" before the text is sent to the agent's system prompt — the stored value is unchanged.

---

### Step 3/5 — Save metadata to Postgres

`FileMetadata` row written (or updated if re-ingesting):

| Column | Source |
|---|---|
| `columns_info` | DuckDB sample |
| `row_count` | DuckDB sample (500-row cap, so this is ≤ 500 until analytics runs) |
| `ai_description` | LLM summary |
| `good_for`, `key_metrics`, `key_dimensions` | LLM JSON |
| `sample_rows` | First 500 rows from DuckDB |
| `date_range_start`, `date_range_end` | Parsed from LLM output, `date.fromisoformat()` — silently skipped if the LLM hallucinated a non-ISO string |

`file.ingest_status` is set to `"ingested"` at the end of step 3 — the file is visible in the UI before analytics or Parquet conversion finish.

---

### Step 4/5 — Build search text + embed

`retrieval/embeddings.py` → `build_search_text()` + `embed_text()`

`build_search_text()` concatenates: filename, description, column names, key metrics, key dimensions into a single plain-text string. This string is:
1. Stored as `FileMetadata.search_text` (used for BM25 `tsvector` at query time).
2. Sent to the embedding model → stored as `FileMetadata.description_embedding` (1536-float vector for HNSW search at query time).

Embedding failure is non-fatal. The file falls back to BM25 + fuzzy search only.

---

### Step 5/5 — Analytics + Parquet conversion (background)

Two things happen concurrently after `ingest_status = "ingested"` is committed:

**Analytics** (`services/analytics_service.py` → `compute_and_store_analytics()`): runs DuckDB aggregations against the full file (row count, column stats, value distributions). Overwrites the `row_count` that was set from the 500-row sample. Takes seconds to minutes depending on file size.

**Parquet conversion** (`services/parquet_service.py` → `convert_csv_to_parquet()`): fired as `asyncio.ensure_future()` — fire-and-forget, not awaited. Runs `_run_conversion()` inside `asyncio.to_thread()`.

#### What `_run_conversion()` actually does

```python
# 1. DuckDB reads CSV from Azure → writes local Parquet temp file
conn.execute(f"""
    COPY (
        SELECT * FROM read_csv_auto(
            'az://container/blob.csv',
            null_padding=true,
            ignore_errors=true
        )
    )
    TO '/tmp/tmpXXXXXX.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
""")

# 2. Azure SDK uploads temp file back to blob storage
parquet_blob_client.upload_blob(open(tmp_parquet_path, "rb"), overwrite=True)

# 3. os.unlink(tmp_parquet_path) in the finally block
```

There is no `pd.read_csv()` and no `df.to_parquet()`. The entire CSV→Parquet conversion is one DuckDB `COPY` statement. DuckDB reads the CSV from Azure over HTTP using parallel range requests, converts inline, and writes ZSTD-compressed Parquet with 100k-row row groups to a local temp file. PyArrow is only used after conversion to read the Parquet metadata (`pq.ParquetFile()`) for the row count and row group count — it does not do the conversion.

The Parquet blob path is the CSV path with `.csv` replaced by `.parquet`. After upload the temp file is deleted.

Until Parquet conversion finishes, query-time `run_sql` falls back to `read_csv_auto()` on the original blob. Once it finishes, the catalog cache stores the Parquet path and subsequent queries use `read_parquet()` instead.

---

## Known Limitations

**Shortlist is a rough heuristic.** If the relevant file has unusual vocabulary or was described poorly at ingest, retrieval misses it. `search_catalog` inside the agent is the fallback, but the agent has to choose to call it.

**broaden_nudge fires at most once.** If the first broaden attempt also fails (wrong file found by `search_catalog`, another cast error), there is no second nudge. The agent gives up.

**JOIN type mismatch is hard to catch proactively.** The prompt instructs the model to call `get_file_schema` on both sides before writing a JOIN and compare column types. In practice the model sometimes writes the JOIN directly from the shortlist descriptions without checking types. When that happens, DuckDB throws a cast error, the error is treated as a failed SQL, and the broaden_nudge fires to redirect.

**MAX_TOOL_CALLS = 8 is a hard stop.** The budget counts `agent_node` iterations, not individual tool calls. A complex query (schema lookup, two JOIN attempts, a `search_catalog`, two more schema lookups, final SQL) can hit 8 before completing. The LLM is given a "summarise what you have" message and halts immediately, even mid-investigation.

**`search_catalog` is keyword-based, not semantic.** It tokenises the query and counts matches against each file's stored text. Vocabulary mismatch (user says "vendor", file says "supplier") can still cause it to miss the right file. The lookup-padding heuristic partially compensates for dimension tables, but not for transactional files with unusual names.

**Stale sample values.** `get_file_schema` returns samples from the catalog cache, not live from the file. If the data was updated since ingestion, the samples may not reflect current values.
