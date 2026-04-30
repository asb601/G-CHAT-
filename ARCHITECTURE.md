# G-CHAT — System Architecture

> Last updated: 26 April 2026

---

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js 16.2 (App Router) + React 19 + Tailwind 4 |
| Backend | FastAPI + Python 3.12 |
| LLM | Azure OpenAI `gpt-4o-mini` |
| Agent framework | LangGraph 0.2+ / LangChain 0.3+ |
| Query engine | DuckDB 1.3+ (reads Azure Blob directly) |
| Metadata DB | Neon PostgreSQL (asyncpg) |
| Storage | Azure Blob Storage |
| Auth | Google OAuth2 + JWT (HS256) |

---

## Two Core Flows

The system has two completely separate flows that never overlap:

```
INGESTION FLOW          QUERY FLOW
──────────────          ──────────
File arrives         →  User sends message
↓                        ↓
Understand it            Find the right files
↓                        ↓
Store metadata           Run SQL on them
↓                        ↓
Convert to Parquet        Return the answer
```

---

## FLOW 1 — INGESTION

> Triggered by: `POST /api/admin/ingest` or `POST /api/admin/sync-container`
> Entry point: `server/app/services/ingestion_service.py` → `ingest_file(file_id, db)`

Every file that arrives in Azure Blob Storage goes through this pipeline before it
is queryable. No steps are optional.

```
Azure Blob Storage (CSV / Excel)
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 1 — DuckDB Sample                                             │
│  file: server/app/core/duckdb_client.py                             │
│  fn:   sample_file(blob_path, connection_string)                    │
│                                                                     │
│  Reads 500 rows from the file on Azure via HTTP range requests.     │
│  Returns: columns_info, sample_rows, row_count, column_names.       │
│  Detects: column names, data types, blank %, junk values.           │
│  Time: 1–5 s (network-dependent)                                    │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 2 — AI Description                                            │
│  file: server/app/core/llm_tasks.py                                 │
│  fn:   generate_file_description(columns_info, sample_rows,         │
│                                  filename)                          │
│                                                                     │
│  Sends column names + 3 sample rows to gpt-4o-mini.                 │
│  Returns: summary, good_for[], key_metrics[], key_dimensions[],     │
│           date_range_start, date_range_end.                         │
│  Time: 2–4 s (OpenAI API round-trip)                                │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 3 — Save Metadata                                             │
│  file: server/app/services/ingestion_service.py                     │
│  fn:   ingest_file() — writes FileMetadata record                   │
│  table: file_metadata (PostgreSQL)                                  │
│                                                                     │
│  Persists: columns_info (JSONB), row_count, ai_description,         │
│            good_for, key_metrics, key_dimensions, sample_rows,      │
│            date_range_start, date_range_end.                        │
│  Also generates + stores the vector embedding for semantic search.  │
│  file: server/app/retrieval/embeddings.py                           │
│  fn:   embed_text(text)  /  build_search_text(metadata)             │
│  Time: < 1 s                                                        │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 4 — Relationship Detection                                    │
│  file: server/app/services/relationship_detector.py                 │
│  fn:   detect_relationships(file_id, columns, db)                   │
│  table: file_relationships (PostgreSQL)                             │
│                                                                     │
│  Compares this file's column names against every other ingested     │
│  file. Records shared columns with a confidence score (0.0–1.0).   │
│  These relationships are used at query time to auto-join files.     │
│  Time: < 2 s (scales with catalog size)                             │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 5 — Analytics + Parquet Conversion                            │
│  file: server/app/services/analytics_service.py                     │
│  fn:   compute_and_store_analytics(file_id, sample_rows, db)        │
│  fn:   trigger_parquet_conversion(file_id, db)   ← background       │
│                                                                     │
│  Analytics (from existing 500-row sample, no extra DuckDB call):   │
│    per-column min/max/mean/sum/std, top-20 value counts,            │
│    cross-tab aggregations → stored in file_analytics (PostgreSQL)   │
│                                                                     │
│  Parquet conversion (background, does NOT block ingest status):     │
│  file: server/app/services/parquet_service.py                       │
│  fn:   convert_csv_to_parquet(file_id, blob_path,                   │
│                               connection_string, db)                │
│  fn:   _run_conversion(csv_path, parquet_path, connection_string)   │
│    DuckDB COPY CSV → Parquet (ZSTD, 100k row groups, nullstr=[...]) │
│    Uploads result back to Azure Blob.                               │
│    Updates file_analytics.parquet_blob_path.                        │
│  Time: 1 s – 5 min depending on file size                           │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
   File status: ingested → parquet_ready
   File is queryable after Step 3 completes.
   Parquet (Step 5) makes large-file queries faster when ready.
```

### Ingest Status Flow

```
not_ingested  →  pending (Step 1 starts)
              →  ingested (Step 3 complete — file is queryable)
              →  parquet_ready (Step 5 background complete)
              →  failed (any step throws)
```

### PostgreSQL Tables Written During Ingestion

| Table | Written in Step | Key Columns |
|-------|----------------|-------------|
| `files` | trigger | `id`, `blob_path`, `ingest_status`, `container_id` |
| `file_metadata` | 3 | `columns_info` (JSONB), `ai_description`, `sample_rows`, `embedding` (vector) |
| `file_analytics` | 5 | `column_stats`, `value_counts`, `parquet_blob_path` |
| `file_relationships` | 4 | `file_a_path`, `file_b_path`, `shared_column`, `confidence_score` |
| `background_jobs` | 5 | `job_type`, `status`, `file_id` |

---

## FLOW 2 — QUERY

> Triggered by: `POST /api/chat/message`
> Entry point: `server/app/agent/graph/graph.py` → `run_agent_query(query, user, db, ...)`

When a user sends a message, the system goes through five stages before responding.

```
User message (plain English)
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 1 — Retrieval: Find the Right Files                          │
│  file: server/app/retrieval/orchestrator.py                         │
│  fn:   retrieve(query, user_id, db, is_admin, top_k=20)             │
│  fn:   retrieve_with_scores(...)  ← used for debug/logging          │
│                                                                     │
│  Reduces the full catalog to a shortlist of ≤20 files using         │
│  six parallel signals fused by Reciprocal Rank Fusion:              │
│                                                                     │
│  a) Temporal filter                                                  │
│     file: server/app/retrieval/temporal.py                          │
│     Parses "last week", "Q1 2026", "yesterday" from the query       │
│     into date bounds. Filters file_metadata by date_range overlap.  │
│                                                                     │
│  b) Permission filter                                                │
│     file: server/app/retrieval/filters.py                           │
│     fn:   permission_clause(user_id, is_admin)                      │
│     fn:   build_base_query(user_id, is_admin, db)                   │
│     Restricts candidates to containers the user owns or has been    │
│     granted access to.                                              │
│                                                                     │
│  c) BM25 keyword search                                              │
│     file: server/app/retrieval/bm25.py                              │
│     fn:   bm25_search(query, base_q, db)                            │
│     Full-text search on ai_description + column names using         │
│     PostgreSQL tsvector + GIN index. Returns ranked matches.        │
│                                                                     │
│  d) Fuzzy / trigram search                                           │
│     file: server/app/retrieval/fuzzy.py                             │
│     fn:   fuzzy_search(query, base_q, db, threshold=0.2)            │
│     pg_trgm similarity on filename and column names.                │
│     Catches typos and partial matches BM25 misses.                  │
│                                                                     │
│  e) Vector / semantic search                                         │
│     file: server/app/retrieval/embeddings_search.py                 │
│     fn:   vector_search(query_vec, base_q, db, top_k)               │
│     file: server/app/retrieval/embeddings.py                        │
│     fn:   embed_text(query)  ← embeds user query                    │
│     Cosine similarity against stored file embeddings (pgvector).    │
│     Finds files by meaning even when keywords don't match.          │
│                                                                     │
│  f) Graph expansion                                                  │
│     file: server/app/retrieval/graph_expand.py                      │
│     fn:   graph_expand(seed_file_ids, db, hops=1)                   │
│     Takes top-scored files and adds their relationship neighbours   │
│     (files sharing a join column) from file_relationships.          │
│                                                                     │
│  g) RRF fusion + budget enforcement                                  │
│     file: server/app/retrieval/rrf.py                               │
│     fn:   rrf_fuse(bm25_ranks, fuzzy_ranks, vector_ranks, ...)      │
│     Combines all signal rankings into one score. Enforces top_k=20  │
│     budget so the LLM never sees more than 20 file summaries.       │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼  20 candidate files
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 2 — Prompt Assembly                                          │
│  file: server/app/agent/prompts/prompt_builder.py                   │
│  fn:   build_system_prompt(catalog, parquet_paths, container, ...)  │
│  fn:   build_parquet_note(catalog, parquet_paths_all, ...)          │
│  fn:   _neutralize_description(desc)  ← strips over-anchoring       │
│                                                                     │
│  Builds the system prompt injecting for each of the 20 files:       │
│    - read_parquet('az://container/file.parquet') path               │
│    - AI description (neutralised)                                   │
│    - key_dimensions, key_metrics                                    │
│  Also injects rules: HOW TO WORK, JOIN HANDLING, EMPTY RESULTS,     │
│  DuckDB syntax reference, max tool call budget.                     │
│                                                                     │
│  file: server/app/agent/graph/graph.py                              │
│  fn:   _build_agent_context(query, user, db, ...)                   │
│  Loads catalog entries, resolves Parquet paths, assembles context.  │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 3 — LangGraph Agent Loop                                     │
│  file: server/app/agent/graph/graph_builder.py                      │
│  fn:   build_graph(all_tools)  ← compiles the StateGraph            │
│  fn:   build_agent_node(all_tools)  ← wraps LLM with tool binding   │
│                                                                     │
│  Graph nodes:                                                       │
│    agent_node  →  GPT-4 decides which tool to call next             │
│    tools_node  →  executes the tool, appends result to messages     │
│    broaden_nudge_node  →  injects corrective system message if      │
│                            agent gives up without searching catalog  │
│    END  →  final answer                                             │
│                                                                     │
│  Routing (fn: route):                                               │
│    agent produces tool_calls?  →  tools_node                        │
│    agent gave up too early?    →  broaden_nudge_node                │
│       (fn: _should_force_broaden, _had_zero_row_sql,                │
│            _all_errors_are_parquet_dtype,                           │
│            _zero_rows_only_from_relative_time,                      │
│            _called_search_catalog)                                  │
│    otherwise                   →  END                               │
│                                                                     │
│  Max tool calls: 8  (set in server/app/agent/state.py)              │
│  Model: gpt-4o-mini, temp=0, max_tokens=1500                        │
│  file: server/app/agent/llm.py                                      │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼  LLM calls tools in a loop until it has an answer
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 4 — Tool Execution                                           │
│                                                                     │
│  Tool 1: run_sql                                                    │
│  file: server/app/agent/tools/sql.py                                │
│  fn:   run_sql(sql)                                                 │
│    Calls execute_query_sync(sql, connection_string, max_rows=1000)  │
│    file: server/app/core/duckdb_client.py                           │
│    fn:   execute_query_sync(sql, connection_string, max_rows)       │
│    fn:   _get_connection(connection_string)  ← thread-local cache   │
│    DuckDB reads Parquet/CSV directly from Azure via HTTP ranges.    │
│    DML blocked: DROP, DELETE, UPDATE, INSERT, CREATE, ALTER.        │
│    Returns: {row_count, total_rows, columns, preview_rows[5]}       │
│    On dtype/Int64 error: returns hint to call get_file_schema first │
│                                                                     │
│  Tool 2: get_file_schema                                            │
│  file: server/app/agent/tools/catalog.py                            │
│    Returns column names, types, sample_values for a specific file.  │
│                                                                     │
│  Tool 3: search_catalog                                             │
│  file: server/app/agent/tools/catalog.py                            │
│    Full-text + score search across all ingested file metadata.      │
│    Uses token matching + lookup-file padding (is_lookup_file check) │
│    file: server/app/agent/search_normalization.py                   │
│                                                                     │
│  Tool 4: inspect_data_format                                        │
│  file: server/app/agent/tools/sample.py                             │
│    Returns up to 20 raw rows for format-checking before SQL.        │
│                                                                     │
│  Tool 5: summarise_dataframe                                        │
│  file: server/app/agent/tools/stats.py                              │
│    Computes min/max/mean/sum/std on the last run_sql result held    │
│    in per-request state store.                                      │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼  LLM produces final answer text
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 5 — Response Extraction                                      │
│  file: server/app/agent/graph/graph.py                              │
│  fn:   run_agent_query(...)  ← collects final agent message         │
│  file: server/app/agent/response_helpers.py                         │
│    Extracts Markdown table rows from answer text into structured    │
│    data. Infers chart type (bar/line/pie/table) from answer shape.  │
│                                                                     │
│  Returns to API:                                                     │
│    {answer, data[], chart{type, x, y, title},                       │
│     route, row_count, tool_calls, files_used}                       │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
   Next.js frontend renders answer + table + chart
```

---

## Agent Tool Call Example

A real trace for: *"Show me the first 10 rows from the sales file"*

```
[iter 1]  LLM → search_catalog("sales")
          tool returns → sales_2025.csv  (Parquet ready at sales_2025.parquet)

[iter 2]  LLM → get_file_schema("sales_2025.csv")
          tool returns → {columns: [order_id, date, customer, amount, ...]}

[iter 3]  LLM → run_sql("SELECT * FROM read_parquet('az://c/sales_2025.parquet') LIMIT 10")
          tool returns → {row_count: 10, columns: [...], preview_rows: [...]}

[iter 4]  LLM produces final answer with Markdown table
          route() → END
```

---

## Data Models (PostgreSQL)

**Location:** `server/app/models/`

```
User ──1:N──▸ Folder
User ──1:N──▸ File (owner_id)
Folder ──1:N──▸ File
ContainerConfig ──1:N──▸ File

File ──1:1──▸ FileMetadata      (ingest output)
File ──1:1──▸ FileAnalytics     (stats + parquet path)
File ──1:N──▸ FileRelationship  (join hints)
File ──1:N──▸ BackgroundJob     (parquet conversion tracking)
```

| Model file | Table | Purpose |
|-----------|-------|---------|
| `models/file.py` | `files` | Core record: blob_path, ingest_status, container |
| `models/file_metadata.py` | `file_metadata` | columns_info, ai_description, embedding |
| `models/file_analytics.py` | `file_analytics` | column_stats, parquet_blob_path |
| `models/file_relationship.py` | `file_relationships` | shared columns, confidence |
| `models/user.py` | `users` | Google OAuth, is_admin flag |
| `models/folder.py` | `folders` | Hierarchical file organisation |
| `models/container.py` | `container_configs` | Encrypted Azure connection strings |
| `models/background_job.py` | `background_jobs` | Async job tracking |
| `models/conversation.py` | `conversations` | Chat message history |

---

## API Routes

**Base path:** `/api`

| Method | Path | Auth | Handler |
|--------|------|------|---------|
| POST | `/chat/message` | JWT | `run_agent_query()` in `agent/graph/graph.py` |
| GET | `/auth/google/login` | — | OAuth redirect |
| GET | `/auth/google/callback` | — | JWT issue → redirect |
| GET | `/auth/me` | JWT | Current user |
| POST | `/admin/ingest` | Admin | `ingest_file()` in `services/ingestion_service.py` |
| POST | `/admin/sync-container` | Admin | Scan Azure container → create File records |
| POST | `/admin/reingest-all` | Admin | Reset all ingest_status → re-run pipeline |
| POST | `/files/upload-url` | Admin | SAS URL for direct Azure upload |
| POST | `/files/confirm-upload` | Admin | Create File record after direct upload |
| GET | `/folders/{id}/contents` | JWT | List files/folders |
| GET | `/admin/cost-summary` | JWT | LLM + Azure cost breakdown |

---

## Security

| Concern | Implementation | File |
|---------|---------------|------|
| Auth | Google OAuth2 + JWT HS256 (7-day) | `core/security.py`, `lib/auth.ts` |
| Admin gate | `require_admin` dependency → HTTP 403 | `dependencies.py` |
| SQL injection | DML blocked at tool level | `agent/tools/sql.py` |
| Secrets at rest | Azure connection strings Fernet-encrypted | `core/crypto.py` |
| CORS | Restricted to `FRONTEND_URL` | `main.py` |
| Data isolation | Permission clause on every DB query | `retrieval/filters.py` → `permission_clause()` |

---

## Key Configuration

| Setting | Value | File |
|---------|-------|------|
| Max tool calls per query | 8 | `agent/state.py` → `MAX_TOOL_CALLS` |
| LLM temperature | 0 (deterministic) | `agent/llm.py` |
| LLM max tokens | 1500 | `agent/llm.py` |
| DuckDB query timeout | 30 s | `core/duckdb_client.py` |
| Max result rows | 1000 | `core/duckdb_client.py` → `execute_query_sync()` |
| Retrieval top-k | 20 files | `retrieval/orchestrator.py` → `retrieve()` |
| Ingest sample size | 500 rows | `core/duckdb_client.py` → `sample_file()` |
| Parquet compression | ZSTD, 100k row groups | `services/parquet_service.py` → `_run_conversion()` |
