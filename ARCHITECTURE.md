# G-CHAT Architecture

> Last verified: 17 April 2026

## Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Frontend | Next.js (App Router) | 16.2 |
| UI | React + Tailwind CSS | 19 / 4 |
| Backend | FastAPI + Python | 0.115+ / 3.12 |
| LLM | Azure OpenAI (`gpt-4o-mini`) | 2024-02-01 API |
| Agent | LangGraph + LangChain | 0.2+ / 0.3+ |
| Query Engine | DuckDB (reads Azure Blob directly) | 1.3+ |
| Metadata DB | Neon PostgreSQL (asyncpg) | — |
| Storage | Azure Blob Storage | — |
| Auth | Google OAuth2 (Authlib) + JWT | — |

---

## High-Level Flow

```
┌──────────────┐    POST /api/chat/message    ┌─────────────────────────────────┐
│              │ ───────────────────────────▸  │  FastAPI Backend                │
│  Next.js     │                               │                                 │
│  Frontend    │    POST /api/chat/ingest      │  ┌──────────────┐               │
│              │ ───────────────────────────▸  │  │ Query Router │               │
│  (React 19)  │                               │  └──────┬───────┘               │
│              │ ◂──────────── JSON ─────────  │         │                       │
└──────────────┘                               │    ┌────┴────┬──────────┐       │
                                               │    ▾         ▾          ▾       │
                                               │ metadata  precomp.    agent     │
                                               │    │         │          │       │
                                               │    ▾         ▾          ▾       │
                                               │ Postgres  Postgres  LangGraph   │
                                               │                      │          │
                                               │                      ▾          │
                                               │                 DuckDB → Azure  │
                                               └─────────────────────────────────┘
```

---

## 1. Query Router

Every chat message is classified by keyword matching — no LLM call, instant.

**File:** `server/app/services/query_router.py`

| Route | Trigger Keywords | Data Source | Latency |
|-------|-----------------|-------------|---------|
| **metadata** | "how many files", "what columns", "schema", "row count" | `file_metadata` (Postgres) | <50ms |
| **precomputed** | "analytics", "average", "total", "distribution", "breakdown" | `file_analytics` (Postgres) | <100ms |
| **agent** | Everything else, or override patterns (`where`, `top`, `last`, `filter`, `group by`, `join`) | LangGraph → DuckDB → Azure Blob | 3–10s |

**Override rule:** Data retrieval patterns force agent routing even when metadata/precomputed keywords are present. Override patterns: `last`, `first`, `top`, `bottom`, `show me row`, `get me row`, `fetch row`, `10th row`, `nth row`, `specific row`, `where`, `filter`, `group by`, `join`, `between`.

---

## 2. LangGraph Agent

Only invoked when the query router can't answer from pre-computed data.

**Files:** `server/app/agent/graph.py`, `state.py`, `llm.py`

### Agent Loop

```
START ──▸ Agent Node (LLM) ──▸ has tool_calls? ──yes──▸ Tool Node ──▸ Agent Node (loop)
                                     │
                                     no
                                     │
                                     ▾
                                    END (final answer)
```

### Configuration

| Setting | Value | Source |
|---------|-------|--------|
| Model | `gpt-4o-mini` | Azure OpenAI |
| Temperature | 0 (deterministic) | `llm.py` |
| Max tokens | 1500 | `llm.py` |
| Timeout | 60 seconds | `llm.py` |
| Max retries | 2 (auto-retry on 429/500/503) | `llm.py` |
| Max tool calls | 6 | `state.py` |
| DuckDB timeout | 30 seconds per query | `duckdb_client.py` |
| Max result rows | 1000 (hard cap) | `duckdb_client.py` |

### System Prompt (built per request)

The system prompt is dynamically constructed with:

1. **Parquet file paths** — injected from `file_analytics.parquet_blob_path`, formatted as `read_parquet('az://container/file.parquet')`
2. **Column names per file** — injected from `file_metadata.columns_info`, e.g. `Columns: id, user_id, name, email, amount, status, region, ...`
3. **AI description per file** — from `file_metadata.ai_description`
4. **Data format preview note** — if sample rows available from ingest

This means the LLM sees exact file paths and column names, eliminating guesswork.

### 5 Agent Tools

| # | Tool | File | Purpose |
|---|------|------|---------|
| 1 | `run_sql` | `tools/sql.py` | Execute any DuckDB SQL. Returns rows + columns + truncation warning if >1000 rows. Blocks DML. |
| 2 | `search_catalog` | `tools/catalog.py` | Full-text search across file descriptions, columns, good_for tags. |
| 3 | `get_file_schema` | `tools/catalog.py` | Returns column names, types, sample values, unique counts for a file. |
| 4 | `inspect_data_format` | `tools/sample.py` | Preview up to 20 example rows for value format checking before SQL. |
| 5 | `summarise_dataframe` | `tools/stats.py` | Compute min/max/mean/sum/std on the last `run_sql` result in memory. |

### Tool Execution Detail

**`run_sql`:**
- DML blocked: `DROP`, `DELETE`, `UPDATE`, `INSERT`, `CREATE`, `ALTER`, `TRUNCATE`
- Timeout: 120s (for large scans)
- Returns: `{row_count, total_rows, columns, preview_rows (first 5), warning?}`
- If `total_rows > 1000`: adds `"Results truncated: showing 1000 of X total rows."`
- Stores result rows in per-request state store (for `summarise_dataframe`)

**`execute_query` (DuckDB client):**
- Signature: `execute_query(sql, connection_string, timeout_seconds=30, max_rows=1000) -> tuple[list[dict], int]`
- Returns `(rows, total_row_count)` — `total` is true DuckDB count, `rows` is capped at `max_rows`
- Connection: thread-local, auto-reconnects, Azure extension loaded lazily

### Per-Request State Store

Each agent invocation gets a unique `request_id`. A thread-safe dict (`_request_stores`) holds mutable state (SQL results) that tools share during a single query. Cleaned up after the query completes.

### Chart Inference

After the agent answers, `_infer_chart()` auto-detects chart type from the answer text and result shape:

| Pattern | Chart Type |
|---------|-----------|
| "over time", "trend", "monthly" | `line` |
| "distribution", "proportion", "percent" | `pie` |
| >50 rows | `table` |
| Default | `bar` |

Returns: `{type, x_column, y_column, title}`

---

## 3. Ingest Pipeline

Triggered by `POST /api/chat/ingest` (admin only). Runs as a FastAPI background task.

**Files:** `server/app/services/ingestion_service.py`, `analytics_service.py`, `parquet_service.py`

### 5 Steps

| Step | Name | What It Does | Duration |
|------|------|-------------|----------|
| 1/5 | **DuckDB Sample** | Read 500 rows from Azure Blob via DuckDB. Detect columns, types, sample values. | 1–5s |
| 2/5 | **AI Description** | Send columns + 3 sample rows to `gpt-4o-mini`. Get summary, good_for, key_metrics, key_dimensions, date_range. | ~3s |
| 3/5 | **Save Metadata** | Write `FileMetadata` record to Postgres (columns_info, row_count, ai_description, sample_rows). | <1s |
| 4/5 | **Detect Relationships** | Find shared column names across files. Create `FileRelationship` records with confidence scores. | <2s |
| 5/5 | **Compute Analytics + Parquet** | Compute stats from 500-row sample -> `FileAnalytics`. Fire background Parquet conversion. | 2–5 min (Parquet) |

### Parquet Conversion (Background)

- DuckDB reads CSV from `az://container/file.csv` (HTTP range requests)
- Converts to local temp Parquet (ZSTD compression, 100k row groups)
- Azure SDK uploads back to `az://container/file.parquet`
- Updates `file_analytics.parquet_blob_path` and `row_count`
- Peak memory: ~256–512 MB
- Creates `BackgroundJob` record to track status

### Status Flow

```
not_ingested -> pending (Step 1) -> ingested (Step 3) -> parquet ready (Step 5 background)
                                 -> failed (on error)
```

---

## 4. Data Models (Postgres)

**File:** `server/app/models/`

### Entity Relationship

```
User ──1:N──▸ Folder
User ──1:N──▸ File (via owner_id)
Folder ──1:N──▸ File
ContainerConfig ──1:N──▸ File

File ──1:1──▸ FileMetadata
File ──1:1──▸ FileAnalytics
File ──1:N──▸ FileRelationship (as file_a or file_b)
File ──1:N──▸ BackgroundJob
```

### Key Tables

**`files`** — uploaded file records

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| name | string | |
| blob_path | string (unique) | Azure Blob path |
| container_id | FK -> ContainerConfig | |
| ingest_status | enum | `not_ingested`, `pending`, `ingested`, `failed` |
| size | BigInteger | supports 10 GB+ |

**`file_metadata`** — ingest results (1:1 with files)

| Column | Type | Notes |
|--------|------|-------|
| columns_info | JSONB | `[{name, type, sample_values, unique_values}]` |
| row_count | BigInteger | from 500-row sample |
| ai_description | text | LLM-generated summary |
| good_for | JSONB | `["sales analysis", "regional trends"]` |
| key_metrics | JSONB | numeric column names |
| key_dimensions | JSONB | categorical column names |
| sample_rows | JSONB | up to 500 row dicts |

**`file_analytics`** — pre-computed stats + parquet (1:1 with files)

| Column | Type | Notes |
|--------|------|-------|
| column_stats | JSONB | per-column: min, max, mean, sum, std, nulls |
| value_counts | JSONB | top 20 values per categorical column |
| cross_tabs | JSONB | dimension x metric aggregations |
| parquet_blob_path | string | path to converted parquet in Azure |
| parquet_size_bytes | BigInteger | |

**`file_relationships`** — cross-file join hints

| Column | Type | Notes |
|--------|------|-------|
| file_a_path | string | blob path of first file |
| file_b_path | string | blob path of second file |
| shared_column | string | column name they share |
| confidence_score | float | 0.0–1.0 |

**`container_configs`** — Azure Blob container credentials

| Column | Type | Notes |
|--------|------|-------|
| connection_string | EncryptedText | Fernet encrypted at rest |
| container_name | string | Azure container name |

**`background_jobs`** — async job tracking

| Column | Type | Notes |
|--------|------|-------|
| job_type | string | e.g. `parquet_conversion` |
| status | enum | `running`, `done`, `failed` |

---

## 5. API Routes

**Base path:** `/api`

### Auth (`/api/auth`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/auth/google/login` | — | Redirect to Google consent screen |
| GET | `/auth/google/callback` | — | OAuth callback -> create user -> JWT -> redirect to frontend |
| GET | `/auth/me` | JWT | Get current user |

- First user auto-promoted to admin
- JWT lifetime: 7 days (HS256)
- Token stored in `localStorage` + cookie (for Next.js middleware)

### Chat (`/api/chat`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/chat/message` | JWT | Send query -> router -> response |
| POST | `/chat/ingest` | Admin | Queue files for ingestion (background) |
| GET | `/chat/ingest-status/{file_id}` | JWT | Check ingest progress |

**POST /chat/message:**
- Validates: not empty, max 2000 chars
- Sets `trace_id` for structured logging
- Routes via `classify_intent()` -> metadata / precomputed / agent
- Returns: `{answer, data, chart, route, row_count, tool_calls, files_used}`

### Files (`/api/files`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/files/upload-url` | Admin | Get SAS URL for direct Azure upload |
| POST | `/files/confirm-upload` | Admin | Confirm upload + save File metadata |

**Upload flow:**
1. Frontend requests SAS URL (2h expiry)
2. Frontend uploads directly to Azure via `@azure/storage-blob` SDK (64 MB blocks, 4 parallel)
3. Frontend confirms upload -> backend creates File record

### Containers (`/api/containers`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/containers` | Admin | Create container config (encrypts connection string) |
| GET | `/containers` | JWT | List containers |
| POST | `/containers/{id}/sync` | Admin | Sync Azure blobs -> File records |

### Folders (`/api/folders`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/folders/{id}/contents` | JWT | List subfolders + files (`id=root` for root) |
| POST | `/folders` | Admin | Create folder |
| PATCH | `/folders/{id}` | Admin | Rename / move |
| DELETE | `/folders/{id}` | Admin | Delete |

### Users (`/api/users`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/users` | Admin | List all users |
| PATCH | `/users/{id}/toggle-admin` | Admin | Toggle admin status |

### Admin (`/api/admin`)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/admin/cost-summary` | JWT | Session cost breakdown (LLM + Azure) |

---

## 6. Frontend Architecture

**Framework:** Next.js 16.2 with App Router

### Route Structure

```
/                          -> Root (redirects)
/login                     -> Google OAuth login
/auth/callback?token=JWT   -> OAuth callback, stores token
/chat                      -> Main chat interface (protected)
/folders                   -> File/folder manager (protected)
/admin/containers          -> Azure container config (admin)
/profile                   -> User profile (protected)
```

### Middleware (`middleware.ts`)

- `/login` -> public
- All other routes -> require token cookie
- Logged-in user on `/login` -> redirect to `/chat`
- Unauthenticated user on protected route -> redirect to `/login`

### Chat Page Features

- Message thread (user / assistant messages)
- **Data table:** results grid (horizontal scroll, up to 100 visible rows)
- **Analytics grid:** auto-computed from response data
  - Numeric columns -> min, max, mean, sum, std
  - Categorical columns -> top 8 values with frequency
- **Chart:** auto-inferred type (bar / line / pie / table) rendered from response data

### Direct Azure Upload

Using `@azure/storage-blob` SDK:
- 64 MB block size
- 4 parallel upload streams
- Progress callback with speed (MB/s) and ETA
- Supports files 10 GB+

### Key Dependencies

| Package | Purpose |
|---------|---------|
| `next` 16.2 | React framework |
| `react` 19 | UI library |
| `tailwindcss` 4 | Styling |
| `swr` 2.4 | Data fetching (stale-while-revalidate) |
| `@azure/storage-blob` 12.31 | Direct blob upload |
| `lucide-react` | Icons |

---

## 7. DuckDB + Azure Blob Integration

**File:** `server/app/core/duckdb_client.py`

### How It Works

DuckDB reads Parquet (or CSV) files directly from Azure Blob Storage via HTTP range requests. No download needed.

```
Agent writes SQL:
  SELECT region, COUNT(*)
  FROM read_parquet('az://container/file.parquet')
  GROUP BY region
          |
          v
  DuckDB Azure extension
          |
          v
  HTTP range requests to Azure Blob
```

### Connection Management

- Thread-local connection cache (keyed by MD5 of connection string)
- Lazy Azure extension install on first use
- Auto-reconnects on idle failures
- Uses `curl` HTTP transport

### Two Entry Points

| Function | When Used | Returns |
|----------|-----------|---------|
| `sample_file()` | Ingest time (Step 1) | `{columns_info, sample_rows, row_count, column_names}` — 500 rows max |
| `execute_query()` | Chat time (agent) | `tuple[list[dict], int]` — (rows capped at 1000, total count) |

---

## 8. Security

### Authentication Flow

```
User -> Google OAuth -> FastAPI callback -> JWT created -> Frontend stores token
                                                                   |
                                                            Cookie + localStorage
                                                                   |
                                                            All API calls: Authorization: Bearer {token}
```

### Security Measures

| Area | Implementation |
|------|---------------|
| Auth | Google OAuth2 + JWT (HS256, 7-day expiry) |
| Admin | `require_admin` dependency — HTTP 403 if not admin |
| SQL injection | DML blocked (`DROP`, `DELETE`, etc.) — read-only SQL only |
| Secrets at rest | Azure connection strings Fernet-encrypted in Postgres |
| CORS | Restricted to `FRONTEND_URL` origin |
| Token | `SECRET_KEY` signs all JWTs (must be changed from default in production) |

---

## 9. Logging & Cost Tracking

### Structured Logging (`structlog`)

**File:** `server/app/core/logger.py`

| Logger | Events |
|--------|--------|
| `chat_logger` | `chain_start`, `query_routed`, `agent_start`, `agent_complete`, `agent_error`, `chain_end` |
| `llm_logger` | `llm_call` (model, tokens, duration, tool_calls, iteration) |
| `ingest_logger` | Pipeline step start/complete/error |
| `blob_logger` | Azure Blob operations |
| `upload_logger` | File uploads |

Output: rotating JSON logs in `server/logs/` (10 MB per file)

### Cost Tracking

**File:** `server/app/core/cost_tracker.py`, `ai_client.py`

| Model | Prompt | Completion |
|-------|--------|------------|
| gpt-4o-mini | $0.00015/1K tokens | $0.0006/1K tokens |
| gpt-4o | $0.005/1K tokens | $0.015/1K tokens |

Session accumulators track:
- Total LLM calls, tokens, cost
- Azure Blob operations (read/write), egress bytes, cost
- Available via `GET /api/admin/cost-summary`

---

## 10. Module Layout

```
server/
├── app/
│   ├── main.py                         # FastAPI app, lifespan, middleware, routers
│   ├── agent/
│   │   ├── graph.py                    # LangGraph StateGraph, system prompt, run_agent_query()
│   │   ├── state.py                    # AgentState TypedDict, MAX_TOOL_CALLS=6
│   │   ├── llm.py                      # AzureChatOpenAI singleton (temp=0, timeout=60s)
│   │   └── tools/
│   │       ├── sql.py                  # run_sql (DuckDB SQL execution)
│   │       ├── catalog.py              # search_catalog, get_file_schema
│   │       ├── sample.py              # inspect_data_format (preview rows)
│   │       ├── stats.py               # summarise_dataframe (in-memory pandas)
│   │       └── analytics.py           # query_precomputed_analytics (DEAD CODE — not imported)
│   │
│   ├── api/
│   │   ├── auth.py                     # Google OAuth + JWT routes
│   │   ├── chat.py                     # /chat/message, /chat/ingest, /chat/ingest-status
│   │   ├── files.py                    # Upload URL + confirm upload
│   │   ├── folders.py                  # Folder CRUD
│   │   ├── containers.py              # Azure container config + sync
│   │   ├── users.py                    # User list + toggle admin
│   │   └── admin.py                    # Cost summary
│   │
│   ├── services/
│   │   ├── query_router.py             # classify_intent() + metadata/precomputed handlers
│   │   ├── ingestion_service.py        # 5-step ingest pipeline
│   │   ├── analytics_service.py        # Compute stats from sample (no DuckDB)
│   │   └── parquet_service.py          # CSV -> Parquet conversion + Azure upload
│   │
│   ├── core/
│   │   ├── config.py                   # Pydantic settings (env vars)
│   │   ├── database.py                 # SQLAlchemy async engine (pool=5, recycle=300s)
│   │   ├── duckdb_client.py            # DuckDB + Azure Blob (sample_file, execute_query)
│   │   ├── ai_client.py               # Azure OpenAI client, token counting, cost calc
│   │   ├── cost_tracker.py            # Session cost accumulators
│   │   ├── security.py                # JWT create/decode, get_current_user, require_admin
│   │   └── logger.py                  # structlog category loggers
│   │
│   └── models/
│       ├── user.py                     # User (id, email, name, is_admin)
│       ├── file.py                     # File (blob_path, ingest_status, size)
│       ├── folder.py                   # Folder (tree structure)
│       ├── container.py               # ContainerConfig (encrypted connection_string)
│       ├── file_metadata.py           # FileMetadata (columns_info, ai_description)
│       ├── file_analytics.py          # FileAnalytics (stats, parquet_blob_path)
│       ├── file_relationship.py       # FileRelationship (shared columns)
│       └── background_job.py          # BackgroundJob (async task tracking)
│
├── pyproject.toml                      # Dependencies (uv)
└── logs/                               # Rotating JSON logs

client/
├── app/
│   ├── layout.tsx                      # Root layout (AuthProvider)
│   ├── (app)/
│   │   ├── chat/page.tsx              # Chat interface + data table + charts
│   │   ├── folders/page.tsx           # File/folder manager + upload
│   │   ├── admin/containers/page.tsx  # Container config (admin)
│   │   └── profile/page.tsx           # User profile
│   ├── auth/callback/page.tsx         # OAuth redirect handler
│   └── login/page.tsx                 # Login page
├── components/                         # Shared UI components
├── lib/
│   ├── auth.ts                        # Token management, apiFetch(), fetchMe()
│   ├── upload.ts                      # Direct Azure upload (64MB blocks, 4 parallel)
│   └── utils.ts                       # cn() for Tailwind class merging
├── middleware.ts                       # Route protection (token cookie check)
└── package.json                        # Dependencies (Next.js 16, React 19, Tailwind 4)
```

---

## 11. Environment Variables

### Server (`server/.env`)

| Variable | Required | Purpose |
|----------|----------|---------|
| `DATABASE_URL` | Yes | PostgreSQL connection (Neon) |
| `AZURE_OPENAI_ENDPOINT` | Yes | Azure OpenAI endpoint |
| `AZURE_OPENAI_KEY` | Yes | Azure OpenAI API key |
| `AZURE_OPENAI_DEPLOYMENT` | Yes | Model deployment name |
| `AZURE_OPENAI_API_VERSION` | No | API version (default: `2024-02-01`) |
| `GOOGLE_CLIENT_ID` | Yes | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Yes | Google OAuth client secret |
| `SECRET_KEY` | Yes | JWT signing key (**change from default**) |
| `FRONTEND_URL` | Yes | CORS origin (e.g. `http://localhost:3000`) |
| `STORAGE_ENCRYPTION_KEY` | Yes | Fernet key for encrypting connection strings |
| `ADMIN_EMAIL` | No | Auto-admin on first login |

### Client (`client/.env`)

| Variable | Required | Purpose |
|----------|----------|---------|
| `NEXT_PUBLIC_API_URL` | Yes | Backend URL (e.g. `http://localhost:8000`) |

---

## 12. Deployment

### Current Setup

- **VM:** Azure Standard_D2ds_v4 (2 vCPU, 8 GB RAM), East US, Ubuntu 24.04
- **IP:** `40.90.236.191`
- **Backend:** `uv run uvicorn app.main:app --host 0.0.0.0 --port 8000`
- **Frontend:** Next.js on same VM or Vercel

### Deploy Commands

```bash
# SSH into VM
ssh -i server/uploads/genaivm_key.pem azureuser@40.90.236.191

# Redeploy backend
pkill -f uvicorn
cd ~/G-CHAT-/server
git pull
source $HOME/.local/bin/env
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```
