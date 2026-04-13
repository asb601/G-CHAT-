# G-CHAT System Improvement Plan

## Current State (What is broken and why)

### Problem 1 — Parquet conversion never completes
`convert_to_parquet()` in `duckdb_client.py` uses DuckDB's `az://` protocol.
DuckDB Azure extension has documented open bugs (GitHub issues #96, #155):
- 1MB buffer → 80 separate HTTP calls per 80MB read → CPU-bound, not network-bound
- Token re-auth overhead on nearly every request
- For a 3GB CSV: thousands of DownloadTo calls → always times out, even at 1800s

**Evidence:** `parquet_blob_path: None` confirmed in DB — has never succeeded once.

### Problem 2 — No job tracking
`trigger_parquet_conversion()` is fire-and-forget via `asyncio.ensure_future()`.
If it fails, nothing is recorded. No status, no error message, no retry.
The agent cannot tell the user "conversion is running" vs "conversion failed".

### Problem 3 — Agent hardcodes single file
`graph.py` line: `first_meta = all_meta[0]`
`query_router.py` line: `analytics = all_analytics[0]`
If multiple files are uploaded, the agent always uses the first one.

### Problem 4 — Two dead files causing confusion
`services/query_service.py` — 391 lines, never imported anywhere
`services/agent_service.py` — never imported anywhere
Both are old pipelines replaced by the current `app/agent/` package.

---

## Target State (What "fixed" looks like)

```
User uploads CSV
      ↓
FastAPI → ingestion_service (unchanged)
      ├── Step 1: DuckDB sample 500 rows          ← works today
      ├── Step 2: AI description                  ← works today
      ├── Step 3: Save metadata + sample_rows     ← works today
      ├── Step 4: Detect relationships            ← works today
      └── Step 5: Compute analytics (pandas)      ← works today
            ↓
            User can chat immediately (sample-based answers)
            ↓
      Background job: PyArrow converts CSV → Parquet
            ├── Creates BackgroundJob record: status="running"
            ├── Azure SDK streams CSV (one connection, no DuckDB)
            ├── PyArrow converts block by block (256MB at a time)
            ├── Azure SDK uploads Parquet
            └── Updates: BackgroundJob status="done" | "failed"
                        FileAnalytics.parquet_blob_path = "..."

Agent gets a query
      ↓
Checks parquet status via BackgroundJob table
      → "done":    run DuckDB SQL on Parquet — fast, full data
      → "running": answer from sample + tell user "Parquet ready in ~X min"
      → "failed":  answer from sample + tell user why it failed
      → None:      answer from sample only
```

---

## Dependency Map (Every relationship that each change touches)

### Step 1 — Add pyarrow to dependencies
**File changed:** `server/pyproject.toml`
**Downstream effects:** None. pyarrow is already available in the environment but not declared.
**Risk:** Low.

---

### Step 2 — Create `parquet_service.py`
**New file:** `server/app/services/parquet_service.py`
**Does:** Streams CSV from Azure → PyArrow converts → uploads Parquet back
**Imports needed in new file:**
- `azure.storage.blob.BlobServiceClient` (already in pyproject.toml)
- `pyarrow.csv`, `pyarrow.parquet` (Step 1)
- `app.core.logger.ingest_logger`

**Files that will import this new file:**
- `server/app/services/analytics_service.py` — `trigger_parquet_conversion()` will call it
- Nothing else touches it

**Files NOT touched by this step:**
- `duckdb_client.py` — `convert_to_parquet()` stays there for now (deleted in Step 4)
- `graph.py` — unchanged
- `ingestion_service.py` — unchanged
- All models — unchanged
- All API routes — unchanged

**Risk:** Medium. New code, needs to be tested with actual Azure credentials.

---

### Step 3 — Create `BackgroundJob` model
**New file:** `server/app/models/background_job.py`
**Columns:** `id`, `file_id`, `job_type`, `status`, `error_message`, `started_at`, `completed_at`

**Files that must be updated:**
- `server/app/core/database.py` — must import the new model so SQLAlchemy Base knows about it
  (check if it auto-imports via `__init__.py` or explicitly lists models)
- New Alembic migration needed OR use `Base.metadata.create_all()` in main.py

**Files that will write to this table:**
- `server/app/services/analytics_service.py` — `trigger_parquet_conversion()` writes status

**Files that will read from this table:**
- New `GET /api/files/{file_id}/job-status` endpoint (Step 5)
- `server/app/agent/graph.py` — needs to read job status to tell the agent

**Risk:** Medium. Requires a DB migration. Must not break existing tables.

---

### Step 4 — Update `trigger_parquet_conversion()` in `analytics_service.py`
**File changed:** `server/app/services/analytics_service.py`

**Current call chain:**
```
ingestion_service.py
  → asyncio.ensure_future(trigger_parquet_conversion(...))   [line ~197]
    → analytics_service.trigger_parquet_conversion()
      → duckdb_client.convert_to_parquet()                   [BROKEN]
        → writes FileAnalytics.parquet_blob_path
```

**New call chain:**
```
ingestion_service.py
  → asyncio.ensure_future(trigger_parquet_conversion(...))   [unchanged]
    → analytics_service.trigger_parquet_conversion()
      → BackgroundJob record: status="running"               [NEW - Step 3]
      → parquet_service.convert_csv_to_parquet()             [NEW - Step 2]
        → writes FileAnalytics.parquet_blob_path             [unchanged]
        → updates BackgroundJob: status="done"               [NEW - Step 3]
      → on failure: BackgroundJob: status="failed", error=.. [NEW - Step 3]
```

**Import changes in analytics_service.py:**
- Remove: `from app.core.duckdb_client import convert_to_parquet`
- Add: `from app.services.parquet_service import convert_csv_to_parquet`
- Add: `from app.models.background_job import BackgroundJob`

**Files NOT touched by this step:**
- `ingestion_service.py` — still calls `trigger_parquet_conversion()`, nothing changes
- `graph.py` — still reads `analytics_row.parquet_blob_path`, nothing changes
- `duckdb_client.py` — `convert_to_parquet()` still exists, just no longer called

**Risk:** Low-medium. The `trigger_parquet_conversion()` function signature doesn't change.

---

### Step 5 — Add job status API endpoint
**New or updated file:** `server/app/api/files.py`
**New route:** `GET /api/files/{file_id}/job-status`
**Returns:** `{ job_type, status, error_message, started_at, completed_at }`

**Files that must be updated:**
- `server/app/main.py` — check if `files` router is already registered (it is: `app/api/files.py` exists)

**Frontend impact:** Frontend can poll this endpoint to show conversion progress.
**Risk:** Low. Read-only endpoint.

---

### Step 6 — Fix single-file hardcoding in graph.py
**File changed:** `server/app/agent/graph.py`

**Current code:**
```python
first_meta = all_meta[0]                           # line ~247
container = await db.get(ContainerConfig, first_meta.container_id)
analytics_result = await db.execute(
    select(FileAnalytics).where(FileAnalytics.file_id == first_meta.file_id)
)
```

**Problem:** If 3 files are uploaded, agent always uses the connection_string and
container of file[0], even if the query is about file[1] or file[2].

**Fix:** Agent should load ALL analytics rows (already done for `all_meta`),
not resolve the container until after `search_catalog` identifies the relevant file.
For now: load all analytics into a dict keyed by file_id, pass to tools.

**Files touched:** Only `graph.py`.
**Files NOT touched:** `query_router.py` (same bug there — fix in same step).
**Risk:** Medium. Must not break the single-file case that works today.

---

### Step 7 — Delete dead files
**Files to delete:**
- `server/app/services/query_service.py`
- `server/app/services/agent_service.py`

**Verification before delete:**
- `grep -r "query_service" server/app/` — must return 0 results
- `grep -r "agent_service" server/app/` — must return 0 results

**Files NOT affected:** None — confirmed not imported anywhere.
**Risk:** Low (read-only deletion of unused code).

---

### Step 8 — Remove `convert_to_parquet` from `duckdb_client.py`
**File changed:** `server/app/core/duckdb_client.py`
**Action:** Delete `convert_to_parquet()` function (lines ~173-217)

**Verify before deleting:**
- After Step 4 is done, `analytics_service.py` no longer imports `convert_to_parquet`
- `grep -r "convert_to_parquet" server/app/` — must return 0 results after Step 4

**Risk:** Low (only done after Step 4 confirmed working).

---

## Execution Order

The order matters because of dependencies:

```
Step 1 (pyarrow dep)
  → Step 2 (parquet_service.py)          [needs pyarrow]
    → Step 3 (BackgroundJob model)        [needs migration]
      → Step 4 (update analytics_service) [needs Steps 2 + 3]
        → Step 5 (API endpoint)           [needs Step 3]
        → Step 6 (fix graph.py)           [independent of 2-5, do anytime]
        → Step 7 (delete dead files)      [independent, do anytime]
        → Step 8 (clean duckdb_client)    [must be last, after Step 4 confirmed]
```

Steps 6 and 7 are independent — can be done in any order.
Step 8 is always last.

---

## Progress Tracker

| Step | Description | Status | Notes |
|------|-------------|--------|-------|
| 1 | Add `pyarrow` to `pyproject.toml` | ✅ Done | pyarrow installed (1 package) |
| 2 | Create `parquet_service.py` | ✅ Done | New file, Azure SDK + PyArrow streaming |
| 3 | Create `BackgroundJob` model + migration | ✅ Done | New model, registered in main.py |
| 4 | Update `trigger_parquet_conversion()` | ✅ Done | Now uses parquet_service + BackgroundJob |
| 5 | Add `GET /api/files/{file_id}/job-status` | ✅ Done | Added to api/files.py |
| 6 | Fix single-file hardcoding in graph.py + query_router.py | ✅ Done | All analytics loaded, parquet map built |
| 7 | Delete `query_service.py` and `agent_service.py` | ✅ Done | Confirmed not imported, deleted |
| 8 | Remove `convert_to_parquet` from `duckdb_client.py` | ✅ Done | Confirmed not imported, removed |

---

## Cross-Check — What must still work after all steps

| Feature | How it works today | Must still work |
|---------|-------------------|-----------------|
| File upload | `api/files.py` → Azure Blob upload | Yes — not touched |
| Ingest trigger | `chat.py /ingest` → `ingest_file()` | Yes — not touched |
| Sample 500 rows | `duckdb_client.sample_file()` | Yes — not touched |
| AI description | `ai_client.generate_file_description()` | Yes — not touched |
| Analytics (pandas) | `analytics_service.compute_and_store_analytics()` | Yes — not touched |
| Relationship detection | `ingestion_service.detect_relationships()` | Yes — not touched |
| Chat → metadata route | `query_router.answer_from_metadata()` | Yes — not touched |
| Chat → precomputed route | `query_router.answer_from_precomputed()` | Yes — query_router gets single-file fix in Step 6 |
| Chat → agent route | `agent/graph.py` → LangGraph | Yes — graph.py gets multi-file fix in Step 6 |
| Agent sample tool | `agent/tools/sample.py` | Yes — not touched |
| Agent SQL tool | `agent/tools/sql.py` | Yes — not touched |
| Auth | `api/auth.py`, `core/security.py` | Yes — not touched |
| Containers/Folders/Users | All other API routes | Yes — not touched |

---

## Files Being Changed — Summary

| File | Action | Step |
|------|--------|------|
| `server/pyproject.toml` | Add pyarrow | 1 |
| `server/app/services/parquet_service.py` | **NEW** | 2 |
| `server/app/models/background_job.py` | **NEW** | 3 |
| `server/app/services/analytics_service.py` | Modify `trigger_parquet_conversion()` | 4 |
| `server/app/api/files.py` | Add job-status endpoint | 5 |
| `server/app/agent/graph.py` | Fix `first_meta = all_meta[0]` | 6 |
| `server/app/services/query_router.py` | Fix `all_analytics[0]` | 6 |
| `server/app/services/query_service.py` | **DELETE** | 7 |
| `server/app/services/agent_service.py` | **DELETE** | 7 |
| `server/app/core/duckdb_client.py` | Remove `convert_to_parquet()` | 8 |

**Total: 3 new files, 4 modified files, 2 deleted files, 1 pyproject.toml change**
