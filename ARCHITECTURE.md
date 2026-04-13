# G-CHAT Architecture

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js 14 (App Router) |
| Backend | FastAPI + Python 3.12 |
| LLM | Azure OpenAI `gpt-4o-mini` |
| Agent | LangGraph 1.1 + LangChain |
| Analytics DB | DuckDB (reads Azure Blob directly) |
| Metadata DB | Neon PostgreSQL (asyncpg) |
| Storage | Azure Blob Storage |

---

## High-Level Flow

```mermaid
flowchart LR
    USER([User]) --> FE[Next.js Frontend]
    FE -->|POST /api/chat/message| ROUTER
    FE -->|POST /api/chat/ingest| INGEST

    subgraph Backend
        ROUTER[Query Router] -->|metadata query| PG[(Postgres)]
        ROUTER -->|analytics query| PG
        ROUTER -->|complex query| AGENT[LangGraph Agent]
        AGENT --> DUCK[(DuckDB → Azure Blob)]
        INGEST[Ingest Pipeline] --> DUCK
        INGEST --> PG
    end
```

---

## 1. Ingest Pipeline

Triggered manually by admin. Runs as a FastAPI **background task** — non-blocking.

```mermaid
flowchart TD
    UPLOAD[File uploaded to Azure Blob] --> TRIGGER[POST /api/chat/ingest]
    TRIGGER --> BG[Background Task]

    subgraph Pipeline["Ingest Pipeline (ingestion_service.py)"]
        S1["Step 1/5 · DuckDB Sample\n500 rows, detect columns\n~90s for 3GB CSV"] --> S2
        S2["Step 2/5 · AI Description\ngpt-4o-mini describes the file\n~3s"] --> S3
        S3["Step 3/5 · Save Metadata\nfile_metadata table\n~2s"] --> S4
        S4["Step 4/5 · Detect Relationships\nmatch columns across files\n~2s"] --> S5
        S5["Step 5/5 · Pre-compute Analytics\nanalytics_service.py\n~2-5 min"]
    end

    BG --> S1
    S3 -->|commit| PG[(Postgres\nfile_metadata)]
    S5 -->|commit| PA[(Postgres\nfile_analytics)]
    S5 -->|write| PARQUET[Azure Blob\n.parquet file]
```

### What Step 5 computes

All queries run on a **5% BERNOULLI sample** — fast even on 3GB files.

```mermaid
flowchart LR
    S5[Step 5 starts] --> RC[Row count\n5% sample × 20]
    S5 --> NS[Numeric stats\nmin/max/mean/sum/std per column]
    S5 --> VC[Value counts\ntop 20 per categorical column]
    S5 --> CT[Cross-tabs\ndimension × metric combos]
    S5 --> PQ[Parquet conversion\nCSV → Parquet ZSTD\n300s timeout]

    RC & NS & VC & CT & PQ --> DB[(file_analytics\nPostgres)]
```

---

## 2. Query Router

Every chat message is classified **without an LLM call** — pure keyword matching, instant.

```mermaid
flowchart TD
    Q[User Question] --> CL{classify_intent}

    CL -->|"how many rows / what columns\nlist files / schema"| M[Metadata Route\n<50ms]
    CL -->|"analytics / summary / average\ntotal / distribution / breakdown"| P[Pre-computed Route\n<100ms]
    CL -->|"everything else"| A[Agent Route\n5-30s]

    M --> PG1[(Postgres\nfile_metadata)]
    P --> PG2[(Postgres\nfile_analytics)]
    A --> AGENT[LangGraph Agent]

    PG1 --> RESP
    PG2 --> RESP
    AGENT --> RESP[Response to Frontend]
```

---

## 3. LangGraph Agent

Only invoked when the query router can't answer from pre-computed data.  
**MAX_TOOL_CALLS = 6**, hard 30s timeout per DuckDB query.

```mermaid
flowchart TD
    START([START]) --> AGENT_NODE[Agent Node\ngpt-4o-mini]
    AGENT_NODE -->|tool_calls?| COND{has tool calls?}
    COND -->|yes| TOOLS[Tool Node]
    COND -->|no| END([END → answer])
    TOOLS --> AGENT_NODE

    subgraph Tools
        T0[query_precomputed_analytics\ninstant from Postgres ← try FIRST]
        T1[search_catalog\nfind relevant files]
        T2[get_file_schema\ncolumn names + types]
        T3[run_aggregation\nGROUP BY helper]
        T4[run_sql\nfull DuckDB SQL]
        T5[summarise_dataframe\nin-memory pandas stats]
    end
```

### Tool priority order

```
1. query_precomputed_analytics  ← no DuckDB, instant
2. search_catalog               ← no DuckDB, catalog only
3. get_file_schema              ← no DuckDB, catalog only
4. run_aggregation              ← DuckDB, GROUP BY, 30s timeout
5. run_sql                      ← DuckDB, full SQL, 30s timeout
6. summarise_dataframe          ← in-memory pandas, no DuckDB
```

---

## 4. Data Storage

```mermaid
erDiagram
    files {
        string id PK
        string name
        string blob_path
        string container_id FK
        string ingest_status
    }
    file_metadata {
        string file_id FK
        jsonb columns_info
        int row_count
        text ai_description
        jsonb good_for
        jsonb key_metrics
        jsonb sample_rows
    }
    file_analytics {
        string file_id FK
        int row_count
        jsonb column_stats
        jsonb value_counts
        jsonb cross_tabs
        string parquet_blob_path
    }
    file_relationships {
        string file_a_id FK
        string file_b_id FK
        string shared_column
        float confidence_score
    }

    files ||--o| file_metadata : "1:1"
    files ||--o| file_analytics : "1:1"
    files ||--o{ file_relationships : "1:many"
```

---

## 5. Module Layout

```
server/app/
├── agent/                      ← LangGraph pipeline
│   ├── graph.py                ← StateGraph builder + run_agent_query()
│   ├── state.py                ← AgentState TypedDict
│   ├── llm.py                  ← Azure OpenAI singleton
│   └── tools/
│       ├── analytics.py        ← query_precomputed_analytics
│       ├── catalog.py          ← search_catalog, get_file_schema
│       ├── sql.py              ← run_sql, run_aggregation
│       └── stats.py            ← summarise_dataframe
│
├── services/
│   ├── ingestion_service.py    ← 5-step ingest pipeline
│   ├── analytics_service.py    ← pre-compute stats (Step 5)
│   └── query_router.py         ← intent classifier + fast-path handlers
│
├── core/
│   ├── duckdb_client.py        ← DuckDB + Azure Blob, 30s timeout
│   ├── database.py             ← SQLAlchemy async engine (pool_recycle=300)
│   └── config.py               ← env vars
│
├── models/
│   ├── file_metadata.py
│   ├── file_analytics.py       ← pre-computed stats table
│   └── file_relationship.py
│
└── api/
    └── chat.py                 ← POST /message (router) + POST /ingest
```

---

## 6. Why Parquet?

```
3GB CSV over Azure Blob
├── Full scan:  ~7 min per query   (reads every byte)
└── Parquet:    ~15 sec per query  (columnar, reads only needed columns)

Storage:  3GB CSV → ~400MB Parquet (ZSTD compression)
Cost:     ~$78/month at 10 queries/day (CSV) → ~$2/month (Parquet)
```

Once Parquet is written at ingest time, the agent's `run_aggregation` and `run_sql` tools automatically use `read_parquet()` instead of `read_csv_auto()`.
