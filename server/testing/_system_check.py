"""
Full system health check — imports, routes, DB, retrieval pipeline.
Usage: cd server && source .venv/bin/activate && python3 -m testing._system_check
"""
import sys
import os
import asyncio

# Ensure server root is on the path when run as a script
_server_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _server_root not in sys.path:
    sys.path.insert(0, _server_root)

PASS = 0
FAIL = 0


def ok(label):
    global PASS
    PASS += 1
    print(f"  [PASS] {label}")


def fail(label, reason=""):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {label}{' — ' + reason if reason else ''}")


# ── 1. Module imports ─────────────────────────────────────────────────────────
print("\n── 1. Module imports ──────────────────────────────────────────────────")

_modules = [
    "app.core.config",
    "app.core.database",
    "app.core.security",
    "app.core.cost_tracker",
    "app.core.ai_client",
    "app.core.duckdb_client",
    "app.core.token_counter",
    "app.models.user",
    "app.models.folder",
    "app.models.file",
    "app.models.file_metadata",
    "app.models.file_relationship",
    "app.models.file_analytics",
    "app.models.conversation",
    "app.models.background_job",
    "app.retrieval.filters",
    "app.retrieval.temporal",
    "app.retrieval.bm25",
    "app.retrieval.fuzzy",
    "app.retrieval.embeddings_search",
    "app.retrieval.graph_expand",
    "app.retrieval.rrf",
    "app.retrieval.orchestrator",
    "app.retrieval.embeddings",
    "app.agent.graph.graph",
    "app.agent.llm",
    "app.api.v1.admin",
    "app.schemas.user",
    "app.schemas.folder",
    "app.schemas.file",
    "app.migrations.retrieval_schema_upgrade",
    "app.migrations.domain_schema_upgrade",
    "app.migrations.backfill_embeddings",
    "app.main",
]

for m in _modules:
    try:
        __import__(m)
        ok(m)
    except Exception as e:
        fail(m, str(e)[:120])

# ── 2. FastAPI app routes ─────────────────────────────────────────────────────
print("\n── 2. FastAPI app routes ───────────────────────────────────────────────")
try:
    from app.main import app
    routes = {r.path for r in app.routes}

    _expected_routes = [
        "/api/auth/google/callback",
        "/api/auth/google/login",
        "/api/auth/me",
        "/api/chat/message",
        "/api/chat/message/stream",
        "/api/files/upload-url",
        "/api/files/{file_id}",
        "/api/folders",
        "/api/folders/{folder_id}",
        "/api/users",
        "/api/users/{user_id}/toggle-admin",
        "/api/admin/cost-summary",
        "/api/admin/reingest-all",
        "/api/admin/domains",
        "/api/admin/users/{user_id}/domains",
        "/api/admin/folders/{folder_id}/domain",
        "/api/health",
    ]
    for r in _expected_routes:
        if r in routes:
            ok(f"route registered: {r}")
        else:
            fail(f"route MISSING: {r}", f"registered: {sorted(routes)[:5]}…")
except Exception as e:
    fail("FastAPI app load", str(e)[:200])

# ── 3. Model columns ──────────────────────────────────────────────────────────
print("\n── 3. Model schema columns ─────────────────────────────────────────────")
try:
    from app.models.user import User
    from app.models.folder import Folder
    from app.models.file_metadata import FileMetadata

    for attr in ("id", "email", "is_admin", "allowed_domains"):
        if hasattr(User, attr):
            ok(f"User.{attr}")
        else:
            fail(f"User.{attr} missing")

    for attr in ("id", "name", "owner_id", "domain_tag"):
        if hasattr(Folder, attr):
            ok(f"Folder.{attr}")
        else:
            fail(f"Folder.{attr} missing")

    for attr in ("file_id", "description_embedding", "search_text"):
        if hasattr(FileMetadata, attr):
            ok(f"FileMetadata.{attr}")
        else:
            fail(f"FileMetadata.{attr} missing")
except Exception as e:
    fail("Model check", str(e)[:120])

# ── 4. Pydantic schemas ───────────────────────────────────────────────────────
print("\n── 4. Pydantic schemas ─────────────────────────────────────────────────")
try:
    from app.schemas.user import UserOut
    from app.schemas.folder import FolderOut

    for f in ("id", "email", "is_admin", "allowed_domains"):
        if f in UserOut.model_fields:
            ok(f"UserOut.{f}")
        else:
            fail(f"UserOut.{f} missing")

    for f in ("id", "name", "domain_tag"):
        if f in FolderOut.model_fields:
            ok(f"FolderOut.{f}")
        else:
            fail(f"FolderOut.{f} missing")
except Exception as e:
    fail("Schema check", str(e)[:120])

# ── 5. Retrieval function signatures ─────────────────────────────────────────
print("\n── 5. Retrieval function signatures ────────────────────────────────────")
import inspect
try:
    from app.retrieval.bm25 import bm25_search
    from app.retrieval.fuzzy import fuzzy_search
    from app.retrieval.embeddings_search import vector_search
    from app.retrieval.graph_expand import graph_expand
    from app.retrieval.orchestrator import retrieve, retrieve_with_scores
    from app.retrieval.filters import build_base_query, domain_clause, permission_clause

    for fn, param in [
        (bm25_search, "allowed_domains"),
        (fuzzy_search, "allowed_domains"),
        (vector_search, "allowed_domains"),
        (graph_expand, "allowed_domains"),
        (build_base_query, "allowed_domains"),
    ]:
        sig = inspect.signature(fn)
        if param in sig.parameters:
            ok(f"{fn.__name__} has {param}")
        else:
            fail(f"{fn.__name__} missing {param}")

    # orchestrator external signatures unchanged
    sig_rws = inspect.signature(retrieve_with_scores)
    if list(sig_rws.parameters.keys()) == ["query", "user_id", "is_admin", "db", "top_k"]:
        ok("retrieve_with_scores signature stable")
    else:
        fail("retrieve_with_scores signature changed", str(list(sig_rws.parameters.keys())))
except Exception as e:
    fail("Retrieval signatures", str(e)[:200])

# ── 6. DB connectivity ────────────────────────────────────────────────────────
print("\n── 6. DB connectivity ──────────────────────────────────────────────────")

async def _check_db():
    try:
        from sqlalchemy import text
        from app.core.database import engine
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1 AS ping"))
            row = result.fetchone()
            if row and row[0] == 1:
                ok("Postgres connection (SELECT 1)")
            else:
                fail("Postgres connection", "unexpected result")

            # Check key tables exist
            for table in ("users", "folders", "files", "file_metadata", "file_relationships"):
                res = await conn.execute(text(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    f"WHERE table_name = '{table}')"
                ))
                exists = res.scalar()
                if exists:
                    ok(f"table exists: {table}")
                else:
                    fail(f"table MISSING: {table}")

            # Check PHASE 15 columns
            for table, col in [("folders", "domain_tag"), ("users", "allowed_domains")]:
                res = await conn.execute(text(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                    f"WHERE table_name='{table}' AND column_name='{col}')"
                ))
                if res.scalar():
                    ok(f"{table}.{col} column exists in DB")
                else:
                    fail(f"{table}.{col} column MISSING in DB (migration not run yet)")

            # Check pgvector + pg_trgm
            res = await conn.execute(text("SELECT extname FROM pg_extension WHERE extname IN ('vector','pg_trgm')"))
            exts = {r[0] for r in res.fetchall()}
            for ext in ("vector", "pg_trgm"):
                if ext in exts:
                    ok(f"extension: {ext}")
                else:
                    fail(f"extension MISSING: {ext}")

        await engine.dispose()
    except Exception as e:
        fail("DB connectivity", str(e)[:200])

asyncio.run(_check_db())

# ── 7. Agent graph imports ────────────────────────────────────────────────────
print("\n── 7. Agent graph ──────────────────────────────────────────────────────")
try:
    from app.agent.graph.graph import run_agent_query, run_agent_query_stream, invalidate_catalog_cache
    ok("run_agent_query importable")
    ok("run_agent_query_stream importable")
    ok("invalidate_catalog_cache importable")

    # Check signatures include user_id + is_admin
    import inspect
    sig = inspect.signature(run_agent_query_stream)
    for p in ("user_id", "is_admin"):
        if p in sig.parameters:
            ok(f"run_agent_query_stream has param: {p}")
        else:
            fail(f"run_agent_query_stream missing param: {p}")
except Exception as e:
    fail("Agent graph", str(e)[:200])

# ── Summary ───────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n{'='*70}")
print(f"SYSTEM CHECK: {PASS}/{total} PASS  |  {FAIL} FAIL")
print(f"{'='*70}")
if FAIL:
    sys.exit(1)
