"""
PHASE 15 — Domain access control smoke-test.

Checks:
1.  ModelUser has allowed_domains Mapped column
2.  ModelFolder has domain_tag Mapped column
3.  filters.domain_clause returns None for empty allowed_domains
4.  filters.domain_clause returns a ColumnElement for non-empty allowed_domains
5.  filters.build_base_query accepts allowed_domains kwarg without crash
6.  bm25_search / fuzzy_search / vector_search / graph_expand signatures accept allowed_domains
7.  orchestrator.retrieve_with_scores accepts its required args (no domain args needed — loads internally)
8.  UserOut schema has allowed_domains field
9.  FolderOut schema has domain_tag field
10. admin router has /domains, /users/{user_id}/domains, /folders/{folder_id}/domain routes

Run:  cd server && python -m testing._phase15_check
"""
import sys
import inspect
import types


def check(label: str, cond: bool) -> None:
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {label}")
    if not cond:
        sys.exit(1)


print("PHASE 15 — domain access control checks\n")

# ── 1. User model ─────────────────────────────────────────────────────────────
from app.models.user import User as ModelUser
check(
    "User.allowed_domains column exists",
    hasattr(ModelUser, "allowed_domains"),
)

# ── 2. Folder model ───────────────────────────────────────────────────────────
from app.models.folder import Folder as ModelFolder
check(
    "Folder.domain_tag column exists",
    hasattr(ModelFolder, "domain_tag"),
)

# ── 3-4. domain_clause ────────────────────────────────────────────────────────
from app.retrieval.filters import domain_clause
result_none = domain_clause(None)
check("domain_clause(None) returns None", result_none is None)

result_none2 = domain_clause([])
check("domain_clause([]) returns None", result_none2 is None)

result_clause = domain_clause(["finance"])
check("domain_clause(['finance']) returns non-None expression", result_clause is not None)

# ── 5. build_base_query signature ─────────────────────────────────────────────
from app.retrieval.filters import build_base_query
sig = inspect.signature(build_base_query)
check(
    "build_base_query has allowed_domains param",
    "allowed_domains" in sig.parameters,
)

# ── 6a. bm25_search signature ─────────────────────────────────────────────────
from app.retrieval.bm25 import bm25_search
sig_bm25 = inspect.signature(bm25_search)
check("bm25_search has allowed_domains param", "allowed_domains" in sig_bm25.parameters)

# ── 6b. fuzzy_search signature ────────────────────────────────────────────────
from app.retrieval.fuzzy import fuzzy_search
sig_fz = inspect.signature(fuzzy_search)
check("fuzzy_search has allowed_domains param", "allowed_domains" in sig_fz.parameters)

# ── 6c. vector_search signature ───────────────────────────────────────────────
from app.retrieval.embeddings_search import vector_search
sig_vs = inspect.signature(vector_search)
check("vector_search has allowed_domains param", "allowed_domains" in sig_vs.parameters)

# ── 6d. graph_expand signature ────────────────────────────────────────────────
from app.retrieval.graph_expand import graph_expand
sig_ge = inspect.signature(graph_expand)
check("graph_expand has allowed_domains param", "allowed_domains" in sig_ge.parameters)

# ── 7. orchestrator signatures ────────────────────────────────────────────────
from app.retrieval.orchestrator import retrieve_with_scores, retrieve
sig_rws = inspect.signature(retrieve_with_scores)
sig_r = inspect.signature(retrieve)
# allowed_domains is loaded inside — check it doesn't require a new mandatory param
check(
    "retrieve_with_scores signature unchanged (internal domain load)",
    list(sig_rws.parameters.keys()) == ["query", "user_id", "is_admin", "db", "top_k"],
)
check(
    "retrieve signature unchanged",
    list(sig_r.parameters.keys()) == ["query", "user_id", "is_admin", "db", "top_k"],
)

# ── 8. UserOut schema ─────────────────────────────────────────────────────────
from app.schemas.user import UserOut
check(
    "UserOut has allowed_domains field",
    "allowed_domains" in UserOut.model_fields,
)
check(
    "UserOut.allowed_domains is optional (default None)",
    UserOut.model_fields["allowed_domains"].default is None,
)

# ── 9. FolderOut schema ───────────────────────────────────────────────────────
from app.schemas.folder import FolderOut
check(
    "FolderOut has domain_tag field",
    "domain_tag" in FolderOut.model_fields,
)
check(
    "FolderOut.domain_tag is optional (default None)",
    FolderOut.model_fields["domain_tag"].default is None,
)

# ── 10. Admin router routes ───────────────────────────────────────────────────
from app.api.v1.admin import router as admin_router
route_paths = [r.path for r in admin_router.routes]
check(
    "admin router has /domains route",
    "/admin/domains" in route_paths,
)
check(
    "admin router has /users/{user_id}/domains route",
    "/admin/users/{user_id}/domains" in route_paths,
)
check(
    "admin router has /folders/{folder_id}/domain route",
    "/admin/folders/{folder_id}/domain" in route_paths,
)

print("\nAll PHASE 15 checks PASSED ✓")
